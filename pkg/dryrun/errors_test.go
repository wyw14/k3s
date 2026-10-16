package dryrun

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"
)

// TestErrorCodeValues locks the stable diagnostic contract: these string
// values are parsed by automation and must never change.
func TestErrorCodeValues(t *testing.T) {
	codes := map[ErrorCode]string{
		CodeValidation: "E_VALIDATION",
		CodeNotFound:   "E_NOT_FOUND",
		CodePermission: "E_PERMISSION",
		CodeConflict:   "E_CONFLICT",
		CodeIO:         "E_IO",
		CodeCorrupt:    "E_CORRUPT",
		CodeCancelled:  "E_CANCELLED",
		CodeInternal:   "E_INTERNAL",
	}
	for code, want := range codes {
		if string(code) != want {
			t.Errorf("error code %q changed to %q", want, code)
		}
	}
	if string(PhaseScan) != "scan" || string(PhasePlan) != "plan" ||
		string(PhaseApply) != "apply" || string(PhaseVerify) != "verify" {
		t.Fatal("phase names are not stable")
	}
	if SchemaVersion == "" {
		t.Fatal("schema version must be set")
	}
}

type fakeErr string

func (e fakeErr) Error() string { return string(e) }

func TestClassify(t *testing.T) {
	var cancelCtx struct{}
	_ = cancelCtx
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	deadline, kill := context.WithTimeout(context.Background(), time.Nanosecond)
	defer kill()
	<-deadline.Done()

	tests := []struct {
		name      string
		err       error
		want      ErrorCode
		retryable bool
	}{
		{"nil", nil, "", false},
		{"context canceled", cancelled.Err(), CodeCancelled, true},
		{"context deadline", deadline.Err(), CodeCancelled, true},
		{"fs not exist", fs.ErrNotExist, CodeNotFound, false},
		{"os stat missing", &os.PathError{Op: "stat", Path: "x", Err: fs.ErrNotExist}, CodeNotFound, false},
		{"fs permission", fs.ErrPermission, CodePermission, false},
		{"json syntax", &json.SyntaxError{}, CodeCorrupt, false},
		{"path io error", &os.PathError{Op: "write", Path: "x", Err: fakeErr("disk gone")}, CodeIO, true},
		{"unknown", fakeErr("boom"), CodeInternal, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(tt.err, PhaseApply)
			if tt.err == nil {
				if got != nil {
					t.Fatalf("expected nil classification, got %v", got)
				}
				return
			}
			if got.Code != tt.want {
				t.Fatalf("code = %s, want %s", got.Code, tt.want)
			}
			if got.Phase != PhaseApply {
				t.Fatalf("phase = %s, want apply", got.Phase)
			}
			if got.Retryable() != tt.retryable {
				t.Fatalf("retryable = %v, want %v", got.Retryable(), tt.retryable)
			}
		})
	}
}

func TestClassifyPreservesTypedError(t *testing.T) {
	original := &Error{Code: CodeConflict, Phase: PhaseVerify, Path: "/x", Message: "drift"}
	got := Classify(original, PhaseApply)
	if got != original {
		t.Fatal("Classify must preserve an already classified error")
	}
	if got.Phase != PhaseVerify {
		t.Fatal("classified phase must not be overwritten")
	}
}

func TestErrorIsAndAs(t *testing.T) {
	err := annotate(ErrDuplicateTarget, PhasePlan, "/data/x")
	var de *Error
	if !errors.As(err, &de) {
		t.Fatal("errors.As failed for *dryrun.Error")
	}
	if de.Code != CodeValidation || de.Path != "/data/x" || de.Phase != PhasePlan {
		t.Fatalf("unexpected classified error: %+v", de)
	}
	if !errors.Is(err, ErrDuplicateTarget) {
		t.Fatal("errors.Is must match the duplicate sentinel")
	}
	if !errors.Is(err, &Error{Code: CodeValidation}) {
		t.Fatal("errors.Is must match by code only")
	}
	if errors.Is(err, &Error{Code: CodeIO}) {
		t.Fatal("errors.Is must not match a different code")
	}
	if errors.Is(err, &Error{Code: CodeValidation, Phase: PhaseApply}) {
		t.Fatal("errors.Is must not match a different phase constraint")
	}

	wrapped := errWithCause()
	if !errors.Is(wrapped, fs.ErrNotExist) {
		t.Fatal("Unwrap must expose wrapped cause")
	}
}

func errWithCause() error {
	return &Error{Code: CodeNotFound, Phase: PhaseScan, Cause: fs.ErrNotExist}
}

func TestErrorJSON(t *testing.T) {
	e := &Error{Code: CodeIO, Phase: PhaseApply, Action: ActionUpdate, Path: "/x", Cause: fakeErr("disk")}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{`"code":"E_IO"`, `"phase":"apply"`, `"action":"update"`, `"path":"/x"`, `"retryable":true`, `"cause":"disk"`} {
		if !strings.Contains(s, want) {
			t.Fatalf("JSON %s missing %s", s, want)
		}
	}
	var back Error
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if back.Code != CodeIO || !back.Retryable() {
		t.Fatalf("round trip failed: %+v", back)
	}
}

func TestWrapNilSafe(t *testing.T) {
	if Wrap(nil, PhaseApply, "/x") != nil {
		t.Fatal("Wrap(nil) must be nil")
	}
}
