package dryrun

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// ErrorCode is the stable, machine-readable classification of a dry-run
// failure. The string values are part of the diagnostic contract and must not
// change once released; new categories may only be added.
type ErrorCode string

const (
	// CodeValidation covers malformed caller input: empty paths, paths outside
	// the diagnostic root, duplicate targets, oversized content, etc.
	CodeValidation ErrorCode = "E_VALIDATION"
	// CodeNotFound covers a required path that is missing where an existing
	// entry was expected.
	CodeNotFound ErrorCode = "E_NOT_FOUND"
	// CodePermission covers access denied by the filesystem.
	CodePermission ErrorCode = "E_PERMISSION"
	// CodeConflict covers incompatible on-disk state: parent occupied by a
	// regular file, symlink escape, verification drift, journal mismatch.
	CodeConflict ErrorCode = "E_CONFLICT"
	// CodeIO covers transient filesystem I/O failures.
	CodeIO ErrorCode = "E_IO"
	// CodeCorrupt covers undecodable or malformed payload data.
	CodeCorrupt ErrorCode = "E_CORRUPT"
	// CodeCancelled covers context cancellation and deadline expiry.
	CodeCancelled ErrorCode = "E_CANCELLED"
	// CodeInternal covers failures that do not fit any other category.
	CodeInternal ErrorCode = "E_INTERNAL"
)

// Stable sentinel errors. They are *Error values so errors.Is works against
// them directly.
var (
	ErrEmptyPath        = &Error{Code: CodeValidation, Message: "empty path"}
	ErrRelativePath     = &Error{Code: CodeValidation, Message: "path must be absolute"}
	ErrPathOutsideRoot  = &Error{Code: CodeValidation, Message: "path is outside of the diagnostic root"}
	ErrDuplicateTarget  = &Error{Code: CodeValidation, Message: "duplicate target path"}
	ErrParentNotDir     = &Error{Code: CodeConflict, Message: "parent path exists and is not a directory"}
	ErrTargetIsDir      = &Error{Code: CodeConflict, Message: "target path exists and is a directory"}
	ErrTargetIsLink     = &Error{Code: CodeConflict, Message: "refusing to replace an existing symlink"}
	ErrFileTooLarge     = &Error{Code: CodeValidation, Message: "proposed content exceeds the configured size limit"}
	ErrStateDrift       = &Error{Code: CodeConflict, Message: "verified content does not match planned content"}
	ErrJournalMismatch  = &Error{Code: CodeConflict, Message: "journal does not match the current plan"}
	ErrInvalidEncoding  = &Error{Code: CodeCorrupt, Message: "invalid data encoding"}
)

// retryableCodes is the stable retry classification. Only transient conditions
// are considered safe to resume.
var retryableCodes = map[ErrorCode]bool{
	CodeIO:        true,
	CodeCancelled: true,
}

// Error is the classified error type returned by all dry-run entry points.
type Error struct {
	Code    ErrorCode `json:"code"`
	Phase   Phase     `json:"phase,omitempty"`
	Action  Action    `json:"action,omitempty"`
	Path    string    `json:"path,omitempty"`
	Message string    `json:"message,omitempty"`
	Cause   error     `json:"-"`
}

// Fail creates a classified error without a wrapped cause.
func Fail(code ErrorCode, phase Phase, format string, args ...any) *Error {
	return &Error{
		Code:    code,
		Phase:   phase,
		Message: fmt.Sprintf(format, args...),
	}
}

// Wrap classifies (if necessary) and annotates an error with phase/path
// context. It returns nil when err is nil. Errors already carrying *Error
// keep their original code and only gain missing context.
func Wrap(err error, phase Phase, path string) error {
	if err == nil {
		return nil
	}
	e := Classify(err, phase)
	if e.Phase == "" {
		e.Phase = phase
	}
	if e.Path == "" && path != "" {
		e.Path = path
	}
	return e
}

// Error implements the error interface with a stable, parseable shape:
//
//	dry-run: E_VALIDATION(plan) /data/x: duplicate target path: <cause>
func (e *Error) Error() string {
	msg := "dry-run: " + string(e.Code)
	if e.Phase != "" {
		msg += "(" + string(e.Phase) + ")"
	}
	if e.Path != "" {
		msg += " " + e.Path
	}
	if e.Message != "" {
		msg += ": " + e.Message
	}
	if e.Cause != nil {
		msg += ": " + e.Cause.Error()
	}
	return msg
}

// Unwrap exposes the wrapped cause to errors.Is/errors.As.
func (e *Error) Unwrap() error {
	return e.Cause
}

// Is allows matching by code (and optionally phase) without requiring pointer
// identity, e.g. errors.Is(err, &Error{Code: CodeValidation}).
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok {
		return false
	}
	if t.Code != "" && t.Code != e.Code {
		return false
	}
	if t.Phase != "" && t.Phase != e.Phase {
		return false
	}
	if t.Action != "" && t.Action != e.Action {
		return false
	}
	return true
}

// Retryable reports whether the failure condition is transient and the same
// operation may be attempted again (or resumed from a journal).
func (e *Error) Retryable() bool {
	return retryableCodes[e.Code]
}

// MarshalJSON emits a stable representation including the rendered cause.
func (e *Error) MarshalJSON() ([]byte, error) {
	type alias struct {
		Code      ErrorCode `json:"code"`
		Phase     Phase     `json:"phase,omitempty"`
		Action    Action    `json:"action,omitempty"`
		Path      string    `json:"path,omitempty"`
		Message   string    `json:"message,omitempty"`
		Retryable bool      `json:"retryable"`
		Cause     string    `json:"cause,omitempty"`
	}
	a := alias{
		Code:      e.Code,
		Phase:     e.Phase,
		Action:    e.Action,
		Path:      e.Path,
		Message:   e.Message,
		Retryable: e.Retryable(),
	}
	if e.Cause != nil {
		a.Cause = e.Cause.Error()
	}
	return json.Marshal(a)
}

// AsError returns err as *Error or nil.
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return nil
}

// Classify maps standard library errors to the stable error categories. It
// returns nil for nil input and preserves errors that are already *Error.
func Classify(err error, phase Phase) *Error {
	if err == nil {
		return nil
	}
	if e := AsError(err); e != nil {
		return e
	}

	e := &Error{Phase: phase, Cause: err}

	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		e.Code = CodeCancelled
	case errors.Is(err, fs.ErrNotExist), os.IsNotExist(err):
		e.Code = CodeNotFound
	case errors.Is(err, fs.ErrPermission), os.IsPermission(err):
		e.Code = CodePermission
	case isCorrupt(err):
		e.Code = CodeCorrupt
	case isPathOrIOError(err):
		e.Code = CodeIO
	default:
		e.Code = CodeInternal
		e.Message = err.Error()
	}
	return e
}

// isPathOrIOError recognizes the filesystem error wrapper types from the os
// package; their concrete inner syscall errors are platform specific and must
// not be relied upon directly.
func isPathOrIOError(err error) bool {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return true
	}
	var linkErr *os.LinkError
	if errors.As(err, &linkErr) {
		return true
	}
	var syscallErr *os.SyscallError
	return errors.As(err, &syscallErr)
}

// isCorrupt recognizes payload decoding failures from encoding/json.
func isCorrupt(err error) bool {
	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return true
	}
	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return true
	}
	var invalidErr *json.InvalidUnmarshalError
	return errors.As(err, &invalidErr)
}
