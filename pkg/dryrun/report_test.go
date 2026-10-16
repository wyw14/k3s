package dryrun

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

func TestReportJSONDeterministic(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "tls", "a.crt"), "line1\nline2\nline3\n", time2020)

	specs := []FileSpec{
		{Path: filepath.Join(root, "tls", "a.crt"), Content: []byte("line1\nchanged\nline3\n")},
		{Path: filepath.Join(root, "tls", "b.key"), Content: []byte("newkey")},
	}
	e := Executor{Root: root}
	r1, err := e.Run(context.Background(), specs)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := e.Run(context.Background(), specs)
	if err != nil {
		t.Fatal(err)
	}
	j1, err := r1.JSON()
	if err != nil {
		t.Fatal(err)
	}
	j2, err := r2.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(j1, j2) {
		t.Fatalf("report JSON is not deterministic:\n%s\n----\n%s", j1, j2)
	}

	var parsed Report
	if err := json.Unmarshal(j1, &parsed); err != nil {
		t.Fatalf("report JSON does not parse: %v", err)
	}
	if parsed.SchemaVersion != SchemaVersion {
		t.Fatalf("schema = %s", parsed.SchemaVersion)
	}
	if len(parsed.Phases) != 4 {
		t.Fatalf("phases = %d, want 4", len(parsed.Phases))
	}
	wantPhases := []Phase{PhaseScan, PhasePlan, PhaseApply, PhaseVerify}
	for i, p := range parsed.Phases {
		if p.Name != wantPhases[i] {
			t.Fatalf("phase %d = %s", i, p.Name)
		}
		if p.Status != phaseOK {
			t.Fatalf("phase %s status = %s", p.Name, p.Status)
		}
	}
	if len(parsed.PlanID) != 64 {
		t.Fatalf("plan id = %q", parsed.PlanID)
	}
}

func TestReportTextRendering(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "tls", "a.crt"), "same\nold-line\n", time2020)
	specs := []FileSpec{
		{Path: filepath.Join(root, "tls", "a.crt"), Content: []byte("same\nnew-line\n")},
		{Path: filepath.Join(root, "tls", "b.key"), Content: []byte("key")},
	}
	ex := Executor{Root: root}
	rep, err := ex.Run(context.Background(), specs)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := rep.RenderText(&buf); err != nil {
		t.Fatal(err)
	}
	text := buf.String()
	for _, want := range []string{
		"k3s dry-run report " + SchemaVersion,
		"Mode:   dry-run",
		"Phases:",
		"scan    ok",
		"plan    ok",
		"apply   ok",
		"verify  ok",
		"Summary:",
		"Changes:",
		"@@",
		"-old-line",
		"+new-line",
		"A tls/b.key",
		"M tls/a.crt",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("text report missing %q:\n%s", want, text)
		}
	}
}

func TestReportBinaryAndLargeDiffs(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "bin"), "\x00\x01\x02", time2020)
	writeTestFile(t, filepath.Join(root, "big.txt"), strings.Repeat("a", 200), time2020)

	specs := []FileSpec{
		{Path: filepath.Join(root, "bin"), Content: []byte("\x00\x02\x03")},
		{Path: filepath.Join(root, "big.txt"), Content: []byte(strings.Repeat("b", 200))},
	}
	ex := Executor{Root: root, DiffThresholdBytes: 64}
	rep, err := ex.Run(context.Background(), specs)
	if err != nil {
		t.Fatal(err)
	}
	var bin, big *DiffEntry
	for i := range rep.Diffs {
		d := &rep.Diffs[i]
		switch d.Relative {
		case "bin":
			bin = d
		case "big.txt":
			big = d
		}
	}
	if bin == nil || !bin.Binary || bin.Patch != "" {
		t.Fatalf("binary diff wrong: %+v", bin)
	}
	if big == nil || !big.PatchLimited || big.Patch != "" {
		t.Fatalf("large diff wrong: %+v", big)
	}
}

func TestScanReport(t *testing.T) {
	root := t.TempDir()
	writeTestFile(t, filepath.Join(root, "tls", "present"), "data", time2020)
	paths := []string{
		filepath.Join(root, "tls", "present"),
		filepath.Join(root, "tls", "missing"),
	}
	ex := Executor{Root: root}
	rep, err := ex.Scan(context.Background(), paths)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != StatusScanned {
		t.Fatalf("status = %s", rep.Status)
	}
	if rep.Summary.Unchanged != 1 || rep.Summary.Missing != 1 {
		t.Fatalf("summary = %+v", rep.Summary)
	}
	if rep.phaseStatus(PhaseScan) != phaseOK {
		t.Fatal("scan phase not ok")
	}
	if rep.phaseStatus(PhaseApply) != phaseSkipped {
		t.Fatal("apply phase must be skipped in scan mode")
	}
	j, err := rep.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var parsed Report
	if err := json.Unmarshal(j, &parsed); err != nil {
		t.Fatal(err)
	}
}

func TestScanMissingRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "does-not-exist")
	ex := Executor{Root: root}
	rep, err := ex.Scan(context.Background(), []string{filepath.Join(root, "x")})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Warnings) == 0 {
		t.Fatal("missing root must produce a warning")
	}
}

func TestUnifiedDiffEmpty(t *testing.T) {
	if d := unifiedDiff("f", []byte("a"), []byte("a")); d != "" {
		t.Fatalf("identical content must not produce a diff, got %q", d)
	}
	d := unifiedDiff("f", nil, []byte("created\n"))
	if !strings.Contains(d, "@@ -0,0 +1") || !strings.Contains(d, "+created") {
		t.Fatalf("creation diff wrong: %q", d)
	}
}
