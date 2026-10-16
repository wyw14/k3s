package dryrun

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"unicode/utf8"
)

// PhaseReport records the outcome of a single executor phase.
type PhaseReport struct {
	Name    Phase  `json:"name"`
	Status  string `json:"status"`
	Ops     int    `json:"ops,omitempty"`
	Skipped int    `json:"skipped,omitempty"`
	Note    string `json:"note,omitempty"`
}

// Summary aggregates per-change counters across all target paths.
type Summary struct {
	Paths     int `json:"paths"`
	Dirs      int `json:"dirs,omitempty"`
	Added     int `json:"added"`
	Modified  int `json:"modified"`
	Metadata  int `json:"metadata"`
	Unchanged int `json:"unchanged"`
	Missing   int `json:"missing,omitempty"`
	Skipped   int `json:"skipped,omitempty"`
}

// DiffEntry is the per-path stable difference record.
type DiffEntry struct {
	Path         string     `json:"path"`
	Relative     string     `json:"relative"`
	Change       ChangeType `json:"change"`
	Action       Action     `json:"action,omitempty"`
	BeforeHash   string     `json:"beforeHash,omitempty"`
	AfterHash    string     `json:"afterHash,omitempty"`
	BeforeSize   int64      `json:"beforeSize,omitempty"`
	AfterSize    int64      `json:"afterSize,omitempty"`
	Mode         uint32     `json:"mode,omitempty"`
	Binary       bool       `json:"binary,omitempty"`
	PatchLimited bool       `json:"patchLimited,omitempty"`
	Patch        string     `json:"patch,omitempty"`
}

// Report is the immutable, deterministic output of a diagnostic run.
type Report struct {
	SchemaVersion string        `json:"schemaVersion"`
	DryRun        bool          `json:"dryRun"`
	Root          string        `json:"root"`
	PlanID        string        `json:"planId,omitempty"`
	Status        string        `json:"status"`
	Summary       Summary       `json:"summary"`
	Phases        []PhaseReport `json:"phases"`
	Diffs         []DiffEntry   `json:"diffs"`
	Warnings      []string      `json:"warnings,omitempty"`
}

func newReport(root string, dryRun bool) *Report {
	return &Report{
		SchemaVersion: SchemaVersion,
		DryRun:        dryRun,
		Root:          root,
		Status:        StatusPlanned,
		Phases: []PhaseReport{
			{Name: PhaseScan, Status: phaseSkipped},
			{Name: PhasePlan, Status: phaseSkipped},
			{Name: PhaseApply, Status: phaseSkipped},
			{Name: PhaseVerify, Status: phaseSkipped},
		},
	}
}

func (r *Report) setPhase(p PhaseReport) {
	for i := range r.Phases {
		if r.Phases[i].Name == p.Name {
			r.Phases[i] = p
			return
		}
	}
	r.Phases = append(r.Phases, p)
}

func (r *Report) phaseStatus(name Phase) string {
	for _, p := range r.Phases {
		if p.Name == name {
			return p.Status
		}
	}
	return phaseSkipped
}

func (r *Report) warn(format string, args ...any) {
	r.Warnings = append(r.Warnings, fmt.Sprintf(format, args...))
}

// buildDiffs computes the ordered per-path difference set. Before-content is
// read from disk (read-only) while the change plan still describes the current
// on-disk state.
func buildDiffs(plan *Plan, specs []FileSpec, base Snapshot, threshold int64) []DiffEntry {
	byPath := make(map[string]FileSpec, len(specs))
	for _, s := range specs {
		byPath[s.Path] = s
	}

	diffs := make([]DiffEntry, 0, len(specs))
	for _, s := range specs {
		state, _ := base.State(s.Path)
		entry := DiffEntry{
			Path:       s.Path,
			Relative:   relativeTo(plan.Root, s.Path),
			BeforeHash: state.SHA256,
			BeforeSize: state.Size,
			AfterHash:  hashBytes(s.Content),
			AfterSize:  int64(len(s.Content)),
		}
		if s.Mode != 0 {
			entry.Mode = perm(s.Mode)
		} else {
			entry.Mode = perm(DefaultFileMode)
		}

		switch {
		case !state.Exists:
			entry.Change = ChangeAdded
			entry.Action = ActionCreate
		case state.SHA256 != entry.AfterHash:
			entry.Change = ChangeModified
			entry.Action = ActionUpdate
		case metadataDiffers(state, s, effectiveFileMode(s.Mode)):
			entry.Change = ChangeMetadata
			entry.Action = ActionSetTimes
		default:
			entry.Change = ChangeUnchanged
		}

		if entry.Change != ChangeUnchanged {
			fillPatch(&entry, state, s.Content, threshold)
		}
		diffs = append(diffs, entry)
	}

	// Directory creations are reported alongside file changes.
	for _, op := range plan.Ops {
		if op.Action != ActionMkdir {
			continue
		}
		diffs = append(diffs, DiffEntry{
			Path:     op.Path,
			Relative: op.Relative,
			Change:   ChangeAdded,
			Action:   ActionMkdir,
			Mode:     op.Mode,
		})
	}

	sort.Slice(diffs, func(i, j int) bool { return diffs[i].Path < diffs[j].Path })
	return diffs
}

// buildScanDiffs renders scan-only output (no proposed file set).
func buildScanDiffs(snap Snapshot) []DiffEntry {
	states := snap.States()
	diffs := make([]DiffEntry, 0, len(states))
	for _, st := range states {
		if st.IsDir {
			continue
		}
		e := DiffEntry{
			Path:       st.Path,
			Relative:   relativeTo(snap.Root, st.Path),
			BeforeHash: st.SHA256,
			BeforeSize: st.Size,
			Mode:       st.Mode,
		}
		if st.Exists {
			e.Change = ChangeUnchanged
		} else {
			e.Change = ChangeMissing
		}
		diffs = append(diffs, e)
	}
	sort.Slice(diffs, func(i, j int) bool { return diffs[i].Path < diffs[j].Path })
	return diffs
}

func effectiveFileMode(m os.FileMode) os.FileMode {
	if m == 0 {
		return DefaultFileMode
	}
	return m
}

// fillPatch attaches a unified diff for textual content, or a hash-only
// summary for binary/oversized payloads.
func fillPatch(entry *DiffEntry, state FileState, after []byte, threshold int64) {
	if isBinary(after) {
		entry.Binary = true
		return
	}
	var before []byte
	if state.Exists && state.Size >= 0 && (threshold <= 0 || state.Size <= threshold) {
		if b, err := os.ReadFile(entry.Path); err == nil {
			before = b
		}
	}
	if isBinary(before) {
		entry.Binary = true
		return
	}
	if threshold > 0 && (int64(len(after)) > threshold || int64(len(before)) > threshold) {
		entry.PatchLimited = true
		return
	}
	entry.Patch = unifiedDiff(entry.Relative, before, after)
}

func isBinary(b []byte) bool {
	if bytes.IndexByte(b, 0) >= 0 {
		return true
	}
	if len(b) == 0 {
		return false
	}
	return !utf8.Valid(b)
}

func summarize(rep *Report) {
	var s Summary
	s.Paths = len(rep.Diffs)
	for _, d := range rep.Diffs {
		switch d.Change {
		case ChangeAdded:
			if d.Action == ActionMkdir {
				s.Dirs++
			} else {
				s.Added++
			}
		case ChangeModified:
			s.Modified++
		case ChangeMetadata:
			s.Metadata++
		case ChangeUnchanged:
			s.Unchanged++
		case ChangeMissing:
			s.Missing++
		}
	}
	for _, p := range rep.Phases {
		s.Skipped += p.Skipped
	}
	rep.Summary = s
}

// JSON returns the deterministic, indented JSON encoding of the report.
func (r *Report) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// RenderText writes the human-readable diagnostic report.
func (r *Report) RenderText(w io.Writer) error {
	mode := "dry-run (no changes are written)"
	if !r.DryRun {
		mode = "apply"
	}
	fmt.Fprintf(w, "k3s dry-run report %s\n", r.SchemaVersion)
	fmt.Fprintf(w, "  Root:   %s\n", r.Root)
	fmt.Fprintf(w, "  Mode:   %s\n", mode)
	if r.PlanID != "" {
		fmt.Fprintf(w, "  Plan:   %s\n", r.PlanID)
	}
	fmt.Fprintf(w, "  Status: %s\n", r.Status)

	fmt.Fprintln(w, "\nPhases:")
	for _, p := range r.Phases {
		line := fmt.Sprintf("  %-7s %s", p.Name, p.Status)
		if p.Ops > 0 || p.Skipped > 0 {
			line += fmt.Sprintf(" (%d ops", p.Ops)
			if p.Skipped > 0 {
				line += fmt.Sprintf(", %d resumed", p.Skipped)
			}
			line += ")"
		}
		if p.Note != "" {
			line += " - " + p.Note
		}
		fmt.Fprintln(w, line)
	}

	fmt.Fprintf(w, "\nSummary: %d paths", r.Summary.Paths)
	parts := []string{}
	if r.Summary.Dirs > 0 {
		parts = append(parts, fmt.Sprintf("%d directories created", r.Summary.Dirs))
	}
	if r.Summary.Added > 0 {
		parts = append(parts, fmt.Sprintf("%d added", r.Summary.Added))
	}
	if r.Summary.Modified > 0 {
		parts = append(parts, fmt.Sprintf("%d modified", r.Summary.Modified))
	}
	if r.Summary.Metadata > 0 {
		parts = append(parts, fmt.Sprintf("%d metadata-only", r.Summary.Metadata))
	}
	if r.Summary.Unchanged > 0 {
		parts = append(parts, fmt.Sprintf("%d unchanged", r.Summary.Unchanged))
	}
	if r.Summary.Missing > 0 {
		parts = append(parts, fmt.Sprintf("%d missing", r.Summary.Missing))
	}
	if len(parts) > 0 {
		fmt.Fprintf(w, " (%s)\n", strings.Join(parts, ", "))
	} else {
		fmt.Fprintln(w)
	}

	if len(r.Diffs) > 0 {
		fmt.Fprintln(w, "\nChanges:")
		for _, d := range r.Diffs {
			fmt.Fprintln(w, textDiffLine(d))
			if d.Patch != "" {
				for _, line := range strings.Split(strings.TrimSuffix(d.Patch, "\n"), "\n") {
					fmt.Fprintln(w, "    "+line)
				}
			}
		}
	}

	for _, wn := range r.Warnings {
		fmt.Fprintf(w, "\nWarning: %s\n", wn)
	}
	return nil
}

func textDiffLine(d DiffEntry) string {
	switch {
	case d.Action == ActionMkdir:
		return fmt.Sprintf("  D %s (new directory, mode %03o)", d.Relative, d.Mode)
	case d.Change == ChangeUnchanged:
		return fmt.Sprintf("  · %s (unchanged, %d bytes)", d.Relative, d.BeforeSize)
	case d.Change == ChangeMissing:
		return fmt.Sprintf("  ! %s (missing on disk)", d.Relative)
	}

	tag := map[Action]string{
		ActionCreate:   "A",
		ActionUpdate:   "M",
		ActionSetTimes: "T",
	}[d.Action]
	line := fmt.Sprintf("  %s %s (%s, %d -> %d bytes, mode %03o", tag, d.Relative, d.Change, d.BeforeSize, d.AfterSize, d.Mode)
	if d.Binary {
		line += ", binary content"
	}
	if d.PatchLimited {
		line += ", diff omitted (size threshold)"
	}
	line += ")"
	return line
}
