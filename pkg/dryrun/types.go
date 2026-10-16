// Package dryrun implements a configurable, phased diagnostic planner that
// predicts the changes a core k3s workflow (currently bootstrap data
// reconciliation between a datastore and the local data directory) would make
// on disk, without modifying persistent state.
//
// An Executor runs four phases: scan (read-only snapshot), plan (deterministic
// change set), apply (simulated in memory when dry-run, atomic journaled writes
// when Apply is explicitly enabled), and verify (hash comparison against the
// expected end state).
//
// The zero value of Executor is a safe, read-only dry-run: callers must opt in
// to persistent writes by setting Apply to true.
package dryrun

import (
	"os"
	"time"
)

// SchemaVersion is the stable report/plan schema identifier. Any breaking
// change to the serialized shapes must bump this value; additive changes do
// not require a bump.
const SchemaVersion = "dryrun.k3s.io/v1"

// Phase identifies one of the executor stages.
type Phase string

const (
	PhaseScan   Phase = "scan"
	PhasePlan   Phase = "plan"
	PhaseApply  Phase = "apply"
	PhaseVerify Phase = "verify"
)

// String returns the stable phase name.
func (p Phase) String() string {
	return string(p)
}

// Action identifies a single planned filesystem operation.
type Action string

const (
	// ActionMkdir creates a missing directory.
	ActionMkdir Action = "mkdir"
	// ActionCreate writes a file that does not currently exist.
	ActionCreate Action = "create"
	// ActionUpdate atomically replaces an existing file whose content changes.
	ActionUpdate Action = "update"
	// ActionSetTimes adjusts metadata (mtime and/or mode) of a file whose
	// content is unchanged.
	ActionSetTimes Action = "set-times"
)

// ChangeType is the per-path classification rendered in diff reports.
type ChangeType string

const (
	ChangeAdded     ChangeType = "added"
	ChangeModified  ChangeType = "modified"
	ChangeMetadata  ChangeType = "metadata"
	ChangeUnchanged ChangeType = "unchanged"
	ChangeMissing   ChangeType = "missing"
)

// Report status values.
const (
	StatusScanned   = "scanned"
	StatusNoChanges = "no-changes"
	StatusPlanned   = "planned"
	StatusApplied   = "applied"
	StatusFailed    = "failed"
)

// Per-phase status values.
const (
	phaseOK      = "ok"
	phaseSkipped = "skipped"
	phaseFailed  = "failed"
)

// Default permission bits match pkg/bootstrap.WriteToDiskFromStorage:
// directories are created with 0700 and files with 0600.
const (
	DefaultDirMode  os.FileMode = 0o700
	DefaultFileMode os.FileMode = 0o600
)

// DefaultDiffThreshold is the maximum size of a text file whose unified diff
// is embedded in the report. Larger files are summarized by hash.
const DefaultDiffThreshold int64 = 64 * 1024

// FileSpec is a desired filesystem state entry proposed by a caller.
// The zero Mode selects DefaultFileMode; the zero ModTime means mtime is not
// enforced.
type FileSpec struct {
	Path    string
	Content []byte
	Mode    os.FileMode
	ModTime time.Time
}

// FileState is an immutable snapshot of a single filesystem entry.
type FileState struct {
	Path    string `json:"path"`
	Exists  bool   `json:"exists"`
	IsDir   bool   `json:"isDir,omitempty"`
	IsLink  bool   `json:"isLink,omitempty"`
	Escapes bool   `json:"escapesRoot,omitempty"`
	Size    int64  `json:"size,omitempty"`
	Mode    uint32 `json:"mode,omitempty"`
	SHA256  string `json:"sha256,omitempty"`

	// modTime is used internally for metadata-change planning. It is not
	// serialized so report JSON stays independent of wall-clock precision.
	modTime time.Time
}

// Snapshot is the published, immutable result of the scan phase.
type Snapshot struct {
	Root  string
	files map[string]FileState
}

// State returns the captured state of an absolute cleaned path. The boolean
// reports whether the path was part of the snapshot set; a captured missing
// file returns state with Exists == false and ok == true.
func (s Snapshot) State(absPath string) (FileState, bool) {
	st, ok := s.files[cleanPath(absPath)]
	return st, ok
}

// Paths returns the captured absolute paths in sorted order.
func (s Snapshot) Paths() []string {
	out := make([]string, 0, len(s.files))
	for p := range s.files {
		out = append(out, p)
	}
	sortStrings(out)
	return out
}

// States returns the captured entries in sorted path order.
func (s Snapshot) States() []FileState {
	paths := s.Paths()
	out := make([]FileState, 0, len(paths))
	for _, p := range paths {
		out = append(out, s.files[p])
	}
	return out
}

// Op is a single entry of a Plan.
type Op struct {
	Index      int        `json:"index"`
	Action     Action     `json:"action"`
	Path       string     `json:"path"`
	Relative   string     `json:"relative"`
	Change     ChangeType `json:"change"`
	Mode       uint32     `json:"mode"`
	BeforeHash string     `json:"beforeHash,omitempty"`
	AfterHash  string     `json:"afterHash,omitempty"`
	BeforeSize int64      `json:"beforeSize,omitempty"`
	AfterSize  int64      `json:"afterSize,omitempty"`
	BeforeTime time.Time  `json:"beforeTime,omitempty"`
	AfterTime  time.Time  `json:"afterTime,omitempty"`
}

// Plan is the deterministic result of the plan phase. Identical desired state
// (relative paths, modes, content hashes, timestamps) yields the same ID even
// when the plan is rebuilt across process restarts.
type Plan struct {
	SchemaVersion string `json:"schemaVersion"`
	ID            string `json:"id"`
	Root          string `json:"root"`
	Ops           []Op   `json:"ops"`
}

// ChangeOps returns the subset of operations that mutate state (excluding
// unchanged entries).
func (p *Plan) ChangeOps() []Op {
	out := make([]Op, 0, len(p.Ops))
	for _, op := range p.Ops {
		if op.Action != "" {
			out = append(out, op)
		}
	}
	return out
}
