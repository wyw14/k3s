package dryrun

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// journalEntry records one atomically completed operation.
type journalEntry struct {
	Relative string `json:"relative"`
	Action   Action `json:"action"`
	SHA256   string `json:"sha256,omitempty"`
	Mode     uint32 `json:"mode,omitempty"`
	ModTime  int64  `json:"modTime,omitempty"`
}

// journalData is the restart-recovery state of an applying executor.
type journalData struct {
	SchemaVersion string         `json:"schemaVersion"`
	PlanID        string         `json:"planId"`
	Root          string         `json:"root"`
	Entries       []journalEntry `json:"entries"`
	Done          bool           `json:"done"`
}

// loadJournal reads a journal. A missing journal yields (nil, false, nil);
// malformed content is reported as E_CORRUPT.
func loadJournal(path string) (*journalData, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, Wrap(err, PhaseApply, path)
	}
	var jd journalData
	if err := json.Unmarshal(b, &jd); err != nil {
		return nil, false, annotate(ErrInvalidEncoding, PhaseApply, path)
	}
	return &jd, true, nil
}

// saveJournal atomically persists the journal so a crash can never leave a
// torn checkpoint file. The temporary file is synced before the rename.
func saveJournal(path string, jd *journalData) error {
	if path == "" {
		return nil
	}
	b, err := json.MarshalIndent(jd, "", "  ")
	if err != nil {
		return Fail(CodeInternal, PhaseApply, "encoding journal: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), DefaultDirMode); err != nil {
		return Wrap(err, PhaseApply, filepath.Dir(path))
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, DefaultFileMode)
	if err != nil {
		return Wrap(err, PhaseApply, tmp)
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return Wrap(err, PhaseApply, tmp)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return Wrap(err, PhaseApply, tmp)
	}
	if err := f.Close(); err != nil {
		return Wrap(err, PhaseApply, tmp)
	}
	if err := os.Rename(tmp, path); err != nil {
		return Wrap(err, PhaseApply, path)
	}
	return nil
}

// removeStaleJournalTemp removes a half-written journal temporary file left by
// a process that crashed between writing and renaming it.
func removeStaleJournalTemp(path string) {
	if path == "" {
		return
	}
	if err := os.Remove(path + ".tmp"); err != nil && !os.IsNotExist(err) {
		// Stale temp cleanup is best-effort; the next save replaces it.
		_ = err
	}
}

// add records an operation checkpoint, replacing an earlier checkpoint for the
// same relative/action so a re-executed operation (for example one whose file
// was deleted after an earlier crash) never produces duplicate entries.
func (jd *journalData) add(e journalEntry) {
	for i, existing := range jd.Entries {
		if existing.Relative == e.Relative && existing.Action == e.Action {
			jd.Entries[i] = e
			return
		}
	}
	jd.Entries = append(jd.Entries, e)
}

// has reports whether the operation was already checkpointed, returning the
// recorded post-condition hash.
func (jd *journalData) has(rel string, action Action) (journalEntry, bool) {
	for _, e := range jd.Entries {
		if e.Relative == rel && e.Action == action {
			return e, true
		}
	}
	return journalEntry{}, false
}
