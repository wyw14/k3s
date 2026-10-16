package dryrun

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Hook is invoked before every mutating operation in both dry-run and apply
// modes. Returning an error aborts the run; *Error values are preserved, other
// errors are classified as E_INTERNAL. It exists for fault injection and for
// callers that need to observe planned ops just before they happen.
type Hook func(op Op) error

// Executor runs the phased diagnostic. The zero value is a safe, read-only
// dry-run; persistent writes require explicitly setting Apply to true.
type Executor struct {
	// Root is the data directory below which all target paths must live.
	Root string
	// Apply opts into persistent writes; the zero value (false) runs the
	// fail-closed dry-run simulation.
	Apply bool
	// JournalPath enables restart recovery for apply mode. Every completed
	// operation is checkpointed atomically; a later Executor run with the same
	// plan resumes from the checkpoint. Dry-run never writes a journal.
	JournalPath string
	// MaxConcurrency bounds scan-phase readers (default: NumCPU capped at 8).
	MaxConcurrency int
	// MaxFileSize rejects proposed content above this size when positive.
	MaxFileSize int64
	// DirMode defaults to 0700 and FileMode to 0600.
	DirMode  os.FileMode
	FileMode os.FileMode
	// DiffThresholdBytes limits embedded unified diffs (default 64KiB).
	DiffThresholdBytes int64
	// Hook is invoked before each mutating operation.
	Hook Hook
}

func (e *Executor) diffThreshold() int64 {
	if e.DiffThresholdBytes > 0 {
		return e.DiffThresholdBytes
	}
	return DefaultDiffThreshold
}

// Scan performs a read-only scan of the given paths and returns a report with
// scan/plan/apply/verify phases marked skipped except scan. It never mutates
// persistent state.
func (e *Executor) Scan(ctx context.Context, paths []string) (*Report, error) {
	root := cleanPath(e.Root)
	rep := newReport(root, true)
	rep.Status = StatusScanned

	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			rep.warn("diagnostic root does not exist: %s", root)
		} else {
			return rep, Wrap(err, PhaseScan, root)
		}
	}

	snap, err := capture(ctx, root, paths, e.MaxConcurrency)
	if err != nil {
		rep.setPhase(PhaseReport{Name: PhaseScan, Status: phaseFailed, Note: string(AsError(err).Code)})
		rep.Status = StatusFailed
		rep.Diffs = buildScanDiffs(Snapshot{Root: root})
		summarize(rep)
		return rep, err
	}
	rep.setPhase(PhaseReport{Name: PhaseScan, Status: phaseOK, Ops: len(paths)})
	rep.Diffs = buildScanDiffs(snap)
	summarize(rep)
	if rep.Summary.Paths == 0 {
		rep.Status = StatusNoChanges
	}
	return rep, nil
}

// Run executes scan, plan, apply and verify over the desired file set. In
// dry-run mode (the default) no persistent state is modified; on failure or
// cancellation the returned error is classified and the partial report is
// still returned.
func (e *Executor) Run(ctx context.Context, specs []FileSpec) (*Report, error) {
	root := cleanPath(e.Root)
	dryRun := !e.Apply
	rep := newReport(root, dryRun)

	if _, err := os.Stat(root); err != nil {
		if os.IsNotExist(err) {
			if dryRun {
				rep.warn("diagnostic root does not exist: %s", root)
			}
		} else {
			return rep, Wrap(err, PhaseScan, root)
		}
	}

	// Normalize all targets to absolute paths once; every subsequent phase
	// works against this same slice.
	specs = normalizeSpecs(root, specs)
	paths := make([]string, 0, len(specs))
	for _, s := range specs {
		paths = append(paths, s.Path)
	}

	// Phase 1: scan.
	snap, err := capture(ctx, root, paths, e.MaxConcurrency)
	if err != nil {
		return e.fail(rep, PhaseScan, err)
	}
	rep.setPhase(PhaseReport{Name: PhaseScan, Status: phaseOK, Ops: len(paths)})

	// Phase 2: plan.
	plan, err := BuildPlan(PlanConfig{
		Root:        root,
		Specs:       specs,
		Base:        snap,
		DirMode:     e.DirMode,
		FileMode:    e.FileMode,
		MaxFileSize: e.MaxFileSize,
	})
	if err != nil {
		return e.fail(rep, PhasePlan, err)
	}
	changeOps := plan.ChangeOps()
	rep.PlanID = plan.ID
	rep.Diffs = buildDiffs(plan, specs, snap, e.diffThreshold())

	note := ""
	if dryRun {
		note = "simulated; no persistent changes"
	}
	if len(changeOps) == 0 {
		rep.setPhase(PhaseReport{Name: PhasePlan, Status: phaseOK, Ops: 0})
		rep.setPhase(PhaseReport{Name: PhaseApply, Status: phaseSkipped, Note: "no changes required"})
		rep.setPhase(PhaseReport{Name: PhaseVerify, Status: phaseOK})
		rep.Status = StatusNoChanges
		summarize(rep)
		return rep, nil
	}
	rep.setPhase(PhaseReport{Name: PhasePlan, Status: phaseOK, Ops: len(changeOps)})

	// Phase 3: apply.
	var skipped int
	if dryRun {
		if err := e.simulate(ctx, changeOps); err != nil {
			return e.fail(rep, PhaseApply, err)
		}
		rep.setPhase(PhaseReport{Name: PhaseApply, Status: phaseOK, Ops: len(changeOps), Note: note})
	} else {
		skipped, err = e.apply(ctx, plan, changeOps, specs)
		if err != nil {
			rep.setPhase(PhaseReport{Name: PhaseApply, Status: phaseFailed, Ops: len(changeOps), Skipped: skipped})
			rep.Status = StatusFailed
			summarize(rep)
			return rep, err
		}
		rep.setPhase(PhaseReport{Name: PhaseApply, Status: phaseOK, Ops: len(changeOps), Skipped: skipped})
		rep.Status = StatusApplied
	}

	// Phase 4: verify.
	if dryRun {
		err = verifyPlan(plan, specs, snap)
	} else {
		err = e.verify(ctx, specs)
	}
	if err != nil {
		return e.fail(rep, PhaseVerify, err)
	}
	rep.setPhase(PhaseReport{Name: PhaseVerify, Status: phaseOK, Ops: len(specs)})
	if dryRun {
		rep.Status = StatusPlanned
	}
	summarize(rep)
	return rep, nil
}

// normalizeSpecs returns specs cleaned to absolute paths (BuildPlan mutates a
// local copy, so report building must perform the same normalization).
func normalizeSpecs(root string, specs []FileSpec) []FileSpec {
	out := make([]FileSpec, len(specs))
	for i, s := range specs {
		s.Path = cleanPath(s.Path)
		out[i] = s
	}
	return out
}

// simulate plays the change operations over the in-memory snapshot. It makes
// no write syscalls at all.
func (e *Executor) simulate(ctx context.Context, ops []Op) error {
	for _, op := range ops {
		select {
		case <-ctx.Done():
			return Wrap(ctx.Err(), PhaseApply, op.Path)
		default:
		}
		if e.Hook != nil {
			if err := e.invokeHook(op); err != nil {
				return err
			}
		}
	}
	return nil
}

// apply performs the journaled, atomic filesystem operations and returns the
// number of operations skipped thanks to an existing journal.
func (e *Executor) apply(ctx context.Context, plan *Plan, ops []Op, specs []FileSpec) (int, error) {
	content := make(map[string][]byte, len(specs))
	for _, s := range specs {
		content[cleanPath(s.Path)] = s.Content
	}

	jd, err := e.openJournal(plan)
	if err != nil {
		return 0, err
	}

	parentDirs := map[string]struct{}{}
	for _, op := range ops {
		if op.Action != ActionMkdir {
			parentDirs[filepath.Dir(op.Path)] = struct{}{}
		}
	}
	cleanupStaleTempFiles(parentDirs)

	// At resume, operations completed in the previous session are usually
	// absent from the current plan (scanning sees their results). Verify each
	// checkpoint directly against disk and count the recovered operations.
	skipped := 0
	if jd != nil {
		for _, entry := range jd.Entries {
			candidate := Op{
				Action:    entry.Action,
				Path:      filepath.Join(plan.Root, filepath.FromSlash(entry.Relative)),
				Relative:  entry.Relative,
				AfterHash: entry.SHA256,
				Mode:      entry.Mode,
			}
			if entry.ModTime != 0 {
				candidate.AfterTime = time.Unix(entry.ModTime, 0)
			}
			if journalEntrySatisfied(candidate, entry) {
				skipped++
			}
		}
	}

	for _, op := range ops {
		select {
		case <-ctx.Done():
			return skipped, Wrap(ctx.Err(), PhaseApply, op.Path)
		default:
		}

		if jd != nil {
			if entry, done := jd.has(op.Relative, op.Action); done && journalEntrySatisfied(op, entry) {
				skipped++
				continue
			}
		}

		if e.Hook != nil {
			if err := e.invokeHook(op); err != nil {
				return skipped, err
			}
		}

		switch op.Action {
		case ActionMkdir:
			if err := os.MkdirAll(op.Path, os.FileMode(op.Mode)); err != nil {
				return skipped, Wrap(err, PhaseApply, op.Path)
			}
		case ActionCreate, ActionUpdate:
			if err := atomicWriteFile(op, content[op.Path]); err != nil {
				return skipped, err
			}
		case ActionSetTimes:
			if err := os.Chmod(op.Path, os.FileMode(op.Mode)); err != nil {
				return skipped, Wrap(err, PhaseApply, op.Path)
			}
			if !op.AfterTime.IsZero() {
				if err := os.Chtimes(op.Path, op.AfterTime, op.AfterTime); err != nil {
					return skipped, Wrap(err, PhaseApply, op.Path)
				}
			}
		default:
			return skipped, Fail(CodeInternal, PhaseApply, "unknown action %q for %s", op.Action, op.Path)
		}

		if jd != nil {
			entry := journalEntry{
				Relative: op.Relative,
				Action:   op.Action,
				SHA256:   op.AfterHash,
				Mode:     op.Mode,
			}
			if !op.AfterTime.IsZero() {
				entry.ModTime = op.AfterTime.Unix()
			}
			jd.add(entry)
			if err := saveJournal(e.JournalPath, jd); err != nil {
				return skipped, err
			}
		}
	}

	if jd != nil {
		jd.Done = true
		if err := saveJournal(e.JournalPath, jd); err != nil {
			return skipped, err
		}
		if err := os.Remove(e.JournalPath); err != nil && !os.IsNotExist(err) {
			return skipped, Wrap(err, PhaseApply, e.JournalPath)
		}
		removeStaleJournalTemp(e.JournalPath)
	}
	return skipped, nil
}

// verifyPlan checks a dry-run against the immutable inputs only: every
// unchanged target still matches the scanned state and every change operation
// produces the desired content hash. It performs no filesystem access.
func verifyPlan(plan *Plan, specs []FileSpec, base Snapshot) error {
	changed := make(map[string]bool, len(plan.Ops))
	for _, op := range plan.Ops {
		if op.Action == ActionMkdir {
			continue
		}
		changed[op.Path] = true
		if op.AfterHash != hashBytes(specContent(specs, op.Path)) {
			return &Error{Code: CodeConflict, Phase: PhaseVerify, Path: op.Path,
				Message: ErrStateDrift.Message}
		}
	}
	for _, s := range specs {
		abs := cleanPath(s.Path)
		if changed[abs] {
			continue
		}
		st, ok := base.State(abs)
		if !ok || !st.Exists || st.SHA256 != hashBytes(s.Content) {
			return &Error{Code: CodeConflict, Phase: PhaseVerify, Path: abs,
				Message: ErrStateDrift.Message + ": unchanged target diverged from snapshot"}
		}
	}
	return nil
}

func specContent(specs []FileSpec, path string) []byte {
	for _, s := range specs {
		if cleanPath(s.Path) == path {
			return s.Content
		}
	}
	return nil
}

// invokeHook classifies hook errors.
func (e *Executor) invokeHook(op Op) error {
	if err := e.Hook(op); err != nil {
		if AsError(err) != nil {
			return err
		}
		return Fail(CodeInternal, PhaseApply, "hook failed for %s: %v", op.Path, err)
	}
	return nil
}

// openJournal loads or initializes the resume journal according to policy.
func (e *Executor) openJournal(plan *Plan) (*journalData, error) {
	if e.JournalPath == "" {
		return nil, nil
	}
	removeStaleJournalTemp(e.JournalPath)
	existing, found, err := loadJournal(e.JournalPath)
	if err != nil {
		return nil, err
	}
	if !found {
		return &journalData{SchemaVersion: SchemaVersion, PlanID: plan.ID, Root: plan.Root}, nil
	}
	if existing.Done {
		// A completed journal from a previous run: normal reconciliation will
		// produce no remaining operations; remove the stale journal.
		if err := os.Remove(e.JournalPath); err != nil && !os.IsNotExist(err) {
			return nil, Wrap(err, PhaseApply, e.JournalPath)
		}
		return &journalData{SchemaVersion: SchemaVersion, PlanID: plan.ID, Root: plan.Root}, nil
	}
	if existing.PlanID != plan.ID || existing.Root != plan.Root {
		return nil, annotate(ErrJournalMismatch, PhaseApply, e.JournalPath)
	}
	return existing, nil
}

// journalEntrySatisfied verifies on disk that a checkpointed operation really
// completed; a stale entry forces the operation to run again.
func journalEntrySatisfied(op Op, entry journalEntry) bool {
	switch op.Action {
	case ActionMkdir:
		info, err := os.Stat(op.Path)
		return err == nil && info.IsDir()
	case ActionSetTimes:
		info, err := os.Stat(op.Path)
		return err == nil && !info.IsDir() && modeSatisfied(os.FileMode(op.Mode), info.Mode()) &&
			(op.AfterTime.IsZero() || info.ModTime().Unix() == op.AfterTime.Unix())
	default:
		f, err := os.Open(op.Path)
		if err != nil {
			return false
		}
		defer f.Close()
		sum, err := hashFile(f)
		return err == nil && sum == entry.SHA256 && sum == op.AfterHash
	}
}

// atomicWriteFile writes content to a same-directory temporary file, sets the
// requested mode and mtime, and renames it into place. A crash at any point
// leaves either the previous file or the complete new file, never a partial
// target.
func atomicWriteFile(op Op, content []byte) error {
	dir := filepath.Dir(op.Path)
	if err := os.MkdirAll(dir, DefaultDirMode); err != nil {
		return Wrap(err, PhaseApply, dir)
	}

	var randb [8]byte
	if _, err := rand.Read(randb[:]); err != nil {
		return Fail(CodeInternal, PhaseApply, "generating temp name: %v", err)
	}
	tmp := filepath.Join(dir, ".k3s-dryrun-tmp-"+hex.EncodeToString(randb[:]))

	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(op.Mode))
	if err != nil {
		return Wrap(err, PhaseApply, tmp)
	}
	cleanup := true
	defer func() {
		if cleanup {
			os.Remove(tmp)
		}
	}()

	if _, err := f.Write(content); err != nil {
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
	if !op.AfterTime.IsZero() {
		if err := os.Chtimes(tmp, op.AfterTime, op.AfterTime); err != nil {
			return Wrap(err, PhaseApply, tmp)
		}
	}
	if err := os.Rename(tmp, op.Path); err != nil {
		return Wrap(err, PhaseApply, op.Path)
	}
	cleanup = false
	return nil
}

// cleanupStaleTempFiles removes temporary files left by a crashed earlier
// apply, restricted to the exact parent directories of planned files and the
// fixed internal prefix.
func cleanupStaleTempFiles(dirs map[string]struct{}) {
	for dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			continue
		}
		for _, ent := range entries {
			name := ent.Name()
			if len(name) > 0 && name[0] == '.' && strings.HasPrefix(name, ".k3s-dryrun-tmp-") && !ent.IsDir() {
				os.Remove(filepath.Join(dir, name))
			}
		}
	}
}

// verify re-reads the target files and compares them against the desired
// content. In dry-run mode no writes occurred, so this also proves the
// simulation stayed consistent.
func (e *Executor) verify(ctx context.Context, specs []FileSpec) error {
	paths := make([]string, 0, len(specs))
	for _, s := range specs {
		paths = append(paths, s.Path)
	}
	snap, err := capture(ctx, cleanPath(e.Root), paths, e.MaxConcurrency)
	if err != nil {
		return err
	}
	for _, s := range specs {
		select {
		case <-ctx.Done():
			return Wrap(ctx.Err(), PhaseVerify, s.Path)
		default:
		}
		abs := cleanPath(s.Path)
		st, ok := snap.State(abs)
		if !ok || !st.Exists || st.IsDir {
			return &Error{
				Code:    CodeConflict,
				Phase:   PhaseVerify,
				Path:    abs,
				Message: ErrStateDrift.Message + ": target missing after apply",
			}
		}
		if st.SHA256 != hashBytes(s.Content) {
			return &Error{
				Code:    CodeConflict,
				Phase:   PhaseVerify,
				Path:    abs,
				Message: ErrStateDrift.Message,
			}
		}
		effMode := s.Mode
		if effMode == 0 {
			effMode = e.FileMode
			if effMode == 0 {
				effMode = DefaultFileMode
			}
		}
		if !modeSatisfied(effMode, os.FileMode(st.Mode)) {
			return &Error{
				Code: CodeConflict, Phase: PhaseVerify, Path: abs,
				Message: ErrStateDrift.Message + ": mode mismatch",
			}
		}
		if !s.ModTime.IsZero() && st.modTime.Unix() != s.ModTime.Unix() {
			return &Error{
				Code: CodeConflict, Phase: PhaseVerify, Path: abs,
				Message: ErrStateDrift.Message + ": mtime mismatch",
			}
		}
	}
	return nil
}

// fail marks the report failed and returns the classified error alongside the
// partial report so callers can render what ran.
func (e *Executor) fail(rep *Report, phase Phase, err error) (*Report, error) {
	rep.setPhase(PhaseReport{Name: phase, Status: phaseFailed, Note: errText(err)})
	rep.Status = StatusFailed
	summarize(rep)
	return rep, err
}

func errText(err error) string {
	if e := AsError(err); e != nil {
		return string(e.Code)
	}
	return err.Error()
}
