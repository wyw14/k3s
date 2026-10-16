package dryrun

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// PlanConfig configures BuildPlan.
type PlanConfig struct {
	// Root is the directory below which every target path must live.
	Root string
	// Specs is the desired file set. Nil content is treated as an empty file.
	Specs []FileSpec
	// Base is the scan-phase snapshot of the spec paths.
	Base Snapshot
	// DirMode is used for directories that need to be created (default 0700).
	DirMode os.FileMode
	// FileMode is used when Spec.Mode is zero (default 0600).
	FileMode os.FileMode
	// MaxFileSize rejects specs with larger content when positive.
	MaxFileSize int64
}

// BuildPlan validates the desired file set against the scanned state and
// returns a deterministic, ordered plan of the operations required to reach
// the desired state.
func BuildPlan(cfg PlanConfig) (*Plan, error) {
	root := cleanPath(cfg.Root)
	dirMode := cfg.DirMode
	if dirMode == 0 {
		dirMode = DefaultDirMode
	}
	fileMode := cfg.FileMode
	if fileMode == 0 {
		fileMode = DefaultFileMode
	}

	// Validate and normalize specs.
	specs := make([]FileSpec, len(cfg.Specs))
	targets := make(map[string]struct{}, len(cfg.Specs))
	for i, spec := range cfg.Specs {
		abs, err := validateTarget(root, spec, targets, cfg.MaxFileSize)
		if err != nil {
			return nil, err
		}
		spec.Path = abs
		specs[i] = spec
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Path < specs[j].Path })

	mkdirs := make(map[string]bool)
	ops := make([]Op, 0, len(specs))
	idRows := make([]string, 0, len(specs))

	for _, spec := range specs {
		abs := spec.Path

		state, found := cfg.Base.State(abs)
		if !found {
			// The executor normally scans every spec path; fall back to a
			// direct stat so callers building plans without a snapshot work.
			st, err := statAny(abs)
			if err != nil {
				return nil, err
			}
			state = st
		}

		if state.Escapes {
			return nil, annotate(ErrPathOutsideRoot, PhasePlan, abs)
		}
		if state.Exists && state.IsLink {
			return nil, annotate(ErrTargetIsLink, PhasePlan, abs)
		}
		if state.Exists && state.IsDir {
			return nil, annotate(ErrTargetIsDir, PhasePlan, abs)
		}

		// Record missing ancestor directories and reject non-directory
		// ancestors. The walk itself is read-only.
		if err := collectAncestors(root, abs, mkdirs); err != nil {
			return nil, err
		}

		effMode := spec.Mode
		if effMode == 0 {
			effMode = fileMode
		}
		afterHash := hashBytes(spec.Content)
		idRows = append(idRows, strings.Join([]string{
			relativeTo(root, abs),
			strconv.FormatUint(uint64(perm(effMode)), 8),
			afterHash,
			strconv.FormatInt(int64(len(spec.Content)), 10),
			strconv.FormatInt(spec.ModTime.Unix(), 10),
		}, "\x00"))

		op := Op{
			Path:       abs,
			Relative:   relativeTo(root, abs),
			Mode:       perm(effMode),
			AfterHash:  afterHash,
			AfterSize:  int64(len(spec.Content)),
			AfterTime:  spec.ModTime,
			BeforeHash: state.SHA256,
			BeforeSize: state.Size,
			BeforeTime: state.modTime,
		}

		switch {
		case !state.Exists:
			op.Action = ActionCreate
			op.Change = ChangeAdded
		case state.SHA256 != afterHash:
			op.Action = ActionUpdate
			op.Change = ChangeModified
		case metadataDiffers(state, spec, effMode):
			op.Action = ActionSetTimes
			op.Change = ChangeMetadata
		default:
			// Content and metadata already match: no operation.
			continue
		}
		ops = append(ops, op)
	}

	// Materialize mkdir operations (deepest path last is enforced by the
	// final ordering).
	for dir := range mkdirs {
		ops = append(ops, Op{
			Action:   ActionMkdir,
			Path:     dir,
			Relative: relativeTo(root, dir),
			Change:   ChangeAdded,
			Mode:     perm(dirMode),
		})
	}

	sortOps(ops)
	for i := range ops {
		ops[i].Index = i
	}

	return &Plan{
		SchemaVersion: SchemaVersion,
		ID:            desiredStateID(idRows),
		Root:          root,
		Ops:           ops,
	}, nil
}

// validateTarget applies the boundary rules to a single spec.
func validateTarget(root string, spec FileSpec, targets map[string]struct{}, maxSize int64) (string, error) {
	if spec.Path == "" {
		return "", annotate(ErrEmptyPath, PhasePlan, "")
	}
	if !filepath.IsAbs(filepath.Clean(spec.Path)) {
		return "", annotate(ErrRelativePath, PhasePlan, spec.Path)
	}
	abs := cleanPath(spec.Path)
	if abs == root || !withinRoot(root, abs) {
		return "", annotate(ErrPathOutsideRoot, PhasePlan, spec.Path)
	}
	if _, dup := targets[abs]; dup {
		return "", annotate(ErrDuplicateTarget, PhasePlan, abs)
	}
	if maxSize > 0 && int64(len(spec.Content)) > maxSize {
		return "", annotate(ErrFileTooLarge, PhasePlan, abs)
	}
	targets[abs] = struct{}{}
	return abs, nil
}

// collectAncestors walks from the file's parent up to the root, recording
// missing directories in mkdirs.
func collectAncestors(root, abs string, mkdirs map[string]bool) error {
	chain := []string{}
	for dir := filepath.Dir(abs); dir != root; dir = filepath.Dir(dir) {
		if !withinRoot(root, dir) {
			return annotate(ErrPathOutsideRoot, PhasePlan, dir)
		}
		chain = append(chain, dir)
		if filepath.Dir(dir) == dir {
			// Defensive: filesystem root reached without hitting root.
			return annotate(ErrPathOutsideRoot, PhasePlan, dir)
		}
	}
	// Inspect from closest to root outward so conflicts surface in order.
	for i := len(chain) - 1; i >= 0; i-- {
		dir := chain[i]
		if mkdirs[dir] {
			continue
		}
		// Fail closed on a symlinked ancestor that resolves outside the
		// diagnostic root: directory creation and atomic replacement would
		// otherwise follow the link and write outside the root. An in-root
		// link to an existing directory is accepted.
		if li, lerr := os.Lstat(dir); lerr == nil && li.Mode()&os.ModeSymlink != 0 {
			resolved, rerr := filepath.EvalSymlinks(dir)
			if rerr != nil || !withinRoot(root, cleanPath(resolved)) {
				return annotate(ErrPathOutsideRoot, PhasePlan, dir)
			}
		}
		info, err := os.Stat(dir)
		switch {
		case err == nil:
			if !info.IsDir() {
				return annotate(ErrParentNotDir, PhasePlan, dir)
			}
		case os.IsNotExist(err):
			mkdirs[dir] = true
		default:
			return Wrap(err, PhasePlan, dir)
		}
	}
	return nil
}

// metadataDiffers reports whether mtime or permission bits need reconciliation
// for a file whose content already matches.
func metadataDiffers(state FileState, spec FileSpec, effMode os.FileMode) bool {
	if !modeSatisfied(effMode, os.FileMode(state.Mode)) {
		return true
	}
	if !spec.ModTime.IsZero() && state.modTime.Unix() != spec.ModTime.Unix() {
		return true
	}
	return false
}

// statAny is the read-only fallback used when a target was not part of the
// scanned set.
func statAny(abs string) (FileState, error) {
	li, err := os.Lstat(abs)
	if err != nil {
		if os.IsNotExist(err) {
			return FileState{Path: abs, Exists: false}, nil
		}
		return FileState{}, Wrap(err, PhasePlan, abs)
	}
	st := FileState{
		Path:    abs,
		Exists:  true,
		Size:    li.Size(),
		Mode:    perm(li.Mode()),
		modTime: li.ModTime(),
	}
	if li.IsDir() {
		st.IsDir = true
	}
	if li.Mode()&os.ModeSymlink != 0 {
		st.IsLink = true
	}
	return st, nil
}

// sortOps orders mkdir operations before any operation below them; all other
// ordering is lexical, making the result fully deterministic.
func sortOps(ops []Op) {
	sort.Slice(ops, func(i, j int) bool {
		a, b := ops[i].Path, ops[j].Path
		ai, bj := ops[i].Action == ActionMkdir, ops[j].Action == ActionMkdir
		switch {
		case ai && !bj && strings.HasPrefix(b, a+string(filepath.Separator)):
			return true
		case bj && !ai && strings.HasPrefix(a, b+string(filepath.Separator)):
			return false
		default:
			return a < b
		}
	})
}

// desiredStateID derives a stable identifier from the complete desired file
// set, independent of how much of that state is already present on disk. This
// is what makes journal resume across restarts safe: a partially applied run
// and its resumption carry the same plan ID.
func desiredStateID(rows []string) string {
	h := sha256.New()
	for _, row := range rows {
		h.Write([]byte(row))
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// annotate attaches phase/path context to a sentinel error.
func annotate(sentinel *Error, phase Phase, path string) *Error {
	return &Error{
		Code:    sentinel.Code,
		Phase:   phase,
		Path:    path,
		Message: sentinel.Message,
	}
}
