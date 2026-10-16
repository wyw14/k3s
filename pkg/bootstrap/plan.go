package bootstrap

import (
	"context"
	"encoding/json"
	"io"
	"sort"

	"github.com/k3s-io/k3s/pkg/daemons/config"
	"github.com/k3s-io/k3s/pkg/dryrun"
)

// FileSpecs converts bootstrap data read from a datastore into dry-run file
// specifications, applying the same path resolution and filtering rules as
// WriteToDiskFromStorage: path keys that are absent from the bootstrap struct
// and entries resolving to an empty path are skipped. The result is ordered by
// path key so the diagnostic plan is deterministic.
//
// Files use mode 0600 and parent directories 0700 via the dryrun defaults,
// matching WriteToDiskFromStorage exactly.
func FileSpecs(files PathsDataformat, crb *config.ControlRuntimeBootstrap) ([]dryrun.FileSpec, error) {
	paths, err := ObjToMap(crb)
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(files))
	for key := range files {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	specs := make([]dryrun.FileSpec, 0, len(keys))
	for _, key := range keys {
		bsf := files[key]
		path, ok := paths[key]
		if !ok || path == "" {
			continue
		}
		specs = append(specs, dryrun.FileSpec{
			Path:    path,
			Content: bsf.Content,
			ModTime: bsf.Timestamp,
		})
	}
	return specs, nil
}

// DryRunToDisk plans what reconciling files against the given bootstrap layout
// would change under root (normally the data directory), without modifying
// persistent state.
func DryRunToDisk(ctx context.Context, root string, files PathsDataformat, crb *config.ControlRuntimeBootstrap) (*dryrun.Report, error) {
	specs, err := FileSpecs(files, crb)
	if err != nil {
		return nil, dryrun.Wrap(err, dryrun.PhasePlan, "")
	}
	executor := dryrun.Executor{Root: root}
	return executor.Run(ctx, specs)
}

// ScanToDisk produces a read-only scan report of the bootstrap layout under
// root (present and missing files) without proposing changes.
func ScanToDisk(ctx context.Context, root string, crb *config.ControlRuntimeBootstrap) (*dryrun.Report, error) {
	paths, err := ObjToMap(crb)
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(paths))
	for _, path := range paths {
		if path == "" {
			continue
		}
		keys = append(keys, path)
	}
	sort.Strings(keys)

	executor := dryrun.Executor{Root: root}
	return executor.Scan(ctx, keys)
}

// DecodeBootstrap parses bootstrap data in either the current
// PathsDataformat JSON encoding or the legacy map[string][]byte format used by
// older clusters (see migrateBootstrapData in pkg/cluster). Malformed payloads
// return a dryrun E_CORRUPT classified error.
func DecodeBootstrap(r io.Reader) (PathsDataformat, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, dryrun.Wrap(err, dryrun.PhasePlan, "")
	}

	files := make(PathsDataformat)
	if err := json.Unmarshal(data, &files); err == nil {
		return files, nil
	}

	legacy := map[string][]byte{}
	if err := json.Unmarshal(data, &legacy); err != nil {
		return nil, &dryrun.Error{
			Code:    dryrun.CodeCorrupt,
			Phase:   dryrun.PhasePlan,
			Message: "decoding bootstrap data: expected PathsDataformat or legacy map JSON",
			Cause:   err,
		}
	}

	files = make(PathsDataformat, len(legacy))
	for key, content := range legacy {
		files[key] = File{Content: content}
	}
	return files, nil
}
