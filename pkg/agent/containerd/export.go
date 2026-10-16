package containerd

import (
	"context"
	stderrors "errors"
	"fmt"
	"io"
	"sort"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/core/images/archive"
	"github.com/containerd/errdefs"
)

// BatchExportImages resolves and exports the named images to w as a single
// multi-image OCI/Docker tar archive.
//
// Each reference is validated and resolved individually before the archive is
// written: invalid references are marked Invalid, and images missing from the
// local store are marked Failed and do not abort the batch. The returned
// BatchResult reports the explicit status of every reference, including
// Partial success when only some images are available. Duplicate references
// are de-duplicated. A canceled context aborts resolution and no archive is
// written; the returned result carries the Canceled status and can be retried
// idempotently on a later call.
func BatchExportImages(ctx context.Context, client *containerd.Client, imageNames []string, w io.Writer) (*BatchResult, error) {
	result, resolved := resolveImagesForExport(ctx, client.ImageService(), imageNames)

	if result.Phase == BatchCanceled {
		return result, ctx.Err()
	}
	if len(resolved) == 0 {
		if result.Phase != BatchInvalid {
			result.Phase = BatchFailed
		}
		if result.Reason == "" {
			result.Reason = "no images available for export"
		}
		return result, stderrors.New(result.Reason)
	}

	if err := client.Export(ctx, w, archive.WithAllPlatforms(), archive.WithImages(resolved)); err != nil {
		result.Phase = BatchFailed
		result.Reason = err.Error()
		return result, err
	}

	return result, nil
}

// resolveImagesForExport validates the references and resolves them against the
// image store. Resolution runs through the batch engine, so missing images are
// permanent failures, transient store errors are retried, and the per-reference
// status is available even when some references cannot be exported.
func resolveImagesForExport(ctx context.Context, store images.Store, imageNames []string) (*BatchResult, []images.Image) {
	resolved := map[string]images.Image{}
	result := RunBatch(ctx, imageNames, BatchOptions{
		Validator: validateImageReference,
		Worker: func(ctx context.Context, name string) ([]string, error) {
			image, err := store.Get(ctx, name)
			if err != nil {
				if errdefs.IsNotFound(err) {
					// Retrying cannot pull an image that is not in the local store.
					return nil, Permanent(err)
				}
				return nil, err
			}
			resolved[name] = image
			return []string{image.Name}, nil
		},
	})

	names := make([]string, 0, len(resolved))
	for name := range resolved {
		names = append(names, name)
	}
	sort.Strings(names)

	resolvedImages := make([]images.Image, 0, len(names))
	for _, name := range names {
		resolvedImages = append(resolvedImages, resolved[name])
	}
	return result, resolvedImages
}

// validateImageReference validates that a batch export entry parses as a
// tagged, normalized image reference.
func validateImageReference(name string) error {
	if _, err := parseNamedTagged(name); err != nil {
		return fmt.Errorf("%w: %s: %v", ErrInvalidInput, name, err)
	}
	return nil
}
