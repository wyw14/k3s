// Package dryrun implements the in-process action behind the
// `k3s dry-run` diagnostic command.
package dryrun

import (
	"bytes"
	"context"
	"io"
	"os"

	"github.com/k3s-io/k3s/pkg/bootstrap"
	"github.com/k3s-io/k3s/pkg/daemons/config"
	"github.com/k3s-io/k3s/pkg/datadir"
	k3sdryrun "github.com/k3s-io/k3s/pkg/dryrun"
	"github.com/urfave/cli/v2"
)

// Run is the cli.Action for `k3s dry-run`. It returns a classified
// *dryrun.Error on failure; the command never modifies the data directory.
func Run(clx *cli.Context) error {
	out := clx.App.Writer

	format := clx.String("output")
	if format != "text" && format != "json" {
		return k3sdryrun.Fail(k3sdryrun.CodeValidation, "",
			"invalid --output value %q: valid values are text, json", format)
	}

	dataDir, err := datadir.Resolve(clx.String("data-dir"))
	if err != nil {
		return k3sdryrun.Wrap(err, k3sdryrun.PhaseScan, clx.String("data-dir"))
	}
	crb := bootstrap.DefaultPaths(dataDir)

	var rep *k3sdryrun.Report
	switch bootstrapFile := clx.String("bootstrap-file"); bootstrapFile {
	case "":
		rep, err = bootstrap.ScanToDisk(context.Background(), dataDir, crb)
	case "-":
		rep, err = planFromReader(context.Background(), dataDir, crb, clx.App.Reader)
	default:
		f, openErr := os.Open(bootstrapFile)
		if openErr != nil {
			return k3sdryrun.Wrap(openErr, k3sdryrun.PhasePlan, bootstrapFile)
		}
		defer f.Close()
		rep, err = planFromReader(context.Background(), dataDir, crb, f)
	}

	// A failed run still carries a partial report worth rendering.
	if rep != nil {
		if renderErr := render(clx, rep, format, out); renderErr != nil {
			return renderErr
		}
	}
	return err
}

func planFromReader(ctx context.Context, root string, crb *config.ControlRuntimeBootstrap, r io.Reader) (*k3sdryrun.Report, error) {
	files, err := bootstrap.DecodeBootstrap(r)
	if err != nil {
		return nil, err
	}
	return bootstrap.DryRunToDisk(ctx, root, files, crb)
}

func render(clx *cli.Context, rep *k3sdryrun.Report, format string, out io.Writer) error {
	var data []byte
	switch format {
	case "json":
		b, err := rep.JSON()
		if err != nil {
			return err
		}
		data = b
	default:
		var buf bytes.Buffer
		if err := rep.RenderText(&buf); err != nil {
			return err
		}
		data = buf.Bytes()
	}
	data = append(data, '\n')

	if reportFile := clx.String("report-file"); reportFile != "" {
		return os.WriteFile(reportFile, data, 0o644)
	}
	_, err := out.Write(data)
	return err
}
