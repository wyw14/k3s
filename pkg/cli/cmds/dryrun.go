package cmds

import (
	"github.com/urfave/cli/v2"
)

// DryRunCommand is the name of the dry-run diagnostic subcommand.
const DryRunCommand = "dry-run"

// NewDryRunCommand returns the `k3s dry-run` diagnostic command. The action is
// injected so cmd/k3s can register an in-process implementation.
func NewDryRunCommand(action func(*cli.Context) error) *cli.Command {
	return &cli.Command{
		Name:        DryRunCommand,
		Usage:       "Show the changes a bootstrap reconciliation would make without writing to disk",
		Description: "Read-only diagnostic that scans the data directory bootstrap files and, when given proposed bootstrap data via --bootstrap-file, reports the phased diff (scan/plan/apply/verify) that a reconciliation would perform. No persistent state is modified.",
		Flags: []cli.Flag{
			DataDirFlag,
			&cli.StringFlag{
				Name:    "bootstrap-file",
				Aliases: []string{"f"},
				Usage:   "(diagnostic) Proposed bootstrap data JSON file; use '-' for stdin. When omitted, only the current on-disk layout is scanned",
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "(diagnostic) Report output format (valid values: text, json)",
				Value:   "text",
			},
			&cli.StringFlag{
				Name:  "report-file",
				Usage: "(diagnostic) Write the report to this file instead of stdout",
			},
		},
		Action: action,
	}
}
