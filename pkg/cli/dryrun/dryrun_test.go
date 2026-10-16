package dryrun

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/k3s-io/k3s/pkg/cli/cmds"
	k3sdryrun "github.com/k3s-io/k3s/pkg/dryrun"
	"github.com/urfave/cli/v2"
)

func newTestApp() *cli.App {
	app := cli.NewApp()
	app.Commands = []*cli.Command{cmds.NewDryRunCommand(Run)}
	return app
}

func runApp(t *testing.T, args []string, stdin io.Reader) (string, string, error) {
	t.Helper()
	app := newTestApp()
	var stdout, stderr bytes.Buffer
	app.Writer = &stdout
	app.ErrWriter = &stderr
	if stdin != nil {
		app.Reader = stdin
	}
	err := app.Run(append([]string{"k3s"}, args...))
	return stdout.String(), stderr.String(), err
}

func writeCert(t *testing.T, dataDir, name, content string) {
	t.Helper()
	path := filepath.Join(dataDir, "tls", name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestCLIScanJSON(t *testing.T) {
	dataDir := t.TempDir()
	writeCert(t, dataDir, "server-ca.crt", "ca-data")

	out, _, err := runApp(t, []string{"dry-run", "--data-dir", dataDir, "--output", "json"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	var rep k3sdryrun.Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, out)
	}
	if rep.SchemaVersion != k3sdryrun.SchemaVersion {
		t.Fatalf("schema = %s", rep.SchemaVersion)
	}
	if rep.Status != k3sdryrun.StatusScanned {
		t.Fatalf("status = %s", rep.Status)
	}
	if rep.Summary.Unchanged != 1 || rep.Summary.Missing < 10 {
		t.Fatalf("summary = %+v", rep.Summary)
	}
}

func TestCLIPlanFromFileText(t *testing.T) {
	dataDir := t.TempDir()
	writeCert(t, dataDir, "server-ca.crt", "stale")

	input := filepath.Join(t.TempDir(), "bootstrap.json")
	if err := os.WriteFile(input, []byte(`{"ServerCA":{"Timestamp":"2024-01-02T03:04:05Z","Content":"c2VydmVyLWNh"}}`), 0o600); err != nil {
		t.Fatal(err)
	}

	out, _, err := runApp(t, []string{"dry-run", "-d", dataDir, "-f", input}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"k3s dry-run report", "Mode:   dry-run", "M tls/server-ca.crt", "Phases:", "verify  ok"} {
		if !strings.Contains(out, want) {
			t.Fatalf("text output missing %q:\n%s", want, out)
		}
	}
	// The existing file must remain unchanged.
	b, err := os.ReadFile(filepath.Join(dataDir, "tls", "server-ca.crt"))
	if err != nil || string(b) != "stale" {
		t.Fatalf("target file changed: %q %v", b, err)
	}
}

func TestCLIPlanFromStdin(t *testing.T) {
	dataDir := t.TempDir()
	stdin := strings.NewReader(`{"PasswdFile":{"Timestamp":"2024-01-02T03:04:05Z","Content":"cGFzc3dk"}}`)
	out, _, err := runApp(t, []string{"dry-run", "-d", dataDir, "-f", "-", "-o", "json"}, stdin)
	if err != nil {
		t.Fatal(err)
	}
	var rep k3sdryrun.Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if rep.Status != k3sdryrun.StatusPlanned {
		t.Fatalf("status = %s", rep.Status)
	}
	if rep.Summary.Added != 1 {
		t.Fatalf("summary = %+v", rep.Summary)
	}
	if _, err := os.Stat(filepath.Join(dataDir, "cred", "passwd")); !os.IsNotExist(err) {
		t.Fatal("diagnostic must not create cred/passwd")
	}
}

func TestCLILegacyFormatAccepted(t *testing.T) {
	dataDir := t.TempDir()
	stdin := strings.NewReader(`{"ServerCA":"b2xk"}`)
	out, _, err := runApp(t, []string{"dry-run", "-d", dataDir, "-f", "-", "-o", "json"}, stdin)
	if err != nil {
		t.Fatal(err)
	}
	var rep k3sdryrun.Report
	if err := json.Unmarshal([]byte(out), &rep); err != nil {
		t.Fatalf("invalid JSON: %v\n%s", err, out)
	}
	if rep.Summary.Added != 1 {
		t.Fatalf("legacy format summary = %+v", rep.Summary)
	}
}

func TestCLICorruptBootstrap(t *testing.T) {
	dataDir := t.TempDir()
	stdin := strings.NewReader("{not json")
	_, _, err := runApp(t, []string{"dry-run", "-d", dataDir, "-f", "-"}, stdin)
	var de *k3sdryrun.Error
	if !errors.As(err, &de) || de.Code != k3sdryrun.CodeCorrupt {
		t.Fatalf("expected E_CORRUPT, got %v", err)
	}
}

func TestCLIInvalidOutput(t *testing.T) {
	dataDir := t.TempDir()
	_, _, err := runApp(t, []string{"dry-run", "-d", dataDir, "-o", "yaml"}, nil)
	var de *k3sdryrun.Error
	if !errors.As(err, &de) || de.Code != k3sdryrun.CodeValidation {
		t.Fatalf("expected E_VALIDATION, got %v", err)
	}
}

func TestCLIReportFile(t *testing.T) {
	dataDir := t.TempDir()
	reportPath := filepath.Join(t.TempDir(), "report.json")
	stdout, _, err := runApp(t, []string{
		"dry-run", "-d", dataDir, "-o", "json", "--report-file", reportPath,
	}, strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if stdout != "" {
		t.Fatalf("stdout must be empty with --report-file, got %q", stdout)
	}
	b, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var rep k3sdryrun.Report
	if err := json.Unmarshal(b, &rep); err != nil {
		t.Fatalf("report file is not valid JSON: %v", err)
	}
}

func TestCLIScanIsReadOnly(t *testing.T) {
	dataDir := t.TempDir()
	before, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := runApp(t, []string{"dry-run", "-d", dataDir, "-o", "json"}, nil); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatalf("scan-only changed the data directory: %d -> %d entries", len(before), len(after))
	}
}
