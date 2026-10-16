package bootstrap

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/k3s-io/k3s/pkg/daemons/config"
	"github.com/k3s-io/k3s/pkg/dryrun"
)

func TestDefaultPaths(t *testing.T) {
	dataDir := filepath.Clean(t.TempDir())
	crb := DefaultPaths(dataDir)

	wants := map[string]string{
		"ServerCA":           filepath.Join("tls", "server-ca.crt"),
		"ServerCAKey":        filepath.Join("tls", "server-ca.key"),
		"ClientCA":           filepath.Join("tls", "client-ca.crt"),
		"ClientCAKey":        filepath.Join("tls", "client-ca.key"),
		"ETCDServerCA":       filepath.Join("tls", "etcd", "server-ca.crt"),
		"ETCDServerCAKey":    filepath.Join("tls", "etcd", "server-ca.key"),
		"ETCDPeerCA":         filepath.Join("tls", "etcd", "peer-ca.crt"),
		"ETCDPeerCAKey":      filepath.Join("tls", "etcd", "peer-ca.key"),
		"ServiceKey":         filepath.Join("tls", "service.key"),
		"PasswdFile":         filepath.Join("cred", "passwd"),
		"RequestHeaderCA":    filepath.Join("tls", "request-header-ca.crt"),
		"RequestHeaderCAKey": filepath.Join("tls", "request-header-ca.key"),
		"IPSECKey":           filepath.Join("cred", "ipsec.psk"),
	}
	got, err := ObjToMap(crb)
	if err != nil {
		t.Fatal(err)
	}
	for field, rel := range wants {
		want := filepath.Join(dataDir, rel)
		if got[field] != want {
			t.Errorf("%s = %q, want %q", field, got[field], want)
		}
	}
	if crb.EncryptionConfig != "" || crb.EncryptionHash != "" {
		t.Fatal("encryption-dependent paths must default to empty")
	}
}

func TestFileSpecsFilteringAndOrder(t *testing.T) {
	dataDir := t.TempDir()
	crb := DefaultPaths(dataDir)

	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	files := PathsDataformat{
		"ServerCA":           {Content: []byte("new-ca"), Timestamp: when},
		"UnknownPathKey":     {Content: []byte("ignored")},
		"EncryptionConfig":   {Content: []byte("skipped-empty-path")},
		"ETCDPeerCAKey":      {Content: []byte("peer-key"), Timestamp: when},
	}

	specs, err := FileSpecs(files, crb)
	if err != nil {
		t.Fatal(err)
	}
	if len(specs) != 2 {
		t.Fatalf("specs = %d entries, want 2 (unknown/empty skipped): %+v", len(specs), specs)
	}
	// Deterministic ordering: ETCDPeerCAKey < ServerCA.
	if specs[0].Path != crb.ETCDPeerCAKey || specs[1].Path != crb.ServerCA {
		t.Fatalf("unexpected order: %q then %q", specs[0].Path, specs[1].Path)
	}
	if !bytes.Equal(specs[1].Content, []byte("new-ca")) || !specs[1].ModTime.Equal(when) {
		t.Fatalf("content/timestamp not mapped: %+v", specs[1])
	}
	if specs[0].Mode != 0 {
		t.Fatal("explicit mode must stay zero so dryrun applies the 0600 default")
	}
}

// TestDryRunSameSourceAsWriter proves the diagnostic plan and the real writer
// converge on the same on-disk end state, and that the dry-run itself never
// touches disk.
func TestDryRunSameSourceAsWriter(t *testing.T) {
	dataDir := t.TempDir()
	crb := DefaultPaths(dataDir)
	when := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)

	files := PathsDataformat{
		"ServerCA":      {Content: []byte("server-ca-data"), Timestamp: when},
		"PasswdFile":    {Content: []byte("server-passwd\n"), Timestamp: when},
		"ETCDServerCA":  {Content: []byte("etcd-ca"), Timestamp: when},
	}

	// An existing file that will be updated; directories do not exist yet.
	if err := os.MkdirAll(filepath.Dir(crb.ServerCA), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crb.ServerCA, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}

	rep, err := DryRunToDisk(context.Background(), dataDir, files, crb)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Status != dryrun.StatusPlanned {
		t.Fatalf("dry-run status = %s", rep.Status)
	}
	// 2 creates + 1 update + 2 mkdirs (cred, tls/etcd)
	if rep.Summary.Added != 2 || rep.Summary.Modified != 1 || rep.Summary.Dirs != 2 {
		t.Fatalf("unexpected summary: %+v", rep.Summary)
	}
	// Nothing must have been written.
	if _, err := os.Stat(crb.PasswdFile); !os.IsNotExist(err) {
		t.Fatal("dry-run created PasswdFile")
	}
	if b, err := os.ReadFile(crb.ServerCA); err != nil || string(b) != "stale" {
		t.Fatalf("dry-run modified ServerCA: %q %v", b, err)
	}

	// Real reconciliation.
	if err := WriteToDiskFromStorage(files, crb); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string][]byte{
		"ServerCA":     files["ServerCA"].Content,
		"PasswdFile":   files["PasswdFile"].Content,
		"ETCDServerCA": files["ETCDServerCA"].Content,
	} {
		path, _ := ObjToMap(crb)
		b, err := os.ReadFile(path[key])
		if err != nil {
			t.Fatalf("%s missing after write: %v", key, err)
		}
		if !bytes.Equal(b, want) {
			t.Fatalf("%s = %q, want %q", key, b, want)
		}
		if runtime.GOOS != "windows" {
			info, _ := os.Stat(path[key])
			if info.Mode().Perm() != 0o600 {
				t.Fatalf("%s mode = %o", key, info.Mode().Perm())
			}
		}
	}

	// Second dry-run against the reconciled disk must be a no-op.
	rep2, err := DryRunToDisk(context.Background(), dataDir, files, crb)
	if err != nil {
		t.Fatal(err)
	}
	if rep2.Status != dryrun.StatusNoChanges {
		t.Fatalf("status after reconciliation = %s, want no-changes", rep2.Status)
	}
	if rep2.Summary.Added+rep2.Summary.Modified+rep2.Summary.Metadata+rep2.Summary.Dirs != 0 {
		t.Fatalf("reconciled state still shows changes: %+v", rep2.Summary)
	}
}

func TestScanToDisk(t *testing.T) {
	dataDir := t.TempDir()
	crb := DefaultPaths(dataDir)
	if err := os.MkdirAll(filepath.Dir(crb.ServerCA), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(crb.ServerCA, []byte("ca"), 0o600); err != nil {
		t.Fatal(err)
	}
	rep, err := ScanToDisk(context.Background(), dataDir, crb)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Summary.Unchanged < 1 || rep.Summary.Missing < 10 {
		t.Fatalf("scan summary = %+v", rep.Summary)
	}
	if rep.Status != dryrun.StatusScanned {
		t.Fatalf("status = %s", rep.Status)
	}
}

func TestDecodeBootstrap(t *testing.T) {
	when := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)

	// Current format.
	current := `{"ServerCA":{"Timestamp":"2024-01-02T03:04:05Z","Content":"bmV3LWNh"}}`
	files, err := DecodeBootstrap(strings.NewReader(current))
	if err != nil {
		t.Fatal(err)
	}
	if string(files["ServerCA"].Content) != "new-ca" || !files["ServerCA"].Timestamp.Equal(when) {
		t.Fatalf("current format decode wrong: %+v", files["ServerCA"])
	}

	// Legacy format.
	legacy := `{"ServerCA":"b2xk"}`
	files, err = DecodeBootstrap(strings.NewReader(legacy))
	if err != nil {
		t.Fatal(err)
	}
	if string(files["ServerCA"].Content) != "old" || !files["ServerCA"].Timestamp.IsZero() {
		t.Fatalf("legacy decode wrong: %+v", files["ServerCA"])
	}

	// Corrupt.
	_, err = DecodeBootstrap(strings.NewReader("{not json"))
	var de *dryrun.Error
	if !errors.As(err, &de) || de.Code != dryrun.CodeCorrupt {
		t.Fatalf("expected E_CORRUPT, got %v", err)
	}
}

func TestFileSpecsEmptyBootstrapStructNoop(t *testing.T) {
	if specs, err := FileSpecs(PathsDataformat{"x": {}}, &config.ControlRuntimeBootstrap{}); err != nil || len(specs) != 0 {
		t.Fatalf("empty bootstrap struct must yield no specs: %+v err=%v", specs, err)
	}
}
