package bootstrap

import (
	"path/filepath"

	"github.com/k3s-io/k3s/pkg/daemons/config"
)

// DefaultPaths returns the on-disk bootstrap file layout derived from a data
// directory, matching the paths populated by
// pkg/daemons/control/deps.CreateRuntimeCertFiles for the
// ControlRuntimeBootstrap fields. Unlike the deps package it depends only on
// filepath operations and is safe to use from cross-platform tooling (such as
// the dry-run diagnostic CLI).
//
// Paths that are only populated when secrets encryption is enabled
// (EncryptionConfig, EncryptionHash) are left empty.
func DefaultPaths(dataDir string) *config.ControlRuntimeBootstrap {
	tlsDir := filepath.Join(dataDir, "tls")
	etcdDir := filepath.Join(tlsDir, "etcd")
	credDir := filepath.Join(dataDir, "cred")

	return &config.ControlRuntimeBootstrap{
		ETCDServerCA:       filepath.Join(etcdDir, "server-ca.crt"),
		ETCDServerCAKey:    filepath.Join(etcdDir, "server-ca.key"),
		ETCDPeerCA:         filepath.Join(etcdDir, "peer-ca.crt"),
		ETCDPeerCAKey:      filepath.Join(etcdDir, "peer-ca.key"),
		ServerCA:           filepath.Join(tlsDir, "server-ca.crt"),
		ServerCAKey:        filepath.Join(tlsDir, "server-ca.key"),
		ClientCA:           filepath.Join(tlsDir, "client-ca.crt"),
		ClientCAKey:        filepath.Join(tlsDir, "client-ca.key"),
		ServiceKey:         filepath.Join(tlsDir, "service.key"),
		PasswdFile:         filepath.Join(credDir, "passwd"),
		RequestHeaderCA:    filepath.Join(tlsDir, "request-header-ca.crt"),
		RequestHeaderCAKey: filepath.Join(tlsDir, "request-header-ca.key"),
		IPSECKey:           filepath.Join(credDir, "ipsec.psk"),
	}
}
