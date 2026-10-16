package clientaccess

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/blang/semver/v4"
	"github.com/k3s-io/k3s/pkg/util/errors"
	"github.com/k3s-io/k3s/pkg/version"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	// NegotiationProtocolVersion is the version of the capability negotiation
	// wire protocol spoken by this build. It is independent of the k3s release
	// version and is only bumped when the negotiation contract itself changes.
	NegotiationProtocolVersion = 1

	// maxSupportedVersionSkew is the maximum number of minor releases by which
	// a client may be newer than the server and still expect to interoperate,
	// in a degraded fashion. Servers are expected to tolerate older clients.
	maxSupportedVersionSkew = 1
)

// Well-known optional features that may be advertised by servers and
// requested by clients during capability negotiation.
const (
	FeatureCapabilityNegotiation = "capability-negotiation"
	FeatureReadyz                = "readyz"
	FeatureAgentConfig           = "agent-config"
	FeatureAPIServers            = "apiservers"
	FeatureClientCSR             = "client-csr"
	FeatureNetworkPolicy         = "network-policy"
	FeatureKubeProxy             = "kube-proxy"
)

// negotiationPath is the server endpoint used by Info.Negotiate.
var negotiationPath = "/v1-" + version.Program + "/negotiate"

// ErrNegotiationUnsupported is returned (wrapped) when a down-level server
// does not implement the capability negotiation endpoint. The accompanying
// response is marked degraded so that callers can safely fall back to legacy
// behavior.
var ErrNegotiationUnsupported = errors.New("capability negotiation is not supported by the server")

// NegotiationStatus is the explicit compatibility verdict returned by the server.
type NegotiationStatus string

const (
	// StatusCompatible indicates that the client version and all requested
	// features are fully supported.
	StatusCompatible NegotiationStatus = "compatible"
	// StatusDegraded indicates interoperability is possible, but one or more
	// optional capabilities could not be satisfied and have been disabled.
	StatusDegraded NegotiationStatus = "degraded"
	// StatusIncompatible indicates the client cannot operate against the
	// server; either the version skew is unsupported or a required feature
	// is missing.
	StatusIncompatible NegotiationStatus = "incompatible"
)

// Feature describes a single negotiated capability. When sent by a client,
// Required marks the feature as mandatory: negotiation fails if the server
// cannot provide it. When sent by a server, the field is ignored; only the
// set of advertised names is meaningful.
type Feature struct {
	Name     string `json:"name"`
	Required bool   `json:"required,omitempty"`
}

// NegotiationRequest is sent by a client to declare its release version and
// the set of optional features it would like to use. All fields are optional
// so that the schema can grow without breaking old callers; unknown fields
// in either direction are ignored by the JSON decoder.
type NegotiationRequest struct {
	// Version is the client's semantic version, e.g. "v1.31.2+k3s1".
	Version string `json:"version,omitempty"`
	// ProtocolVersion is the negotiation wire-protocol version understood by
	// the client. If omitted, the current protocol is assumed.
	ProtocolVersion int `json:"protocolVersion,omitempty"`
	// ClientID is an optional, non-secret identifier (such as a node name)
	// used for logging and correlation. It is echoed back unchanged.
	ClientID string `json:"clientID,omitempty"`
	// RequestID is an optional caller-generated idempotency token. The server
	// is stateless and simply echoes it, so callers may safely retry.
	RequestID string `json:"requestID,omitempty"`
	// Features lists the capabilities the client wants enabled, each marked
	// as required or optional.
	Features []Feature `json:"features,omitempty"`
}

// DowngradeReason explains why a single capability or the negotiated
// protocol could not be provided at the level requested by the client.
type DowngradeReason struct {
	// Feature names the affected capability, or is empty when the reason
	// applies to the negotiation protocol or client version as a whole.
	Feature string `json:"feature,omitempty"`
	// Reason is a human-readable explanation of the downgrade.
	Reason string `json:"reason"`
}

// NegotiationResponse is the server's explicit compatibility verdict. It is
// returned for every well-formed request, even when the verdict is degraded
// or incompatible; transport-level failures use HTTP status codes instead.
type NegotiationResponse struct {
	// Status is the overall negotiation verdict.
	Status NegotiationStatus `json:"status"`
	// Reason is a short summary when the verdict is not compatible; the
	// Downgrades slice carries the per-capability details.
	Reason string `json:"reason,omitempty"`
	// ServerVersion is the release version of the answering server.
	ServerVersion string `json:"serverVersion,omitempty"`
	// ProtocolVersion is the wire-protocol version actually negotiated.
	ProtocolVersion int `json:"protocolVersion"`
	// ClientID and RequestID are echoed from the request for correlation.
	ClientID  string `json:"clientID,omitempty"`
	RequestID string `json:"requestID,omitempty"`
	// Features is the set of requested capabilities the server supports.
	Features []Feature `json:"features,omitempty"`
	// Downgrades lists the capabilities that could not be satisfied, along
	// with the reason each was disabled or rejected.
	Downgrades []DowngradeReason `json:"downgrades,omitempty"`
}

// ServerInfo describes the capabilities offered by a server. The server HTTP
// handler builds one of these from its version and current configuration;
// tests construct them directly.
type ServerInfo struct {
	// Version is the server's semantic version. If it cannot be parsed,
	// version-skew checks are skipped rather than failing negotiation.
	Version string
	// ProtocolVersion is the negotiation wire-protocol version spoken by the
	// server. A zero value defaults to NegotiationProtocolVersion.
	ProtocolVersion int
	// Features is the set of capabilities the server is able to provide.
	Features []Feature
}

// Negotiate evaluates a request against a server's advertised capabilities
// and returns the explicit compatibility verdict. The function is pure and
// stateless: identical inputs always produce identical outputs, so it is
// safe to call repeatedly, including from retries.
func Negotiate(server ServerInfo, request *NegotiationRequest) (*NegotiationResponse, error) {
	if request == nil {
		return nil, errors.New("negotiation request must not be nil")
	}

	if server.ProtocolVersion <= 0 {
		server.ProtocolVersion = NegotiationProtocolVersion
	}

	response := &NegotiationResponse{
		Status:          StatusCompatible,
		ServerVersion:   server.Version,
		ProtocolVersion: server.ProtocolVersion,
		ClientID:        request.ClientID,
		RequestID:       request.RequestID,
		Features:        []Feature{},
		Downgrades:      []DowngradeReason{},
	}

	negotiateProtocol(request, server, response)
	if err := negotiateVersion(request, server, response); err != nil {
		return nil, err
	}
	negotiateFeatures(request, server, response)

	if response.Status != StatusCompatible && response.Reason == "" {
		response.Reason = fmt.Sprintf("client negotiated as %s; %d downgrade reason(s) reported",
			response.Status, len(response.Downgrades))
	}

	return response, nil
}

// negotiateProtocol settles the wire-protocol version. A client speaking a
// newer protocol than the server forces a degradation; an older client is
// always accommodated by negotiating down to its protocol version.
func negotiateProtocol(request *NegotiationRequest, server ServerInfo, response *NegotiationResponse) {
	switch {
	case request.ProtocolVersion > server.ProtocolVersion:
		response.downgrade(StatusDegraded, DowngradeReason{
			Feature: FeatureCapabilityNegotiation,
			Reason: fmt.Sprintf("client negotiation protocol version %d is newer than server version %d; falling back to protocol version %d",
				request.ProtocolVersion, server.ProtocolVersion, server.ProtocolVersion),
		})
	case request.ProtocolVersion > 0 && request.ProtocolVersion < server.ProtocolVersion:
		// Honor the older protocol; this is the normal rolling-upgrade path.
		response.ProtocolVersion = request.ProtocolVersion
	}
}

// negotiateVersion evaluates release-version skew. Newer clients beyond the
// supported skew (or on a different major version) are incompatible; a
// slightly newer client degrades. Servers tolerate older clients.
func negotiateVersion(request *NegotiationRequest, server ServerInfo, response *NegotiationResponse) error {
	clientVersion := strings.TrimSpace(request.Version)
	if clientVersion == "" {
		// A client that does not declare a version is treated as a legacy
		// client rather than rejected.
		response.downgrade(StatusDegraded, DowngradeReason{
			Reason: "client did not advertise a version; legacy behavior assumed",
		})
		return nil
	}

	clientSemver, err := semver.ParseTolerant(clientVersion)
	if err != nil {
		return fmt.Errorf("invalid client version %q: %w", clientVersion, err)
	}

	serverSemver, err := semver.ParseTolerant(strings.TrimSpace(server.Version))
	if err != nil {
		// The server version cannot be compared (for example a development
		// build); do not punish the client for it.
		return nil
	}

	switch {
	case clientSemver.Major != serverSemver.Major:
		response.downgrade(StatusIncompatible, DowngradeReason{
			Reason: fmt.Sprintf("client major version %d is incompatible with server major version %d (%s vs %s)",
				clientSemver.Major, serverSemver.Major, clientSemver, serverSemver),
		})
	case int(clientSemver.Minor)-int(serverSemver.Minor) > maxSupportedVersionSkew:
		response.downgrade(StatusIncompatible, DowngradeReason{
			Reason: fmt.Sprintf("client version %s is more than %d minor release(s) newer than server version %s; upgrade the server",
				clientSemver, maxSupportedVersionSkew, serverSemver),
		})
	case clientSemver.Minor > serverSemver.Minor:
		response.downgrade(StatusDegraded, DowngradeReason{
			Reason: fmt.Sprintf("client version %s is newer than server version %s; some client features may be unavailable",
				clientSemver, serverSemver),
		})
	}

	return nil
}

// negotiateFeatures intersects the requested features with the server's
// advertised set. Missing optional features degrade; missing required
// features are incompatible. Duplicate requests for the same feature are
// collapsed so they never produce duplicate verdicts.
func negotiateFeatures(request *NegotiationRequest, server ServerInfo, response *NegotiationResponse) {
	supported := make(map[string]bool, len(server.Features))
	for _, feature := range server.Features {
		name := strings.TrimSpace(feature.Name)
		if name != "" {
			supported[name] = true
		}
	}

	seen := map[string]bool{}
	for _, feature := range request.Features {
		name := strings.TrimSpace(feature.Name)
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true

		if supported[name] {
			response.Features = append(response.Features, Feature{Name: name})
			continue
		}

		status := StatusDegraded
		reason := fmt.Sprintf("feature %q is not supported by the server and has been disabled", name)
		if feature.Required {
			status = StatusIncompatible
			reason = fmt.Sprintf("required feature %q is not supported by the server", name)
		}
		response.downgrade(status, DowngradeReason{Feature: name, Reason: reason})
	}
}

// downgrade records a reason and escalates the overall verdict. The
// incompatible verdict is sticky; degraded never downgrades it back.
func (r *NegotiationResponse) downgrade(status NegotiationStatus, reason DowngradeReason) {
	if status == StatusIncompatible || (status == StatusDegraded && r.Status == StatusCompatible) {
		r.Status = status
	}
	r.Downgrades = append(r.Downgrades, reason)
}

// HasFeature reports whether a capability was successfully negotiated.
func (r *NegotiationResponse) HasFeature(name string) bool {
	if r == nil {
		return false
	}
	for _, feature := range r.Features {
		if feature.Name == name {
			return true
		}
	}
	return false
}

// Err returns an error when the verdict is incompatible, and nil otherwise.
// It allows callers to write: if err := result.Err(); err != nil { ... }
func (r *NegotiationResponse) Err() error {
	if r == nil || r.Status != StatusIncompatible {
		return nil
	}
	if r.Reason != "" {
		return errors.New(r.Reason)
	}
	return errors.New("capability negotiation failed: client is incompatible with server")
}

// Negotiate performs capability negotiation against the server, posting the
// client's declared version and optional features and decoding the explicit
// compatibility verdict.
//
// Negotiation is stateless and idempotent; it is safe to retry the same
// request. When the remote server is down-level and does not implement the
// negotiation endpoint, the returned response is marked degraded and the
// error wraps ErrNegotiationUnsupported; callers may ignore that specific
// error and fall back to legacy behavior.
func (i *Info) Negotiate(request *NegotiationRequest) (*NegotiationResponse, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	data, err := i.Post(negotiationPath, body,
		WithHeader("Accept", "application/json"),
		WithHeader("Content-Type", "application/json"))
	if err != nil {
		if apierrors.IsNotFound(err) || strings.Contains(err.Error(), fmt.Sprintf("%d %s", http.StatusNotFound, http.StatusText(http.StatusNotFound))) {
			return unsupportedNegotiationResponse(request), errors.WithMessagef(ErrNegotiationUnsupported, "%s", negotiationPath)
		}
		return nil, err
	}

	response := &NegotiationResponse{}
	if err := json.Unmarshal(data, response); err != nil {
		return nil, errors.WithMessage(err, "failed to decode negotiation response")
	}
	return response, nil
}

// unsupportedNegotiationResponse builds the degraded verdict used when a
// down-level server has no negotiation endpoint.
func unsupportedNegotiationResponse(request *NegotiationRequest) *NegotiationResponse {
	return &NegotiationResponse{
		Status:    StatusDegraded,
		Reason:    "server does not implement capability negotiation; legacy behavior assumed",
		ClientID:  request.ClientID,
		RequestID: request.RequestID,
		Downgrades: []DowngradeReason{{
			Feature: FeatureCapabilityNegotiation,
			Reason:  "server does not implement the negotiation endpoint",
		}},
	}
}
