package handlers

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/k3s-io/k3s/pkg/clientaccess"
	"github.com/k3s-io/k3s/pkg/daemons/config"
	"github.com/k3s-io/k3s/pkg/util"
	"github.com/k3s-io/k3s/pkg/util/errors"
	"github.com/k3s-io/k3s/pkg/version"
)

// Negotiation returns an http.Handler that performs backward-compatible
// capability negotiation with joining clients. The client declares its
// release version and the optional features it would like to use; the server
// answers with an explicit compatibility verdict and, when capabilities
// cannot be satisfied, per-feature downgrade reasons.
//
// The endpoint is stateless and therefore safe to call repeatedly or as a
// retry: an identical NegotiationRequest always yields an identical
// NegotiationResponse. Unknown JSON fields are intentionally ignored so that
// newer clients can never break older servers, and older clients that omit
// the new fields simply negotiate as legacy clients.
func Negotiation(control *config.Control) http.Handler {
	server := clientaccess.ServerInfo{
		Version:         version.Version,
		ProtocolVersion: clientaccess.NegotiationProtocolVersion,
		Features:        serverFeatures(control),
	}

	return http.HandlerFunc(func(resp http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			util.SendError(errors.New("method not allowed"), resp, req, http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			util.SendError(errors.WithMessage(err, "failed to read negotiation request"), resp, req, http.StatusBadRequest)
			return
		}

		// Decode with the standard unmarshaler, which discards unknown fields.
		// This is the key forward-compatibility guarantee: a request from a
		// newer client carrying fields this server does not know about still
		// negotiates against the fields it understands.
		request := clientaccess.NegotiationRequest{}
		if err := json.Unmarshal(body, &request); err != nil {
			util.SendError(errors.WithMessage(err, "failed to decode negotiation request"), resp, req, http.StatusBadRequest)
			return
		}

		result, err := clientaccess.Negotiate(server, &request)
		if err != nil {
			// A syntactically valid request with an invalid version (or other
			// invalid negotiation data) is a client error rather than an
			// incompatibility verdict.
			util.SendError(err, resp, req, http.StatusBadRequest)
			return
		}

		resp.Header().Set("Content-Type", "application/json")
		resp.WriteHeader(http.StatusOK)
		if err := json.NewEncoder(resp).Encode(result); err != nil {
			util.SendError(errors.WithMessage(err, "failed to encode negotiation response"), resp, req, http.StatusInternalServerError)
		}
	})
}

// serverFeatures returns the optional capabilities this server is able to
// provide. Protocol-level features are always available; network-policy and
// kube-proxy integration are only advertised when the corresponding
// components are enabled.
func serverFeatures(control *config.Control) []clientaccess.Feature {
	features := []clientaccess.Feature{
		{Name: clientaccess.FeatureCapabilityNegotiation},
		{Name: clientaccess.FeatureReadyz},
		{Name: clientaccess.FeatureAgentConfig},
		{Name: clientaccess.FeatureAPIServers},
		{Name: clientaccess.FeatureClientCSR},
	}

	if control != nil {
		if !control.DisableNPC {
			features = append(features, clientaccess.Feature{Name: clientaccess.FeatureNetworkPolicy})
		}
		if !control.DisableKubeProxy {
			features = append(features, clientaccess.Feature{Name: clientaccess.FeatureKubeProxy})
		}
	}

	return features
}
