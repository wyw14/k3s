package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/k3s-io/k3s/pkg/clientaccess"
	"github.com/k3s-io/k3s/pkg/daemons/config"
	"github.com/k3s-io/k3s/pkg/version"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// doNegotiation invokes the negotiation handler directly, bypassing the
// authn/authz middleware stack.
func doNegotiation(t *testing.T, handler http.Handler, method, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "/v1-"+version.Program+"/negotiate", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// decodeNegotiation decodes a successful negotiation response body.
func decodeNegotiation(t *testing.T, rec *httptest.ResponseRecorder) clientaccess.NegotiationResponse {
	t.Helper()
	result := clientaccess.NegotiationResponse{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	return result
}

// Test_UnitNegotiationHandler covers the HTTP contract of the negotiation
// endpoint: successful negotiation, unknown-field tolerance, malformed and
// invalid error responses, method enforcement, config-gated features, and
// idempotency across duplicate requests. Mixed-version verdicts are covered
// at the protocol level in pkg/clientaccess/negotiate_test.go.
func Test_UnitNegotiationHandler(t *testing.T) {
	handler := Negotiation(&config.Control{})

	t.Run("valid request negotiates compatible verdict", func(t *testing.T) {
		rec := doNegotiation(t, handler, http.MethodPost, `{
			"version": "v1.31.2+k3s1",
			"clientID": "node-1",
			"requestID": "req-1",
			"features": [{"name": "readyz"}, {"name": "apiservers"}]
		}`)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Header().Get("Content-Type"), "application/json")

		result := decodeNegotiation(t, rec)
		assert.Equal(t, clientaccess.StatusCompatible, result.Status)
		assert.Equal(t, version.Version, result.ServerVersion)
		assert.Equal(t, clientaccess.NegotiationProtocolVersion, result.ProtocolVersion)
		assert.Equal(t, "node-1", result.ClientID)
		assert.Equal(t, "req-1", result.RequestID)
		assert.True(t, result.HasFeature(clientaccess.FeatureReadyz))
		assert.True(t, result.HasFeature(clientaccess.FeatureAPIServers))
		assert.Empty(t, result.Downgrades)
		assert.NoError(t, result.Err())
	})

	t.Run("unknown fields from newer client are ignored", func(t *testing.T) {
		rec := doNegotiation(t, handler, http.MethodPost, `{
			"version": "v1.31.2+k3s1",
			"futureTopLevel": {"nested": true},
			"features": [
				{"name": "readyz", "futureFlag": "yes"},
				{"name": "future-feature", "required": false}
			]
		}`)

		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		result := decodeNegotiation(t, rec)
		assert.Equal(t, clientaccess.StatusDegraded, result.Status)
		assert.True(t, result.HasFeature(clientaccess.FeatureReadyz))
		assert.False(t, result.HasFeature("future-feature"))
		require.Len(t, result.Downgrades, 1)
		assert.Equal(t, "future-feature", result.Downgrades[0].Feature)
		assert.NotEmpty(t, result.Downgrades[0].Reason)
	})

	t.Run("required unsupported feature returns incompatible verdict", func(t *testing.T) {
		rec := doNegotiation(t, handler, http.MethodPost, `{
			"version": "v1.31.2+k3s1",
			"features": [{"name": "future-required", "required": true}]
		}`)

		assert.Equal(t, http.StatusOK, rec.Code)
		result := decodeNegotiation(t, rec)
		assert.Equal(t, clientaccess.StatusIncompatible, result.Status)
		assert.Error(t, result.Err())
	})

	t.Run("legacy client omitting version degrades rather than failing", func(t *testing.T) {
		rec := doNegotiation(t, handler, http.MethodPost, `{}`)
		assert.Equal(t, http.StatusOK, rec.Code)
		result := decodeNegotiation(t, rec)
		assert.Equal(t, clientaccess.StatusDegraded, result.Status)
		assert.NotEmpty(t, result.Downgrades)
	})

	t.Run("mixed version older client negotiates against dev server", func(t *testing.T) {
		// A development server reports an unparseable version and skips skew
		// checks; older or newer clients still negotiate their features.
		rec := doNegotiation(t, handler, http.MethodPost, `{
			"version": "v1.28.0+k3s1",
			"features": [{"name": "readyz"}]
		}`)
		assert.Equal(t, http.StatusOK, rec.Code)
		result := decodeNegotiation(t, rec)
		assert.Equal(t, clientaccess.StatusCompatible, result.Status)
		assert.True(t, result.HasFeature(clientaccess.FeatureReadyz))
	})

	t.Run("malformed JSON is a bad request", func(t *testing.T) {
		rec := doNegotiation(t, handler, http.MethodPost, `{not json`)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid version is a bad request", func(t *testing.T) {
		rec := doNegotiation(t, handler, http.MethodPost, `{"version": "not-a-version"}`)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("unsupported method is rejected", func(t *testing.T) {
		rec := doNegotiation(t, handler, http.MethodGet, ``)
		assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
	})

	t.Run("duplicate requests return identical responses", func(t *testing.T) {
		body := `{
			"version": "v1.31.2+k3s1",
			"clientID": "node-dup",
			"requestID": "req-dup",
			"features": [{"name": "readyz"}, {"name": "future-thing"}]
		}`

		first := doNegotiation(t, handler, http.MethodPost, body)
		second := doNegotiation(t, handler, http.MethodPost, body)

		assert.Equal(t, http.StatusOK, first.Code)
		assert.Equal(t, http.StatusOK, second.Code)
		assert.Equal(t, first.Body.String(), second.Body.String())

		result := decodeNegotiation(t, first)
		assert.Equal(t, clientaccess.StatusDegraded, result.Status)
		assert.Equal(t, "req-dup", result.RequestID)
	})

	t.Run("config-gated features reflect server configuration", func(t *testing.T) {
		body := `{"version":"v1.31.2+k3s1","features":[{"name":"kube-proxy"},{"name":"network-policy"}]}`

		enabled := Negotiation(&config.Control{})
		rec := doNegotiation(t, enabled, http.MethodPost, body)
		assert.Equal(t, http.StatusOK, rec.Code)
		result := decodeNegotiation(t, rec)
		assert.True(t, result.HasFeature(clientaccess.FeatureKubeProxy))
		assert.True(t, result.HasFeature(clientaccess.FeatureNetworkPolicy))

		disabled := Negotiation(&config.Control{
			DisableKubeProxy: true,
			CriticalControlArgs: config.CriticalControlArgs{
				DisableNPC: true,
			},
		})
		rec = doNegotiation(t, disabled, http.MethodPost, body)
		assert.Equal(t, http.StatusOK, rec.Code)
		result = decodeNegotiation(t, rec)
		assert.False(t, result.HasFeature(clientaccess.FeatureKubeProxy))
		assert.False(t, result.HasFeature(clientaccess.FeatureNetworkPolicy))
		assert.Equal(t, clientaccess.StatusDegraded, result.Status)
		assert.Len(t, result.Downgrades, 2)
	})
}
