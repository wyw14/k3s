package clientaccess

import (
	"encoding/json"
	stderrors "errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var negotiateTestServer = ServerInfo{
	Version:         "v1.31.2+k3s1",
	ProtocolVersion: NegotiationProtocolVersion,
	Features: []Feature{
		{Name: FeatureReadyz},
		{Name: FeatureAPIServers},
		{Name: FeatureClientCSR},
		{Name: FeatureNetworkPolicy},
	},
}

// downgradedFeatures returns the set of feature names that carry a downgrade reason.
func downgradedFeatures(response *NegotiationResponse) map[string]bool {
	names := map[string]bool{}
	for _, downgrade := range response.Downgrades {
		if downgrade.Feature != "" {
			names[downgrade.Feature] = true
		}
	}
	return names
}

// Test_UnitNegotiate exercises the pure, stateless negotiation logic across
// mixed versions, optional and required features, and protocol skew.
func Test_UnitNegotiate(t *testing.T) {
	tests := []struct {
		name              string
		server            ServerInfo
		request           *NegotiationRequest
		wantErr           bool
		wantStatus        NegotiationStatus
		wantFeatures      []string
		wantDowngraded    map[string]bool
		wantAnyDowngrades bool
		wantProtocol      int
		wantIncompatible  bool
	}{
		{
			name:         "equal version, no features",
			server:       negotiateTestServer,
			request:      &NegotiationRequest{Version: "v1.31.2+k3s1"},
			wantStatus:   StatusCompatible,
			wantProtocol: NegotiationProtocolVersion,
		},
		{
			name:         "slightly older client tolerated during rolling upgrade",
			server:       negotiateTestServer,
			request:      &NegotiationRequest{Version: "v1.30.7+k3s1"},
			wantStatus:   StatusCompatible,
			wantProtocol: NegotiationProtocolVersion,
		},
		{
			name:         "much older client tolerated, servers are forward compatible",
			server:       negotiateTestServer,
			request:      &NegotiationRequest{Version: "v1.28.0+k3s1"},
			wantStatus:   StatusCompatible,
			wantProtocol: NegotiationProtocolVersion,
		},
		{
			name:              "newer client within skew degrades",
			server:            negotiateTestServer,
			request:           &NegotiationRequest{Version: "v1.32.0+k3s1"},
			wantStatus:        StatusDegraded,
			wantAnyDowngrades: true,
			wantProtocol:      NegotiationProtocolVersion,
		},
		{
			name:             "newer client beyond skew incompatible",
			server:           negotiateTestServer,
			request:          &NegotiationRequest{Version: "v1.33.5+k3s1"},
			wantStatus:       StatusIncompatible,
			wantIncompatible: true,
			wantProtocol:     NegotiationProtocolVersion,
		},
		{
			name:             "different major version incompatible",
			server:           negotiateTestServer,
			request:          &NegotiationRequest{Version: "v2.0.0"},
			wantStatus:       StatusIncompatible,
			wantIncompatible: true,
			wantProtocol:     NegotiationProtocolVersion,
		},
		{
			name:              "client omitting version degrades as legacy",
			server:            negotiateTestServer,
			request:           &NegotiationRequest{},
			wantStatus:        StatusDegraded,
			wantAnyDowngrades: true,
			wantProtocol:      NegotiationProtocolVersion,
		},
		{
			name:    "client advertising invalid version is an error",
			server:  negotiateTestServer,
			request: &NegotiationRequest{Version: "banana"},
			wantErr: true,
		},
		{
			// The real server binary reports "dev" outside of a release;
			// negotiation must not punish clients when skew cannot be checked.
			name:         "unparseable server version skips skew checks",
			server:       ServerInfo{Version: "dev", ProtocolVersion: 1, Features: negotiateTestServer.Features},
			request:      &NegotiationRequest{Version: "v1.99.0+k3s1"},
			wantStatus:   StatusCompatible,
			wantProtocol: NegotiationProtocolVersion,
		},
		{
			name:         "optional supported feature accepted",
			server:       negotiateTestServer,
			request:      &NegotiationRequest{Version: "v1.31.2+k3s1", Features: []Feature{{Name: FeatureReadyz}}},
			wantStatus:   StatusCompatible,
			wantFeatures: []string{FeatureReadyz},
			wantProtocol: NegotiationProtocolVersion,
		},
		{
			name:              "optional unsupported feature degrades with reason",
			server:            negotiateTestServer,
			request:           &NegotiationRequest{Version: "v1.31.2+k3s1", Features: []Feature{{Name: "future-thing"}}},
			wantStatus:        StatusDegraded,
			wantDowngraded:    map[string]bool{"future-thing": true},
			wantAnyDowngrades: true,
			wantProtocol:      NegotiationProtocolVersion,
		},
		{
			name:         "required supported feature accepted",
			server:       negotiateTestServer,
			request:      &NegotiationRequest{Version: "v1.31.2+k3s1", Features: []Feature{{Name: FeatureReadyz, Required: true}}},
			wantStatus:   StatusCompatible,
			wantFeatures: []string{FeatureReadyz},
			wantProtocol: NegotiationProtocolVersion,
		},
		{
			name:             "required unsupported feature incompatible",
			server:           negotiateTestServer,
			request:          &NegotiationRequest{Version: "v1.31.2+k3s1", Features: []Feature{{Name: "future-thing", Required: true}}},
			wantStatus:       StatusIncompatible,
			wantIncompatible: true,
			wantDowngraded:   map[string]bool{"future-thing": true},
			wantProtocol:     NegotiationProtocolVersion,
		},
		{
			name:              "mixed: older client plus unsupported optional feature",
			server:            negotiateTestServer,
			request:           &NegotiationRequest{Version: "v1.30.0+k3s1", Features: []Feature{{Name: "future-thing"}}},
			wantStatus:        StatusDegraded,
			wantDowngraded:    map[string]bool{"future-thing": true},
			wantAnyDowngrades: true,
			wantProtocol:      NegotiationProtocolVersion,
		},
		{
			name:              "newer negotiation protocol degrades",
			server:            negotiateTestServer,
			request:           &NegotiationRequest{Version: "v1.31.2+k3s1", ProtocolVersion: 99},
			wantStatus:        StatusDegraded,
			wantDowngraded:    map[string]bool{FeatureCapabilityNegotiation: true},
			wantAnyDowngrades: true,
			wantProtocol:      NegotiationProtocolVersion,
		},
		{
			name:         "older negotiation protocol honored",
			server:       ServerInfo{Version: "v1.31.2+k3s1", ProtocolVersion: 2, Features: negotiateTestServer.Features},
			request:      &NegotiationRequest{Version: "v1.31.2+k3s1", ProtocolVersion: 1},
			wantStatus:   StatusCompatible,
			wantProtocol: 1,
		},
		{
			name:              "duplicate feature entries collapsed",
			server:            negotiateTestServer,
			request:           &NegotiationRequest{Version: "v1.31.2+k3s1", Features: []Feature{{Name: FeatureReadyz}, {Name: FeatureReadyz}, {Name: "future-thing"}, {Name: "future-thing"}}},
			wantStatus:        StatusDegraded,
			wantFeatures:      []string{FeatureReadyz},
			wantDowngraded:    map[string]bool{"future-thing": true},
			wantAnyDowngrades: true,
			wantProtocol:      NegotiationProtocolVersion,
		},
		{
			name:    "nil request rejected",
			server:  negotiateTestServer,
			request: nil,
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := Negotiate(test.server, test.request)
			if test.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.NotNil(t, result)

			assert.Equal(t, test.wantStatus, result.Status, "unexpected status: %s", result.Reason)
			assert.Equal(t, test.server.Version, result.ServerVersion)
			assert.Equal(t, test.wantProtocol, result.ProtocolVersion)
			assert.Equal(t, test.wantIncompatible, result.Err() != nil)

			accepted := map[string]bool{}
			for _, feature := range result.Features {
				accepted[feature.Name] = true
			}
			for _, name := range test.wantFeatures {
				assert.True(t, result.HasFeature(name), "expected negotiated feature %q", name)
				assert.True(t, accepted[name])
			}

			if test.wantAnyDowngrades {
				assert.NotEmpty(t, result.Downgrades)
				assert.NotEmpty(t, result.Reason)
			}
			actualDowngraded := downgradedFeatures(result)
			if len(test.wantDowngraded) == 0 {
				assert.Empty(t, actualDowngraded)
			} else {
				assert.Equal(t, test.wantDowngraded, actualDowngraded)
			}
		})
	}
}

// Test_UnitNegotiateIdempotent confirms that negotiation carries no state:
// the same request negotiated more than once returns an identical verdict.
func Test_UnitNegotiateIdempotent(t *testing.T) {
	request := &NegotiationRequest{
		Version:   "v1.32.0+k3s1",
		ClientID:  "node-1",
		RequestID: "request-1",
		Features: []Feature{
			{Name: FeatureReadyz},
			{Name: "future-thing"},
			{Name: "future-required", Required: false},
		},
	}

	first, err := Negotiate(negotiateTestServer, request)
	require.NoError(t, err)
	second, err := Negotiate(negotiateTestServer, request)
	require.NoError(t, err)

	assert.Equal(t, first, second)
}

// Test_UnitNegotiationUnknownFields verifies the forward/backward
// compatibility guarantee: unknown fields sent by newer peers must never
// break decoders running older builds, in either direction.
func Test_UnitNegotiationUnknownFields(t *testing.T) {
	// A newer client adds fields the server does not know about.
	rawRequest := []byte(`{
		"version": "v1.31.2+k3s1",
		"protocolVersion": 1,
		"clientID": "node-1",
		"futureTopLevel": {"nested": [1, 2, 3]},
		"futureString": "surprise",
		"features": [{"name": "readyz", "required": false, "futureFlag": true}]
	}`)

	var request NegotiationRequest
	require.NoError(t, json.Unmarshal(rawRequest, &request))

	result, err := Negotiate(negotiateTestServer, &request)
	require.NoError(t, err)
	assert.Equal(t, StatusCompatible, result.Status)
	assert.Equal(t, "node-1", result.ClientID)
	assert.True(t, result.HasFeature(FeatureReadyz))

	// A newer server adds response fields an older client does not know about.
	rawResponse := []byte(`{
		"status": "compatible",
		"serverVersion": "v9.9.9",
		"protocolVersion": 1,
		"futureResponseField": "ignored",
		"features": [{"name": "readyz", "extra": true}]
	}`)

	var legacyView NegotiationResponse
	require.NoError(t, json.Unmarshal(rawResponse, &legacyView))
	assert.Equal(t, StatusCompatible, legacyView.Status)
	assert.Equal(t, "v9.9.9", legacyView.ServerVersion)
	assert.True(t, legacyView.HasFeature(FeatureReadyz))
	assert.NoError(t, legacyView.Err())
}

// negotiationTestServer spins up a minimal HTTP server implementing the
// negotiation endpoint, mirroring the real handler's decoding behavior.
func negotiationTestServer(t *testing.T, serverInfo ServerInfo) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != negotiationPath {
			http.NotFound(w, req)
			return
		}
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(req.Body)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		request := NegotiationRequest{}
		if err := json.Unmarshal(body, &request); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		result, err := Negotiate(serverInfo, &request)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(result); err != nil {
			t.Errorf("failed to encode response: %v", err)
		}
	}))
}

// Test_UnitInfoNegotiate covers the client HTTP path: successful negotiation,
// repeated/idempotent requests, unknown fields sent over the wire, down-level
// servers without the endpoint (404), and malformed error responses (400).
func Test_UnitInfoNegotiate(t *testing.T) {
	server := negotiationTestServer(t, negotiateTestServer)
	defer server.Close()

	info := &Info{BaseURL: server.URL}
	request := &NegotiationRequest{
		Version:   "v1.31.2+k3s1",
		ClientID:  "node-a",
		RequestID: "req-a",
		Features: []Feature{
			{Name: FeatureReadyz},
			{Name: "future-thing"},
		},
	}

	result, err := info.Negotiate(request)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, StatusDegraded, result.Status)
	assert.Equal(t, negotiateTestServer.Version, result.ServerVersion)
	assert.Equal(t, "node-a", result.ClientID)
	assert.Equal(t, "req-a", result.RequestID)
	assert.True(t, result.HasFeature(FeatureReadyz))
	assert.False(t, result.HasFeature("future-thing"))
	assert.True(t, downgradedFeatures(result)["future-thing"])
	assert.NoError(t, result.Err())

	// Repeating the same request returns an identical verdict.
	retry, err := info.Negotiate(request)
	require.NoError(t, err)
	assert.True(t, reflect.DeepEqual(result, retry))

	// Unknown fields posted by a newer client do not break the server.
	raw, err := info.Post(negotiationPath, []byte(`{"version":"v1.31.2+k3s1","futureField":123,"features":[{"name":"readyz","futureFlag":true}]}`))
	require.NoError(t, err)
	forwardCompatible := NegotiationResponse{}
	require.NoError(t, json.Unmarshal(raw, &forwardCompatible))
	assert.Equal(t, StatusCompatible, forwardCompatible.Status)
	assert.True(t, forwardCompatible.HasFeature(FeatureReadyz))

	// A down-level server answering 404 with a metav1.Status body yields a
	// degraded fallback response and a sentinel error callers can ignore.
	statusNotFound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, `{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"not found","reason":"NotFound","code":404}`)
	}))
	defer statusNotFound.Close()

	fallback, err := (&Info{BaseURL: statusNotFound.URL}).Negotiate(request)
	require.Error(t, err)
	assert.True(t, stderrors.Is(err, ErrNegotiationUnsupported), "expected ErrNegotiationUnsupported, got %v", err)
	require.NotNil(t, fallback)
	assert.Equal(t, StatusDegraded, fallback.Status)

	// A plain-text 404 (older http.ServeMux behavior) is also tolerated.
	plainNotFound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.NotFound(w, &http.Request{})
	}))
	defer plainNotFound.Close()

	fallback, err = (&Info{BaseURL: plainNotFound.URL}).Negotiate(request)
	require.Error(t, err)
	assert.True(t, stderrors.Is(err, ErrNegotiationUnsupported), "expected ErrNegotiationUnsupported, got %v", err)
	require.NotNil(t, fallback)
	assert.Equal(t, StatusDegraded, fallback.Status)

	// A malformed request rejected by the server surfaces as a plain error.
	badRequest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer badRequest.Close()

	result, err = (&Info{BaseURL: badRequest.URL}).Negotiate(request)
	assert.Error(t, err)
	assert.Nil(t, result)
}
