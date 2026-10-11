package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/metrics"
)

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)
	return rec.Code, string(body)
}

func TestMetricsEndpoint(t *testing.T) {
	m := metrics.New("test")
	m.ConnectionsCurrent.Store(1)
	h := New(Options{MetricsHandler: metrics.NewHandler(m, metrics.PromOptions{})})

	code, body := get(t, h, "/metrics")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `mayfly_build_info{version="test"} 1`)
	assert.Contains(t, body, `mayfly_connections 1`)

	code, body = get(t, h, "/healthz")
	require.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ok", body)
}

func TestMetricsEndpointDisabled(t *testing.T) {
	h := New(Options{})
	code, _ := get(t, h, "/metrics")
	// Without a metrics handler the SPA fallback answers instead of the
	// Prometheus exposition.
	assert.NotEqual(t, http.StatusInternalServerError, code)
}
