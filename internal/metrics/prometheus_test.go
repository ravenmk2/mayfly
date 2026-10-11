package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	"github.com/ravenmk2/mayfly/internal/auth"
	"github.com/ravenmk2/mayfly/internal/hook"
)

// scrape performs one GET against the handler and returns the body.
func scrape(t *testing.T, h http.Handler) string {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body, err := io.ReadAll(rec.Result().Body)
	require.NoError(t, err)
	return string(body)
}

func TestPrometheusHandler(t *testing.T) {
	m := New("1.2.3")
	m.ConnectionsTotal.Add(7)
	m.ConnectionsCurrent.Store(2)
	m.AddReceived(1)
	m.AddReceived(1)
	m.AddSent(0)
	m.MessagesDropped.Add(3)
	m.BytesReceived.Add(4096)
	m.BytesSent.Add(8192)
	m.AuthSucceeded.Add(5)
	m.AuthFailures.Add(1)
	m.ACLAllowed.Add(9)
	m.StoreWriteErrors.Add(2)
	m.Subscriptions.Store(4)
	m.Retained.Store(6)
	m.Inflight.Store(8)
	m.SampleRuntime()

	body := scrape(t, NewHandler(m, PromOptions{}))

	for _, want := range []string{
		`mayfly_build_info{version="1.2.3"} 1`,
		`mayfly_connections 2`,
		`mayfly_connections_total 7`,
		`mayfly_messages_received_total{qos="1"} 2`,
		`mayfly_messages_received_total{qos="0"} 0`,
		`mayfly_messages_sent_total{qos="0"} 1`,
		`mayfly_messages_dropped_total 3`,
		`mayfly_messages_oversized_total 0`,
		`mayfly_bytes_total{direction="received"} 4096`,
		`mayfly_bytes_total{direction="sent"} 8192`,
		`mayfly_subscriptions 4`,
		`mayfly_retained 6`,
		`mayfly_inflight 8`,
		`mayfly_auth_results_total{result="success"} 5`,
		`mayfly_auth_results_total{result="failure"} 1`,
		`mayfly_acl_results_total{result="allow"} 9`,
		`mayfly_acl_results_total{result="deny"} 0`,
		`mayfly_store_write_errors_total 2`,
		`mayfly_uptime_seconds`,
		`go_goroutines`,
	} {
		assert.Contains(t, body, want)
	}
	// No per-source breakdown without an auth manager.
	assert.NotContains(t, body, "mayfly_auth_source_results_total")
}

// TestPrometheusAuthSourceStats wires auth.Manager.Stats into the handler
// and checks the per-source breakdown (docs/design/auth.md section 6).
func TestPrometheusAuthSourceStats(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "users.json")
	require.NoError(t, os.WriteFile(path,
		[]byte(`{"users": {"sensor1": {"password": "`+string(hash)+`"}}}`), 0o600))

	mgr, err := auth.NewManager(context.Background(), auth.Config{
		Authentication: []auth.SourceConfig{{Type: "file", Enabled: true, Path: path}},
	}, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	okRes, err := mgr.Authenticate(context.Background(), hook.ConnectInfo{
		ClientID: "c1", Username: "sensor1", Password: []byte("secret"),
	})
	require.NoError(t, err)
	require.True(t, okRes.OK)
	badRes, err := mgr.Authenticate(context.Background(), hook.ConnectInfo{
		ClientID: "c2", Username: "sensor1", Password: []byte("wrong"),
	})
	require.NoError(t, err)
	require.False(t, badRes.OK)
	// Unknown user misses every source and is denied by the chain.
	missRes, err := mgr.Authenticate(context.Background(), hook.ConnectInfo{
		ClientID: "c3", Username: "ghost", Password: []byte("x"),
	})
	require.NoError(t, err)
	require.False(t, missRes.OK)

	body := scrape(t, NewHandler(New("test"), PromOptions{AuthStats: mgr.Stats}))
	// Prometheus escapes backslashes in label values (relevant on Windows).
	src := `source="file(` + strings.ReplaceAll(path, `\`, `\\`) + `)"`
	assert.Contains(t, body, `mayfly_auth_source_results_total{chain="authentication",result="allow",`+src+`} 1`)
	assert.Contains(t, body, `mayfly_auth_source_results_total{chain="authentication",result="deny",`+src+`} 1`)
	assert.Contains(t, body, `mayfly_auth_source_results_total{chain="authentication",result="miss",`+src+`} 1`)
	assert.NotContains(t, body, "mayfly_auth_results_total")
}
