package broker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/hook"
	"github.com/ravenmk2/mayfly/internal/packet"
)

func TestListConnections(t *testing.T) {
	s := newTestServer(t, nil)

	c1 := dial(t, s).v5()
	c1.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect: packet.ConnectParams{
			ProtocolVersion: packet.Version5, Clean: true, ClientIdentifier: "c1",
			UsernameFlag: true, Username: "alice",
		},
	})
	require.Equal(t, packet.CodeSuccess, c1.read().ReasonCode)
	c1.subscribe(1, packet.Subscription{Filter: "a/#", Qos: 1})

	c2 := dial(t, s).v3()
	c2.connect("c2", true, packet.Properties{})

	list := s.ListConnections()
	require.Len(t, list, 2)
	for _, info := range list {
		assert.True(t, info.Connected)
		assert.NotZero(t, info.ConnectedAt)
	}
	assert.Equal(t, "c1", list[0].ClientID)
	assert.Equal(t, "c2", list[1].ClientID)

	info, ok := s.ConnectionInfo("c1")
	require.True(t, ok)
	assert.Equal(t, "alice", info.Username)
	assert.Equal(t, packet.Version5, info.ProtocolVersion)
	assert.Equal(t, 1, info.Subscriptions)

	_, ok = s.ConnectionInfo("ghost")
	assert.False(t, ok)

	c1.close()
	c2.close()
}

func TestKickConnection(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	c.connect("victim", true, packet.Properties{})

	assert.False(t, s.KickConnection("ghost"))
	assert.True(t, s.KickConnection("victim"))

	// The v5 client sees DISCONNECT 0x98 (Administrative Action) before close.
	pk := c.read()
	assert.Equal(t, packet.Disconnect, pk.FixedHeader.Type)
	assert.Equal(t, packet.CodeAdministrativeAction, pk.ReasonCode)
	c.expectClosed(time.Second)

	assert.Eventually(t, func() bool { return !s.KickConnection("victim") },
		time.Second, 10*time.Millisecond)
}

func TestClearSession(t *testing.T) {
	s := newTestServer(t, nil)
	c := dial(t, s).v5()
	c.connect("durable", true, packet.Properties{SessionExpiry: 60, SessionExpiryFlag: true})
	c.subscribe(1, packet.Subscription{Filter: "x/#", Qos: 1})

	assert.False(t, s.ClearSession("ghost"))
	assert.True(t, s.ClearSession("durable"))
	c.expectClosed(time.Second)

	assert.Equal(t, 0, s.ClientCount())
	assert.Empty(t, s.ListSubscriptions(""))
	// The clear is idempotent in effect: a retry reports no session.
	assert.False(t, s.ClearSession("durable"))
}

func TestListSubscriptions(t *testing.T) {
	s := newTestServer(t, nil)
	c1 := dial(t, s).v3()
	c1.connect("c1", true, packet.Properties{})
	c1.subscribe(1, packet.Subscription{Filter: "a/b", Qos: 0})
	c1.subscribe(2, packet.Subscription{Filter: "$share/g/c/#", Qos: 1})
	c2 := dial(t, s).v3()
	c2.connect("c2", true, packet.Properties{})
	c2.subscribe(1, packet.Subscription{Filter: "a/b", Qos: 2})

	all := s.ListSubscriptions("")
	require.Len(t, all, 3)
	assert.Equal(t, "c1", all[0].ClientID)
	assert.Equal(t, "c1", all[1].ClientID)
	assert.Equal(t, "c2", all[2].ClientID)

	only := s.ListSubscriptions("c1")
	require.Len(t, only, 2)
	assert.Equal(t, "$share/g/c/#", only[0].Filter) // '$' sorts before 'a'
	assert.Equal(t, "g", only[0].Shared)
	assert.Equal(t, "a/b", only[1].Filter)

	assert.Empty(t, s.ListSubscriptions("ghost"))

	c1.close()
	c2.close()
}

func TestRetainedAdmin(t *testing.T) {
	s := newTestServer(t, nil)
	require.NoError(t, s.Publish("sensors/1", []byte("on"), true))
	require.NoError(t, s.Publish("sensors/2", []byte("off"), true))

	retained := s.ListRetained()
	require.Len(t, retained, 2)
	assert.Equal(t, "sensors/1", retained[0].TopicName)
	assert.Equal(t, []byte("on"), retained[0].Payload)

	require.NoError(t, s.DeleteRetained("sensors/1"))
	assert.Empty(t, s.DeleteRetained("sensors/1")) // already gone: no-op
	require.Error(t, s.DeleteRetained("bad/#"))

	retained = s.ListRetained()
	require.Len(t, retained, 1)
	assert.Equal(t, "sensors/2", retained[0].TopicName)
}

func TestSetChains(t *testing.T) {
	s := newTestServer(t, nil)

	// Swap in a denying chain: new CONNECTs are rejected.
	s.SetChains(hook.NewAuthChain(denyAuth{}), nil)
	c := dial(t, s).v5()
	c.write(&packet.Packet{
		FixedHeader: packet.FixedHeader{Type: packet.Connect},
		Connect:     packet.ConnectParams{ProtocolVersion: packet.Version5, Clean: true, ClientIdentifier: "c"},
	})
	assert.Equal(t, packet.CodeNotAuthorized, c.read().ReasonCode)
	c.expectClosed(time.Second)

	// Swap back to the allow-all stub.
	s.SetChains(nil, nil)
	ok := dial(t, s).v5()
	ok.connect("c", true, packet.Properties{})
	ok.close()
}
