package session

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ravenmk2/mayfly/internal/packet"
)

func TestSessionPresentMatrix(t *testing.T) {
	// Session Present is set only when a stored session exists and the
	// client did not request a fresh session: v3 Clean Session / v5 Clean
	// Start behave identically for this computation [MQTT-3.1.2-4]
	// [MQTT-3.2.2-2].
	cases := []struct {
		name       string
		version    byte
		cleanStart bool
		existed    bool
		want       bool
	}{
		{"v3 clean, no stored session", packet.Version311, true, false, false},
		{"v3 clean, stored session", packet.Version311, true, true, false},
		{"v3 durable, no stored session", packet.Version311, false, false, false},
		{"v3 durable, stored session", packet.Version311, false, true, true},
		{"v5 clean start, no stored session", packet.Version5, true, false, false},
		{"v5 clean start, stored session", packet.Version5, true, true, false},
		{"v5 resume, no stored session", packet.Version5, false, false, false},
		{"v5 resume, stored session", packet.Version5, false, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, SessionPresent(tc.cleanStart, tc.existed))
		})
	}
}

func TestEffectiveSessionExpiry(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		clean   bool
		props   packet.Properties
		want    uint32
	}{
		{"v3 clean session", packet.Version311, true, packet.Properties{}, 0},
		{"v3 durable session", packet.Version311, false, packet.Properties{}, SessionExpiryNever},
		{"v5 default", packet.Version5, false, packet.Properties{}, 0},
		{"v5 explicit 0", packet.Version5, true, packet.Properties{SessionExpiryFlag: true, SessionExpiry: 0}, 0},
		{"v5 interval", packet.Version5, false, packet.Properties{SessionExpiryFlag: true, SessionExpiry: 300}, 300},
		{"v5 never", packet.Version5, false, packet.Properties{SessionExpiryFlag: true, SessionExpiry: SessionExpiryNever}, SessionExpiryNever},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, EffectiveSessionExpiry(tc.version, tc.clean, tc.props))
		})
	}
}

func TestSessionExpiryDeadline(t *testing.T) {
	stop := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)

	deadline, ok := SessionExpiryDeadline(0, stop)
	require.True(t, ok)
	assert.Equal(t, stop, deadline)

	deadline, ok = SessionExpiryDeadline(60, stop)
	require.True(t, ok)
	assert.Equal(t, stop.Add(time.Minute), deadline)

	_, ok = SessionExpiryDeadline(SessionExpiryNever, stop)
	assert.False(t, ok)
}

func TestSessionExpiryAfterDisconnect(t *testing.T) {
	// No DISCONNECT or no property: the CONNECT interval stands.
	v, err := SessionExpiryAfterDisconnect(100, nil)
	require.NoError(t, err)
	assert.Equal(t, uint32(100), v)

	d := &packet.Packet{FixedHeader: packet.FixedHeader{Type: packet.Disconnect}}
	v, err = SessionExpiryAfterDisconnect(100, d)
	require.NoError(t, err)
	assert.Equal(t, uint32(100), v)

	// A v5 DISCONNECT may shrink the interval, including to 0.
	d.Properties.SessionExpiryFlag = true
	d.Properties.SessionExpiry = 0
	v, err = SessionExpiryAfterDisconnect(100, d)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), v)

	// Extending a session that was to end at disconnect is a Protocol Error
	// [MQTT-3.1.2-11].
	d.Properties.SessionExpiry = 50
	_, err = SessionExpiryAfterDisconnect(0, d)
	require.Error(t, err)
	assert.Equal(t, packet.CodeProtocolError, err.(packet.Code).Code)
}

func TestWillOnDisconnectMatrix(t *testing.T) {
	cases := []struct {
		name       string
		disconnect bool
		reason     byte
		want       bool
	}{
		{"connection lost", false, 0x00, true},
		{"normal disconnection", true, packet.CodeNormalDisconnection, false},
		{"disconnect with will message", true, packet.CodeDisconnectWithWillMessage, true},
		{"other disconnect reason suppresses will", true, packet.CodeServerShuttingDown, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, WillOnDisconnect(tc.disconnect, tc.reason))
		})
	}
}

func TestWillPublishTime(t *testing.T) {
	now := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)

	// Delay wins when the session outlives it.
	assert.Equal(t, now.Add(10*time.Second), WillPublishTime(now, 10, SessionExpiryNever))
	assert.Equal(t, now.Add(10*time.Second), WillPublishTime(now, 10, 300))

	// Session expiry before the delay caps the fire time [MQTT-3.1.3-8].
	assert.Equal(t, now.Add(5*time.Second), WillPublishTime(now, 10, 5))

	// No delay: immediate, even with an expiring session.
	assert.Equal(t, now, WillPublishTime(now, 0, 5))
}

func TestWillSchedulerOrderCancelReplace(t *testing.T) {
	now := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	will := func(id string, at time.Time) ScheduledWill {
		return ScheduledWill{ClientID: id, Message: *publishPacket(0, 0, "will/"+id), At: at}
	}
	var s WillScheduler

	s.Schedule(will("late", now.Add(time.Hour)))
	s.Schedule(will("early", now.Add(time.Minute)))
	s.Schedule(will("mid", now.Add(30*time.Minute)))
	assert.Equal(t, 3, s.Len())

	// Replacing a client's Will cancels the old entry.
	s.Schedule(will("mid", now.Add(2*time.Hour)))
	assert.Equal(t, 3, s.Len())

	// Cancel removes a pending Will (session resumed).
	assert.True(t, s.Cancel("early"))
	assert.False(t, s.Cancel("early"))
	assert.Equal(t, 2, s.Len())

	// Due fires in fire-time order; cancelled entries are purged silently.
	due := s.Due(now.Add(90 * time.Minute))
	require.Len(t, due, 1)
	assert.Equal(t, "late", due[0].ClientID)
	assert.Equal(t, 1, s.Len())

	due = s.Due(now.Add(3 * time.Hour))
	require.Len(t, due, 1)
	assert.Equal(t, "mid", due[0].ClientID)
	assert.Equal(t, 0, s.Len())
	assert.Empty(t, s.Due(now.Add(4*time.Hour)))
}
