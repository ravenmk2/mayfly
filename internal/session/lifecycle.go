package session

import (
	"time"

	"github.com/ravenmk2/mayfly/internal/packet"
)

// SessionExpiryNever is the v5 Session Expiry Interval value meaning the
// session never expires.
const SessionExpiryNever = 0xFFFFFFFF

// EffectiveSessionExpiry computes the Session Expiry Interval in seconds in
// effect for a connection.
//
// MQTT 5.0: the Session Expiry Interval property, defaulting to 0 (session
// deleted at disconnect) when absent [MQTT-3.1.2-11].
//
// MQTT 3.1.1 has no expiry interval: Clean Session 1 deletes the session at
// disconnect, Clean Session 0 keeps it indefinitely [MQTT-3.1.2-6].
func EffectiveSessionExpiry(version byte, cleanStart bool, props packet.Properties) uint32 {
	if version == packet.Version5 {
		if props.SessionExpiryFlag {
			return props.SessionExpiry
		}
		return 0
	}
	if cleanStart {
		return 0
	}
	return SessionExpiryNever
}

// SessionExpiryDeadline converts an expiry interval and a disconnect time
// into an absolute deadline. ok is false when the session never expires
// (interval 0xFFFFFFFF). An interval of 0 yields the disconnect time itself.
func SessionExpiryDeadline(expiry uint32, stoppedAt time.Time) (deadline time.Time, ok bool) {
	if expiry == SessionExpiryNever {
		return time.Time{}, false
	}
	return stoppedAt.Add(time.Duration(expiry) * time.Second), true
}
