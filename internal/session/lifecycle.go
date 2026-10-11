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

// SessionPresent computes the CONNACK Session Present flag: set only when the
// broker holds a stored session for the client ID and the client did not
// request a fresh session (v3 Clean Session 0 / v5 Clean Start 0)
// [MQTT-3.1.2-4] [MQTT-3.2.2-2].
func SessionPresent(cleanStart bool, existed bool) bool {
	return existed && !cleanStart
}

// SessionExpiryAfterDisconnect applies the Session Expiry Interval override a
// v5 DISCONNECT may carry. Setting a non-zero interval when the CONNECT
// interval was 0 is a Protocol Error [MQTT-3.1.2-11].
func SessionExpiryAfterDisconnect(connectExpiry uint32, disconnect *packet.Packet) (uint32, error) {
	if disconnect == nil || !disconnect.Properties.SessionExpiryFlag {
		return connectExpiry, nil
	}
	v := disconnect.Properties.SessionExpiry
	if connectExpiry == 0 && v != 0 {
		return 0, packet.Code{Code: packet.CodeProtocolError,
			Reason: "DISCONNECT must not extend a session expiring at disconnect"}
	}
	return v, nil
}

// WillOnDisconnect reports whether the Will Message must be published: on any
// abnormal connection loss, or when a v5 DISCONNECT carries reason 0x04
// (Disconnect with Will Message). A normal DISCONNECT (0x00) or any other
// explicit DISCONNECT suppresses the Will [MQTT-3.1.2-8].
func WillOnDisconnect(receivedDisconnect bool, reasonCode byte) bool {
	if !receivedDisconnect {
		return true
	}
	return reasonCode == packet.CodeDisconnectWithWillMessage
}

// WillPublishTime returns when a delayed Will Message fires: now plus the
// Will Delay Interval, but no later than the session expiry — when the
// session ends before the delay elapses the Will is published at session end
// [MQTT-3.1.3-8].
func WillPublishTime(now time.Time, willDelay uint32, sessionExpiry uint32) time.Time {
	at := now.Add(time.Duration(willDelay) * time.Second)
	if deadline, ok := SessionExpiryDeadline(sessionExpiry, now); ok && deadline.Before(at) {
		return deadline
	}
	return at
}
