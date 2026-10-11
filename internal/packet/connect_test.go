package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// connectV311 is a minimal MQTT 3.1.1 CONNECT with clean session.
var connectV311 = []byte{
	0x10, 0x10, // fixed header, remaining 16
	0x00, 0x04, 'M', 'Q', 'T', 'T', // protocol name
	0x04,       // protocol level
	0x02,       // flags: clean session
	0x00, 0x3C, // keepalive 60
	0x00, 0x04, 't', 'e', 's', 't', // client id
}

// connectV5 is an MQTT 5.0 CONNECT with will, credentials and properties.
var connectV5 = []byte{
	0x10, 0x25, // fixed header, remaining 37
	0x00, 0x04, 'M', 'Q', 'T', 'T', // protocol name
	0x05,       // protocol level
	0xEE,       // flags: username, password, will retain, will qos1, will, clean
	0x00, 0x0A, // keepalive 10
	0x05, 0x11, 0x00, 0x00, 0x00, 0x3C, // properties: session expiry 60
	0x00, 0x02, 'c', '1', // client id
	0x00,                      // will properties: empty
	0x00, 0x03, 'w', '/', 't', // will topic
	0x00, 0x03, 'b', 'y', 'e', // will payload
	0x00, 0x01, 'u', // username
	0x00, 0x01, 'p', // password
}

func TestConnectDecodeV311(t *testing.T) {
	var pk Packet
	require.NoError(t, pk.Decode(connectV311))
	assert.Equal(t, Version311, pk.ProtocolVersion)
	assert.Equal(t, "MQTT", pk.Connect.ProtocolName)
	assert.Equal(t, Version311, pk.Connect.ProtocolVersion)
	assert.True(t, pk.Connect.Clean)
	assert.Equal(t, uint16(60), pk.Connect.Keepalive)
	assert.Equal(t, "test", pk.Connect.ClientIdentifier)
	assert.False(t, pk.Connect.WillFlag)
	assert.False(t, pk.Connect.UsernameFlag)
	assert.False(t, pk.Connect.PasswordFlag)
}

func TestConnectRoundTripV311(t *testing.T) {
	var pk Packet
	require.NoError(t, pk.Decode(connectV311))
	out, err := pk.Encode()
	require.NoError(t, err)
	assert.Equal(t, connectV311, out)
}

func TestConnectDecodeV5(t *testing.T) {
	var pk Packet
	require.NoError(t, pk.Decode(connectV5))
	assert.Equal(t, Version5, pk.ProtocolVersion)
	c := pk.Connect
	assert.True(t, c.Clean)
	assert.True(t, c.WillFlag)
	assert.Equal(t, byte(1), c.WillQos)
	assert.True(t, c.WillRetain)
	assert.Equal(t, "w/t", c.WillTopic)
	assert.Equal(t, []byte("bye"), c.WillPayload)
	assert.Equal(t, "u", c.Username)
	assert.Equal(t, []byte("p"), c.Password)
	assert.True(t, pk.Properties.SessionExpiryFlag)
	assert.Equal(t, uint32(60), pk.Properties.SessionExpiry)
}

func TestConnectRoundTripV5(t *testing.T) {
	var pk Packet
	require.NoError(t, pk.Decode(connectV5))
	out, err := pk.Encode()
	require.NoError(t, err)
	assert.Equal(t, connectV5, out)
}

func TestConnectDecodeV31Normalized(t *testing.T) {
	// MQTT 3.1 ("MQIsdp", level 3) is accepted and treated as 3.1.1.
	raw := []byte{
		0x10, 0x10,
		0x00, 0x06, 'M', 'Q', 'I', 's', 'd', 'p',
		0x03,
		0x02,
		0x00, 0x3C,
		0x00, 0x02, 'c', '1',
	}
	var pk Packet
	require.NoError(t, pk.Decode(raw))
	assert.Equal(t, Version311, pk.ProtocolVersion)
	assert.Equal(t, "MQIsdp", pk.Connect.ProtocolName)
	assert.Equal(t, Version311, pk.Connect.ProtocolVersion)
}

func TestConnectDecodeMalformed(t *testing.T) {
	cases := map[string]struct {
		raw  []byte
		code byte
	}{
		"bad protocol level": {
			raw:  []byte{0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x06, 0x02, 0x00, 0x3C, 0x00, 0x00},
			code: CodeUnsupportedProtocolVersion,
		},
		"protocol name mismatch": {
			raw:  []byte{0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'X', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x00},
			code: CodeUnsupportedProtocolVersion,
		},
		"reserved connect flag": {
			raw:  []byte{0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x03, 0x00, 0x3C, 0x00, 0x00},
			code: CodeMalformedPacket,
		},
		"will qos 3": {
			raw:  []byte{0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x1E, 0x00, 0x3C, 0x00, 0x00},
			code: CodeMalformedPacket,
		},
		"will retain without will flag": {
			raw:  []byte{0x10, 0x0C, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x22, 0x00, 0x3C, 0x00, 0x00},
			code: CodeProtocolError,
		},
		"truncated variable header": {
			raw:  []byte{0x10, 0x08, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02},
			code: CodeMalformedPacket,
		},
		"missing client id": {
			raw:  []byte{0x10, 0x0A, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C},
			code: CodeMalformedPacket,
		},
		"trailing payload": {
			raw:  []byte{0x10, 0x0D, 0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x00, 0xFF},
			code: CodeMalformedPacket,
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var pk Packet
			err := pk.Decode(c.raw)
			require.Error(t, err)
			assert.Equal(t, c.code, err.(Code).Code)
		})
	}
}

func TestConnectEncodeValidate(t *testing.T) {
	// Will fields without the will flag must be rejected.
	pk := Packet{
		FixedHeader: FixedHeader{Type: Connect},
		Connect: ConnectParams{
			ProtocolVersion: Version311,
			WillQos:         1,
		},
	}
	_, err := pk.Encode()
	require.Error(t, err)

	// Will topic must not contain wildcards [MQTT-3.3.2-2].
	pk = Packet{
		FixedHeader: FixedHeader{Type: Connect},
		Connect: ConnectParams{
			ProtocolVersion: Version311,
			WillFlag:        true,
			WillTopic:       "a/+/c",
		},
	}
	_, err = pk.Encode()
	require.Error(t, err)
	assert.Equal(t, CodeTopicNameInvalid, err.(Code).Code)
}

func TestConnackRoundTrip(t *testing.T) {
	cases := []struct {
		name   string
		raw    []byte
		assert func(t *testing.T, pk *Packet)
	}{
		{
			name: "v3 accepted",
			raw:  []byte{0x20, 0x02, 0x00, 0x00},
			assert: func(t *testing.T, pk *Packet) {
				assert.False(t, pk.SessionPresent)
				assert.Equal(t, byte(0), pk.ReasonCode)
			},
		},
		{
			name: "v3 session present",
			raw:  []byte{0x20, 0x02, 0x01, 0x00},
			assert: func(t *testing.T, pk *Packet) {
				assert.True(t, pk.SessionPresent)
			},
		},
		{
			name: "v3 not authorized",
			raw:  []byte{0x20, 0x02, 0x00, 0x05},
			assert: func(t *testing.T, pk *Packet) {
				assert.Equal(t, byte(5), pk.ReasonCode)
			},
		},
		{
			name: "v5 success empty props",
			raw:  []byte{0x20, 0x03, 0x00, 0x00, 0x00},
			assert: func(t *testing.T, pk *Packet) {
				assert.Equal(t, CodeSuccess, pk.ReasonCode)
			},
		},
		{
			name: "v5 assigned client id",
			raw:  []byte{0x20, 0x08, 0x00, 0x00, 0x05, 0x12, 0x00, 0x02, 'c', '1'},
			assert: func(t *testing.T, pk *Packet) {
				assert.True(t, pk.Properties.AssignedClientIDFlag)
				assert.Equal(t, "c1", pk.Properties.AssignedClientID)
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			version := Version311
			if len(c.raw) > 4 && c.raw[1] != 0x02 {
				version = Version5
			}
			pk := Packet{ProtocolVersion: version}
			require.NoError(t, pk.Decode(c.raw))
			c.assert(t, &pk)
			out, err := pk.Encode()
			require.NoError(t, err)
			assert.Equal(t, c.raw, out)
		})
	}
}

func TestConnackDecodeMalformed(t *testing.T) {
	// Reserved acknowledge flag bits set (spec 3.2.2.1).
	pk := Packet{ProtocolVersion: Version311}
	require.Error(t, pk.Decode([]byte{0x20, 0x02, 0x02, 0x00}))
	// Reserved return code (v3 codes above 5 are not defined).
	pk = Packet{ProtocolVersion: Version311}
	require.Error(t, pk.Decode([]byte{0x20, 0x02, 0x00, 0x06}))
	// Session present with a failure code [MQTT-3.2.2-4].
	pk = Packet{ProtocolVersion: Version311}
	require.Error(t, pk.Decode([]byte{0x20, 0x02, 0x01, 0x05}))
	// v5: invalid reason code for CONNACK.
	pk = Packet{ProtocolVersion: Version5}
	err := pk.Decode([]byte{0x20, 0x03, 0x00, 0x18, 0x00})
	require.Error(t, err)
	assert.Equal(t, CodeProtocolError, err.(Code).Code)
}

func TestPingRoundTrip(t *testing.T) {
	for _, raw := range [][]byte{{0xC0, 0x00}, {0xD0, 0x00}} {
		var pk Packet
		require.NoError(t, pk.Decode(raw))
		out, err := pk.Encode()
		require.NoError(t, err)
		assert.Equal(t, raw, out)
	}
	// PINGREQ / PINGRESP must have remaining length 0 [MQTT-3.12.3-1].
	var pk Packet
	require.Error(t, pk.Decode([]byte{0xC0, 0x01, 0x00}))
	require.Error(t, pk.Decode([]byte{0xD0, 0x01, 0x00}))
}

func TestDisconnectRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		raw     []byte
	}{
		{"v3", Version311, []byte{0xE0, 0x00}},
		{"v5 normal omitted", Version5, []byte{0xE0, 0x00}},
		{"v5 reason only", Version5, []byte{0xE0, 0x01, 0x8B}},
		{"v5 reason and empty props", Version5, []byte{0xE0, 0x02, 0x8B, 0x00}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: c.version}
			require.NoError(t, pk.Decode(c.raw))
			out, err := pk.Encode()
			require.NoError(t, err)
			// The encoder uses the shortest permitted form: the reason-only
			// form is emitted when there are no properties.
			if c.name == "v5 reason and empty props" {
				assert.Equal(t, []byte{0xE0, 0x01, 0x8B}, out)
			} else {
				assert.Equal(t, c.raw, out)
			}
		})
	}
}

func TestDisconnectMalformed(t *testing.T) {
	// MQTT 3.1.1 DISCONNECT must have remaining length 0 [MQTT-3.14.3-1].
	pk := Packet{ProtocolVersion: Version311}
	require.Error(t, pk.Decode([]byte{0xE0, 0x01, 0x00}))
	// v5: reason code not valid on DISCONNECT.
	pk = Packet{ProtocolVersion: Version5}
	require.Error(t, pk.Decode([]byte{0xE0, 0x01, 0x01}))
}

func TestDisconnectV5WithProperties(t *testing.T) {
	pk := Packet{
		FixedHeader:     FixedHeader{Type: Disconnect},
		ProtocolVersion: Version5,
		ReasonCode:      CodeServerShuttingDown,
		Properties: Properties{
			ReasonString:     "bye",
			ReasonStringFlag: true,
		},
	}
	out, err := pk.Encode()
	require.NoError(t, err)
	// E0, remaining 8, 8B, propLen 6, 1F 00 03 'b' 'y' 'e'
	assert.Equal(t, []byte{0xE0, 0x08, 0x8B, 0x06, 0x1F, 0x00, 0x03, 'b', 'y', 'e'}, out)

	var dec Packet
	dec.ProtocolVersion = Version5
	require.NoError(t, dec.Decode(out))
	assert.Equal(t, CodeServerShuttingDown, dec.ReasonCode)
	assert.Equal(t, "bye", dec.Properties.ReasonString)
}
