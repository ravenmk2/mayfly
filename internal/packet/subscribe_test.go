package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubscribeRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		raw     []byte
		assert  func(t *testing.T, pk *Packet)
	}{
		{
			name:    "v3 single filter",
			version: Version311,
			raw: []byte{
				0x82, 0x08, 0x00, 0x0A, // packet id 10
				0x00, 0x03, 'a', '/', 'b', 0x01, // filter a/b qos1
			},
			assert: func(t *testing.T, pk *Packet) {
				require.Len(t, pk.Filters, 1)
				assert.Equal(t, "a/b", pk.Filters[0].Filter)
				assert.Equal(t, byte(1), pk.Filters[0].Qos)
			},
		},
		{
			name:    "v5 two filters with subscription id",
			version: Version5,
			raw: []byte{
				0x82, 0x11, 0x00, 0x0A, // packet id 10
				0x02, 0x0B, 0x03, // properties: subscription id 3
				0x00, 0x03, 'a', '/', 'b', 0x01, // filter a/b qos1
				0x00, 0x03, 'c', '/', '#', 0x2A, // filter c/# rh2 rap qos2
			},
			assert: func(t *testing.T, pk *Packet) {
				require.Len(t, pk.Filters, 2)
				assert.Equal(t, uint32(3), pk.Filters[0].Identifier)
				assert.Equal(t, uint32(3), pk.Filters[1].Identifier)
				assert.Equal(t, "c/#", pk.Filters[1].Filter)
				assert.Equal(t, byte(2), pk.Filters[1].RetainHandling)
				assert.True(t, pk.Filters[1].RetainAsPublished)
				assert.False(t, pk.Filters[1].NoLocal)
				assert.Equal(t, byte(2), pk.Filters[1].Qos)
			},
		},
		{
			name:    "v5 shared subscription",
			version: Version5,
			raw: []byte{
				0x82, 0x11, 0x00, 0x01,
				0x00, // no properties
				0x00, 0x0B, '$', 's', 'h', 'a', 'r', 'e', '/', 'g', '/', 'a', 'b', 0x00,
			},
			assert: func(t *testing.T, pk *Packet) {
				require.Len(t, pk.Filters, 1)
				assert.Equal(t, "$share/g/ab", pk.Filters[0].Filter)
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: c.version}
			require.NoError(t, pk.Decode(c.raw))
			c.assert(t, &pk)
			out, err := pk.Encode()
			require.NoError(t, err)
			assert.Equal(t, c.raw, out)
		})
	}
}

func TestSubscribeMalformed(t *testing.T) {
	cases := map[string]struct {
		version byte
		raw     []byte
		code    byte
	}{
		"packet id zero": {
			version: Version311,
			raw:     []byte{0x82, 0x08, 0x00, 0x00, 0x00, 0x03, 'a', '/', 'b', 0x01},
			code:    CodeProtocolError,
		},
		"no filters": {
			version: Version311,
			raw:     []byte{0x82, 0x02, 0x00, 0x0A},
			code:    CodeProtocolError,
		},
		"missing options byte": {
			version: Version311,
			raw:     []byte{0x82, 0x07, 0x00, 0x0A, 0x00, 0x03, 'a', '/', 'b'},
			code:    CodeMalformedPacket,
		},
		"v3 reserved option bits": {
			version: Version311,
			raw:     []byte{0x82, 0x08, 0x00, 0x0A, 0x00, 0x03, 'a', '/', 'b', 0x04},
			code:    CodeMalformedPacket,
		},
		"v5 reserved option bits": {
			version: Version5,
			raw:     []byte{0x82, 0x09, 0x00, 0x0A, 0x00, 0x00, 0x03, 'a', '/', 'b', 0x81},
			code:    CodeMalformedPacket,
		},
		"options qos3": {
			version: Version5,
			raw:     []byte{0x82, 0x09, 0x00, 0x0A, 0x00, 0x00, 0x03, 'a', '/', 'b', 0x03},
			code:    CodeMalformedPacket,
		},
		"retain handling 3": {
			version: Version5,
			raw:     []byte{0x82, 0x09, 0x00, 0x0A, 0x00, 0x00, 0x03, 'a', '/', 'b', 0x30},
			code:    CodeMalformedPacket,
		},
		"misplaced multi-level wildcard": {
			version: Version311,
			raw:     []byte{0x82, 0x08, 0x00, 0x0A, 0x00, 0x03, 'a', '#', 'b', 0x01},
			code:    CodeTopicFilterInvalid,
		},
		"misplaced single-level wildcard": {
			version: Version311,
			raw:     []byte{0x82, 0x08, 0x00, 0x0A, 0x00, 0x03, 'a', '+', 'b', 0x01},
			code:    CodeTopicFilterInvalid,
		},
		"shared subscription without filter": {
			version: Version5,
			raw:     []byte{0x82, 0x0E, 0x00, 0x0A, 0x00, 0x00, 0x08, '$', 's', 'h', 'a', 'r', 'e', '/', 'g', 0x00},
			code:    CodeTopicFilterInvalid,
		},
		"duplicate subscription identifier": {
			version: Version5,
			raw: []byte{0x82, 0x0B, 0x00, 0x0A,
				0x04, 0x0B, 0x01, 0x0B, 0x02, // two subscription identifiers
				0x00, 0x01, 'a', 0x00},
			code: CodeProtocolError,
		},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: c.version}
			err := pk.Decode(c.raw)
			require.Error(t, err)
			assert.Equal(t, c.code, err.(Code).Code)
		})
	}
}

func TestUnsubscribeRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		raw     []byte
	}{
		{"v3", Version311, []byte{0xA2, 0x07, 0x00, 0x0A, 0x00, 0x03, 'a', '/', 'b'}},
		{"v5", Version5, []byte{0xA2, 0x08, 0x00, 0x0A, 0x00, 0x00, 0x03, 'a', '/', 'b'}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: c.version}
			require.NoError(t, pk.Decode(c.raw))
			require.Len(t, pk.Filters, 1)
			assert.Equal(t, "a/b", pk.Filters[0].Filter)
			out, err := pk.Encode()
			require.NoError(t, err)
			assert.Equal(t, c.raw, out)
		})
	}
}

func TestUnsubscribeMalformed(t *testing.T) {
	// Payload must contain at least one topic filter [MQTT-3.10.3-2].
	pk := Packet{ProtocolVersion: Version311}
	err := pk.Decode([]byte{0xA2, 0x02, 0x00, 0x0A})
	require.Error(t, err)
	assert.Equal(t, CodeProtocolError, err.(Code).Code)

	// Empty filter is invalid [MQTT-4.7.3-1].
	pk = Packet{ProtocolVersion: Version311}
	err = pk.Decode([]byte{0xA2, 0x04, 0x00, 0x0A, 0x00, 0x00})
	require.Error(t, err)
	assert.Equal(t, CodeTopicFilterInvalid, err.(Code).Code)
}

func TestSubackRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		raw     []byte
	}{
		{"v3 granted", Version311, []byte{0x90, 0x04, 0x00, 0x0A, 0x00, 0x02}},
		{"v3 failure", Version311, []byte{0x90, 0x03, 0x00, 0x0A, 0x80}},
		{"v5", Version5, []byte{0x90, 0x08, 0x00, 0x0A,
			0x03, 0x1F, 0x00, 0x00, // reason string (empty)
			0x00, 0x02}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: c.version}
			require.NoError(t, pk.Decode(c.raw))
			out, err := pk.Encode()
			require.NoError(t, err)
			assert.Equal(t, c.raw, out)
		})
	}
}

func TestSubackMalformed(t *testing.T) {
	// v3 return code 0x03 is reserved [MQTT-3.9.3-2].
	pk := Packet{ProtocolVersion: Version311}
	err := pk.Decode([]byte{0x90, 0x03, 0x00, 0x0A, 0x03})
	require.Error(t, err)
	assert.Equal(t, CodeMalformedPacket, err.(Code).Code)

	// Empty payload is not allowed [MQTT-3.9.3-1].
	pk = Packet{ProtocolVersion: Version311}
	require.Error(t, pk.Decode([]byte{0x90, 0x02, 0x00, 0x0A}))

	// v5 reason code not valid on SUBACK.
	pk = Packet{ProtocolVersion: Version5}
	err = pk.Decode([]byte{0x90, 0x04, 0x00, 0x0A, 0x00, 0x10})
	require.Error(t, err)
	assert.Equal(t, CodeProtocolError, err.(Code).Code)
}

func TestUnsubackRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		raw     []byte
	}{
		{"v3", Version311, []byte{0xB0, 0x02, 0x00, 0x0A}},
		{"v5 empty codes", Version5, []byte{0xB0, 0x03, 0x00, 0x0A, 0x00}},
		{"v5 with codes", Version5, []byte{0xB0, 0x05, 0x00, 0x0A, 0x00, 0x00, 0x11}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: c.version}
			require.NoError(t, pk.Decode(c.raw))
			out, err := pk.Encode()
			require.NoError(t, err)
			assert.Equal(t, c.raw, out)
		})
	}

	// v3 UNSUBACK must have remaining length exactly 2 [MQTT-3.11.3-1].
	pk := Packet{ProtocolVersion: Version311}
	require.Error(t, pk.Decode([]byte{0xB0, 0x03, 0x00, 0x0A, 0x00}))
}

func TestAuthRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{"omitted variable header", []byte{0xF0, 0x00}},
		{"continue authentication", []byte{0xF0, 0x01, 0x18}},
		{"re-authenticate with auth method", []byte{0xF0, 0x0A, 0x19,
			0x08, 0x15, 0x00, 0x05, 'm', 'e', 't', 'h', 'd'}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: Version5}
			require.NoError(t, pk.Decode(c.raw))
			out, err := pk.Encode()
			require.NoError(t, err)
			assert.Equal(t, c.raw, out)
		})
	}
}

func TestAuthMalformed(t *testing.T) {
	// AUTH is only valid for MQTT 5.0.
	pk := Packet{ProtocolVersion: Version311}
	err := pk.Decode([]byte{0xF0, 0x00})
	require.Error(t, err)
	assert.Equal(t, CodeProtocolError, err.(Code).Code)

	// Reason code 0x10 is not valid on AUTH.
	pk = Packet{ProtocolVersion: Version5}
	err = pk.Decode([]byte{0xF0, 0x01, 0x10})
	require.Error(t, err)
	assert.Equal(t, CodeProtocolError, err.(Code).Code)
}

func TestSubscribeEncodeValidate(t *testing.T) {
	// SUBSCRIBE with no filters must not encode.
	pk := Packet{FixedHeader: FixedHeader{Type: Subscribe}, PacketID: 1}
	_, err := pk.Encode()
	require.Error(t, err)

	// UNSUBSCRIBE with no filters must not encode.
	pk = Packet{FixedHeader: FixedHeader{Type: Unsubscribe}, PacketID: 1}
	_, err = pk.Encode()
	require.Error(t, err)

	// SUBACK with no reason codes must not encode.
	pk = Packet{FixedHeader: FixedHeader{Type: Suback}, PacketID: 1}
	_, err = pk.Encode()
	require.Error(t, err)
}
