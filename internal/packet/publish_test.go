package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// publishV311Qos0 is a QoS 0 PUBLISH with retain on topic a/b.
var publishV311Qos0 = []byte{
	0x31, 0x0A, // fixed header: PUBLISH retain, remaining 10
	0x00, 0x03, 'a', '/', 'b', // topic name
	'h', 'e', 'l', 'l', 'o', // payload
}

// publishV5Qos1 is a QoS 1 PUBLISH with properties.
var publishV5Qos1 = []byte{
	0x32, 0x10, // fixed header: PUBLISH qos1, remaining 16
	0x00, 0x03, 'a', '/', 'b', // topic name
	0x00, 0x2A, // packet identifier 42
	0x07,       // property length 7
	0x01, 0x01, // payload format indicator 1
	0x02, 0x00, 0x00, 0x00, 0x3C, // message expiry 60
	'x', // payload
}

func TestPublishRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		raw     []byte
		assert  func(t *testing.T, pk *Packet)
	}{
		{
			name:    "v3 qos0 retain",
			version: Version311,
			raw:     publishV311Qos0,
			assert: func(t *testing.T, pk *Packet) {
				assert.Equal(t, "a/b", pk.TopicName)
				assert.Equal(t, []byte("hello"), pk.Payload)
				assert.True(t, pk.FixedHeader.Retain)
				assert.Equal(t, byte(0), pk.FixedHeader.Qos)
				assert.Equal(t, uint16(0), pk.PacketID)
			},
		},
		{
			name:    "v5 qos1 properties",
			version: Version5,
			raw:     publishV5Qos1,
			assert: func(t *testing.T, pk *Packet) {
				assert.Equal(t, "a/b", pk.TopicName)
				assert.Equal(t, uint16(42), pk.PacketID)
				assert.Equal(t, []byte("x"), pk.Payload)
				assert.Equal(t, byte(1), pk.Properties.PayloadFormat)
				assert.Equal(t, uint32(60), pk.Properties.MessageExpiry)
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

func TestPublishDecodeMalformed(t *testing.T) {
	cases := map[string]struct {
		version byte
		raw     []byte
		code    byte
	}{
		"qos3 flags": {
			version: Version311,
			raw:     []byte{0x36, 0x05, 0x00, 0x01, 'a', 'h', 'i'},
			code:    CodeMalformedPacket,
		},
		"wildcard in topic name": {
			version: Version311,
			raw:     []byte{0x30, 0x05, 0x00, 0x03, 'a', '+', 'b'},
			code:    CodeTopicNameInvalid,
		},
		"empty topic v3": {
			version: Version311,
			raw:     []byte{0x30, 0x02, 0x00, 0x00},
			code:    CodeProtocolError,
		},
		"empty topic v5 without alias": {
			version: Version5,
			raw:     []byte{0x30, 0x03, 0x00, 0x00, 0x00},
			code:    CodeProtocolError,
		},
		"qos1 packet id zero": {
			version: Version311,
			raw:     []byte{0x32, 0x05, 0x00, 0x01, 'a', 0x00, 0x00},
			code:    CodeProtocolError,
		},
		"truncated topic": {
			version: Version311,
			raw:     []byte{0x30, 0x02, 0x00, 0x05},
			code:    CodeMalformedPacket,
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

func TestPublishTopicAliasV5(t *testing.T) {
	// Zero length topic name with Topic Alias property is permitted (v5).
	raw := []byte{
		0x30, 0x07,
		0x00, 0x00, // empty topic name
		0x03, 0x23, 0x00, 0x05, // properties: topic alias 5
		0xFF, // payload
	}
	pk := Packet{ProtocolVersion: Version5}
	require.NoError(t, pk.Decode(raw))
	assert.Equal(t, "", pk.TopicName)
	assert.Equal(t, uint16(5), pk.Properties.TopicAlias)
	assert.Equal(t, []byte{0xFF}, pk.Payload)
	out, err := pk.Encode()
	require.NoError(t, err)
	assert.Equal(t, raw, out)
}

func TestPublishEncodeMaxSize(t *testing.T) {
	pk := Packet{
		FixedHeader: FixedHeader{Type: Publish},
		TopicName:   "a/b",
		Payload:     []byte("hello"),
		Mods:        Mods{MaxSize: 4},
	}
	_, err := pk.Encode()
	require.Error(t, err)
	assert.Equal(t, CodePacketTooLarge, err.(Code).Code)
}

func TestPubackFamilyRoundTrip(t *testing.T) {
	cases := []struct {
		name    string
		version byte
		raw     []byte
	}{
		// MQTT 3.1.1: remaining length exactly 2.
		{"v3 puback", Version311, []byte{0x40, 0x02, 0x00, 0x2A}},
		{"v3 pubrec", Version311, []byte{0x50, 0x02, 0x00, 0x2A}},
		{"v3 pubrel", Version311, []byte{0x62, 0x02, 0x00, 0x2A}},
		{"v3 pubcomp", Version311, []byte{0x70, 0x02, 0x00, 0x2A}},
		// MQTT 5.0 shortest form: success with no properties omits the rest.
		{"v5 puback short", Version5, []byte{0x40, 0x02, 0x00, 0x2A}},
		{"v5 pubrec short", Version5, []byte{0x50, 0x02, 0x00, 0x2A}},
		{"v5 pubrel short", Version5, []byte{0x62, 0x02, 0x00, 0x2A}},
		{"v5 pubcomp short", Version5, []byte{0x70, 0x02, 0x00, 0x2A}},
		// v5 with reason code only.
		{"v5 pubrec no matching subscribers", Version5, []byte{0x50, 0x03, 0x00, 0x2A, 0x10}},
		{"v5 pubrel not found", Version5, []byte{0x62, 0x03, 0x00, 0x2A, 0x92}},
		// v5 with reason code and empty property length.
		{"v5 puback reason empty props", Version5, []byte{0x40, 0x04, 0x00, 0x2A, 0x10, 0x00}},
		// v5 with reason code and reason string property.
		{"v5 puback reason string", Version5,
			[]byte{0x40, 0x09, 0x00, 0x2A, 0x87, 0x05, 0x1F, 0x00, 0x02, 'n', 'o'}},
	}
	// shortestForm maps inputs whose canonical re-encoding is shorter
	// (the encoder omits an empty property length).
	shortestForm := map[string][]byte{
		"v5 puback reason empty props": {0x40, 0x03, 0x00, 0x2A, 0x10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: c.version}
			require.NoError(t, pk.Decode(c.raw))
			out, err := pk.Encode()
			require.NoError(t, err)
			if want, ok := shortestForm[c.name]; ok {
				assert.Equal(t, want, out)
			} else {
				assert.Equal(t, c.raw, out)
			}
		})
	}
}

func TestPubackFamilyMalformed(t *testing.T) {
	cases := map[string]struct {
		version byte
		raw     []byte
		code    byte
	}{
		"v3 puback extra bytes": {
			version: Version311,
			raw:     []byte{0x40, 0x03, 0x00, 0x2A, 0x00},
			code:    CodeMalformedPacket,
		},
		"packet id zero": {
			version: Version311,
			raw:     []byte{0x40, 0x02, 0x00, 0x00},
			code:    CodeProtocolError,
		},
		"truncated packet id": {
			version: Version311,
			raw:     []byte{0x40, 0x01, 0x00},
			code:    CodeMalformedPacket,
		},
		"v5 invalid reason code": {
			version: Version5,
			raw:     []byte{0x40, 0x03, 0x00, 0x2A, 0x04},
			code:    CodeProtocolError,
		},
		"v5 pubrel reason not in set": {
			version: Version5,
			raw:     []byte{0x62, 0x03, 0x00, 0x2A, 0x10},
			code:    CodeProtocolError,
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

func TestPubackFamilyEncodeValidate(t *testing.T) {
	pk := Packet{FixedHeader: FixedHeader{Type: Puback}}
	_, err := pk.Encode()
	require.Error(t, err, "packet id 0 must be rejected")

	// v5 reason code not valid for the packet type.
	pk = Packet{
		FixedHeader:     FixedHeader{Type: Pubcomp},
		ProtocolVersion: Version5,
		PacketID:        1,
		ReasonCode:      CodeGrantedQos1,
	}
	_, err = pk.Encode()
	require.Error(t, err)
}
