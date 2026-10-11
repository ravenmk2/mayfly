package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVarintRoundTrip(t *testing.T) {
	cases := []struct {
		value int
		raw   []byte
	}{
		{0, []byte{0x00}},
		{127, []byte{0x7F}},
		{128, []byte{0x80, 0x01}},
		{16383, []byte{0xFF, 0x7F}},
		{16384, []byte{0x80, 0x80, 0x01}},
		{2097151, []byte{0xFF, 0xFF, 0x7F}},
		{2097152, []byte{0x80, 0x80, 0x80, 0x01}},
		{MaxRemainingLength, []byte{0xFF, 0xFF, 0xFF, 0x7F}},
	}
	for _, c := range cases {
		got := encodeVarint(nil, c.value)
		assert.Equal(t, c.raw, got, "encode %d", c.value)
		v, n, err := decodeVarint(c.raw)
		require.NoError(t, err)
		assert.Equal(t, c.value, v)
		assert.Equal(t, len(c.raw), n)
		assert.Equal(t, len(c.raw), varintLen(c.value))
	}
}

func TestDecodeVarintMalformed(t *testing.T) {
	// Truncated: continuation bit set but input ends.
	_, _, err := decodeVarint([]byte{0x80})
	require.Error(t, err)
	assert.Equal(t, CodeMalformedPacket, err.(Code).Code)

	// Five continuation bytes exceed the four byte limit [MQTT-2.1.4].
	_, _, err = decodeVarint([]byte{0x80, 0x80, 0x80, 0x80, 0x01})
	require.Error(t, err)
	assert.Equal(t, CodeMalformedPacket, err.(Code).Code)
}

func TestDecodeString(t *testing.T) {
	s, n, err := decodeString([]byte{0x00, 0x05, 'h', 'e', 'l', 'l', 'o', 0xEE})
	require.NoError(t, err)
	assert.Equal(t, "hello", s)
	assert.Equal(t, 7, n)
}

func TestDecodeStringMalformed(t *testing.T) {
	cases := map[string][]byte{
		"missing length prefix": {},
		"truncated payload":     {0x00, 0x05, 'h', 'i'},
		"invalid utf8":          {0x00, 0x02, 0xFF, 0xFE},
		"contains U+0000":       {0x00, 0x03, 'a', 0x00, 'b'},
		"contains U+FFFF":       {0x00, 0x03, 'a', 0xEF, 0xBF},
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := decodeString(raw)
			require.Error(t, err)
			assert.Equal(t, CodeMalformedPacket, err.(Code).Code)
		})
	}
	// U+FFFF (EF BF BF) and U+FFFE (EF BF BE).
	_, _, err := decodeString([]byte{0x00, 0x03, 0xEF, 0xBF, 0xBF})
	require.Error(t, err)
	_, _, err = decodeString([]byte{0x00, 0x03, 0xEF, 0xBF, 0xBE})
	require.Error(t, err)
}

func TestDecodeBytesSafeCopy(t *testing.T) {
	raw := []byte{0x00, 0x02, 'a', 'b'}
	out, n, err := decodeBytes(raw)
	require.NoError(t, err)
	assert.Equal(t, 4, n)
	assert.Equal(t, []byte("ab"), out)
	raw[2] = 'X'
	assert.Equal(t, []byte("ab"), out, "decoded data must not alias the input")
}

func TestFixedHeaderDecode(t *testing.T) {
	cases := []struct {
		name     string
		raw      []byte
		want     FixedHeader
		consumed int
	}{
		{"connect", []byte{0x10, 0x0A}, FixedHeader{Type: Connect, Remaining: 10}, 2},
		{"publish qos1 dup retain", []byte{0x3B, 0x05}, FixedHeader{Type: Publish, Dup: true, Qos: 1, Retain: true, Remaining: 5}, 2},
		{"pubrel flags 0010", []byte{0x62, 0x02}, FixedHeader{Type: Pubrel, Remaining: 2}, 2},
		{"subscribe flags 0010", []byte{0x82, 0x08}, FixedHeader{Type: Subscribe, Remaining: 8}, 2},
		{"unsubscribe flags 0010", []byte{0xA2, 0x06}, FixedHeader{Type: Unsubscribe, Remaining: 6}, 2},
		{"long remaining", []byte{0x30, 0xFF, 0x7F}, FixedHeader{Type: Publish, Remaining: 16383}, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var fh FixedHeader
			n, err := fh.decode(c.raw)
			require.NoError(t, err)
			assert.Equal(t, c.consumed, n)
			assert.Equal(t, c.want, fh)
			assert.Equal(t, c.raw[0], fh.encodeByte())
		})
	}
}

func TestFixedHeaderDecodeBadFlags(t *testing.T) {
	cases := map[string][]byte{
		"reserved type 0":       {0x00, 0x00},
		"connect bad flags":     {0x11, 0x00},
		"pubrel wrong flags":    {0x60, 0x02},
		"subscribe wrong flags": {0x80, 0x00},
		"pingreq bad flags":     {0xC1, 0x00},
		"publish qos3":          {0x36, 0x00},
		"puback bad flags":      {0x4F, 0x02},
	}
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			var fh FixedHeader
			_, err := fh.decode(raw)
			require.Error(t, err)
			assert.Equal(t, CodeMalformedPacket, err.(Code).Code)
		})
	}
}

func TestFixedHeaderDecodeTruncated(t *testing.T) {
	var fh FixedHeader
	_, err := fh.decode(nil)
	require.Error(t, err)
	_, err = fh.decode([]byte{0x10})
	require.Error(t, err)
}

func TestCodeError(t *testing.T) {
	var err error = Code{Code: CodeNotAuthorized}
	assert.Contains(t, err.Error(), "Not authorized")
	assert.Contains(t, err.Error(), "0x87")

	custom := Code{Code: CodeMalformedPacket, Reason: "bad flags"}
	assert.Contains(t, custom.Error(), "bad flags")
}

func TestReasonString(t *testing.T) {
	assert.Equal(t, "Success", ReasonString(CodeSuccess))
	assert.Equal(t, "Malformed Packet", ReasonString(CodeMalformedPacket))
	assert.Contains(t, ReasonString(0x55), "unknown")
}

func TestV5ToV3(t *testing.T) {
	assert.Equal(t, V3ConnackAccepted, V5ToV3(Connack, CodeSuccess))
	assert.Equal(t, V3ConnackUnacceptableProtocolVersion, V5ToV3(Connack, CodeUnsupportedProtocolVersion))
	assert.Equal(t, V3ConnackIdentifierRejected, V5ToV3(Connack, CodeClientIdentifierNotValid))
	assert.Equal(t, V3ConnackServerUnavailable, V5ToV3(Connack, CodeServerUnavailable))
	assert.Equal(t, V3ConnackBadUsernameOrPassword, V5ToV3(Connack, CodeBadUserNameOrPassword))
	assert.Equal(t, V3ConnackNotAuthorized, V5ToV3(Connack, CodeBanned))

	assert.Equal(t, byte(0), V5ToV3(Suback, CodeGrantedQos0))
	assert.Equal(t, byte(1), V5ToV3(Suback, CodeGrantedQos1))
	assert.Equal(t, byte(2), V5ToV3(Suback, CodeGrantedQos2))
	assert.Equal(t, V3SubackFailure, V5ToV3(Suback, CodeNotAuthorized))
	assert.Equal(t, V3SubackFailure, V5ToV3(Suback, CodeUnspecifiedError))

	// Packet types without a v3 mapping pass through.
	assert.Equal(t, CodeMalformedPacket, V5ToV3(Disconnect, CodeMalformedPacket))
}
