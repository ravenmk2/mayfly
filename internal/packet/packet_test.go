package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// goldenCase is a golden byte table entry pairing exact wire bytes with the
// packet they decode to (TPacketCase style).
type goldenCase struct {
	name    string
	version byte
	raw     []byte
	packet  Packet
	// enc, when non-nil, is the canonical re-encoding if it differs from raw
	// (e.g. shortest-form v5 acks).
	enc []byte
}

func goldenCases() []goldenCase {
	return []goldenCase{
		{
			name: "connect v3.1.1 minimal",
			raw:  connectV311,
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Connect, Remaining: 16},
				ProtocolVersion: Version311,
				Connect: ConnectParams{
					ProtocolName:     "MQTT",
					ProtocolVersion:  Version311,
					Clean:            true,
					Keepalive:        60,
					ClientIdentifier: "test",
				},
			},
		},
		{
			name: "connect v5 full",
			raw:  connectV5,
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Connect, Remaining: 37},
				ProtocolVersion: Version5,
				Connect: ConnectParams{
					ProtocolName:     "MQTT",
					ProtocolVersion:  Version5,
					Clean:            true,
					Keepalive:        10,
					ClientIdentifier: "c1",
					WillFlag:         true,
					WillQos:          1,
					WillRetain:       true,
					WillTopic:        "w/t",
					WillPayload:      []byte("bye"),
					UsernameFlag:     true,
					Username:         "u",
					PasswordFlag:     true,
					Password:         []byte("p"),
				},
				Properties: Properties{SessionExpiry: 60, SessionExpiryFlag: true},
			},
		},
		{
			name:    "connack v3.1.1 accepted",
			version: Version311,
			raw:     []byte{0x20, 0x02, 0x00, 0x00},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Connack, Remaining: 2},
				ProtocolVersion: Version311,
			},
		},
		{
			name:    "connack v3.1.1 server unavailable",
			version: Version311,
			raw:     []byte{0x20, 0x02, 0x00, 0x03},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Connack, Remaining: 2},
				ProtocolVersion: Version311,
				ReasonCode:      3,
			},
		},
		{
			name:    "connack v5 with receive maximum",
			version: Version5,
			raw: []byte{0x20, 0x06, 0x01, 0x00,
				0x03, 0x21, 0x00, 0x64}, // receive maximum 100
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Connack, Remaining: 6},
				ProtocolVersion: Version5,
				SessionPresent:  true,
				Properties:      Properties{ReceiveMaximum: 100, ReceiveMaximumFlag: true},
			},
		},
		{
			name:    "publish v3.1.1 qos0 retain",
			version: Version311,
			raw:     publishV311Qos0,
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Publish, Retain: true, Remaining: 10},
				ProtocolVersion: Version311,
				TopicName:       "a/b",
				Payload:         []byte("hello"),
			},
		},
		{
			name:    "publish v5 qos1 dup with properties",
			version: Version5,
			raw:     publishV5Qos1,
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Publish, Qos: 1, Remaining: 16},
				ProtocolVersion: Version5,
				PacketID:        42,
				TopicName:       "a/b",
				Payload:         []byte("x"),
				Properties: Properties{
					PayloadFormat:     1,
					PayloadFormatFlag: true,
					MessageExpiry:     60,
					MessageExpiryFlag: true,
				},
			},
		},
		{
			name:    "puback v3.1.1",
			version: Version311,
			raw:     []byte{0x40, 0x02, 0x00, 0x2A},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Puback, Remaining: 2},
				ProtocolVersion: Version311,
				PacketID:        42,
			},
		},
		{
			name:    "pubrec v5 no matching subscribers",
			version: Version5,
			raw:     []byte{0x50, 0x03, 0x00, 0x2A, 0x10},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Pubrec, Remaining: 3},
				ProtocolVersion: Version5,
				PacketID:        42,
				ReasonCode:      CodeNoMatchingSubscribers,
			},
		},
		{
			name:    "pubrel v5 not found",
			version: Version5,
			raw:     []byte{0x62, 0x03, 0x00, 0x2A, 0x92},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Pubrel, Remaining: 3},
				ProtocolVersion: Version5,
				PacketID:        42,
				ReasonCode:      CodePacketIdentifierNotFound,
			},
		},
		{
			name:    "pubcomp v5 shortest form",
			version: Version5,
			raw:     []byte{0x70, 0x02, 0x00, 0x2A},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Pubcomp, Remaining: 2},
				ProtocolVersion: Version5,
				PacketID:        42,
			},
		},
		{
			name:    "subscribe v3.1.1",
			version: Version311,
			raw:     []byte{0x82, 0x08, 0x00, 0x0A, 0x00, 0x03, 'a', '/', 'b', 0x01},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Subscribe, Remaining: 8},
				ProtocolVersion: Version311,
				PacketID:        10,
				Filters:         []Subscription{{Filter: "a/b", Qos: 1}},
			},
		},
		{
			name:    "subscribe v5 with subscription id",
			version: Version5,
			raw: []byte{0x82, 0x11, 0x00, 0x0A,
				0x02, 0x0B, 0x03,
				0x00, 0x03, 'a', '/', 'b', 0x01,
				0x00, 0x03, 'c', '/', '#', 0x2A},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Subscribe, Remaining: 17},
				ProtocolVersion: Version5,
				PacketID:        10,
				Filters: []Subscription{
					{Filter: "a/b", Qos: 1, Identifier: 3},
					{Filter: "c/#", Qos: 2, RetainAsPublished: true, RetainHandling: 2, Identifier: 3},
				},
				Properties: Properties{SubscriptionID: []uint32{3}},
			},
		},
		{
			name:    "suback v3.1.1 mixed",
			version: Version311,
			raw:     []byte{0x90, 0x05, 0x00, 0x0A, 0x00, 0x01, 0x80},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Suback, Remaining: 5},
				ProtocolVersion: Version311,
				PacketID:        10,
				ReasonCodes:     []byte{0, 1, 0x80},
			},
		},
		{
			name:    "suback v5",
			version: Version5,
			raw:     []byte{0x90, 0x05, 0x00, 0x0A, 0x00, 0x00, 0x87},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Suback, Remaining: 5},
				ProtocolVersion: Version5,
				PacketID:        10,
				ReasonCodes:     []byte{0x00, 0x87},
			},
		},
		{
			name:    "unsubscribe v3.1.1",
			version: Version311,
			raw:     []byte{0xA2, 0x07, 0x00, 0x0A, 0x00, 0x03, 'a', '/', 'b'},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Unsubscribe, Remaining: 7},
				ProtocolVersion: Version311,
				PacketID:        10,
				Filters:         []Subscription{{Filter: "a/b"}},
			},
		},
		{
			name:    "unsubscribe v5",
			version: Version5,
			raw:     []byte{0xA2, 0x08, 0x00, 0x0A, 0x00, 0x00, 0x03, 'a', '/', 'b'},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Unsubscribe, Remaining: 8},
				ProtocolVersion: Version5,
				PacketID:        10,
				Filters:         []Subscription{{Filter: "a/b"}},
			},
		},
		{
			name:    "unsuback v3.1.1",
			version: Version311,
			raw:     []byte{0xB0, 0x02, 0x00, 0x0A},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Unsuback, Remaining: 2},
				ProtocolVersion: Version311,
				PacketID:        10,
			},
		},
		{
			name:    "unsuback v5 no subscription existed",
			version: Version5,
			raw:     []byte{0xB0, 0x04, 0x00, 0x0A, 0x00, 0x11},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Unsuback, Remaining: 4},
				ProtocolVersion: Version5,
				PacketID:        10,
				ReasonCodes:     []byte{0x11},
			},
		},
		{
			name:    "pingreq",
			version: Version311,
			raw:     []byte{0xC0, 0x00},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Pingreq},
				ProtocolVersion: Version311,
			},
		},
		{
			name:    "pingresp",
			version: Version5,
			raw:     []byte{0xD0, 0x00},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Pingresp},
				ProtocolVersion: Version5,
			},
		},
		{
			name:    "disconnect v3.1.1",
			version: Version311,
			raw:     []byte{0xE0, 0x00},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Disconnect},
				ProtocolVersion: Version311,
			},
		},
		{
			name:    "disconnect v5 with reason string",
			version: Version5,
			raw: []byte{0xE0, 0x08, 0x8B,
				0x06, 0x1F, 0x00, 0x03, 'b', 'y', 'e'},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Disconnect, Remaining: 8},
				ProtocolVersion: Version5,
				ReasonCode:      CodeServerShuttingDown,
				Properties:      Properties{ReasonString: "bye", ReasonStringFlag: true},
			},
		},
		{
			name:    "auth v5 re-authenticate",
			version: Version5,
			raw: []byte{0xF0, 0x0A, 0x19,
				0x08, 0x15, 0x00, 0x05, 'm', 'e', 't', 'h', 'd'},
			packet: Packet{
				FixedHeader:     FixedHeader{Type: Auth, Remaining: 10},
				ProtocolVersion: Version5,
				ReasonCode:      CodeReAuthenticate,
				Properties:      Properties{AuthMethod: "methd", AuthMethodFlag: true},
			},
		},
	}
}

func TestGoldenDecode(t *testing.T) {
	for _, c := range goldenCases() {
		t.Run(c.name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: c.version}
			require.NoError(t, pk.Decode(c.raw))
			assert.Equal(t, c.packet, pk)
		})
	}
}

func TestGoldenEncode(t *testing.T) {
	for _, c := range goldenCases() {
		t.Run(c.name, func(t *testing.T) {
			pk := c.packet
			pk.ProtocolVersion = c.version
			out, err := pk.Encode()
			require.NoError(t, err)
			want := c.raw
			if c.enc != nil {
				want = c.enc
			}
			assert.Equal(t, want, out)
		})
	}
}

// TestRoundTrip checks that every golden sample survives a decode -> encode
// -> decode cycle with identical semantics.
func TestRoundTrip(t *testing.T) {
	for _, c := range goldenCases() {
		t.Run(c.name, func(t *testing.T) {
			pk := Packet{ProtocolVersion: c.version}
			require.NoError(t, pk.Decode(c.raw))
			out, err := pk.Encode()
			require.NoError(t, err)
			pk2 := Packet{ProtocolVersion: pk.ProtocolVersion}
			require.NoError(t, pk2.Decode(out))
			assert.Equal(t, pk, pk2)
		})
	}
}
