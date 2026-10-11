package packet

import (
	"fmt"
	"time"
)

// MQTT control packet types [MQTT-2.2.1].
const (
	Connect byte = iota + 1
	Connack
	Publish
	Puback
	Pubrec
	Pubrel
	Pubcomp
	Subscribe
	Suback
	Unsubscribe
	Unsuback
	Pingreq
	Pingresp
	Disconnect
	Auth
)

// Protocol version levels used by ProtocolVersion.
const (
	// Version311 is MQTT 3.1.1 (protocol level 4). MQTT 3.1 connections
	// (level 3, "MQIsdp") are normalized to this value.
	Version311 byte = 4
	// Version5 is MQTT 5.0 (protocol level 5).
	Version5 byte = 5
)

// TypeNames maps packet types to their protocol names.
var TypeNames = map[byte]string{
	Connect:     "CONNECT",
	Connack:     "CONNACK",
	Publish:     "PUBLISH",
	Puback:      "PUBACK",
	Pubrec:      "PUBREC",
	Pubrel:      "PUBREL",
	Pubcomp:     "PUBCOMP",
	Subscribe:   "SUBSCRIBE",
	Suback:      "SUBACK",
	Unsubscribe: "UNSUBSCRIBE",
	Unsuback:    "UNSUBACK",
	Pingreq:     "PINGREQ",
	Pingresp:    "PINGRESP",
	Disconnect:  "DISCONNECT",
	Auth:        "AUTH",
}

// requiredFlags holds the fixed header flag bits mandated for each packet
// type [MQTT-2.1.3-1]. PUBLISH flags carry per-message semantics (DUP, QoS,
// RETAIN) and are validated separately, as is the reserved type 0.
var requiredFlags = [16]byte{
	0xff, // 0 is reserved and always invalid
	0,    // CONNECT
	0,    // CONNACK
	0xff, // PUBLISH: flags are message-defined
	0,    // PUBACK
	0,    // PUBREC
	2,    // PUBREL
	0,    // PUBCOMP
	2,    // SUBSCRIBE
	0,    // SUBACK
	2,    // UNSUBSCRIBE
	0,    // UNSUBACK
	0,    // PINGREQ
	0,    // PINGRESP
	0,    // DISCONNECT
	0,    // AUTH
}

// FixedHeader is the fixed header present in every MQTT control packet
// [MQTT-2.2].
type FixedHeader struct {
	Type      byte
	Dup       bool
	Qos       byte
	Retain    bool
	Remaining int
}

// String returns a human readable summary of the fixed header.
func (fh FixedHeader) String() string {
	return fmt.Sprintf("%s dup=%t qos=%d retain=%t remaining=%d",
		TypeNames[fh.Type], fh.Dup, fh.Qos, fh.Retain, fh.Remaining)
}

// decode parses the fixed header from the front of b, returning the number of
// bytes consumed. Flag bits are strictly validated [MQTT-2.1.3-1].
func (fh *FixedHeader) decode(b []byte) (int, error) {
	if len(b) < 2 {
		return 0, errMalformed("fixed header truncated")
	}
	t := b[0] >> 4
	if t < Connect || t > Auth {
		return 0, errMalformed("invalid packet type %d", t)
	}
	flags := b[0] & 0x0f
	fh.Type = t
	if t == Publish {
		fh.Dup = flags&0x08 != 0
		fh.Qos = (flags >> 1) & 0x03
		fh.Retain = flags&0x01 != 0
		if fh.Qos == 3 {
			// A PUBLISH packet MUST NOT have both QoS bits set [MQTT-3.3.1-4].
			return 0, errMalformed("publish with reserved QoS 3")
		}
	} else {
		fh.Dup, fh.Qos, fh.Retain = false, 0, false
		if flags != requiredFlags[t] {
			// Reserved flag bits MUST be set exactly as specified
			// [MQTT-2.1.3-1].
			return 0, errMalformed("invalid fixed header flags 0x%x for %s", flags, TypeNames[t])
		}
	}
	remaining, n, err := decodeVarint(b[1:])
	if err != nil {
		return 0, err
	}
	fh.Remaining = remaining
	return 1 + n, nil
}

// encodeByte returns the first byte of the fixed header. Non-PUBLISH types
// always use their mandated flag bits [MQTT-2.1.3-1].
func (fh FixedHeader) encodeByte() byte {
	if fh.Type == Publish {
		var flags byte
		if fh.Dup {
			flags |= 0x08
		}
		flags |= (fh.Qos & 0x03) << 1
		if fh.Retain {
			flags |= 0x01
		}
		return fh.Type<<4 | flags
	}
	return fh.Type<<4 | requiredFlags[fh.Type]
}

// Subscription represents a single topic filter subscription within a
// SUBSCRIBE or UNSUBSCRIBE packet [MQTT-3.8.3].
type Subscription struct {
	Filter            string
	Qos               byte
	NoLocal           bool
	RetainAsPublished bool
	RetainHandling    byte
	// Identifier mirrors the v5 Subscription Identifier property on decode;
	// it is not used when encoding.
	Identifier uint32
}

// ConnectParams holds the fields of the CONNECT variable header and payload.
type ConnectParams struct {
	ProtocolName     string
	ProtocolVersion  byte
	Clean            bool // Clean Start (v5) / Clean Session (v3)
	Keepalive        uint16
	ClientIdentifier string
	WillFlag         bool
	WillQos          byte
	WillRetain       bool
	WillTopic        string
	WillPayload      []byte
	WillProperties   Properties
	UsernameFlag     bool
	Username         string
	PasswordFlag     bool
	Password         []byte
}

// Mods contains optional modifiers influencing encoding.
type Mods struct {
	// MaxSize is the maximum permitted encoded packet size in bytes;
	// 0 means unlimited.
	MaxSize int
}

// Packet is the single structure carrying all 15 MQTT control packet types
// for both MQTT 3.1.1 and 5.0 (architecture decision A1).
type Packet struct {
	FixedHeader FixedHeader
	// ProtocolVersion selects the codec dialect (Version311 or Version5);
	// properties are encoded only for Version5. Zero is treated as
	// Version311. Decoding a CONNECT overwrites it from the wire.
	ProtocolVersion byte

	PacketID       uint16
	TopicName      string
	Payload        []byte
	Filters        []Subscription // SUBSCRIBE / UNSUBSCRIBE
	ReasonCodes    []byte         // SUBACK / UNSUBACK
	ReasonCode     byte           // CONNACK / PUBACK / PUBREC / PUBREL / PUBCOMP / DISCONNECT / AUTH
	SessionPresent bool           // CONNACK

	Properties Properties    // v5 only
	Connect    ConnectParams // CONNECT

	Mods Mods

	// Broker bookkeeping fields; never encoded.
	Origin  byte
	Created time.Time
	Expiry  time.Time
}

// isV5 reports whether the packet uses the MQTT 5.0 dialect.
func (pk *Packet) isV5() bool {
	return pk.ProtocolVersion == Version5
}

// version returns the effective protocol version, treating zero as MQTT 3.1.1.
func (pk *Packet) version() byte {
	if pk.ProtocolVersion == 0 {
		return Version311
	}
	return pk.ProtocolVersion
}

// Decode decodes exactly one complete MQTT control packet from b. Malformed
// input (bad flags, truncation, overlong Remaining Length, invalid
// properties) is reported as a Code error; Decode never panics.
func (pk *Packet) Decode(b []byte) error {
	n, err := pk.FixedHeader.decode(b)
	if err != nil {
		return err
	}
	body := b[n:]
	if len(body) < pk.FixedHeader.Remaining {
		return errMalformed("%s truncated: remaining length %d, have %d",
			TypeNames[pk.FixedHeader.Type], pk.FixedHeader.Remaining, len(body))
	}
	if len(body) > pk.FixedHeader.Remaining {
		return errMalformed("%s has %d trailing bytes beyond remaining length %d",
			TypeNames[pk.FixedHeader.Type], len(body)-pk.FixedHeader.Remaining, pk.FixedHeader.Remaining)
	}
	pk.ProtocolVersion = pk.version()
	switch pk.FixedHeader.Type {
	case Connect:
		return pk.connectDecode(body)
	case Connack:
		return pk.connackDecode(body)
	case Publish:
		return pk.publishDecode(body)
	case Puback, Pubrec, Pubrel, Pubcomp:
		return pk.pubackDecode(body)
	case Pingreq:
		return pk.pingreqDecode(body)
	case Pingresp:
		return pk.pingrespDecode(body)
	case Disconnect:
		return pk.disconnectDecode(body)
	default:
		return errMalformed("unsupported packet type %d", pk.FixedHeader.Type)
	}
}

// Encode validates and encodes the packet into a newly allocated byte slice.
func (pk *Packet) Encode() ([]byte, error) {
	if err := pk.Validate(); err != nil {
		return nil, err
	}
	bp := getBuffer()
	defer putBuffer(bp)
	if err := pk.encodeBody(bp); err != nil {
		return nil, err
	}
	remaining := len(*bp)
	headerLen := 1 + varintLen(remaining)
	if pk.Mods.MaxSize > 0 && headerLen+remaining > pk.Mods.MaxSize {
		return nil, Code{Code: CodePacketTooLarge,
			Reason: fmt.Sprintf("encoded size %d exceeds maximum %d", headerLen+remaining, pk.Mods.MaxSize)}
	}
	out := make([]byte, 0, headerLen+remaining)
	out = append(out, pk.FixedHeader.encodeByte())
	out = encodeVarint(out, remaining)
	return append(out, *bp...), nil
}

// encodeBody encodes the variable header and payload into dst.
func (pk *Packet) encodeBody(dst *[]byte) error {
	switch pk.FixedHeader.Type {
	case Connect:
		return pk.connectEncode(dst)
	case Connack:
		return pk.connackEncode(dst)
	case Publish:
		return pk.publishEncode(dst)
	case Puback, Pubrec, Pubrel, Pubcomp:
		return pk.pubackEncode(dst)
	case Pingreq, Pingresp:
		return nil
	case Disconnect:
		return pk.disconnectEncode(dst)
	default:
		return errMalformed("unsupported packet type %d", pk.FixedHeader.Type)
	}
}

// Validate checks the semantic constraints of the packet for its type.
func (pk *Packet) Validate() error {
	switch pk.FixedHeader.Type {
	case Connect:
		return pk.connectValidate()
	case Connack:
		return pk.connackValidate()
	case Publish:
		return pk.publishValidate()
	case Puback, Pubrec, Pubrel, Pubcomp:
		return pk.pubackValidate()
	case Disconnect:
		return pk.disconnectValidate()
	default:
		return nil
	}
}
