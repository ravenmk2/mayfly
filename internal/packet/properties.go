package packet

// MQTT v5 property identifiers (MQTT 5.0 specification section 2.2.2.1).
const (
	PropPayloadFormat        byte = 0x01
	PropMessageExpiry        byte = 0x02
	PropContentType          byte = 0x03
	PropResponseTopic        byte = 0x08
	PropCorrelationData      byte = 0x09
	PropSubscriptionID       byte = 0x0B
	PropSessionExpiry        byte = 0x11
	PropAssignedClientID     byte = 0x12
	PropServerKeepalive      byte = 0x13
	PropAuthMethod           byte = 0x15
	PropAuthData             byte = 0x16
	PropRequestProblemInfo   byte = 0x17
	PropWillDelay            byte = 0x18
	PropRequestResponseInfo  byte = 0x19
	PropResponseInfo         byte = 0x1A
	PropServerReference      byte = 0x1C
	PropReasonString         byte = 0x1F
	PropReceiveMaximum       byte = 0x21
	PropTopicAliasMaximum    byte = 0x22
	PropTopicAlias           byte = 0x23
	PropMaximumQos           byte = 0x24
	PropRetainAvailable      byte = 0x25
	PropUserProperty         byte = 0x26
	PropMaximumPacketSize    byte = 0x27
	PropWildcardSubAvailable byte = 0x28
	PropSubIDAvailable       byte = 0x29
	PropSharedSubAvailable   byte = 0x2A
)

// UserProperty is a UTF-8 string pair property (identifier 0x26) that may be
// repeated on a packet.
type UserProperty struct {
	Key   string
	Value string
}

// Properties carries the MQTT v5 properties of a packet as a flat struct.
// Every scalar property has an accompanying Flag field so that an unset
// property can be distinguished from a property set to its zero value.
type Properties struct {
	PayloadFormat            byte
	PayloadFormatFlag        bool
	MessageExpiry            uint32
	MessageExpiryFlag        bool
	ContentType              string
	ContentTypeFlag          bool
	ResponseTopic            string
	ResponseTopicFlag        bool
	CorrelationData          []byte
	CorrelationDataFlag      bool
	SubscriptionID           []uint32
	SessionExpiry            uint32
	SessionExpiryFlag        bool
	AssignedClientID         string
	AssignedClientIDFlag     bool
	ServerKeepalive          uint16
	ServerKeepaliveFlag      bool
	AuthMethod               string
	AuthMethodFlag           bool
	AuthData                 []byte
	AuthDataFlag             bool
	RequestProblemInfo       byte
	RequestProblemInfoFlag   bool
	WillDelay                uint32
	WillDelayFlag            bool
	RequestResponseInfo      byte
	RequestResponseInfoFlag  bool
	ResponseInfo             string
	ResponseInfoFlag         bool
	ServerReference          string
	ServerReferenceFlag      bool
	ReasonString             string
	ReasonStringFlag         bool
	ReceiveMaximum           uint16
	ReceiveMaximumFlag       bool
	TopicAliasMaximum        uint16
	TopicAliasMaximumFlag    bool
	TopicAlias               uint16
	TopicAliasFlag           bool
	MaximumQos               byte
	MaximumQosFlag           bool
	RetainAvailable          byte
	RetainAvailableFlag      bool
	User                     []UserProperty
	MaximumPacketSize        uint32
	MaximumPacketSizeFlag    bool
	WildcardSubAvailable     byte
	WildcardSubAvailableFlag bool
	SubIDAvailable           byte
	SubIDAvailableFlag       bool
	SharedSubAvailable       byte
	SharedSubAvailableFlag   bool
}

// isEmpty reports whether no property is set.
func (p *Properties) isEmpty() bool {
	return !p.PayloadFormatFlag &&
		!p.MessageExpiryFlag &&
		!p.ContentTypeFlag &&
		!p.ResponseTopicFlag &&
		!p.CorrelationDataFlag &&
		len(p.SubscriptionID) == 0 &&
		!p.SessionExpiryFlag &&
		!p.AssignedClientIDFlag &&
		!p.ServerKeepaliveFlag &&
		!p.AuthMethodFlag &&
		!p.AuthDataFlag &&
		!p.RequestProblemInfoFlag &&
		!p.WillDelayFlag &&
		!p.RequestResponseInfoFlag &&
		!p.ResponseInfoFlag &&
		!p.ServerReferenceFlag &&
		!p.ReasonStringFlag &&
		!p.ReceiveMaximumFlag &&
		!p.TopicAliasMaximumFlag &&
		!p.TopicAliasFlag &&
		!p.MaximumQosFlag &&
		!p.RetainAvailableFlag &&
		len(p.User) == 0 &&
		!p.MaximumPacketSizeFlag &&
		!p.WildcardSubAvailableFlag &&
		!p.SubIDAvailableFlag &&
		!p.SharedSubAvailableFlag
}

// willPropsContext is the pseudo packet type used when decoding or checking
// Will Properties, which are carried inside the CONNECT payload but follow
// their own validity set (MQTT 5.0 specification section 3.1.3.2).
const willPropsContext byte = 0

// mask builds a bitmask of packet types for the validity table.
func mask(types ...byte) uint16 {
	var m uint16
	for _, t := range types {
		m |= 1 << t
	}
	return m
}

// packetContextName names a packet type or the Will Properties context for
// error messages.
func packetContextName(t byte) string {
	if t == willPropsContext {
		return "Will Properties"
	}
	return TypeNames[t]
}

// validPacketProperties is the "property x packet type" legality table
// (MQTT 5.0 specification section 2.2.2.2): bit t set means the property may
// appear on packet type t. Bit 0 (willPropsContext) covers Will Properties.
var validPacketProperties = map[byte]uint16{
	PropPayloadFormat:        mask(Publish) | mask(willPropsContext),
	PropMessageExpiry:        mask(Publish) | mask(willPropsContext),
	PropContentType:          mask(Publish) | mask(willPropsContext),
	PropResponseTopic:        mask(Publish) | mask(willPropsContext),
	PropCorrelationData:      mask(Publish) | mask(willPropsContext),
	PropSubscriptionID:       mask(Publish, Subscribe),
	PropSessionExpiry:        mask(Connect, Connack, Disconnect),
	PropAssignedClientID:     mask(Connack),
	PropServerKeepalive:      mask(Connack),
	PropAuthMethod:           mask(Connect, Connack, Auth),
	PropAuthData:             mask(Connect, Connack, Auth),
	PropRequestProblemInfo:   mask(Connect),
	PropWillDelay:            mask(willPropsContext),
	PropRequestResponseInfo:  mask(Connect),
	PropResponseInfo:         mask(Connack),
	PropServerReference:      mask(Connack, Disconnect),
	PropReasonString:         mask(Connack, Puback, Pubrec, Pubrel, Pubcomp, Suback, Unsuback, Disconnect, Auth),
	PropReceiveMaximum:       mask(Connect, Connack),
	PropTopicAliasMaximum:    mask(Connect, Connack),
	PropTopicAlias:           mask(Publish),
	PropMaximumQos:           mask(Connack),
	PropRetainAvailable:      mask(Connack),
	PropUserProperty:         mask(Connect, Connack, Publish, Puback, Pubrec, Pubrel, Pubcomp, Subscribe, Suback, Unsubscribe, Unsuback, Disconnect, Auth) | mask(willPropsContext),
	PropMaximumPacketSize:    mask(Connect, Connack),
	PropWildcardSubAvailable: mask(Connack),
	PropSubIDAvailable:       mask(Connack),
	PropSharedSubAvailable:   mask(Connack),
}

// decode reads the v5 properties from the front of b (a variable byte integer
// property length followed by the properties [MQTT-2.2.2-1]) for the given
// packet type, returning the number of bytes consumed.
func (p *Properties) decode(b []byte, packetType byte) (int, error) {
	total, n, err := decodeVarint(b)
	if err != nil {
		return 0, err
	}
	if len(b)-n < total {
		return 0, errMalformed("property length %d exceeds available %d bytes", total, len(b)-n)
	}
	props := b[n : n+total]
	for len(props) > 0 {
		// Property identifiers are encoded as variable byte integers.
		id, m, err := decodeVarint(props)
		if err != nil {
			return 0, err
		}
		props = props[m:]
		allowed, ok := validPacketProperties[byte(id)]
		if !ok || id > 0xFF {
			return 0, errProtocol("unknown property identifier 0x%x", id)
		}
		if allowed&(1<<packetType) == 0 {
			// A property that is not valid for this packet type is a
			// Protocol Error [MQTT-2.2.2-2].
			return 0, errProtocol("property 0x%x not valid on %s", id, packetContextName(packetType))
		}
		d, err := p.decodeValue(byte(id), props)
		if err != nil {
			return 0, err
		}
		props = props[d:]
	}
	return n + total, nil
}

// decodeValue decodes a single property value from the front of b, returning
// the number of bytes consumed. Including a non-repeatable property twice is
// a Protocol Error.
func (p *Properties) decodeValue(id byte, b []byte) (int, error) {
	// set flags a scalar property as present, rejecting duplicates.
	set := func(flag *bool) error {
		if *flag {
			return errProtocol("property 0x%x included more than once", id)
		}
		*flag = true
		return nil
	}
	// boolean reads a byte property restricted to 0 or 1.
	boolean := func(flag *bool, dst *byte) (int, error) {
		if len(b) < 1 {
			return 0, errMalformed("truncated property 0x%x", id)
		}
		if b[0] > 1 {
			return 0, errProtocol("property 0x%x must be 0 or 1, got %d", id, b[0])
		}
		if err := set(flag); err != nil {
			return 0, err
		}
		*dst = b[0]
		return 1, nil
	}
	switch id {
	case PropPayloadFormat:
		return boolean(&p.PayloadFormatFlag, &p.PayloadFormat)
	case PropRequestProblemInfo:
		return boolean(&p.RequestProblemInfoFlag, &p.RequestProblemInfo)
	case PropRequestResponseInfo:
		return boolean(&p.RequestResponseInfoFlag, &p.RequestResponseInfo)
	case PropMaximumQos:
		return boolean(&p.MaximumQosFlag, &p.MaximumQos)
	case PropRetainAvailable:
		return boolean(&p.RetainAvailableFlag, &p.RetainAvailable)
	case PropWildcardSubAvailable:
		return boolean(&p.WildcardSubAvailableFlag, &p.WildcardSubAvailable)
	case PropSubIDAvailable:
		return boolean(&p.SubIDAvailableFlag, &p.SubIDAvailable)
	case PropSharedSubAvailable:
		return boolean(&p.SharedSubAvailableFlag, &p.SharedSubAvailable)
	case PropMessageExpiry:
		v, n, err := decodeUint32(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.MessageExpiryFlag); err != nil {
			return 0, err
		}
		p.MessageExpiry = v
		return n, nil
	case PropSessionExpiry:
		v, n, err := decodeUint32(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.SessionExpiryFlag); err != nil {
			return 0, err
		}
		p.SessionExpiry = v
		return n, nil
	case PropWillDelay:
		v, n, err := decodeUint32(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.WillDelayFlag); err != nil {
			return 0, err
		}
		p.WillDelay = v
		return n, nil
	case PropMaximumPacketSize:
		v, n, err := decodeUint32(b)
		if err != nil {
			return 0, err
		}
		if v == 0 {
			// Maximum Packet Size of 0 is a Protocol Error (spec 3.1.2.11).
			return 0, errProtocol("maximum packet size property must not be 0")
		}
		if err := set(&p.MaximumPacketSizeFlag); err != nil {
			return 0, err
		}
		p.MaximumPacketSize = v
		return n, nil
	case PropServerKeepalive:
		v, n, err := decodeUint16(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.ServerKeepaliveFlag); err != nil {
			return 0, err
		}
		p.ServerKeepalive = v
		return n, nil
	case PropReceiveMaximum:
		v, n, err := decodeUint16(b)
		if err != nil {
			return 0, err
		}
		if v == 0 {
			// Receive Maximum of 0 is a Protocol Error (spec 3.1.2.11).
			return 0, errProtocol("receive maximum property must not be 0")
		}
		if err := set(&p.ReceiveMaximumFlag); err != nil {
			return 0, err
		}
		p.ReceiveMaximum = v
		return n, nil
	case PropTopicAliasMaximum:
		v, n, err := decodeUint16(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.TopicAliasMaximumFlag); err != nil {
			return 0, err
		}
		p.TopicAliasMaximum = v
		return n, nil
	case PropTopicAlias:
		v, n, err := decodeUint16(b)
		if err != nil {
			return 0, err
		}
		if v == 0 {
			// A Topic Alias of 0 is a Protocol Error (spec 3.3.2.3).
			return 0, errProtocol("topic alias property must not be 0")
		}
		if err := set(&p.TopicAliasFlag); err != nil {
			return 0, err
		}
		p.TopicAlias = v
		return n, nil
	case PropSubscriptionID:
		// Repeatable on PUBLISH; the SUBSCRIBE codec rejects multiples.
		v, n, err := decodeVarint(b)
		if err != nil {
			return 0, err
		}
		if v == 0 {
			// Subscription Identifier of 0 is a Protocol Error (spec 3.8.2.1).
			return 0, errProtocol("subscription identifier property must not be 0")
		}
		p.SubscriptionID = append(p.SubscriptionID, uint32(v))
		return n, nil
	case PropContentType:
		v, n, err := decodeString(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.ContentTypeFlag); err != nil {
			return 0, err
		}
		p.ContentType = v
		return n, nil
	case PropResponseTopic:
		v, n, err := decodeString(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.ResponseTopicFlag); err != nil {
			return 0, err
		}
		p.ResponseTopic = v
		return n, nil
	case PropAssignedClientID:
		v, n, err := decodeString(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.AssignedClientIDFlag); err != nil {
			return 0, err
		}
		p.AssignedClientID = v
		return n, nil
	case PropAuthMethod:
		v, n, err := decodeString(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.AuthMethodFlag); err != nil {
			return 0, err
		}
		p.AuthMethod = v
		return n, nil
	case PropResponseInfo:
		v, n, err := decodeString(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.ResponseInfoFlag); err != nil {
			return 0, err
		}
		p.ResponseInfo = v
		return n, nil
	case PropServerReference:
		v, n, err := decodeString(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.ServerReferenceFlag); err != nil {
			return 0, err
		}
		p.ServerReference = v
		return n, nil
	case PropReasonString:
		v, n, err := decodeString(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.ReasonStringFlag); err != nil {
			return 0, err
		}
		p.ReasonString = v
		return n, nil
	case PropCorrelationData:
		v, n, err := decodeBytes(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.CorrelationDataFlag); err != nil {
			return 0, err
		}
		p.CorrelationData = v
		return n, nil
	case PropAuthData:
		v, n, err := decodeBytes(b)
		if err != nil {
			return 0, err
		}
		if err := set(&p.AuthDataFlag); err != nil {
			return 0, err
		}
		p.AuthData = v
		return n, nil
	case PropUserProperty:
		// User Property is repeatable on every packet that allows it.
		k, v, n, err := decodeStringPair(b)
		if err != nil {
			return 0, err
		}
		p.User = append(p.User, UserProperty{Key: k, Value: v})
		return n, nil
	default:
		return 0, errProtocol("unknown property identifier 0x%x", id)
	}
}

// encode appends the properties to dst as a variable byte integer length
// prefix followed by the properties in ascending identifier order
// [MQTT-2.2.2-1]. The buffer pool is used for the intermediate encoding.
func (p *Properties) encode(dst []byte) []byte {
	bp := getBuffer()
	defer putBuffer(bp)
	body := (*bp)[:0]
	if p.PayloadFormatFlag {
		body = append(body, PropPayloadFormat, p.PayloadFormat)
	}
	if p.MessageExpiryFlag {
		body = append(body, PropMessageExpiry)
		body = encodeUint32(body, p.MessageExpiry)
	}
	if p.ContentTypeFlag {
		body = append(body, PropContentType)
		body = encodeString(body, p.ContentType)
	}
	if p.ResponseTopicFlag {
		body = append(body, PropResponseTopic)
		body = encodeString(body, p.ResponseTopic)
	}
	if p.CorrelationDataFlag {
		body = append(body, PropCorrelationData)
		body = encodeBytes(body, p.CorrelationData)
	}
	for _, id := range p.SubscriptionID {
		body = append(body, PropSubscriptionID)
		body = encodeVarint(body, int(id))
	}
	if p.SessionExpiryFlag {
		body = append(body, PropSessionExpiry)
		body = encodeUint32(body, p.SessionExpiry)
	}
	if p.AssignedClientIDFlag {
		body = append(body, PropAssignedClientID)
		body = encodeString(body, p.AssignedClientID)
	}
	if p.ServerKeepaliveFlag {
		body = append(body, PropServerKeepalive)
		body = encodeUint16(body, p.ServerKeepalive)
	}
	if p.AuthMethodFlag {
		body = append(body, PropAuthMethod)
		body = encodeString(body, p.AuthMethod)
	}
	if p.AuthDataFlag {
		body = append(body, PropAuthData)
		body = encodeBytes(body, p.AuthData)
	}
	if p.RequestProblemInfoFlag {
		body = append(body, PropRequestProblemInfo, p.RequestProblemInfo)
	}
	if p.WillDelayFlag {
		body = append(body, PropWillDelay)
		body = encodeUint32(body, p.WillDelay)
	}
	if p.RequestResponseInfoFlag {
		body = append(body, PropRequestResponseInfo, p.RequestResponseInfo)
	}
	if p.ResponseInfoFlag {
		body = append(body, PropResponseInfo)
		body = encodeString(body, p.ResponseInfo)
	}
	if p.ServerReferenceFlag {
		body = append(body, PropServerReference)
		body = encodeString(body, p.ServerReference)
	}
	if p.ReasonStringFlag {
		body = append(body, PropReasonString)
		body = encodeString(body, p.ReasonString)
	}
	if p.ReceiveMaximumFlag {
		body = append(body, PropReceiveMaximum)
		body = encodeUint16(body, p.ReceiveMaximum)
	}
	if p.TopicAliasMaximumFlag {
		body = append(body, PropTopicAliasMaximum)
		body = encodeUint16(body, p.TopicAliasMaximum)
	}
	if p.TopicAliasFlag {
		body = append(body, PropTopicAlias)
		body = encodeUint16(body, p.TopicAlias)
	}
	if p.MaximumQosFlag {
		body = append(body, PropMaximumQos, p.MaximumQos)
	}
	if p.RetainAvailableFlag {
		body = append(body, PropRetainAvailable, p.RetainAvailable)
	}
	for _, u := range p.User {
		body = append(body, PropUserProperty)
		body = encodeString(body, u.Key)
		body = encodeString(body, u.Value)
	}
	if p.MaximumPacketSizeFlag {
		body = append(body, PropMaximumPacketSize)
		body = encodeUint32(body, p.MaximumPacketSize)
	}
	if p.WildcardSubAvailableFlag {
		body = append(body, PropWildcardSubAvailable, p.WildcardSubAvailable)
	}
	if p.SubIDAvailableFlag {
		body = append(body, PropSubIDAvailable, p.SubIDAvailable)
	}
	if p.SharedSubAvailableFlag {
		body = append(body, PropSharedSubAvailable, p.SharedSubAvailable)
	}
	dst = encodeVarint(dst, len(body))
	return append(dst, body...)
}

// encodeValidated encodes the properties after checking that every set
// property is valid for the given packet type [MQTT-2.2.2-2].
func (p *Properties) encodeValidated(dst []byte, packetType byte) ([]byte, error) {
	if err := p.validateFor(packetType); err != nil {
		return nil, err
	}
	return p.encode(dst), nil
}

// validateFor checks the set properties against the validity table.
func (p *Properties) validateFor(packetType byte) error {
	check := func(id byte, flag bool) error {
		if !flag {
			return nil
		}
		allowed, ok := validPacketProperties[id]
		if !ok || allowed&(1<<packetType) == 0 {
			return errProtocol("property 0x%x not valid on %s", id, packetContextName(packetType))
		}
		return nil
	}
	checks := []struct {
		id   byte
		flag bool
	}{
		{PropPayloadFormat, p.PayloadFormatFlag},
		{PropMessageExpiry, p.MessageExpiryFlag},
		{PropContentType, p.ContentTypeFlag},
		{PropResponseTopic, p.ResponseTopicFlag},
		{PropCorrelationData, p.CorrelationDataFlag},
		{PropSubscriptionID, len(p.SubscriptionID) > 0},
		{PropSessionExpiry, p.SessionExpiryFlag},
		{PropAssignedClientID, p.AssignedClientIDFlag},
		{PropServerKeepalive, p.ServerKeepaliveFlag},
		{PropAuthMethod, p.AuthMethodFlag},
		{PropAuthData, p.AuthDataFlag},
		{PropRequestProblemInfo, p.RequestProblemInfoFlag},
		{PropWillDelay, p.WillDelayFlag},
		{PropRequestResponseInfo, p.RequestResponseInfoFlag},
		{PropResponseInfo, p.ResponseInfoFlag},
		{PropServerReference, p.ServerReferenceFlag},
		{PropReasonString, p.ReasonStringFlag},
		{PropReceiveMaximum, p.ReceiveMaximumFlag},
		{PropTopicAliasMaximum, p.TopicAliasMaximumFlag},
		{PropTopicAlias, p.TopicAliasFlag},
		{PropMaximumQos, p.MaximumQosFlag},
		{PropRetainAvailable, p.RetainAvailableFlag},
		{PropUserProperty, len(p.User) > 0},
		{PropMaximumPacketSize, p.MaximumPacketSizeFlag},
		{PropWildcardSubAvailable, p.WildcardSubAvailableFlag},
		{PropSubIDAvailable, p.SubIDAvailableFlag},
		{PropSharedSubAvailable, p.SharedSubAvailableFlag},
	}
	for _, c := range checks {
		if err := check(c.id, c.flag); err != nil {
			return err
		}
	}
	// The Subscription Identifier may appear at most once on SUBSCRIBE.
	if packetType == Subscribe && len(p.SubscriptionID) > 1 {
		return errProtocol("subscription identifier included more than once on SUBSCRIBE")
	}
	return nil
}
