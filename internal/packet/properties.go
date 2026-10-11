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
