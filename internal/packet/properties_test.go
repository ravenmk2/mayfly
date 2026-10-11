package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPropertiesRoundTrip(t *testing.T) {
	// All properties valid on PUBLISH (spec 2.2.2.2).
	props := Properties{
		PayloadFormat:       1,
		PayloadFormatFlag:   true,
		MessageExpiry:       3600,
		MessageExpiryFlag:   true,
		ContentType:         "application/json",
		ContentTypeFlag:     true,
		ResponseTopic:       "reply/topic",
		ResponseTopicFlag:   true,
		CorrelationData:     []byte{0xDE, 0xAD},
		CorrelationDataFlag: true,
		SubscriptionID:      []uint32{7, 300},
		TopicAlias:          3,
		TopicAliasFlag:      true,
		User: []UserProperty{
			{Key: "k1", Value: "v1"},
			{Key: "k2", Value: "v2"},
		},
	}
	assert.False(t, props.isEmpty())

	enc, err := props.encodeValidated(nil, Publish)
	require.NoError(t, err)
	var dec Properties
	n, err := dec.decode(enc, Publish)
	require.NoError(t, err)
	assert.Equal(t, len(enc), n)
	assert.Equal(t, props, dec)
}

func TestPropertiesRoundTripValid(t *testing.T) {
	props := Properties{
		SessionExpiry:         60,
		SessionExpiryFlag:     true,
		ReceiveMaximum:        10,
		ReceiveMaximumFlag:    true,
		MaximumPacketSize:     1024,
		MaximumPacketSizeFlag: true,
		TopicAliasMaximum:     16,
		TopicAliasMaximumFlag: true,
		User:                  []UserProperty{{Key: "k", Value: "v"}},
	}
	enc, err := props.encodeValidated(nil, Connect)
	require.NoError(t, err)

	var dec Properties
	n, err := dec.decode(enc, Connect)
	require.NoError(t, err)
	assert.Equal(t, len(enc), n)
	assert.Equal(t, props, dec)
}

func TestPropertiesGoldenBytes(t *testing.T) {
	// Session Expiry Interval = 60 followed by one User Property.
	props := Properties{
		SessionExpiry:     60,
		SessionExpiryFlag: true,
		User:              []UserProperty{{Key: "a", Value: "b"}},
	}
	want := []byte{
		0x0C,                         // property length (12 bytes)
		0x11, 0x00, 0x00, 0x00, 0x3C, // session expiry
		0x26, 0x00, 0x01, 'a', 0x00, 0x01, 'b', // user property
	}
	assert.Equal(t, want, props.encode(nil))

	var dec Properties
	n, err := dec.decode(want, Connect)
	require.NoError(t, err)
	assert.Equal(t, len(want), n)
	assert.Equal(t, props, dec)
}

func TestPropertiesDecodeEmpty(t *testing.T) {
	var p Properties
	n, err := p.decode([]byte{0x00}, Connect)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.True(t, p.isEmpty())
}

func TestPropertiesDecodeMalformed(t *testing.T) {
	cases := map[string]struct {
		raw        []byte
		packetType byte
		code       byte
	}{
		"truncated length":          {[]byte{}, Connect, CodeMalformedPacket},
		"length overruns input":     {[]byte{0x05, 0x11}, Connect, CodeMalformedPacket},
		"unknown identifier":        {[]byte{0x02, 0x7F, 0x00}, Connect, CodeProtocolError},
		"not valid for packet type": {[]byte{0x02, 0x22, 0x00}, Publish, CodeProtocolError}, // topic alias maximum on PUBLISH
		"duplicate property":        {[]byte{0x0A, 0x11, 0, 0, 0, 1, 0x11, 0, 0, 0, 2}, Connect, CodeProtocolError},
		"truncated value":           {[]byte{0x02, 0x11, 0x00}, Connect, CodeMalformedPacket},
		"boolean out of range":      {[]byte{0x02, 0x17, 0x02}, Connect, CodeProtocolError},
		"receive maximum zero":      {[]byte{0x03, 0x21, 0x00, 0x00}, Connect, CodeProtocolError},
		"topic alias zero":          {[]byte{0x03, 0x23, 0x00, 0x00}, Publish, CodeProtocolError},
		"maximum packet size zero":  {[]byte{0x05, 0x27, 0, 0, 0, 0}, Connect, CodeProtocolError},
		"subscription id zero":      {[]byte{0x02, 0x0B, 0x00}, Subscribe, CodeProtocolError},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var p Properties
			_, err := p.decode(c.raw, c.packetType)
			require.Error(t, err)
			assert.Equal(t, c.code, err.(Code).Code)
		})
	}
}

func TestPropertiesConnackRoundTrip(t *testing.T) {
	// All properties valid on CONNACK (spec 2.2.2.2).
	props := Properties{
		SessionExpiry:            60,
		SessionExpiryFlag:        true,
		AssignedClientID:         "client-1",
		AssignedClientIDFlag:     true,
		ServerKeepalive:          30,
		ServerKeepaliveFlag:      true,
		AuthMethod:               "SCRAM",
		AuthMethodFlag:           true,
		AuthData:                 []byte{0x01},
		AuthDataFlag:             true,
		ResponseInfo:             "info",
		ResponseInfoFlag:         true,
		ServerReference:          "server-b",
		ServerReferenceFlag:      true,
		ReasonString:             "because",
		ReasonStringFlag:         true,
		ReceiveMaximum:           10,
		ReceiveMaximumFlag:       true,
		TopicAliasMaximum:        16,
		TopicAliasMaximumFlag:    true,
		MaximumQos:               1,
		MaximumQosFlag:           true,
		RetainAvailable:          1,
		RetainAvailableFlag:      true,
		MaximumPacketSize:        1024,
		MaximumPacketSizeFlag:    true,
		WildcardSubAvailable:     1,
		WildcardSubAvailableFlag: true,
		SubIDAvailable:           1,
		SubIDAvailableFlag:       true,
		SharedSubAvailable:       1,
		SharedSubAvailableFlag:   true,
		User:                     []UserProperty{{Key: "k", Value: "v"}},
	}
	enc, err := props.encodeValidated(nil, Connack)
	require.NoError(t, err)
	var dec Properties
	n, err := dec.decode(enc, Connack)
	require.NoError(t, err)
	assert.Equal(t, len(enc), n)
	assert.Equal(t, props, dec)
}

func TestWillPropertiesRoundTrip(t *testing.T) {
	// All properties valid as Will Properties (spec 3.1.3.2).
	props := Properties{
		PayloadFormat:       1,
		PayloadFormatFlag:   true,
		MessageExpiry:       3600,
		MessageExpiryFlag:   true,
		ContentType:         "text/plain",
		ContentTypeFlag:     true,
		ResponseTopic:       "reply/topic",
		ResponseTopicFlag:   true,
		CorrelationData:     []byte{0x01},
		CorrelationDataFlag: true,
		WillDelay:           5,
		WillDelayFlag:       true,
		User:                []UserProperty{{Key: "k", Value: "v"}},
	}
	enc, err := props.encodeValidated(nil, willPropsContext)
	require.NoError(t, err)
	var dec Properties
	n, err := dec.decode(enc, willPropsContext)
	require.NoError(t, err)
	assert.Equal(t, len(enc), n)
	assert.Equal(t, props, dec)
}

func TestPropertiesValidateFor(t *testing.T) {
	p := Properties{TopicAliasMaximum: 4, TopicAliasMaximumFlag: true}
	require.NoError(t, p.validateFor(Connect))
	require.NoError(t, p.validateFor(Connack))
	err := p.validateFor(Publish)
	require.Error(t, err)
	assert.Equal(t, CodeProtocolError, err.(Code).Code)

	// Will Properties context: Will Delay is valid, Receive Maximum is not.
	will := Properties{WillDelay: 10, WillDelayFlag: true}
	require.NoError(t, will.validateFor(willPropsContext))
	bad := Properties{ReceiveMaximum: 10, ReceiveMaximumFlag: true}
	require.Error(t, bad.validateFor(willPropsContext))

	// Subscription Identifier may appear at most once on SUBSCRIBE.
	sub := Properties{SubscriptionID: []uint32{1}}
	require.NoError(t, sub.validateFor(Subscribe))
	sub.SubscriptionID = append(sub.SubscriptionID, 2)
	require.Error(t, sub.validateFor(Subscribe))
	// ...but repeats are fine on PUBLISH.
	require.NoError(t, sub.validateFor(Publish))
}
