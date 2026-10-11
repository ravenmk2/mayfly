package packet

import "fmt"

// MQTT v5 reason codes (MQTT 5.0 specification section 2.4). Codes with equal
// numeric values share a constant value: 0x00 is Success, Normal
// Disconnection and Granted QoS 0 depending on the packet it appears in.
const (
	CodeSuccess                           byte = 0x00
	CodeNormalDisconnection               byte = 0x00
	CodeGrantedQos0                       byte = 0x00
	CodeGrantedQos1                       byte = 0x01
	CodeGrantedQos2                       byte = 0x02
	CodeDisconnectWithWillMessage         byte = 0x04
	CodeNoMatchingSubscribers             byte = 0x10
	CodeNoSubscriptionExisted             byte = 0x11
	CodeContinueAuthentication            byte = 0x18
	CodeReAuthenticate                    byte = 0x19
	CodeUnspecifiedError                  byte = 0x80
	CodeMalformedPacket                   byte = 0x81
	CodeProtocolError                     byte = 0x82
	CodeImplementationSpecificError       byte = 0x83
	CodeUnsupportedProtocolVersion        byte = 0x84
	CodeClientIdentifierNotValid          byte = 0x85
	CodeBadUserNameOrPassword             byte = 0x86
	CodeNotAuthorized                     byte = 0x87
	CodeServerUnavailable                 byte = 0x88
	CodeServerBusy                        byte = 0x89
	CodeBanned                            byte = 0x8A
	CodeServerShuttingDown                byte = 0x8B
	CodeBadAuthenticationMethod           byte = 0x8C
	CodeKeepAliveTimeout                  byte = 0x8D
	CodeSessionTakenOver                  byte = 0x8E
	CodeTopicFilterInvalid                byte = 0x8F
	CodeTopicNameInvalid                  byte = 0x90
	CodePacketIdentifierInUse             byte = 0x91
	CodePacketIdentifierNotFound          byte = 0x92
	CodeReceiveMaximumExceeded            byte = 0x93
	CodeTopicAliasInvalid                 byte = 0x94
	CodePacketTooLarge                    byte = 0x95
	CodeMessageRateTooHigh                byte = 0x96
	CodeQuotaExceeded                     byte = 0x97
	CodeAdministrativeAction              byte = 0x98
	CodePayloadFormatInvalid              byte = 0x99
	CodeRetainNotSupported                byte = 0x9A
	CodeQosNotSupported                   byte = 0x9B
	CodeUseAnotherServer                  byte = 0x9C
	CodeServerMoved                       byte = 0x9D
	CodeSharedSubscriptionsNotSupported   byte = 0x9E
	CodeConnectionRateExceeded            byte = 0x9F
	CodeMaximumConnectTime                byte = 0xA0
	CodeSubscriptionIDsNotSupported       byte = 0xA1
	CodeWildcardSubscriptionsNotSupported byte = 0xA2
)

// codeNames holds the standard name of every defined v5 reason code.
var codeNames = map[byte]string{
	0x00: "Success",
	0x01: "Granted QoS 1",
	0x02: "Granted QoS 2",
	0x04: "Disconnect with Will Message",
	0x10: "No matching subscribers",
	0x11: "No subscription existed",
	0x18: "Continue authentication",
	0x19: "Re-authenticate",
	0x80: "Unspecified error",
	0x81: "Malformed Packet",
	0x82: "Protocol Error",
	0x83: "Implementation specific error",
	0x84: "Unsupported Protocol Version",
	0x85: "Client Identifier not valid",
	0x86: "Bad User Name or Password",
	0x87: "Not authorized",
	0x88: "Server unavailable",
	0x89: "Server busy",
	0x8A: "Banned",
	0x8B: "Server shutting down",
	0x8C: "Bad authentication method",
	0x8D: "Keep Alive timeout",
	0x8E: "Session taken over",
	0x8F: "Topic Filter invalid",
	0x90: "Topic Name invalid",
	0x91: "Packet Identifier in use",
	0x92: "Packet Identifier not found",
	0x93: "Receive Maximum exceeded",
	0x94: "Topic Alias invalid",
	0x95: "Packet too large",
	0x96: "Message rate too high",
	0x97: "Quota exceeded",
	0x98: "Administrative action",
	0x99: "Payload format invalid",
	0x9A: "Retain not supported",
	0x9B: "QoS not supported",
	0x9C: "Use another server",
	0x9D: "Server moved",
	0x9E: "Shared Subscriptions not supported",
	0x9F: "Connection rate exceeded",
	0xA0: "Maximum connect time",
	0xA1: "Subscription Identifiers not supported",
	0xA2: "Wildcard Subscriptions not supported",
}

// Code is an MQTT error carrying a v5 reason code. It implements error.
type Code struct {
	Code   byte
	Reason string
}

// Error returns a human readable description including the reason code.
func (e Code) Error() string {
	reason := e.Reason
	if reason == "" {
		reason = ReasonString(e.Code)
	}
	return fmt.Sprintf("mqtt: %s (reason code 0x%02X)", reason, e.Code)
}

// ReasonString returns the standard name for a v5 reason code.
func ReasonString(code byte) string {
	if s, ok := codeNames[code]; ok {
		return s
	}
	return fmt.Sprintf("unknown reason code 0x%02X", code)
}

// errMalformed builds a Malformed Packet (0x81) codec error.
func errMalformed(format string, args ...any) Code {
	return Code{Code: CodeMalformedPacket, Reason: fmt.Sprintf(format, args...)}
}

// errProtocol builds a Protocol Error (0x82) codec error.
func errProtocol(format string, args ...any) Code {
	return Code{Code: CodeProtocolError, Reason: fmt.Sprintf(format, args...)}
}

// MQTT 3.1.1 CONNACK return codes [MQTT-3.2.3].
const (
	V3ConnackAccepted byte = iota
	V3ConnackUnacceptableProtocolVersion
	V3ConnackIdentifierRejected
	V3ConnackServerUnavailable
	V3ConnackBadUsernameOrPassword
	V3ConnackNotAuthorized
)

// V3SubackFailure is the MQTT 3.1.1 SUBACK failure return code.
const V3SubackFailure byte = 0x80

// V5ToV3 downgrades a v5 reason code to the closest MQTT 3.1.1 return code
// for CONNACK and SUBACK packets. Codes without a closer mapping fall back to
// the generic failure (CONNACK "not authorized" / SUBACK 0x80). Other packet
// types have no v3 equivalent and are returned unchanged.
func V5ToV3(packetType byte, code byte) byte {
	switch packetType {
	case Connack:
		switch code {
		case CodeSuccess:
			return V3ConnackAccepted
		case CodeUnsupportedProtocolVersion:
			return V3ConnackUnacceptableProtocolVersion
		case CodeClientIdentifierNotValid:
			return V3ConnackIdentifierRejected
		case CodeServerUnavailable, CodeServerBusy:
			return V3ConnackServerUnavailable
		case CodeBadUserNameOrPassword:
			return V3ConnackBadUsernameOrPassword
		default:
			return V3ConnackNotAuthorized
		}
	case Suback:
		switch code {
		case CodeGrantedQos0:
			return 0
		case CodeGrantedQos1:
			return 1
		case CodeGrantedQos2:
			return 2
		default:
			return V3SubackFailure
		}
	}
	return code
}
