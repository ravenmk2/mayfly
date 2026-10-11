package packet

import "fmt"

// connectDecode decodes the CONNECT variable header and payload [MQTT-3.1].
func (pk *Packet) connectDecode(body []byte) error {
	c := &pk.Connect
	proto, n, err := decodeString(body)
	if err != nil {
		return err
	}
	rest := body[n:]
	if len(rest) < 4 {
		return errMalformed("CONNECT variable header truncated")
	}
	// Protocol level: 4 is MQTT 3.1.1, 5 is MQTT 5.0. Level 3 with the
	// "MQIsdp" protocol name (MQTT 3.1) is accepted and treated as 3.1.1.
	level := rest[0]
	switch {
	case proto == "MQTT" && (level == Version311 || level == Version5):
	case proto == "MQIsdp" && level == 3:
		level = Version311
	default:
		return Code{Code: CodeUnsupportedProtocolVersion,
			Reason: fmt.Sprintf("unsupported protocol %q level %d", proto, level)}
	}
	pk.ProtocolVersion = level
	c.ProtocolName = proto
	c.ProtocolVersion = level

	flags := rest[1]
	if flags&0x01 != 0 {
		// The reserved bit of the connect flags MUST be 0 [MQTT-3.1.2-3].
		return errMalformed("CONNECT reserved flag bit set")
	}
	c.UsernameFlag = flags&0x80 != 0
	c.PasswordFlag = flags&0x40 != 0
	c.WillRetain = flags&0x20 != 0
	c.WillQos = (flags >> 3) & 0x03
	c.WillFlag = flags&0x04 != 0
	c.Clean = flags&0x02 != 0
	if c.WillQos == 3 {
		// Will QoS MUST NOT be 3 (spec 3.1.2.6).
		return errMalformed("CONNECT will QoS 3 is reserved")
	}
	if !c.WillFlag && (c.WillQos != 0 || c.WillRetain) {
		// If the Will Flag is 0 the Will QoS and Will Retain fields MUST
		// be 0 (spec 3.1.2.5).
		return errProtocol("CONNECT will QoS/retain set without will flag")
	}
	c.Keepalive = uint16(rest[2])<<8 | uint16(rest[3])
	rest = rest[4:]

	if pk.isV5() {
		m, err := pk.Properties.decode(rest, Connect)
		if err != nil {
			return err
		}
		rest = rest[m:]
	}

	// Payload: client identifier, then will fields, then credentials,
	// strictly in this order [MQTT-3.1.3-1]..[MQTT-3.1.3-5].
	if c.ClientIdentifier, n, err = decodeString(rest); err != nil {
		return err
	}
	rest = rest[n:]
	if c.WillFlag {
		if pk.isV5() {
			m, err := c.WillProperties.decode(rest, willPropsContext)
			if err != nil {
				return err
			}
			rest = rest[m:]
		}
		if c.WillTopic, n, err = decodeString(rest); err != nil {
			return err
		}
		rest = rest[n:]
		if err := validateTopicName(c.WillTopic); err != nil {
			return err
		}
		if c.WillPayload, n, err = decodeBytes(rest); err != nil {
			return err
		}
		rest = rest[n:]
	}
	if c.UsernameFlag {
		if c.Username, n, err = decodeString(rest); err != nil {
			return err
		}
		rest = rest[n:]
	}
	if c.PasswordFlag {
		if c.Password, n, err = decodeBytes(rest); err != nil {
			return err
		}
		rest = rest[n:]
	}
	if len(rest) != 0 {
		return errMalformed("CONNECT has %d trailing payload bytes", len(rest))
	}
	return nil
}

// connectEncode appends the CONNECT variable header and payload to dst.
func (pk *Packet) connectEncode(dst *[]byte) error {
	c := &pk.Connect
	level := c.ProtocolVersion
	if level == 0 {
		level = pk.version()
	}
	name := c.ProtocolName
	if name == "" || name == "MQIsdp" {
		// MQTT 3.1 is normalized to 3.1.1 on decode; the encoder always
		// emits the 3.1.1 protocol name for levels 4 and 5.
		name = "MQTT"
	}
	out := encodeString(*dst, name)
	out = append(out, level)
	var flags byte
	if c.UsernameFlag {
		flags |= 0x80
	}
	if c.PasswordFlag {
		flags |= 0x40
	}
	if c.WillFlag {
		flags |= 0x04
		flags |= (c.WillQos & 0x03) << 3
		if c.WillRetain {
			flags |= 0x20
		}
	}
	if c.Clean {
		flags |= 0x02
	}
	out = append(out, flags)
	out = encodeUint16(out, c.Keepalive)
	if level == Version5 {
		var err error
		out, err = pk.Properties.encodeValidated(out, Connect)
		if err != nil {
			return err
		}
	}
	out = encodeString(out, c.ClientIdentifier)
	if c.WillFlag {
		if level == Version5 {
			var err error
			out, err = c.WillProperties.encodeValidated(out, willPropsContext)
			if err != nil {
				return err
			}
		}
		out = encodeString(out, c.WillTopic)
		out = encodeBytes(out, c.WillPayload)
	}
	if c.UsernameFlag {
		out = encodeString(out, c.Username)
	}
	if c.PasswordFlag {
		out = encodeBytes(out, c.Password)
	}
	*dst = out
	return nil
}

// connectValidate checks CONNECT semantic constraints.
func (pk *Packet) connectValidate() error {
	c := &pk.Connect
	switch c.ProtocolVersion {
	case 0, Version311, Version5:
	default:
		return Code{Code: CodeUnsupportedProtocolVersion,
			Reason: "unsupported protocol level"}
	}
	if c.WillQos > 2 {
		return errMalformed("CONNECT will QoS 3 is reserved")
	}
	if !c.WillFlag && (c.WillQos != 0 || c.WillRetain || c.WillTopic != "" || c.WillPayload != nil) {
		return errProtocol("CONNECT will fields set without will flag")
	}
	if len(c.ClientIdentifier) > maxFieldLen || len(c.WillTopic) > maxFieldLen ||
		len(c.WillPayload) > maxFieldLen || len(c.Username) > maxFieldLen ||
		len(c.Password) > maxFieldLen {
		return errMalformed("CONNECT field exceeds two byte length limit")
	}
	if c.WillFlag {
		if err := validateTopicName(c.WillTopic); err != nil {
			return err
		}
	}
	return nil
}

// connackDecode decodes the CONNACK variable header [MQTT-3.2].
func (pk *Packet) connackDecode(body []byte) error {
	if len(body) < 2 {
		return errMalformed("CONNACK truncated")
	}
	ackFlags := body[0]
	if ackFlags&0xFE != 0 {
		// Bits 7-1 of the acknowledge flags are reserved and MUST be 0
		// (spec 3.2.2.1).
		return errMalformed("CONNACK reserved acknowledge flag bits set")
	}
	pk.SessionPresent = ackFlags&0x01 != 0
	pk.ReasonCode = body[1]
	if pk.isV5() {
		if err := checkReasonCode(Connack, pk.ReasonCode); err != nil {
			return err
		}
		n, err := pk.Properties.decode(body[2:], Connack)
		if err != nil {
			return err
		}
		if 2+n != len(body) {
			return errMalformed("CONNACK has %d trailing bytes", len(body)-2-n)
		}
	} else {
		if pk.ReasonCode > V3ConnackNotAuthorized {
			// Return codes 6-255 are reserved [MQTT-3.2.3].
			return errMalformed("CONNACK reserved return code 0x%x", pk.ReasonCode)
		}
		if len(body) != 2 {
			return errMalformed("CONNACK has %d trailing bytes", len(body)-2)
		}
	}
	if pk.SessionPresent && pk.ReasonCode != CodeSuccess {
		// Session Present may only be 1 on a successful connection
		// [MQTT-3.2.2-4].
		return errProtocol("CONNACK session present with non-success reason code")
	}
	return nil
}

// connackEncode appends the CONNACK variable header to dst.
func (pk *Packet) connackEncode(dst *[]byte) error {
	out := *dst
	var ackFlags byte
	if pk.SessionPresent {
		ackFlags = 0x01
	}
	out = append(out, ackFlags)
	if pk.isV5() {
		var err error
		out = append(out, pk.ReasonCode)
		out, err = pk.Properties.encodeValidated(out, Connack)
		if err != nil {
			return err
		}
	} else {
		// ReasonCode holds a v3 return code (0-5) when the packet was
		// decoded from the wire; v5 reason codes are downgraded.
		rc := pk.ReasonCode
		if rc > V3ConnackNotAuthorized {
			rc = V5ToV3(Connack, rc)
		}
		out = append(out, rc)
	}
	*dst = out
	return nil
}

// connackValidate checks CONNACK semantic constraints.
func (pk *Packet) connackValidate() error {
	if pk.SessionPresent && pk.ReasonCode != CodeSuccess {
		return errProtocol("CONNACK session present with non-success reason code")
	}
	if pk.isV5() {
		return checkReasonCode(Connack, pk.ReasonCode)
	}
	return nil
}
