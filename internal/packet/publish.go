package packet

// publishDecode decodes the PUBLISH variable header and payload [MQTT-3.3].
func (pk *Packet) publishDecode(body []byte) error {
	topic, n, err := decodeString(body)
	if err != nil {
		return err
	}
	rest := body[n:]
	if pk.FixedHeader.Qos > 0 {
		id, m, err := decodeUint16(rest)
		if err != nil {
			return err
		}
		if id == 0 {
			// PUBLISH with QoS > 0 MUST contain a non-zero Packet
			// Identifier [MQTT-2.2.1-2].
			return errProtocol("PUBLISH packet identifier must not be 0")
		}
		pk.PacketID = id
		rest = rest[m:]
	}
	if pk.isV5() {
		m, err := pk.Properties.decode(rest, Publish)
		if err != nil {
			return err
		}
		rest = rest[m:]
	}
	if topic == "" {
		// A zero length topic name is only allowed together with a Topic
		// Alias property (v5, spec 3.3.2.3).
		if !pk.isV5() || !pk.Properties.TopicAliasFlag {
			return errProtocol("PUBLISH topic name empty without topic alias")
		}
	} else if err := validateTopicName(topic); err != nil {
		return err
	}
	pk.TopicName = topic
	// The payload may be zero length [MQTT-3.3.3]. Copy it so the packet
	// never aliases the caller's read buffer.
	if len(rest) > 0 {
		pk.Payload = append([]byte(nil), rest...)
	}
	return nil
}

// publishEncode appends the PUBLISH variable header and payload to dst.
func (pk *Packet) publishEncode(dst *[]byte) error {
	out := encodeString(*dst, pk.TopicName)
	if pk.FixedHeader.Qos > 0 {
		out = encodeUint16(out, pk.PacketID)
	}
	if pk.isV5() {
		var err error
		out, err = pk.Properties.encodeValidated(out, Publish)
		if err != nil {
			return err
		}
	}
	*dst = append(out, pk.Payload...)
	return nil
}

// publishValidate checks PUBLISH semantic constraints.
func (pk *Packet) publishValidate() error {
	if pk.FixedHeader.Qos > 2 {
		return errMalformed("publish with reserved QoS %d", pk.FixedHeader.Qos)
	}
	if pk.FixedHeader.Qos > 0 && pk.PacketID == 0 {
		return errProtocol("PUBLISH packet identifier must not be 0")
	}
	if pk.TopicName == "" {
		if !pk.isV5() || !pk.Properties.TopicAliasFlag {
			return errProtocol("PUBLISH topic name empty without topic alias")
		}
	} else if err := validateTopicName(pk.TopicName); err != nil {
		return err
	}
	return nil
}

// pubackDecode decodes PUBACK, PUBREC, PUBREL and PUBCOMP, which share the
// same variable header layout: Packet Identifier, then (v5 only) an optional
// reason code and optional properties [MQTT-3.4]..[MQTT-3.7].
func (pk *Packet) pubackDecode(body []byte) error {
	t := pk.FixedHeader.Type
	id, n, err := decodeUint16(body)
	if err != nil {
		return err
	}
	if id == 0 {
		return errProtocol("%s packet identifier must not be 0", TypeNames[t])
	}
	pk.PacketID = id
	rest := body[n:]
	if !pk.isV5() {
		// MQTT 3.1.1 requires remaining length exactly 2 (spec 3.4.3 etc.).
		if len(rest) != 0 {
			return errMalformed("%s must have remaining length 2", TypeNames[t])
		}
		return nil
	}
	if len(rest) == 0 {
		// Omitted reason code means Success.
		pk.ReasonCode = CodeSuccess
		return nil
	}
	pk.ReasonCode = rest[0]
	if err := checkReasonCode(t, pk.ReasonCode); err != nil {
		return err
	}
	rest = rest[1:]
	if len(rest) == 0 {
		// Property Length omitted: no properties.
		return nil
	}
	m, err := pk.Properties.decode(rest, t)
	if err != nil {
		return err
	}
	if m != len(rest) {
		return errMalformed("%s has %d trailing bytes", TypeNames[t], len(rest)-m)
	}
	return nil
}

// pubackEncode appends the shared PUBACK/PUBREC/PUBREL/PUBCOMP variable
// header to dst, using the shortest permitted v5 form.
func (pk *Packet) pubackEncode(dst *[]byte) error {
	t := pk.FixedHeader.Type
	out := encodeUint16(*dst, pk.PacketID)
	if pk.isV5() && (pk.ReasonCode != CodeSuccess || !pk.Properties.isEmpty()) {
		out = append(out, pk.ReasonCode)
		if !pk.Properties.isEmpty() {
			var err error
			out, err = pk.Properties.encodeValidated(out, t)
			if err != nil {
				return err
			}
		}
	}
	*dst = out
	return nil
}

// pubackValidate checks the shared ack packet constraints.
func (pk *Packet) pubackValidate() error {
	if pk.PacketID == 0 {
		return errProtocol("%s packet identifier must not be 0", TypeNames[pk.FixedHeader.Type])
	}
	if pk.isV5() {
		return checkReasonCode(pk.FixedHeader.Type, pk.ReasonCode)
	}
	return nil
}
