package packet

// subscribeDecode decodes the SUBSCRIBE variable header and payload
// [MQTT-3.8].
func (pk *Packet) subscribeDecode(body []byte) error {
	id, n, err := decodeUint16(body)
	if err != nil {
		return err
	}
	if id == 0 {
		// SUBSCRIBE MUST contain a non-zero Packet Identifier
		// [MQTT-2.2.1-2].
		return errProtocol("SUBSCRIBE packet identifier must not be 0")
	}
	pk.PacketID = id
	rest := body[n:]
	if pk.isV5() {
		m, err := pk.Properties.decode(rest, Subscribe)
		if err != nil {
			return err
		}
		if len(pk.Properties.SubscriptionID) > 1 {
			// The Subscription Identifier may appear at most once on
			// SUBSCRIBE (spec 3.8.2.1).
			return errProtocol("SUBSCRIBE subscription identifier included more than once")
		}
		rest = rest[m:]
	}
	// The payload MUST contain at least one topic filter / options pair
	// [MQTT-3.8.3-3].
	if len(rest) == 0 {
		return errProtocol("SUBSCRIBE payload contains no topic filters")
	}
	for len(rest) > 0 {
		filter, m, err := decodeString(rest)
		if err != nil {
			return err
		}
		rest = rest[m:]
		if len(rest) < 1 {
			return errMalformed("SUBSCRIBE missing subscription options byte")
		}
		opts := rest[0]
		rest = rest[1:]
		if opts&0xC0 != 0 {
			// Bits 7-6 of the options byte are reserved and MUST be 0
			// [MQTT-3.8.3-4].
			return errMalformed("SUBSCRIBE options reserved bits set")
		}
		qos := opts & 0x03
		if qos == 3 {
			return errMalformed("SUBSCRIBE options QoS 3 is reserved")
		}
		rh := (opts >> 4) & 0x03
		if rh == 3 {
			// Retain Handling value 3 is reserved [MQTT-3.8.3-5].
			return errMalformed("SUBSCRIBE retain handling 3 is reserved")
		}
		if !pk.isV5() && opts&0xFC != 0 {
			// MQTT 3.1.1 reserves bits 7-2 of the options byte
			// [MQTT-3.8.3-1].
			return errMalformed("SUBSCRIBE options reserved bits set for MQTT 3.1.1")
		}
		if err := validateTopicFilter(filter); err != nil {
			return err
		}
		sub := Subscription{
			Filter:            filter,
			Qos:               qos,
			NoLocal:           opts&0x04 != 0,
			RetainAsPublished: opts&0x08 != 0,
			RetainHandling:    rh,
		}
		if len(pk.Properties.SubscriptionID) > 0 {
			sub.Identifier = pk.Properties.SubscriptionID[0]
		}
		pk.Filters = append(pk.Filters, sub)
	}
	return nil
}

// subscribeEncode appends the SUBSCRIBE variable header and payload to dst.
func (pk *Packet) subscribeEncode(dst *[]byte) error {
	out := encodeUint16(*dst, pk.PacketID)
	if pk.isV5() {
		var err error
		out, err = pk.Properties.encodeValidated(out, Subscribe)
		if err != nil {
			return err
		}
	}
	for _, s := range pk.Filters {
		out = encodeString(out, s.Filter)
		var opts byte
		if pk.isV5() {
			if s.NoLocal {
				opts |= 0x04
			}
			if s.RetainAsPublished {
				opts |= 0x08
			}
			opts |= (s.RetainHandling & 0x03) << 4
		}
		out = append(out, opts|s.Qos&0x03)
	}
	*dst = out
	return nil
}

// subscribeValidate checks SUBSCRIBE semantic constraints.
func (pk *Packet) subscribeValidate() error {
	if pk.PacketID == 0 {
		return errProtocol("SUBSCRIBE packet identifier must not be 0")
	}
	if len(pk.Filters) == 0 {
		return errProtocol("SUBSCRIBE payload contains no topic filters")
	}
	for _, s := range pk.Filters {
		if s.Qos > 2 {
			return errMalformed("SUBSCRIBE options QoS %d is reserved", s.Qos)
		}
		if s.RetainHandling > 2 {
			return errMalformed("SUBSCRIBE retain handling %d is reserved", s.RetainHandling)
		}
		if err := validateTopicFilter(s.Filter); err != nil {
			return err
		}
	}
	return nil
}

// unsubscribeDecode decodes the UNSUBSCRIBE variable header and payload
// [MQTT-3.10].
func (pk *Packet) unsubscribeDecode(body []byte) error {
	id, n, err := decodeUint16(body)
	if err != nil {
		return err
	}
	if id == 0 {
		return errProtocol("UNSUBSCRIBE packet identifier must not be 0")
	}
	pk.PacketID = id
	rest := body[n:]
	if pk.isV5() {
		m, err := pk.Properties.decode(rest, Unsubscribe)
		if err != nil {
			return err
		}
		rest = rest[m:]
	}
	// The payload MUST contain at least one topic filter [MQTT-3.10.3-2].
	if len(rest) == 0 {
		return errProtocol("UNSUBSCRIBE payload contains no topic filters")
	}
	for len(rest) > 0 {
		filter, m, err := decodeString(rest)
		if err != nil {
			return err
		}
		rest = rest[m:]
		if err := validateTopicFilter(filter); err != nil {
			return err
		}
		pk.Filters = append(pk.Filters, Subscription{Filter: filter})
	}
	return nil
}

// unsubscribeEncode appends the UNSUBSCRIBE variable header and payload.
func (pk *Packet) unsubscribeEncode(dst *[]byte) error {
	out := encodeUint16(*dst, pk.PacketID)
	if pk.isV5() {
		var err error
		out, err = pk.Properties.encodeValidated(out, Unsubscribe)
		if err != nil {
			return err
		}
	}
	for _, s := range pk.Filters {
		out = encodeString(out, s.Filter)
	}
	*dst = out
	return nil
}

// unsubscribeValidate checks UNSUBSCRIBE semantic constraints.
func (pk *Packet) unsubscribeValidate() error {
	if pk.PacketID == 0 {
		return errProtocol("UNSUBSCRIBE packet identifier must not be 0")
	}
	if len(pk.Filters) == 0 {
		return errProtocol("UNSUBSCRIBE payload contains no topic filters")
	}
	for _, s := range pk.Filters {
		if err := validateTopicFilter(s.Filter); err != nil {
			return err
		}
	}
	return nil
}

// subackDecode decodes the SUBACK variable header and payload [MQTT-3.9].
func (pk *Packet) subackDecode(body []byte) error {
	id, n, err := decodeUint16(body)
	if err != nil {
		return err
	}
	if id == 0 {
		return errProtocol("SUBACK packet identifier must not be 0")
	}
	pk.PacketID = id
	rest := body[n:]
	if pk.isV5() {
		m, err := pk.Properties.decode(rest, Suback)
		if err != nil {
			return err
		}
		rest = rest[m:]
	}
	// The payload MUST contain at least one reason code [MQTT-3.9.3-1].
	if len(rest) == 0 {
		return errMalformed("SUBACK payload contains no reason codes")
	}
	for _, rc := range rest {
		if pk.isV5() {
			if err := checkReasonCode(Suback, rc); err != nil {
				return err
			}
		} else if rc > 2 && rc != V3SubackFailure {
			// v3 allows only 0x00, 0x01, 0x02 and 0x80 [MQTT-3.9.3-2].
			return errMalformed("SUBACK reserved return code 0x%x", rc)
		}
	}
	pk.ReasonCodes = append([]byte(nil), rest...)
	return nil
}

// subackEncode appends the SUBACK variable header and payload to dst.
func (pk *Packet) subackEncode(dst *[]byte) error {
	out := encodeUint16(*dst, pk.PacketID)
	if pk.isV5() {
		var err error
		out, err = pk.Properties.encodeValidated(out, Suback)
		if err != nil {
			return err
		}
	}
	*dst = append(out, pk.ReasonCodes...)
	return nil
}

// subackValidate checks SUBACK semantic constraints.
func (pk *Packet) subackValidate() error {
	if pk.PacketID == 0 {
		return errProtocol("SUBACK packet identifier must not be 0")
	}
	if len(pk.ReasonCodes) == 0 {
		return errMalformed("SUBACK payload contains no reason codes")
	}
	if !pk.isV5() {
		for _, rc := range pk.ReasonCodes {
			if rc > 2 && rc != V3SubackFailure {
				return errMalformed("SUBACK reserved return code 0x%x", rc)
			}
		}
		return nil
	}
	for _, rc := range pk.ReasonCodes {
		if err := checkReasonCode(Suback, rc); err != nil {
			return err
		}
	}
	return nil
}

// unsubackDecode decodes the UNSUBACK variable header [MQTT-3.11]. MQTT 3.1.1
// requires remaining length exactly 2 [MQTT-3.11.3-1]; MQTT 5.0 adds
// properties and a reason code list.
func (pk *Packet) unsubackDecode(body []byte) error {
	id, n, err := decodeUint16(body)
	if err != nil {
		return err
	}
	if id == 0 {
		return errProtocol("UNSUBACK packet identifier must not be 0")
	}
	pk.PacketID = id
	rest := body[n:]
	if !pk.isV5() {
		if len(rest) != 0 {
			return errMalformed("UNSUBACK must have remaining length 2")
		}
		return nil
	}
	m, err := pk.Properties.decode(rest, Unsuback)
	if err != nil {
		return err
	}
	rest = rest[m:]
	for _, rc := range rest {
		if err := checkReasonCode(Unsuback, rc); err != nil {
			return err
		}
	}
	if len(rest) > 0 {
		pk.ReasonCodes = append([]byte(nil), rest...)
	}
	return nil
}

// unsubackEncode appends the UNSUBACK variable header and payload to dst.
func (pk *Packet) unsubackEncode(dst *[]byte) error {
	out := encodeUint16(*dst, pk.PacketID)
	if pk.isV5() {
		var err error
		out, err = pk.Properties.encodeValidated(out, Unsuback)
		if err != nil {
			return err
		}
		out = append(out, pk.ReasonCodes...)
	}
	*dst = out
	return nil
}

// unsubackValidate checks UNSUBACK semantic constraints.
func (pk *Packet) unsubackValidate() error {
	if pk.PacketID == 0 {
		return errProtocol("UNSUBACK packet identifier must not be 0")
	}
	if pk.isV5() {
		for _, rc := range pk.ReasonCodes {
			if err := checkReasonCode(Unsuback, rc); err != nil {
				return err
			}
		}
	}
	return nil
}
