package packet

// pingreqDecode validates a PINGREQ body: the Remaining Length MUST be 0
// [MQTT-3.12.3-1].
func (pk *Packet) pingreqDecode(body []byte) error {
	if len(body) != 0 {
		return errMalformed("PINGREQ must have remaining length 0")
	}
	return nil
}

// pingrespDecode validates a PINGRESP body: the Remaining Length MUST be 0
// [MQTT-3.13.3-1].
func (pk *Packet) pingrespDecode(body []byte) error {
	if len(body) != 0 {
		return errMalformed("PINGRESP must have remaining length 0")
	}
	return nil
}

// disconnectDecode decodes the optional DISCONNECT variable header
// [MQTT-3.14]. MQTT 3.1.1 requires remaining length 0 [MQTT-3.14.3-1];
// MQTT 5.0 allows an optional reason code and properties.
func (pk *Packet) disconnectDecode(body []byte) error {
	if !pk.isV5() {
		if len(body) != 0 {
			return errMalformed("DISCONNECT must have remaining length 0")
		}
		return nil
	}
	if len(body) == 0 {
		// Omitted variable header means Normal Disconnection.
		pk.ReasonCode = CodeNormalDisconnection
		return nil
	}
	pk.ReasonCode = body[0]
	if err := checkReasonCode(Disconnect, pk.ReasonCode); err != nil {
		return err
	}
	if len(body) == 1 {
		// Property Length omitted: no properties.
		return nil
	}
	n, err := pk.Properties.decode(body[1:], Disconnect)
	if err != nil {
		return err
	}
	if 1+n != len(body) {
		return errMalformed("DISCONNECT has %d trailing bytes", len(body)-1-n)
	}
	return nil
}

// disconnectEncode appends the DISCONNECT variable header to dst.
func (pk *Packet) disconnectEncode(dst *[]byte) error {
	if !pk.isV5() {
		return nil
	}
	if pk.ReasonCode == CodeNormalDisconnection && pk.Properties.isEmpty() {
		// The variable header may be omitted entirely.
		return nil
	}
	out := append(*dst, pk.ReasonCode)
	if !pk.Properties.isEmpty() {
		var err error
		out, err = pk.Properties.encodeValidated(out, Disconnect)
		if err != nil {
			return err
		}
	}
	*dst = out
	return nil
}

// disconnectValidate checks DISCONNECT semantic constraints.
func (pk *Packet) disconnectValidate() error {
	if !pk.isV5() {
		return nil
	}
	return checkReasonCode(Disconnect, pk.ReasonCode)
}
