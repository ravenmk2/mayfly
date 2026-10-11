package packet

// authDecode decodes the AUTH variable header [MQTT-3.15]. AUTH exists only
// in MQTT 5.0; both the reason code and the properties may be omitted.
func (pk *Packet) authDecode(body []byte) error {
	if !pk.isV5() {
		return errProtocol("AUTH is only valid for MQTT 5.0")
	}
	if len(body) == 0 {
		pk.ReasonCode = CodeSuccess
		return nil
	}
	pk.ReasonCode = body[0]
	if err := checkReasonCode(Auth, pk.ReasonCode); err != nil {
		return err
	}
	if len(body) == 1 {
		return nil
	}
	n, err := pk.Properties.decode(body[1:], Auth)
	if err != nil {
		return err
	}
	if 1+n != len(body) {
		return errMalformed("AUTH has %d trailing bytes", len(body)-1-n)
	}
	return nil
}

// authEncode appends the AUTH variable header to dst, using the shortest
// permitted form.
func (pk *Packet) authEncode(dst *[]byte) error {
	if !pk.isV5() {
		return errProtocol("AUTH is only valid for MQTT 5.0")
	}
	if pk.ReasonCode == CodeSuccess && pk.Properties.isEmpty() {
		return nil
	}
	out := append(*dst, pk.ReasonCode)
	if !pk.Properties.isEmpty() {
		var err error
		out, err = pk.Properties.encodeValidated(out, Auth)
		if err != nil {
			return err
		}
	}
	*dst = out
	return nil
}

// authValidate checks AUTH semantic constraints.
func (pk *Packet) authValidate() error {
	if !pk.isV5() {
		return errProtocol("AUTH is only valid for MQTT 5.0")
	}
	return checkReasonCode(Auth, pk.ReasonCode)
}
