package packet

// MaxRemainingLength is the maximum value encodable in the Remaining Length
// field, which uses at most four variable byte integer bytes [MQTT-2.1.4].
const MaxRemainingLength = 268435455

// encodeVarint appends v to dst using the MQTT variable byte integer
// encoding [MQTT-2.1.4]. Callers must ensure 0 <= v <= MaxRemainingLength.
func encodeVarint(dst []byte, v int) []byte {
	for {
		digit := byte(v & 0x7f)
		v >>= 7
		if v > 0 {
			digit |= 0x80
		}
		dst = append(dst, digit)
		if v == 0 {
			return dst
		}
	}
}

// decodeVarint decodes a variable byte integer from the front of b,
// returning the value and the number of bytes consumed [MQTT-2.1.4].
func decodeVarint(b []byte) (value int, n int, err error) {
	for i := 0; i < 4; i++ {
		if i >= len(b) {
			return 0, 0, errMalformed("truncated variable byte integer")
		}
		digit := b[i]
		value |= int(digit&0x7f) << (7 * i)
		if digit&0x80 == 0 {
			return value, i + 1, nil
		}
	}
	// A fifth byte would exceed the four byte encoding limit [MQTT-2.1.4].
	return 0, 0, errMalformed("variable byte integer exceeds 4 bytes")
}

// varintLen returns the encoded length of v as a variable byte integer.
func varintLen(v int) int {
	switch {
	case v < 1<<7:
		return 1
	case v < 1<<14:
		return 2
	case v < 1<<21:
		return 3
	default:
		return 4
	}
}

// encodeUint16 appends a two byte big-endian integer field.
func encodeUint16(dst []byte, v uint16) []byte {
	return append(dst, byte(v>>8), byte(v))
}

// encodeUint32 appends a four byte big-endian integer field.
func encodeUint32(dst []byte, v uint32) []byte {
	return append(dst, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// decodeUint16 decodes a two byte big-endian integer field.
func decodeUint16(b []byte) (uint16, int, error) {
	if len(b) < 2 {
		return 0, 0, errMalformed("truncated two byte integer field")
	}
	return uint16(b[0])<<8 | uint16(b[1]), 2, nil
}

// decodeUint32 decodes a four byte big-endian integer field.
func decodeUint32(b []byte) (uint32, int, error) {
	if len(b) < 4 {
		return 0, 0, errMalformed("truncated four byte integer field")
	}
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3]), 4, nil
}
