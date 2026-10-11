package packet

import (
	"bytes"
	"unicode/utf8"
)

// maxFieldLen is the maximum length of a two byte length-prefixed field.
const maxFieldLen = 65535

var (
	// utf8OverlongFFFE / utf8OverlongFFFF are the UTF-8 encodings of the
	// forbidden noncharacters U+FFFE and U+FFFF [MQTT-1.5.4-2].
	utf8FFFE = []byte{0xEF, 0xBF, 0xBE}
	utf8FFFF = []byte{0xEF, 0xBF, 0xBF}
)

// encodeString appends a UTF-8 string field [MQTT-1.5.4]. Callers must ensure
// the string does not exceed maxFieldLen bytes.
func encodeString(dst []byte, s string) []byte {
	dst = append(dst, byte(len(s)>>8), byte(len(s)))
	return append(dst, s...)
}

// encodeBytes appends a binary data field with a two byte length prefix.
func encodeBytes(dst, b []byte) []byte {
	dst = append(dst, byte(len(b)>>8), byte(len(b)))
	return append(dst, b...)
}

// decodeBytes decodes a two byte length-prefixed binary field, returning a
// copy of the data (never an alias of b) and the number of bytes consumed.
func decodeBytes(b []byte) ([]byte, int, error) {
	if len(b) < 2 {
		return nil, 0, errMalformed("truncated length-prefixed field")
	}
	l := int(b[0])<<8 | int(b[1])
	if len(b) < 2+l {
		return nil, 0, errMalformed("length-prefixed field overruns packet: want %d bytes, have %d", l, len(b)-2)
	}
	out := make([]byte, l)
	copy(out, b[2:2+l])
	return out, 2 + l, nil
}

// decodeString decodes a UTF-8 string field, enforcing the well-formedness
// rules: valid UTF-8, no U+0000 [MQTT-1.5.4-1], and no U+FFFE/U+FFFF
// [MQTT-1.5.4-2]. The returned string is a safe copy made with string(); it
// never aliases the input buffer (no unsafe zero-copy).
func decodeString(b []byte) (string, int, error) {
	raw, n, err := decodeBytes(b)
	if err != nil {
		return "", 0, err
	}
	if !utf8.Valid(raw) {
		return "", 0, errMalformed("malformed UTF-8 in string field")
	}
	if bytes.IndexByte(raw, 0x00) >= 0 {
		return "", 0, errMalformed("string field contains U+0000")
	}
	if bytes.Contains(raw, utf8FFFE) || bytes.Contains(raw, utf8FFFF) {
		return "", 0, errMalformed("string field contains U+FFFE or U+FFFF")
	}
	return string(raw), n, nil
}

// decodeStringPair decodes a UTF-8 string pair (used by v5 User Properties).
func decodeStringPair(b []byte) (k string, v string, n int, err error) {
	k, n, err = decodeString(b)
	if err != nil {
		return "", "", 0, err
	}
	v, m, err := decodeString(b[n:])
	if err != nil {
		return "", "", 0, err
	}
	return k, v, n + m, nil
}
