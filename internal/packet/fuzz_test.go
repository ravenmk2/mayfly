package packet

import (
	"testing"
)

// FuzzDecode fuzzes the decoder entry point: arbitrary bytes must never cause
// a panic, and any packet that decodes successfully must encode and decode
// again without error.
func FuzzDecode(f *testing.F) {
	for _, c := range goldenCases() {
		f.Add(c.raw)
	}
	// Malformed and boundary seeds.
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0x7F})
	f.Add([]byte{0x10, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add([]byte{0x30, 0x03, 0x00, 0x01, 'a'})
	f.Add([]byte{0x82, 0x02, 0x00, 0x01})
	f.Add([]byte{0x20, 0x03, 0x01, 0x00, 0x7F})
	f.Fuzz(func(t *testing.T, data []byte) {
		for _, v := range []byte{Version311, Version5} {
			pk := Packet{ProtocolVersion: v}
			if err := pk.Decode(data); err != nil {
				continue
			}
			out, err := pk.Encode()
			if err != nil {
				t.Fatalf("packet decoded with %s but failed to encode: %v", pk.FixedHeader, err)
			}
			pk2 := Packet{ProtocolVersion: pk.ProtocolVersion}
			if err := pk2.Decode(out); err != nil {
				t.Fatalf("encoded packet failed to re-decode: %v\npacket: %+v\nbytes: %x", err, pk, out)
			}
		}
	})
}
