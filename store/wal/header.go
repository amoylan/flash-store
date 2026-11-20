package wal

import (
	"encoding/binary"
	"fmt"
)

// EncodeHeaderMap 将 map[string][]byte 编码为紧凑的二进制格式，用于 File WAL record header。
//
// 格式：
//
//	[num_pairs: uint16]
//	  [key_len: uint8] [key_bytes...] [val_len: uint16] [val_bytes...]
//	  ...
//
// 限制：key 长度 ≤ 255 字节；value 长度 ≤ 65535 字节。超出返错。
// 当前 Header 的三个 key（"f","s","t"）和 value（≤8 字节）远在限制内。
func EncodeHeaderMap(m map[string][]byte) ([]byte, error) {
	// 预估 size
	size := 2 // num_pairs
	for k, v := range m {
		if len(k) > 255 {
			return nil, fmt.Errorf("header key %q too long (%d > 255)", k, len(k))
		}
		if len(v) > 65535 {
			return nil, fmt.Errorf("header key %q value too long (%d > 65535)", k, len(v))
		}
		size += 1 + len(k) + 2 + len(v)
	}

	buf := make([]byte, size)
	binary.BigEndian.PutUint16(buf[0:2], uint16(len(m)))
	off := 2
	for k, v := range m {
		buf[off] = byte(len(k))
		off++
		off += copy(buf[off:], k)
		binary.BigEndian.PutUint16(buf[off:off+2], uint16(len(v)))
		off += 2
		off += copy(buf[off:], v)
	}
	return buf, nil
}

// DecodeHeaderMap 从紧凑二进制格式解码出 map[string][]byte。
func DecodeHeaderMap(data []byte) (map[string][]byte, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("header map too short: %d bytes", len(data))
	}
	numPairs := int(binary.BigEndian.Uint16(data[0:2]))
	off := 2
	m := make(map[string][]byte, numPairs)
	for i := 0; i < numPairs; i++ {
		if off >= len(data) {
			return nil, fmt.Errorf("header map truncated at pair %d", i)
		}
		keyLen := int(data[off])
		off++
		if off+keyLen > len(data) {
			return nil, fmt.Errorf("header map key truncated at pair %d", i)
		}
		key := string(data[off : off+keyLen])
		off += keyLen
		if off+2 > len(data) {
			return nil, fmt.Errorf("header map val_len truncated at pair %d", i)
		}
		valLen := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if off+valLen > len(data) {
			return nil, fmt.Errorf("header map val truncated at pair %d", i)
		}
		val := make([]byte, valLen)
		copy(val, data[off:off+valLen])
		off += valLen
		m[key] = val
	}
	return m, nil
}
