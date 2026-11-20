package wal

import "github.com/amoylan/flash-store/store/wal/config"

type WalCodec interface {
	Encode(wal *Wal) ([]byte, error)
	Decode(data []byte) (*Wal, error)
}

var walCodecRegistry struct {
	codecs map[config.WalFormat]WalCodec
}

func RegisterWalCodec(format config.WalFormat, codec WalCodec) {
	if walCodecRegistry.codecs == nil {
		walCodecRegistry.codecs = make(map[config.WalFormat]WalCodec)
	}
	walCodecRegistry.codecs[format] = codec
}

func GetWalCodec(format config.WalFormat) (WalCodec, bool) {
	codec, ok := walCodecRegistry.codecs[format]
	return codec, ok
}
