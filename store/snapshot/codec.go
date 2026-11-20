package snapshot

import "github.com/amoylan/flash-store/store/snapshot/config"

// Codec 负责 snapshot 中 Shard/Worker 片段的序列化/反序列化，与存储介质无关。
// Header 始终以 JSON 编码，不走 Codec。
// 不同格式（json、protobuf）各自实现此接口，通过 RegisterCodec 注册。
type Codec interface {
	EncodeShard(shard ShardSnapshot) ([]byte, error)
	DecodeShard(data []byte) (*ShardSnapshot, error)

	EncodeWorker(worker WorkerSnapshot) ([]byte, error)
	DecodeWorker(data []byte) (*WorkerSnapshot, error)
}

var codecRegistry struct {
	codecs map[config.SnapshotFormat]Codec
}

func RegisterCodec(format config.SnapshotFormat, codec Codec) {
	if codecRegistry.codecs == nil {
		codecRegistry.codecs = make(map[config.SnapshotFormat]Codec)
	}
	codecRegistry.codecs[format] = codec
}

func GetCodec(format config.SnapshotFormat) (Codec, bool) {
	codec, ok := codecRegistry.codecs[format]
	return codec, ok
}
