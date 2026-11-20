package snapshot

import (
	"encoding/json"

	"github.com/amoylan/flash-store/store/snapshot/config"
)

type jsonCodec struct{}

func (jsonCodec) EncodeShard(shard ShardSnapshot) ([]byte, error) {
	return json.Marshal(&shard)
}

func (jsonCodec) DecodeShard(data []byte) (*ShardSnapshot, error) {
	var shard ShardSnapshot
	if err := json.Unmarshal(data, &shard); err != nil {
		return nil, err
	}
	return &shard, nil
}

func (jsonCodec) EncodeWorker(worker WorkerSnapshot) ([]byte, error) {
	return json.Marshal(&worker)
}

func (jsonCodec) DecodeWorker(data []byte) (*WorkerSnapshot, error) {
	var worker WorkerSnapshot
	if err := json.Unmarshal(data, &worker); err != nil {
		return nil, err
	}
	return &worker, nil
}

func init() {
	RegisterCodec(config.SnapshotFormatJson, jsonCodec{})
}
