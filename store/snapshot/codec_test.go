package snapshot

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/snapshot/config"
)

func mustKind(t *testing.T, s string) registry.Kind {
	t.Helper()
	k, err := registry.ParseKindFromString(s)
	if err != nil {
		t.Fatalf("parse kind %q: %v", s, err)
	}
	return k
}

func testHeader() Header {
	return Header{
		Version:    1,
		Format:     config.SnapshotFormatJson,
		CreatedAt:  time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC),
		NumShards:  4,
		NumWorkers: 2,
	}
}

func testShardSnapshot(t *testing.T) ShardSnapshot {
	// Row 字段在 snapshot codec 层只是 raw bytes——snapshot codec 不 decode Row
	// 本身（上层 shard 用 schema.EncodeRow / DecodeRow 处理），按字节透传即可。
	return ShardSnapshot{
		ShardID: 3,
		Users: map[Identity]UserData{
			1001: {
				Objects: []ObjectEntry{
					{Kind: mustKind(t, "trade.order"), Key: "order-1", Row: []byte{1, 2, 3}, ExpiredAt: 1700000000},
					{Kind: mustKind(t, "trade.order"), Key: "order-2", Row: []byte{4, 5, 6}, ExpiredAt: 0},
				},
				KVs: []KVEntry{
					{Key: "balance", Value: []byte("12345"), ExpiredAt: 1700000000},
				},
			},
			2002: {
				Objects: nil,
				KVs: []KVEntry{
					{Key: "flag", Value: []byte("1"), ExpiredAt: 0},
				},
			},
		},
	}
}

func testWorkerSnapshot() WorkerSnapshot {
	return WorkerSnapshot{
		WorkerID:  1,
		Seq:       42,
		WalOffset: 100,
		SeqDedup: SeqDedupState{
			MaxSeen:    42,
			WindowSize: 100,
			Seen:       []int64{40, 41, 42},
		},
	}
}

func testEmptyShardSnapshot() ShardSnapshot {
	return ShardSnapshot{
		ShardID: 0,
		Users:   nil,
	}
}

func testEmptyWorkerSnapshot() WorkerSnapshot {
	return WorkerSnapshot{
		WorkerID:  0,
		Seq:       0,
		WalOffset: -1,
		SeqDedup:  SeqDedupState{},
	}
}

var testFormats = []config.SnapshotFormat{
	config.SnapshotFormatProtobuf,
	config.SnapshotFormatJson,
}

// TestHeaderJSONRoundtrip 验证 Header 的 JSON 编解码（Header 始终用 JSON，不走 Codec）。
func TestHeaderJSONRoundtrip(t *testing.T) {
	header := testHeader()

	data, err := json.Marshal(&header)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var got Header
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if got.Version != header.Version {
		t.Errorf("version: got %d, want %d", got.Version, header.Version)
	}
	if got.Format != header.Format {
		t.Errorf("format: got %q, want %q", got.Format, header.Format)
	}
	if !got.CreatedAt.Equal(header.CreatedAt) {
		t.Errorf("created_at: got %v, want %v", got.CreatedAt, header.CreatedAt)
	}
	if got.NumShards != header.NumShards {
		t.Errorf("num_shards: got %d, want %d", got.NumShards, header.NumShards)
	}
	if got.NumWorkers != header.NumWorkers {
		t.Errorf("num_workers: got %d, want %d", got.NumWorkers, header.NumWorkers)
	}
}

func TestCodecShardRoundtrip(t *testing.T) {
	shard := testShardSnapshot(t)

	for _, format := range testFormats {
		t.Run(string(format), func(t *testing.T) {
			codec, ok := GetCodec(format)
			if !ok {
				t.Fatalf("codec %q not registered", format)
			}

			data, err := codec.EncodeShard(shard)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			got, err := codec.DecodeShard(data)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}

			if got.ShardID != shard.ShardID {
				t.Errorf("shard_id: got %d, want %d", got.ShardID, shard.ShardID)
			}
			if len(got.Users) != len(shard.Users) {
				t.Fatalf("users count: got %d, want %d", len(got.Users), len(shard.Users))
			}

			for id, wantUD := range shard.Users {
				gotUD, ok := got.Users[id]
				if !ok {
					t.Fatalf("user %d not found", id)
				}
				assertUserDataEqual(t, id, gotUD, wantUD)
			}
		})
	}
}

func TestCodecShardEmpty(t *testing.T) {
	shard := testEmptyShardSnapshot()

	for _, format := range testFormats {
		t.Run(string(format), func(t *testing.T) {
			codec, ok := GetCodec(format)
			if !ok {
				t.Fatalf("codec %q not registered", format)
			}

			data, err := codec.EncodeShard(shard)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			got, err := codec.DecodeShard(data)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}

			if got.ShardID != 0 {
				t.Errorf("shard_id: got %d, want 0", got.ShardID)
			}
			if len(got.Users) != 0 {
				t.Errorf("users: got %d, want 0", len(got.Users))
			}
		})
	}
}

func TestCodecWorkerRoundtrip(t *testing.T) {
	worker := testWorkerSnapshot()

	for _, format := range testFormats {
		t.Run(string(format), func(t *testing.T) {
			codec, ok := GetCodec(format)
			if !ok {
				t.Fatalf("codec %q not registered", format)
			}

			data, err := codec.EncodeWorker(worker)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			got, err := codec.DecodeWorker(data)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}

			if got.WorkerID != worker.WorkerID {
				t.Errorf("worker_id: got %d, want %d", got.WorkerID, worker.WorkerID)
			}
			if got.Seq != worker.Seq {
				t.Errorf("seq: got %d, want %d", got.Seq, worker.Seq)
			}
			if got.WalOffset != worker.WalOffset {
				t.Errorf("wal_offset: got %d, want %d", got.WalOffset, worker.WalOffset)
			}

			// SeqDedup
			if got.SeqDedup.MaxSeen != worker.SeqDedup.MaxSeen {
				t.Errorf("seq_dedup.max_seen: got %d, want %d", got.SeqDedup.MaxSeen, worker.SeqDedup.MaxSeen)
			}
			if got.SeqDedup.WindowSize != worker.SeqDedup.WindowSize {
				t.Errorf("seq_dedup.window_size: got %d, want %d", got.SeqDedup.WindowSize, worker.SeqDedup.WindowSize)
			}
			assertInt64SliceEqual(t, got.SeqDedup.Seen, worker.SeqDedup.Seen)
		})
	}
}

func TestCodecWorkerEmpty(t *testing.T) {
	worker := testEmptyWorkerSnapshot()

	for _, format := range testFormats {
		t.Run(string(format), func(t *testing.T) {
			codec, ok := GetCodec(format)
			if !ok {
				t.Fatalf("codec %q not registered", format)
			}

			data, err := codec.EncodeWorker(worker)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}

			got, err := codec.DecodeWorker(data)
			if err != nil {
				t.Fatalf("decode: %v", err)
			}

			if got.WorkerID != 0 {
				t.Errorf("worker_id: got %d, want 0", got.WorkerID)
			}
			if got.Seq != 0 {
				t.Errorf("seq: got %d, want 0", got.Seq)
			}
		})
	}
}

// ======================== helpers ========================

func assertUserDataEqual(t *testing.T, id Identity, got, want UserData) {
	t.Helper()

	if len(got.Objects) != len(want.Objects) {
		t.Fatalf("user %d objects count: got %d, want %d", id, len(got.Objects), len(want.Objects))
	}
	for i, wo := range want.Objects {
		go_ := got.Objects[i]
		if go_.Kind.String() != wo.Kind.String() {
			t.Errorf("user %d obj[%d].kind: got %q, want %q", id, i, go_.Kind, wo.Kind)
		}
		if go_.Key != wo.Key {
			t.Errorf("user %d obj[%d].key: got %q, want %q", id, i, go_.Key, wo.Key)
		}
		if string(go_.Row) != string(wo.Row) {
			t.Errorf("user %d obj[%d].row: got %x, want %x", id, i, go_.Row, wo.Row)
		}
		if go_.ExpiredAt != wo.ExpiredAt {
			t.Errorf("user %d obj[%d].expired_at: got %d, want %d", id, i, go_.ExpiredAt, wo.ExpiredAt)
		}
	}

	if len(got.KVs) != len(want.KVs) {
		t.Fatalf("user %d kvs count: got %d, want %d", id, len(got.KVs), len(want.KVs))
	}
	for i, wkv := range want.KVs {
		gkv := got.KVs[i]
		if gkv.Key != wkv.Key {
			t.Errorf("user %d kv[%d].key: got %q, want %q", id, i, gkv.Key, wkv.Key)
		}
		if string(gkv.Value) != string(wkv.Value) {
			t.Errorf("user %d kv[%d].value: got %q, want %q", id, i, gkv.Value, wkv.Value)
		}
		if gkv.ExpiredAt != wkv.ExpiredAt {
			t.Errorf("user %d kv[%d].expired_at: got %d, want %d", id, i, gkv.ExpiredAt, wkv.ExpiredAt)
		}
	}
}

func assertInt64SliceEqual(t *testing.T, got, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("slice len: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("slice[%d]: got %d, want %d", i, got[i], want[i])
		}
	}
}
