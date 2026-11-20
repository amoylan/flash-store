package snapshot

import (
	"fmt"

	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/snapshot/config"
	"github.com/amoylan/flash-store/store/snapshot/pb"
	"google.golang.org/protobuf/proto"
)

type protoCodec struct{}

// ======================== Shard ========================

func (protoCodec) EncodeShard(shard ShardSnapshot) ([]byte, error) {
	wire := &pb.ShardSnapshotPb{
		ShardId: int32(shard.ShardID),
	}

	if len(shard.Users) > 0 {
		wire.Users = make(map[uint64]*pb.UserDataPb, len(shard.Users))
		for id, ud := range shard.Users {
			wire.Users[id] = encodeUserDataProto(ud)
		}
	}

	return proto.Marshal(wire)
}

func encodeUserDataProto(ud UserData) *pb.UserDataPb {
	pud := &pb.UserDataPb{}

	if len(ud.Objects) > 0 {
		pud.Objects = make([]*pb.ObjectEntryPb, len(ud.Objects))
		for i, o := range ud.Objects {
			pud.Objects[i] = &pb.ObjectEntryPb{
				Kind:      o.Kind.String(),
				Key:       o.Key,
				Row:       o.Row,
				ExpiredAt: o.ExpiredAt,
			}
		}
	}

	if len(ud.KVs) > 0 {
		pud.Kvs = make([]*pb.KVEntryPb, len(ud.KVs))
		for i, kv := range ud.KVs {
			pud.Kvs[i] = &pb.KVEntryPb{
				Key:       kv.Key,
				Value:     kv.Value,
				ExpiredAt: kv.ExpiredAt,
			}
		}
	}

	return pud
}

func (protoCodec) DecodeShard(data []byte) (*ShardSnapshot, error) {
	var wire pb.ShardSnapshotPb
	if err := proto.Unmarshal(data, &wire); err != nil {
		return nil, err
	}

	snap := &ShardSnapshot{
		ShardID: int(wire.ShardId),
	}

	if len(wire.Users) > 0 {
		snap.Users = make(map[Identity]UserData, len(wire.Users))
		for id, pud := range wire.Users {
			ud, err := decodeUserDataProto(pud)
			if err != nil {
				return nil, fmt.Errorf("snapshot proto decode user %d: %w", id, err)
			}
			snap.Users[id] = ud
		}
	}

	return snap, nil
}

func decodeUserDataProto(pud *pb.UserDataPb) (UserData, error) {
	var ud UserData

	if len(pud.Objects) > 0 {
		ud.Objects = make([]ObjectEntry, len(pud.Objects))
		for i, po := range pud.Objects {
			kind, err := registry.ParseKindFromString(po.Kind)
			if err != nil {
				return UserData{}, fmt.Errorf("parse kind %q: %w", po.Kind, err)
			}
			ud.Objects[i] = ObjectEntry{
				Kind:      kind,
				Key:       po.Key,
				Row:       po.Row,
				ExpiredAt: po.ExpiredAt,
			}
		}
	}

	if len(pud.Kvs) > 0 {
		ud.KVs = make([]KVEntry, len(pud.Kvs))
		for i, pkv := range pud.Kvs {
			ud.KVs[i] = KVEntry{
				Key:       pkv.Key,
				Value:     pkv.Value,
				ExpiredAt: pkv.ExpiredAt,
			}
		}
	}

	return ud, nil
}

// ======================== Worker ========================

func (protoCodec) EncodeWorker(worker WorkerSnapshot) ([]byte, error) {
	wire := &pb.WorkerSnapshotPb{
		WorkerId:  int32(worker.WorkerID),
		Seq:       worker.Seq,
		WalOffset: worker.WalOffset,
	}

	wire.SeqDedup = &pb.SeqDedupStatePb{
		MaxSeen:    worker.SeqDedup.MaxSeen,
		WindowSize: worker.SeqDedup.WindowSize,
		Seen:       worker.SeqDedup.Seen,
	}

	return proto.Marshal(wire)
}

func (protoCodec) DecodeWorker(data []byte) (*WorkerSnapshot, error) {
	var wire pb.WorkerSnapshotPb
	if err := proto.Unmarshal(data, &wire); err != nil {
		return nil, err
	}

	snap := &WorkerSnapshot{
		WorkerID:  int(wire.WorkerId),
		Seq:       wire.Seq,
		WalOffset: wire.WalOffset,
	}

	if wire.SeqDedup != nil {
		snap.SeqDedup = SeqDedupState{
			MaxSeen:    wire.SeqDedup.MaxSeen,
			WindowSize: wire.SeqDedup.WindowSize,
			Seen:       wire.SeqDedup.Seen,
		}
	}

	return snap, nil
}

func init() {
	RegisterCodec(config.SnapshotFormatProtobuf, protoCodec{})
}
