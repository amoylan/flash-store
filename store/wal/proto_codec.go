package wal

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/amoylan/flash-store/store/registry"
	"github.com/amoylan/flash-store/store/schema"
	"github.com/amoylan/flash-store/store/wal/config"
	"github.com/amoylan/flash-store/store/wal/pb"
	"google.golang.org/protobuf/proto"
)

type protoWalCodec struct{}

func (protoWalCodec) Encode(w *Wal) ([]byte, error) {
	wire := &pb.WalPb{}

	if len(w.Events) > 0 {
		wire.Events = make([]*pb.EventPb, len(w.Events))
		for i, ev := range w.Events {
			pev, err := encodeEventProto(ev)
			if err != nil {
				return nil, err
			}
			wire.Events[i] = pev
		}
	}

	// Schemas 走 JSON 嵌入进 proto——频率极低（每次 master 启动一次），
	// 不值得为它多维护一份 proto schema。
	if len(w.Schemas) > 0 {
		schemasJSON, err := json.Marshal(w.Schemas)
		if err != nil {
			return nil, fmt.Errorf("wal proto encode schemas: %w", err)
		}
		wire.SchemasJson = schemasJSON
	}

	return proto.Marshal(wire)
}

func encodeEventProto(ev Event) (*pb.EventPb, error) {
	pev := &pb.EventPb{
		EventType: ev.EventType,
		Identity:  ev.Identity,
	}

	if len(ev.Cmds) > 0 {
		pev.Cmds = make([]*pb.CmdPb, len(ev.Cmds))
		for i, c := range ev.Cmds {
			pc, err := encodeCmdProto(c)
			if err != nil {
				return nil, err
			}
			pev.Cmds[i] = pc
		}
	}

	if len(ev.KvCmds) > 0 {
		pev.KvCmds = make([]*pb.KvCmdPb, len(ev.KvCmds))
		for i, kc := range ev.KvCmds {
			pev.KvCmds[i] = &pb.KvCmdPb{
				Op:    int32(kc.Op),
				Key:   kc.Key,
				Value: kc.Value,
				TtlNs: int64(kc.Ttl),
			}
		}
	}

	if ev.BizData != nil {
		pev.BizData = &pb.BizDataPb{
			Version: uint32(ev.BizData.Version()),
			Data:    ev.BizData.Bytes(),
		}
	}

	return pev, nil
}

func encodeCmdProto(c Cmd) (*pb.CmdPb, error) {
	pc := &pb.CmdPb{
		Op:    int32(c.Op),
		Key:   c.Key,
		Kind:  c.Kind.String(),
		TtlNs: int64(c.Ttl),
	}
	if c.Op == OpSet {
		rowBytes, err := schema.EncodeRow(c.Row)
		if err != nil {
			return nil, fmt.Errorf("wal proto encode Row: %w", err)
		}
		pc.Row = rowBytes
	}
	return pc, nil
}

func (c protoWalCodec) Decode(data []byte) (*Wal, error) {
	var wire pb.WalPb
	if err := proto.Unmarshal(data, &wire); err != nil {
		return nil, err
	}

	out := &Wal{}

	if len(wire.SchemasJson) > 0 {
		if err := json.Unmarshal(wire.SchemasJson, &out.Schemas); err != nil {
			return nil, fmt.Errorf("wal proto decode schemas: %w", err)
		}
	}

	if len(wire.Events) > 0 {
		out.Events = make([]Event, len(wire.Events))
		for i, pev := range wire.Events {
			ev, err := decodeEventProto(pev)
			if err != nil {
				return nil, err
			}
			out.Events[i] = ev
		}
	}

	return out, nil
}

func decodeEventProto(pev *pb.EventPb) (Event, error) {
	ev := Event{
		EventType: pev.EventType,
		Identity:  pev.Identity,
	}

	if len(pev.Cmds) > 0 {
		ev.Cmds = make([]Cmd, len(pev.Cmds))
		for i, pc := range pev.Cmds {
			c, err := decodeCmdProto(pc)
			if err != nil {
				return Event{}, err
			}
			ev.Cmds[i] = c
		}
	}

	if len(pev.KvCmds) > 0 {
		ev.KvCmds = make([]KvCmd, len(pev.KvCmds))
		for i, pkc := range pev.KvCmds {
			ev.KvCmds[i] = KvCmd{
				Op:    Operation(pkc.Op),
				Key:   pkc.Key,
				Value: pkc.Value,
				Ttl:   time.Duration(pkc.TtlNs),
			}
		}
	}

	if pev.BizData != nil {
		raw := make([]byte, 1+len(pev.BizData.Data))
		raw[0] = uint8(pev.BizData.Version)
		copy(raw[1:], pev.BizData.Data)
		ev.BizData = RawBizData(raw)
	}

	return ev, nil
}

func decodeCmdProto(pc *pb.CmdPb) (Cmd, error) {
	kind, err := registry.ParseKindFromString(pc.Kind)
	if err != nil {
		return Cmd{}, fmt.Errorf("wal proto decode: %w", err)
	}
	c := Cmd{
		Op:   Operation(pc.Op),
		Key:  pc.Key,
		Kind: kind,
		Ttl:  time.Duration(pc.TtlNs),
	}
	if c.Op == OpSet {
		row, err := schema.DecodeRow(pc.Row)
		if err != nil {
			return Cmd{}, fmt.Errorf("wal proto decode Row key %q: %w", pc.Key, err)
		}
		c.Row = row
	}
	return c, nil
}

func init() {
	RegisterWalCodec(config.WalFormatProtobuf, protoWalCodec{})
}
