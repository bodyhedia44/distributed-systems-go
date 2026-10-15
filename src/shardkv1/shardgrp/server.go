package shardgrp

import (
	"bytes"
	"log"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type value struct {
	Val     string
	Version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	m        map[string]value
	shards   map[shardcfg.Tshid]bool          // shards this group currently holds data for
	frozen   map[shardcfg.Tshid]bool          // shards held but rejecting Puts (being moved away)
	shardNum map[shardcfg.Tshid]shardcfg.Tnum // highest config Num seen per shard, to fence stale Freeze/Install/Delete RPCs
}

func (kv *KVServer) shardState(s shardcfg.Tshid) map[string]value {
	st := make(map[string]value)
	for k, v := range kv.m {
		if shardcfg.Key2Shard(k) == s {
			st[k] = v
		}
	}
	return st
}

func (kv *KVServer) DoOp(req any) any {
	switch args := req.(type) {
	case rpc.GetArgs:
		if !kv.shards[shardcfg.Key2Shard(args.Key)] {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		if val, ok := kv.m[args.Key]; ok {
			return rpc.GetReply{Value: val.Val, Version: val.Version, Err: rpc.OK}
		}
		return rpc.GetReply{Err: rpc.ErrNoKey}
	case rpc.PutArgs:
		shard := shardcfg.Key2Shard(args.Key)
		if !kv.shards[shard] || kv.frozen[shard] {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		val, ok := kv.m[args.Key]
		if ok {
			if args.Version == val.Version {
				kv.m[args.Key] = value{Val: args.Value, Version: val.Version + 1}
				return rpc.PutReply{Err: rpc.OK}
			}
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		if args.Version == 0 {
			kv.m[args.Key] = value{Val: args.Value, Version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		return rpc.PutReply{Err: rpc.ErrNoKey}
	case shardrpc.FreezeShardArgs:
		s := args.Shard
		if args.Num >= kv.shardNum[s] {
			kv.shardNum[s] = args.Num
			kv.frozen[s] = true
		}
		if !kv.shards[s] {
			return shardrpc.FreezeShardReply{Err: rpc.ErrWrongGroup}
		}
		st := kv.shardState(s)
		w := new(bytes.Buffer)
		if err := labgob.NewEncoder(w).Encode(st); err != nil {
			log.Fatalf("%d: couldn't encode shard %d state: %v", kv.me, s, err)
		}
		return shardrpc.FreezeShardReply{State: w.Bytes(), Num: kv.shardNum[s], Err: rpc.OK}
	case shardrpc.InstallShardArgs:
		s := args.Shard
		if args.Num <= kv.shardNum[s] {
			// stale or already-installed; idempotent no-op
			return shardrpc.InstallShardReply{Err: rpc.OK}
		}
		var st map[string]value
		if len(args.State) > 0 {
			r := bytes.NewBuffer(args.State)
			if err := labgob.NewDecoder(r).Decode(&st); err != nil {
				log.Fatalf("%d: couldn't decode shard %d state: %v", kv.me, s, err)
			}
			for k, v := range st {
				kv.m[k] = v
			}
		}
		kv.shardNum[s] = args.Num
		kv.shards[s] = true
		kv.frozen[s] = false
		return shardrpc.InstallShardReply{Err: rpc.OK}
	case shardrpc.DeleteShardArgs:
		s := args.Shard
		if args.Num < kv.shardNum[s] {
			// stale; a more recent transition already happened for this shard
			return shardrpc.DeleteShardReply{Err: rpc.OK}
		}
		for k := range kv.m {
			if shardcfg.Key2Shard(k) == s {
				delete(kv.m, k)
			}
		}
		kv.shardNum[s] = args.Num
		kv.shards[s] = false
		kv.frozen[s] = false
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.m); err != nil {
		log.Fatalf("%d: couldn't encode kv map: %v", kv.me, err)
	}
	if err := e.Encode(kv.shards); err != nil {
		log.Fatalf("%d: couldn't encode shards: %v", kv.me, err)
	}
	if err := e.Encode(kv.frozen); err != nil {
		log.Fatalf("%d: couldn't encode frozen: %v", kv.me, err)
	}
	if err := e.Encode(kv.shardNum); err != nil {
		log.Fatalf("%d: couldn't encode shardNum: %v", kv.me, err)
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var m map[string]value
	var shards map[shardcfg.Tshid]bool
	var frozen map[shardcfg.Tshid]bool
	var shardNum map[shardcfg.Tshid]shardcfg.Tnum
	if err := d.Decode(&m); err != nil {
		log.Fatalf("%d: couldn't decode kv map: %v", kv.me, err)
	}
	if err := d.Decode(&shards); err != nil {
		log.Fatalf("%d: couldn't decode shards: %v", kv.me, err)
	}
	if err := d.Decode(&frozen); err != nil {
		log.Fatalf("%d: couldn't decode frozen: %v", kv.me, err)
	}
	if err := d.Decode(&shardNum); err != nil {
		log.Fatalf("%d: couldn't decode shardNum: %v", kv.me, err)
	}
	kv.m = m
	kv.shards = shards
	kv.frozen = frozen
	kv.shardNum = shardNum
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{
		gid:      gid,
		me:       me,
		m:        make(map[string]value),
		shards:   make(map[shardcfg.Tshid]bool),
		frozen:   make(map[shardcfg.Tshid]bool),
		shardNum: make(map[shardcfg.Tshid]shardcfg.Tnum),
	}
	if gid == shardcfg.Gid1 {
		// the first shardgrp owns all shards until a reconfiguration says otherwise
		for s := shardcfg.Tshid(0); s < shardcfg.NShards; s++ {
			kv.shards[s] = true
		}
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
