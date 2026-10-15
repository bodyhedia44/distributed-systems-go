package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get makes one sweep over the group's servers and returns whatever
// definitive answer it gets. It does not retry forever: if the whole
// group is unreachable (e.g. it no longer exists), it returns
// ErrWrongLeader so the caller (shardkv1's Clerk) can re-query the
// configuration instead of blocking on a group that may be gone.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	for i := 0; i < len(ck.servers); i++ {
		reply := rpc.GetReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Get", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Value, reply.Version, reply.Err
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
	return "", 0, rpc.ErrWrongLeader
}

// Put makes one sweep over the group's servers, like Get. The caller
// must treat ErrWrongLeader as ambiguous: some attempt within the
// sweep may have reached the leader without the reply making it back.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	ambiguous := false
	for i := 0; i < len(ck.servers); i++ {
		reply := rpc.PutReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.Put", &args, &reply)
		if !ok {
			ambiguous = true
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		if reply.Err == rpc.ErrWrongLeader {
			ambiguous = true
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		if ambiguous && reply.Err == rpc.ErrVersion {
			return rpc.ErrMaybe
		}
		return reply.Err
	}
	return rpc.ErrWrongLeader
}

// FreezeShard, InstallShard and DeleteShard make one sweep over the
// group's servers, like Get/Put: they don't retry forever, since the
// controller (the only caller) needs to notice when a target group is
// gone for good rather than block on it indefinitely.
func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	for i := 0; i < len(ck.servers); i++ {
		reply := shardrpc.FreezeShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.FreezeShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.State, reply.Err
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
	return nil, rpc.ErrWrongLeader
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	for i := 0; i < len(ck.servers); i++ {
		reply := shardrpc.InstallShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.InstallShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Err
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
	return rpc.ErrWrongLeader
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	for i := 0; i < len(ck.servers); i++ {
		reply := shardrpc.DeleteShardReply{}
		ok := ck.Call(ck.servers[ck.leader], "KVServer.DeleteShard", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Err
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
	return rpc.ErrWrongLeader
}
