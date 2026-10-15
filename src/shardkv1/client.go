package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"time"

	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// clerkFor returns the (cached) shardgrp clerk for gid, creating one if needed.
// A group's member servers never change, so caching by gid is safe.
func (ck *Clerk) clerkFor(gid tester.Tgid, servers []string) *shardgrp.Clerk {
	rck, ok := ck.GetClerk(gid)
	if !ok {
		rck = shardgrp.MakeClerk(ck.clnt, servers)
		ck.rcks[gid] = rck
	}
	return rck
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shard := shardcfg.Key2Shard(key)
	for {
		cfg := ck.sck.Query()
		gid, servers, ok := cfg.GidServers(shard)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		rck := ck.clerkFor(gid, servers)
		val, ver, err := rck.Get(key)
		if err == rpc.ErrWrongGroup {
			continue
		}
		if err == rpc.ErrWrongLeader {
			// shardgrp may be mid-election, unreachable, or gone (shard
			// moved elsewhere); re-check the config and retry.
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return val, ver, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shard := shardcfg.Key2Shard(key)
	ambiguous := false
	for {
		cfg := ck.sck.Query()
		gid, servers, ok := cfg.GidServers(shard)
		if !ok {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		rck := ck.clerkFor(gid, servers)
		err := rck.Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			continue
		}
		if err == rpc.ErrWrongLeader {
			// ambiguous: the shardgrp clerk may have reached the leader
			// without the reply making it back; remember that across retries.
			ambiguous = true
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if ambiguous && err == rpc.ErrVersion {
			return rpc.ErrMaybe
		}
		return err
	}
}
