package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const (
	ConfigKey     = "config"     // the current, in-effect configuration
	NextConfigKey = "nextConfig" // the configuration a reconfiguration is moving towards
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	next, _, ok := sck.readConfig(NextConfigKey)
	if !ok {
		return
	}
	cur, _, ok := sck.readConfig(ConfigKey)
	if !ok || next.Num <= cur.Num {
		return
	}
	sck.complete(next)
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.Put(ConfigKey, cfg.String(), 0)
	sck.Put(NextConfigKey, cfg.String(), 0)
}

func (sck *ShardCtrler) readConfig(key string) (*shardcfg.ShardConfig, rpc.Tversion, bool) {
	val, ver, err := sck.Get(key)
	if err != rpc.OK {
		return nil, 0, false
	}
	return shardcfg.FromString(val), ver, true
}

func (sck *ShardCtrler) recordNext(new *shardcfg.ShardConfig) (target *shardcfg.ShardConfig, ok bool) {
	for {
		next, ver, exists := sck.readConfig(NextConfigKey)
		if exists && next.Num > new.Num {
			return nil, false
		}
		if exists && next.Num == new.Num {
			return next, true
		}
		if sck.Put(NextConfigKey, new.String(), ver) == rpc.OK {
			return new, true
		}
	}
}

func (sck *ShardCtrler) superseded(target *shardcfg.ShardConfig) bool {
	cur, _, ok := sck.readConfig(ConfigKey)
	return ok && cur.Num >= target.Num
}

func (sck *ShardCtrler) freezeShard(srvs []string, s shardcfg.Tshid, target *shardcfg.ShardConfig) (state []byte, aborted bool) {
	ck := shardgrp.MakeClerk(sck.clnt, srvs)
	for {
		st, err := ck.FreezeShard(s, target.Num)
		if err == rpc.OK || err == rpc.ErrWrongGroup {
			return st, false
		}
		if sck.superseded(target) {
			return nil, true
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (sck *ShardCtrler) installShard(srvs []string, s shardcfg.Tshid, state []byte, target *shardcfg.ShardConfig) (aborted bool) {
	ck := shardgrp.MakeClerk(sck.clnt, srvs)
	for {
		if ck.InstallShard(s, state, target.Num) == rpc.OK {
			return false
		}
		if sck.superseded(target) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (sck *ShardCtrler) deleteShard(srvs []string, s shardcfg.Tshid, target *shardcfg.ShardConfig) (aborted bool) {
	ck := shardgrp.MakeClerk(sck.clnt, srvs)
	for {
		if ck.DeleteShard(s, target.Num) == rpc.OK {
			return false
		}
		if sck.superseded(target) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (sck *ShardCtrler) moveShards(cur, target *shardcfg.ShardConfig) bool {
	for s := shardcfg.Tshid(0); s < shardcfg.NShards; s++ {
		oldGid, newGid := cur.Shards[s], target.Shards[s]
		if oldGid == newGid {
			continue
		}
		var state []byte
		if _, srvs, ok := cur.GidServers(s); ok {
			var aborted bool
			if state, aborted = sck.freezeShard(srvs, s, target); aborted {
				return false
			}
		}
		if _, srvs, ok := target.GidServers(s); ok {
			if sck.installShard(srvs, s, state, target) {
				return false
			}
		}
		if _, srvs, ok := cur.GidServers(s); ok {
			if sck.deleteShard(srvs, s, target) {
				return false
			}
		}
	}
	return true
}

func (sck *ShardCtrler) complete(target *shardcfg.ShardConfig) {
	cur, ver, ok := sck.readConfig(ConfigKey)
	if !ok || cur.Num != target.Num-1 {
		return
	}
	if !sck.moveShards(cur, target) {
		return
	}
	sck.Put(ConfigKey, target.String(), ver)
}

func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	cur, _, ok := sck.readConfig(ConfigKey)
	if !ok || cur.Num >= new.Num {
		return
	}
	target, ok := sck.recordNext(new)
	if !ok {
		return
	}
	sck.complete(target)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	val, _, err := sck.Get(ConfigKey)
	if err != rpc.OK {
		return nil
	}
	return shardcfg.FromString(val)
}
