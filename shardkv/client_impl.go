package shardkv

import (
	"time"

	"kvstore/common"
)

// additions to Clerk state
type ClerkImpl struct {
}

// initialize ck.impl.*
func (ck *Clerk) InitImpl() {
}

// fetch the current value for a key.
// return "" if the key does not exist.
// keep retrying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	key_shard := common.Key2Shard(key)
	UUID := common.Nrand()
	var servers = ck.getTargetServers(key_shard)
	for {
		for _, server := range servers {
			args := &GetArgs{
				Key: key,
				Impl: GetArgsImpl{
					UUID: UUID,
				},
			}
			rply := &GetReply{}
			ok := common.Call(server, "ShardKV.Get", args, rply)
			if ok {
				if rply.Err == ErrWrongGroup {
					servers = ck.getTargetServers(key_shard)
					break
				} else {
					return rply.Value
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// send a Put or Append request.
// keep retrying forever until success.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	key_shard := common.Key2Shard(key)
	UUID := common.Nrand()
	var servers = ck.getTargetServers(key_shard)
	for {
		for _, server := range servers {
			args := &PutAppendArgs{
				Key:   key,
				Value: value,
				Impl: PutAppendArgsImpl{
					UUID: UUID,
				},
				Op: op,
			}
			rply := &PutAppendReply{}
			ok := common.Call(server, "ShardKV.PutAppend", args, rply)
			if ok && rply.Err == "" {
				return
			}
		}
		servers = ck.getTargetServers(key_shard)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) getTargetServers(Shard int) []string {
	cf := ck.sm.Query(-1)
	GID := cf.Shards[Shard]
	return cf.Groups[GID]
}
