package shardkv

import (
	"encoding/json"
	"log"
	"time"

	"kvstore/common"
)

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters
type Op struct {
	Shard         int
	ReceiverGroup string
	Receive_DB    string
	Receive_Ids   string
	Op            string
	Key           string
	Value         string
	UUID          int64
}

// Method used by PaxosRSM to determine if two Op values are identical
func equals(v1 interface{}, v2 interface{}) bool {
	Op1 := v1.(Op)
	Op2 := v2.(Op)
	if Op1.Op != Op2.Op || Op1.Shard != Op2.Shard || Op1.UUID != Op2.UUID {
		return false
	}
	Op := Op1.Op
	var result bool
	if Op == "Get" {
		result = Op1.Key == Op2.Key
	} else if Op == "Put" || Op == "Append" {
		result = Op1.Value == Op2.Value
	} else if Op == "Send" { // "Query should always return true"
		result = Op1.ReceiverGroup == Op2.ReceiverGroup
	} else if Op == "Receive" { // "Query should always return true"
		result = Op1.Receive_DB == Op2.Receive_DB && Op1.Receive_Ids == Op2.Receive_Ids
	} else if Op == "DUMMY" {
		result = true
	}
	// log.Printf("Comparing Op1 %v and Op2 %v is %v", Op1, Op2, result)
	return result
}

// additions to ShardKV state
type ShardKVImpl struct {
	localShards map[int]int
	db          map[int]map[string]string
	RequestIds  map[int]map[int64]int
	lock        chan bool
}

// initialize kv.impl.*
func (kv *ShardKV) InitImpl() {
	kv.impl.db = make(map[int]map[string]string)
	kv.impl.RequestIds = make(map[int]map[int64]int)
	kv.impl.localShards = make(map[int]int)
	kv.impl.lock = make(chan bool, 1)
	// hack
	go func() {
		for {
			select {
			case <-time.After(50 * time.Millisecond):
				kv.Setunreliable(false)
			}
		}
	}()
}

// RPC handler for client Get requests
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.TryLock()
	defer kv.UnLock()
	Key := args.Key
	UUID := args.Impl.UUID
	key_shard := common.Key2Shard(Key)
	// log.Printf("GID: %v, Server %v handling Get Request with key: %v, Shard %v", kv.gid, kv.me, Key, key_shard)

	// first stage -- adding a dummy operation to let the server catch up
	dummy_op := Op{
		UUID: common.Nrand(),
		Op:   "DUMMY",
	}
	kv.rsm.AddOp(dummy_op)

	// check if the query shard is assigned to this server
	if _, ok := kv.impl.localShards[key_shard]; ok {
		new_op := Op{
			Shard: key_shard,
			UUID:  UUID,
			Key:   Key,
			Op:    "Get",
		}
		kv.rsm.AddOp(new_op)
		reply.Value = kv.impl.db[key_shard][Key]
		// log.Printf("Server %v Get Request with key: %v returns %v", kv.me, Key, reply.Value)
	} else {
		reply.Err = ErrWrongGroup
	}

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	kv.TryLock()
	defer kv.UnLock()
	key, value, UUID, op := args.Key, args.Value, args.Impl.UUID, args.Op
	key_shard := common.Key2Shard(key)

	// first stage -- adding a dummy operation to let the server catch up
	dummy_op := Op{
		UUID: common.Nrand(),
		Op:   "DUMMY",
	}
	kv.rsm.AddOp(dummy_op)

	// check if the query shard is assigned to this server
	if _, ok := kv.impl.localShards[key_shard]; ok {
		new_op := Op{
			UUID:  UUID,
			Shard: key_shard,
			Key:   key,
			Value: value,
			Op:    op,
		}
		kv.rsm.AddOp(new_op)
		// log.Printf("Server GID: %v index: %v handling PutAppend Request with key: %v, Shard %v and value is %v, SUCCESS!!!", kv.gid, kv.me, key, key_shard, value)
	} else {
		// log.Printf("Server GID: %v index: %v handling PutAppend Request with key: %v, Shard %v and value is %v, FAILED WITH ERRWRONGGROUP!!!",
		// kv.gid, kv.me, key, key_shard, value)
		reply.Err = ErrWrongGroup
	}
	return nil
}

// Execute operation encoded in decided value v and update local state
func (kv *ShardKV) ApplyOp(v interface{}) {
	cur_op := v.(Op)
	Shard, RecGroup, RecDB, RecId, Op, Key, Val, UUID := cur_op.Shard, cur_op.ReceiverGroup, cur_op.Receive_DB,
		cur_op.Receive_Ids, cur_op.Op, cur_op.Key, cur_op.Value, cur_op.UUID
	if Op == "Put" || Op == "Append" {
		kv.ApplyPutAppend(Shard, Key, Val, UUID, Op)
	} else if Op == "Get" {
		kv.ApplyGet(Shard, Key, UUID)
	} else if Op == "Send" {
		var ReceiveGroup []string
		json.Unmarshal([]byte(RecGroup), &ReceiveGroup)
		kv.ApplySend(Shard, ReceiveGroup, UUID)
		// log.Printf("GID: %v, Server Index:  %v handled SEND Request to %v!", kv.gid, kv.me, ReceiveGroup)
	} else if Op == "Receive" {
		var Receive_DB map[string]string
		var Receive_Ids map[int64]int
		json.Unmarshal([]byte(RecDB), &Receive_DB)
		json.Unmarshal([]byte(RecId), &Receive_Ids)
		kv.ApplyReceive(Shard, Receive_Ids, Receive_DB, UUID)
	}
	if Op != "Send" {
		// log.Printf("GID: %v, Server Index:  %v handled %v Request!", kv.gid, kv.me, Op)
	}
	// kv.printDB()
}

// Add RPC handlers for any other RPCs you introduce
func (kv *ShardKV) SendShards(args *common.SendShardsArgs, reply *common.SendShardsReply) error {
	kv.TryLock()
	defer kv.UnLock()
	Shard := args.Shard
	ReceiverGroup := args.ReceiverGroup
	// log.Printf("GID:  %v, Server %v handling Send Shards with Shard %v and Target Groups is %v", kv.gid, kv.me, Shard, ReceiverGroup)
	j, _ := json.Marshal(ReceiverGroup)
	// first stage -- adding a dummy operation to let the server catch up
	dummy_op := Op{
		UUID: common.Nrand(),
		Op:   "DUMMY",
	}
	kv.rsm.AddOp(dummy_op)
	new_op := Op{
		Shard:         Shard,
		ReceiverGroup: string(j),
		UUID:          args.UUID,
		Op:            "Send",
	}
	kv.rsm.AddOp(new_op)
	return nil
}

func (kv *ShardKV) ReceiveShards(args *common.ReceiveShardsArgs, reply *common.ReceiveShardsReply) error {
	kv.TryLock()
	defer kv.UnLock()
	Shard := args.Shard
	RequestIds := args.RequestIds
	DB := args.DB
	// log.Printf("GID:  %v, Server %v handling Receive Shards with Shard %v and DB %v", kv.gid, kv.me, Shard, DB)
	j_ids, _ := json.Marshal(RequestIds)
	j_DB, _ := json.Marshal(DB)
	// first stage -- adding a dummy operation to let the server catch up
	dummy_op := Op{
		UUID: common.Nrand(),
		Op:   "DUMMY",
	}
	kv.rsm.AddOp(dummy_op)

	new_op := Op{
		Shard:       Shard,
		Receive_DB:  string(j_DB),
		Receive_Ids: string(j_ids),
		UUID:        args.UUID,
		Op:          "Receive",
	}
	kv.rsm.AddOp(new_op)
	return nil
}

func (kv *ShardKV) ApplyGet(Shard int, Key string, UUID int64) {
	if _, ok := kv.impl.RequestIds[Shard]; !ok {
		kv.impl.RequestIds[Shard] = make(map[int64]int)
	}
	kv.impl.RequestIds[Shard][UUID] = 1
	return
}

func (kv *ShardKV) ApplyPutAppend(Shard int, Key string, Value string, UUID int64, op string) {
	if kv.checkDuplicateRequest(Shard, UUID) { // Duplicate request detection
		// log.Printf("GID: %v, Server index %v thinks this request with Key: %v, Value: %v, UUID: %v, Operation: %v is DUPLICATE!", kv.gid, kv.me, Key, Value, UUID, op)
		return
	}
	if _, ok1 := kv.impl.db[Shard]; !ok1 { // construct new map for non-exist shard
		kv.impl.db[Shard] = make(map[string]string)
	}
	if op == "Put" {
		kv.impl.db[Shard][Key] = Value
	} else if op == "Append" {
		if db_val, ok := kv.impl.db[Shard][Key]; ok {
			db_val += Value
			kv.impl.db[Shard][Key] = db_val
		} else {
			kv.impl.db[Shard][Key] = Value
		}
	}
	if _, ok1 := kv.impl.RequestIds[Shard]; !ok1 {
		kv.impl.RequestIds[Shard] = make(map[int64]int)
	}
	kv.impl.RequestIds[Shard][UUID] = 1
}

func (kv *ShardKV) ApplySend(Shard int, Receiver_Group []string, UUID int64) {
	if kv.checkDuplicateRequest(Shard, UUID) {
		return
	}
	if len(Receiver_Group) == 0 { // if receiver group is zero then this replica group is the last group, do nothing
		kv.impl.RequestIds[Shard][UUID] = 1
		return
	}
	var RequestIds map[int64]int = make(map[int64]int)
	if v, ok := kv.impl.RequestIds[Shard]; ok {
		RequestIds = v
	}
	var DB map[string]string = make(map[string]string)
	if v, ok := kv.impl.db[Shard]; ok {
		DB = v
	}
	for {
		for _, server := range Receiver_Group {
			args := &common.ReceiveShardsArgs{
				RequestIds: RequestIds,
				Shard:      Shard,
				DB:         DB,
				UUID:       UUID,
			}
			rply := &common.ReceiveShardsReply{}
			// log.Printf("GID: %v, Server index: %v, Sending Receive Request to %v", kv.gid, kv.me, server)
			ok := common.Call(server, "ShardKV.ReceiveShards", args, rply)
			if ok {
				delete(kv.impl.localShards, Shard)
				// delete(kv.impl.db, Shard)
				if _, ok := kv.impl.RequestIds[Shard]; !ok {
					kv.impl.RequestIds[Shard] = make(map[int64]int)
				}
				kv.impl.RequestIds[Shard][UUID] = 1
				// log.Printf("GID: %v, Server index: %v, Done Sending Receive Request to %v", kv.gid, kv.me, server)
				return
			}
		}
		// log.Printf("GID: %v, Server index: %v, RETRIES Sending Receive Request to %v", kv.gid, kv.me, Receiver_Group)
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) ApplyReceive(Shard int, RequestIds map[int64]int, Shard_DB map[string]string, UUID int64) {
	if kv.checkDuplicateRequest(Shard, UUID) {
		// log.Printf("GID: %v, Server index: %v Skipped Receive Request with Shard: %v, Shard_DB: %v UUID: %v", kv.gid, kv.me, Shard, Shard_DB, UUID)
		return
	}
	kv.impl.localShards[Shard] = 1
	if _, ok := kv.impl.RequestIds[Shard]; !ok { // construct empty request ids
		kv.impl.RequestIds[Shard] = make(map[int64]int)
	}
	if _, ok := kv.impl.db[Shard]; !ok { // construct empty request ids
		kv.impl.db[Shard] = map[string]string{}
	}
	for k, v := range Shard_DB {
		kv.impl.db[Shard][k] = v
	}
	for k, v := range RequestIds {
		kv.impl.RequestIds[Shard][k] = v
	}
	// register the request
	kv.impl.RequestIds[Shard][UUID] = 1
}

func (kv *ShardKV) checkDuplicateRequest(Shard int, UUID int64) bool {
	// check if the query shard is assigned to this server
	var result bool
	if _, ok := kv.impl.RequestIds[Shard][UUID]; ok {
		result = true
	} else {
		result = false
	}
	return result
}

func (kv *ShardKV) TryLock() {
	for {
		select {
		case kv.impl.lock <- true:
			return
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (kv *ShardKV) UnLock() {
	<-kv.impl.lock
}

// *****************************************
// *****************************************
// Set of helper functions for printing the
// *****************************************
// *****************************************
func (kv *ShardKV) printShards() func() {
	// method used for printing the internal state of the shards
	log.Printf("============GID:   %v, Server:  %v==============", kv.gid, kv.me)
	log.Printf("GID:  %v, Server: %v, has %v shards", kv.gid, kv.me, kv.impl.localShards)
	tmp := func() {
		for {
			kv.Setunreliable(false)
			time.Sleep(100 * time.Millisecond)
		}
	}
	log.Printf("=================================")
	return tmp
	// method used for printing the internal state of the shards
}

func (kv *ShardKV) printDB() {
	log.Printf("============GID:   %v, Server:  %v==============", kv.gid, kv.me)
	log.Printf("GID:  %v, Server: %v, has %v shards", kv.gid, kv.me, kv.impl.localShards)
	for sh := range kv.impl.localShards {
		log.Printf("GID:  %v, Server: %v, Shard: %v has %v", kv.gid, kv.me, sh, kv.impl.db[sh])
	}
	log.Printf("=================================")
}
