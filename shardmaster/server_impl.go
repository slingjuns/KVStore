package shardmaster

import (
	"encoding/json"
	"log"
	"sort"
	"time"

	common "kvstore/common"
)

// Define what goes into "value" that Paxos is used to agree upon.
// Field names must start with capital letters.
type Op struct {
	GID     int64
	Servers string
	UUID    uint64
	Num     int
	Shard   int
	Op      string // "Join" or "Leave" or "Move" or "Query"
}

// Method used by PaxosRSM to determine if two Op values are identical
func equals(v1 interface{}, v2 interface{}) bool {
	Op1 := v1.(Op)
	Op2 := v2.(Op)
	if Op1.Op != Op2.Op || Op1.UUID != Op2.UUID {
		return false
	}
	Op := Op1.Op
	if Op == "Join" {
		return Op1.GID == Op2.GID && Op1.Servers == Op2.Servers
	} else if Op == "Leave" {
		return Op1.GID == Op2.GID
	} else if Op == "Move" {
		return Op1.GID == Op2.GID && Op1.Shard == Op2.Shard
	} else if Op == "Query" { // "Query should always return true"
		return Op1.Num == Op2.Num
	}
	return false
}

// additions to ShardMaster state
type ShardMasterImpl struct {
}

// initialize sm.impl.*
func (sm *ShardMaster) InitImpl() {
}

// RPC handlers for Join, Leave, Move, and Query RPCs
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// challenge is how to divide existing shards evenly, the number of server groups is flexible
	// i.e  given 100 shards, 10 server groups,  should be [0, 10], [11, 20], ... [90, 100]
	// i.e. given 100 shards, 10 server groups, (adding 2 more server groups) -- 100 / 12 = 8 (which means every existing server group should )
	j, _ := json.Marshal(args.Servers)
	new_op := Op{
		GID:     args.GID,
		Servers: string(j),
		Op:      "Join",
		UUID:    uint64(common.Nrand()),
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.rsm.AddOp(new_op)
	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	new_op := Op{
		GID:  args.GID,
		Op:   "Leave",
		UUID: uint64(common.Nrand()),
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.rsm.AddOp(new_op)
	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	new_op := Op{
		GID:   args.GID,
		Shard: args.Shard,
		Op:    "Move",
		UUID:  uint64(common.Nrand()),
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.rsm.AddOp(new_op)
	// log.Printf("Server %v Handled Move Request: Shard:  %v,  TO GID:  %v", sm.me, args.Shard, args.GID)
	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	new_op := Op{
		Num:  args.Num,
		UUID: uint64(common.Nrand()),
		Op:   "Query",
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.rsm.AddOp(new_op)
	reply.Config = sm.Get_Query(args.Num)
	// sm.printShards(reply.Config.Shards, reply.Config.Groups)
	return nil
}

// Execute operation encoded in decided value v and update local state
func (sm *ShardMaster) ApplyOp(v interface{}) { // reconfiguration
	cur_op := v.(Op)
	GID, _, Shard, Op := cur_op.GID, cur_op.Num, cur_op.Shard, cur_op.Op
	if Op == "Join" {
		// log.Printf("Server index: %v, Apply Join: GID:  %v", sm.me, GID)
		sm.ApplyJoin(GID, cur_op.Servers)
	} else if Op == "Leave" {
		// log.Printf("Server index: %v, Apply Leave: GID:  %v", sm.me, GID)
		sm.ApplyLeave(GID)
	} else if Op == "Move" {
		sm.ApplyMove(GID, Shard)
	}
}

func (sm *ShardMaster) NewConfig(c1 Config) Config { // copy previous config and return a deep copy
	var newConfig Config
	newConfig.Num = c1.Num + 1
	newConfig.Groups = map[int64][]string{}
	// copy existing shards
	for i, v := range c1.Shards {
		newConfig.Shards[i] = v
	}
	// copy existing groups
	for k, v := range c1.Groups {
		newConfig.Groups[k] = v
	}
	return newConfig
}

func (sm *ShardMaster) ApplyJoin(Join_GID int64, Join_Servers string) {
	// Unmarshal json object
	var Servers []string
	json.Unmarshal([]byte(Join_Servers), &Servers)
	latestConfig := sm.configs[len(sm.configs)-1]
	if _, ok := latestConfig.Groups[Join_GID]; ok { // handle repeated JOIN
		return
	}
	shardsCounter := sm.ShardsMapping(latestConfig)
	_, ok1 := latestConfig.Groups[Join_GID]
	_, ok2 := shardsCounter[Join_GID]
	if ok1 && ok2 { // joined group is already balanced
		return
	}
	NServers := len(shardsCounter) + 1

	// sort the shardsCounter -- also accounts for GID group that has no shards assigned
	var sortedCounter []common.Shard2Server // pair of <GID, Count>
	for k, _ := range latestConfig.Groups {
		if v, ok := shardsCounter[k]; ok {
			sortedCounter = append(sortedCounter, common.Shard2Server{k, len(v)})
		} else {
			sortedCounter = append(sortedCounter, common.Shard2Server{k, 0})
		}
	}
	sort.Slice(sortedCounter, func(i, j int) bool {
		if sortedCounter[i].Count == sortedCounter[j].Count {
			return sortedCounter[i].GID > sortedCounter[j].GID
		}
		return sortedCounter[i].Count > sortedCounter[j].Count // Descending Order
	})
	// log.Printf("============Server:  %v==============", sm.me)
	// log.Printf("Sorted Counter is:    %v", sortedCounter)
	// log.Printf("=================================")

	var targetCount = common.NShards / NServers
	var newConfig = sm.NewConfig(latestConfig)
	// register groups
	newConfig.Groups[Join_GID] = Servers
	if NServers == 1 {
		for i := 0; i < len(newConfig.Shards); i++ {
			OLD_GID := newConfig.Shards[i]
			newConfig.Shards[i] = Join_GID
			sm.AssignNewGroup(i, OLD_GID, Join_GID, newConfig, true)
		}
		sm.configs = append(sm.configs, newConfig)
		return
	}

	for targetCount > 0 {
		for i := 0; i < len(sortedCounter); i++ { // Round Robin from the largest count, modify master ...
			if targetCount <= 0 {
				break
			}
			curr := sortedCounter[i]
			currShards := shardsCounter[curr.GID]
			moved_N := currShards[len(currShards)-1]
			OLD_GID := newConfig.Shards[moved_N]
			newConfig.Shards[moved_N] = Join_GID
			sm.AssignNewGroup(moved_N, OLD_GID, Join_GID, newConfig, false)
			shardsCounter[curr.GID] = currShards[:len(currShards)-1] // remove
			curr.Count--                                             // move one shard to the new group
			targetCount--
		}
	}

	// append newConfig
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) ApplyLeave(Leave_GID int64) {
	latestConfig := sm.configs[len(sm.configs)-1]
	shardsCounter := sm.ShardsMapping(latestConfig)
	if _, ok := latestConfig.Groups[Leave_GID]; !ok { // handle repeated Leave
		return
	}
	// sort the shardsCounter
	var sortedCounter []common.Shard2Server // pair of <GID, Count>
	for k, _ := range latestConfig.Groups {
		if k == Leave_GID {
			continue
		}
		if v, ok := shardsCounter[k]; ok {
			sortedCounter = append(sortedCounter, common.Shard2Server{k, len(v)})
		} else {
			sortedCounter = append(sortedCounter, common.Shard2Server{k, 0})
		}
	}
	sort.Slice(sortedCounter, func(i, j int) bool {
		if sortedCounter[i].Count == sortedCounter[j].Count {
			return sortedCounter[i].GID < sortedCounter[j].GID
		}
		return sortedCounter[i].Count < sortedCounter[j].Count // Ascending Order
	})

	var newConfig = sm.NewConfig(latestConfig)
	var targetShards = shardsCounter[Leave_GID]

	if len(targetShards) == len(latestConfig.Shards) { // remove the only existing group
		var tmp [common.NShards]int64
		for i := 0; i < common.NShards; i++ {
			OLD_GID := newConfig.Shards[i]
			sm.AssignNewGroup(i, OLD_GID, 0, newConfig, false)
		}
		newConfig.Shards = tmp
		// remove group
		delete(newConfig.Groups, Leave_GID)
		sm.configs = append(sm.configs, newConfig)
		return
	}
	var i = 0
	var j = 0
	for i < len(targetShards) { // Round Robin from the largest count, modify master ...
		moved_N := targetShards[i]
		curr := sortedCounter[j]
		OLD_GID := newConfig.Shards[moved_N]
		newConfig.Shards[moved_N] = curr.GID // move leaving shard to the existing group
		sm.AssignNewGroup(moved_N, OLD_GID, curr.GID, newConfig, false)
		i++
		if j == len(sortedCounter)-1 {
			j = 0
		} else {
			j++
		}
	}
	// remove group
	delete(newConfig.Groups, Leave_GID)
	// append newConfig
	sm.configs = append(sm.configs, newConfig)
}

func (sm *ShardMaster) ApplyMove(GID int64, Shard int) {
	latestConfig := sm.configs[len(sm.configs)-1]
	var newConfig = sm.NewConfig(latestConfig)
	OLD_GID := newConfig.Shards[Shard]
	if OLD_GID == GID { // send shards within the group, do nothing
		return
	}
	newConfig.Shards[Shard] = GID
	sm.AssignNewGroup(Shard, OLD_GID, GID, newConfig, false)
	// append newConfig
	sm.configs = append(sm.configs, newConfig)
	// log.Printf("Master Server %v moved Shard %v to new Group with GID: %v", sm.me, Shard, GID)
}

func (sm *ShardMaster) Get_Query(Num int) Config {
	latestConfig := sm.configs[len(sm.configs)-1]
	if Num == -1 || Num > latestConfig.Num {
		return latestConfig
	} else {
		return sm.configs[Num]
	}
}

func (sm *ShardMaster) ShardsMapping(latestConfig Config) map[int64][]int {
	// returns empty map when no shards assigned to a valid group(GID not 0)
	// Construct map of GID -> responsible shards
	shardsCounter := make(map[int64][]int)
	for shard_i, GID := range latestConfig.Shards {
		if GID == 0 {
			continue
		}
		if arr, ok := shardsCounter[GID]; ok {
			shardsCounter[GID] = append(arr, shard_i)
		} else {
			shardsCounter[GID] = []int{shard_i}
		}
	}
	return shardsCounter
}

func (sm *ShardMaster) AssignNewGroup(Shard int, OLD_GID int64, NEW_GID int64, newConfig Config, isFirst bool) {
	UUID := common.Nrand()
	for {
		if isFirst { // if the new group is the first group, there is no previous group that can store the data, so everything is empty
			for _, server := range newConfig.Groups[NEW_GID] {
				args := &common.ReceiveShardsArgs{
					Shard:      Shard,
					DB:         make(map[string]string),
					RequestIds: make(map[int64]int),
					UUID:       UUID,
				}
				rply := &common.ReceiveShardsReply{}
				ok := common.Call(server, "ShardKV.ReceiveShards", args, rply)
				if ok {
					return
				}
			}
		} else {
			for _, server := range newConfig.Groups[OLD_GID] {
				args := &common.SendShardsArgs{
					Shard:         Shard,
					ReceiverGroup: newConfig.Groups[NEW_GID],
					UUID:          UUID,
				}
				rply := &common.SendShardsReply{}
				ok := common.Call(server, "ShardKV.SendShards", args, rply)
				if ok {
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func compareSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (sm *ShardMaster) printShards(shards [common.NShards]int64, groups map[int64][]string) {
	tmp := make(map[int64][]int)
	for i, v := range shards {
		tmp[v] = append(tmp[v], i)
	}
	log.Printf("============Server:  %v==============", sm.me)
	for k, _ := range groups {
		if v, ok := tmp[k]; ok {
			log.Printf("GID:  %v has shards %v", k, v)
		} else {
			emp := make([]int, 0)
			log.Printf("GID:  %v has shards %v", k, emp)
		}
	}
	log.Printf("=================================")
}

func (sm *ShardMaster) printGroups(groups map[int64][]string) {
	log.Printf("====Print groups Server:  %v============", sm.me)
	for k, _ := range groups {
		log.Printf("GID:  %v in servers group", k)
	}
	log.Printf("=================================")
}
