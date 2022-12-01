package paxos

import (
	"log"
	"math"
	"math/rand"
	"sort"
	"time"

	"kvstore/common"
)

type SharedInfo struct {
	fate Fate
	Np   float64 // highest prepare proposal number ever seen
}

type AcceptorInfo struct {
	ProposalAcc float64
	ValAcc      interface{}
}

// additions to Paxos state.
type PaxosImpl struct {
	SeqInfo      map[int]SharedInfo   // seq number status
	AcceptorInfo map[int]AcceptorInfo // seq to state
	DoneValues   []int                // done values received from Paxos Peers
}

type Proposal struct {
	id         int
	seq        int
	proposeVal interface{}
}

// your px.impl.* initializations here.
func (px *Paxos) initImpl() {
	px.impl.SeqInfo = make(map[int]SharedInfo)
	px.impl.AcceptorInfo = make(map[int]AcceptorInfo)
	px.impl.DoneValues = make([]int, len(px.peers))
	for i := range px.impl.DoneValues { // initialize done values of -1
		px.impl.DoneValues[i] = -1
	}
	go func() { // Garbage Seq number Collection Thread
		for {
			seq := px.Min()
			px.mu.Lock()
			for k := range px.impl.SeqInfo {
				if k < seq { // forgotten values
					if info, ok := px.impl.AcceptorInfo[k]; ok {
						info.ValAcc = nil
					}
					delete(px.impl.SeqInfo, k)
					delete(px.impl.AcceptorInfo, k)
				}
			}
			px.mu.Unlock()
			time.Sleep(1000 * time.Millisecond)
		}
	}()
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	// log.Printf("Starting a proposal function with seq number of %d with server index of %d", seq, px.me)
	done := px.Min()
	if seq < done {
		return
	}
	go func() {
		proposal := Proposal{
			seq:        seq,
			proposeVal: v,
		}
		px.Propose(&proposal)
	}()
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	px.mu.Lock()
	defer px.mu.Unlock()
	current_done := px.impl.DoneValues[px.me]
	if seq > current_done {
		px.impl.DoneValues[px.me] = seq
	}
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	return max_key(px.impl.SeqInfo)
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another peer's Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s will not increase
// even if all reachable peers call Done(). The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefore cannot forget these
// instances.
func (px *Paxos) Min() int {
	px.mu.Lock()
	defer px.mu.Unlock()
	// log.Printf("Server index %d is checking the Min of Done Values", px.me)
	minVal := math.MaxInt64
	for _, value := range px.impl.DoneValues {
		if value < minVal {
			minVal = value
		}
	}
	// log.Printf("Server index %d Min Done Value is:  %d ", px.me, minVal+1)
	return minVal + 1 // Note: return min + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so, what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	done := px.Min()
	// log.Printf("Server index %d is checking the status of seq number %d, min of Done Values is %d", px.me, seq, done)
	if seq < done { // if status() is called with a sequence number less than min(), status() should return forgotten
		return Forgotten, nil
	}
	px.mu.Lock() // dead lock
	var val interface{} = nil
	fate := Pending
	if info, ok := px.impl.SeqInfo[seq]; ok {
		fate = info.fate
		if fate == Decided {
			val = px.impl.AcceptorInfo[seq].ValAcc
		}
	}
	// log.Printf("Status of the server index %d with seq number %d is:  %v, val is %v", px.me, seq, fate, val)
	px.mu.Unlock()
	return fate, val
}

func (px *Paxos) Propose(proposal *Proposal) {
	px.mu.Lock()
	_, seq, proposeVal := proposal.id, proposal.seq, proposal.proposeVal
	// Choose N, unique and higher than any proposal number seen so far
	var next_np float64
	next_np = uniquePN(1, px.me, len(px.peers)*10) // generate unique proposal number
	px.mu.Unlock()
	for px.isdead() == false {
		// log.Printf("Proposing seq number of %d with a proposal number of %f from server index %d\n, with initial value of %v", seq, next_np, px.me, proposeVal)
		isDecided, _ := px.Status(seq)
		if isDecided == Decided {
			// log.Printf("Server index %d skipping the prepare and accept phase with seq number of %d", px.me, seq)
			break
		}
		// Phase 1: Prepare Phase send prepare to all servers include self
		px.mu.Lock()
		var received_prepare_message []*PrepareReply
		var counter map[interface{}]int = make(map[interface{}]int)
		var max_reject_num float64 = next_np
		var skipAccept bool = false
		var learnPhase bool = false
		var accept_value interface{} = nil

		for i, peer := range px.peers {
			args := &PrepareArgs{
				Seq:         seq,
				ProposalNum: next_np,
				Index:       px.me,
				Done:        px.impl.DoneValues[px.me],
			}
			rply := &PrepareReply{}
			var ok bool
			if i == px.me { // skip sending rpc to self by calling helper function directly
				ok = px.prepare_helper(args, rply)
			} else {
				px.mu.Unlock()
				ok = common.Call(peer, "Paxos.Prepare", args, rply)
				px.mu.Lock()
			}
			if ok {
				if rply.Reject == false {
					received_prepare_message = append(received_prepare_message, rply)
					counter[rply.ValAcc] += 1 // count all accepted value
				} else {
					if rply.ProposalPromise > max_reject_num {
						max_reject_num = rply.ProposalPromise
					}

				}
				// log.Printf("Server index %d,   The message from server index %d is: %v", px.me, i, rply)
			}
		}
		if skip, k, _ := px.checkDecided(counter, len(px.peers)/2); skip { //check skipping accept phase
			if k != nil {
				// log.Printf("Server index %d skipping the accept phase with seq number of %d", px.me, seq)
				accept_value = k
				skipAccept = true
				learnPhase = true
			}
		}
		if !skipAccept && isMajority_Prepare(received_prepare_message, px.peers) { // majority ack the proposal
			// log.Printf("Entering Accept Phase on seq number %d, the proposer server index is: %d", seq, px.me)
			// Phase 2: Accept Phase
			_, acc_val := pickHighestNA(received_prepare_message, proposeVal)
			accept_value = acc_val
			var received_accept_message []*AcceptReply
			for i, peer := range px.peers {
				acc_args := &AcceptArgs{
					Seq:         seq,
					ProposalNum: next_np,
					V:           accept_value,
				}
				acc_rply := &AcceptReply{}
				var ok bool
				if i == px.me { // skip sending rpc to self
					ok = px.accept_helper(acc_args, acc_rply)
				} else {
					px.mu.Unlock()
					ok = common.Call(peer, "Paxos.Accept", acc_args, acc_rply)
					px.mu.Lock()
				}
				if ok {
					if acc_rply.Reject == false {
						received_accept_message = append(received_accept_message, acc_rply)
					} else {
						if acc_rply.ProposalPromise > max_reject_num {
							max_reject_num = acc_rply.ProposalPromise
						}
					}
				}
			}
			if isMajority_Accept(received_accept_message, px.peers) && max_reject_num <= next_np {
				learnPhase = true
			}
		} else {
			int_part, _ := math.Modf(max_reject_num)
			next_np = uniquePN(int(int_part)+1, px.me, len(px.peers)*10)
		}
		if learnPhase {
			px.sendDecide(seq, next_np, accept_value)
		}
		px.mu.Unlock()
		rand.Seed(time.Now().UnixNano())
		r := rand.Intn(100)
		to := time.Duration(r)
		time.Sleep(to * time.Millisecond)
	}
}

func (px *Paxos) sendDecide(seq int, proposal_num float64, Value interface{}) {
	for i, peer := range px.peers {
		if px.isdead() == true {
			break
		}
		args := &DecidedArgs{
			Seq:         seq,
			ProposalNum: proposal_num,
			Index:       px.me,
			Done:        px.impl.DoneValues[px.me],
			V:           Value,
		}
		rply := &DecidedReply{}
		if i == px.me { // skip sending rpc to self
			px.learn_helper(args, rply)
		} else {
			px.mu.Unlock()
			common.Call(peer, "Paxos.Learn", args, rply)
			px.mu.Lock()
		}
	}
}

func (px *Paxos) checkDecided(counter map[interface{}]int, majority_size int) (bool, interface{}, int) {
	for k, v := range counter {
		if v > majority_size {
			return true, k, v
		}
	}
	return false, nil, 0
}

func (px *Paxos) PrintState() {
	tmp := make([]int, 0)
	for k, _ := range px.impl.AcceptorInfo {
		tmp = append(tmp, k)
	}
	sort.Ints(tmp)
	for _, k := range tmp {
		v := px.impl.AcceptorInfo[k]
		log.Printf("Seq number %d:   NA:  %f,  Val:   %v", k, v.ProposalAcc, v.ValAcc)
	}
}

func max_key(store map[int]SharedInfo) int {
	max_v := math.MinInt64
	for seq := range store {
		if seq > max_v {
			max_v = seq
		}
	}
	return max_v
}

func min_key(store map[int]SharedInfo) int {
	min_v := math.MaxInt64
	for seq := range store {
		if seq < min_v {
			min_v = seq
		}
	}
	return min_v
}

func min(a float64, b float64) float64 {
	if a < b {
		return a
	}
	return b
}

func max(a float64, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func pickHighestNA(prepare_messages []*PrepareReply, own_v interface{}) (float64, interface{}) {
	max_proposalAcc := -1.0
	var max_val interface{} = nil
	for _, rply := range prepare_messages {
		proposalAcc, ValAcc := rply.ProposalAcc, rply.ValAcc
		if proposalAcc > max_proposalAcc {
			max_proposalAcc = proposalAcc
			max_val = ValAcc
		}
	}
	if max_val == nil {
		max_val = own_v
	}
	return max_proposalAcc, max_val
}

func uniquePN(base int, index int, divisor int) float64 {
	var res float64
	res = float64(base) + float64(index)/float64(divisor)
	return res
}

func isMajority_Prepare(a []*PrepareReply, b []string) bool { // check a is a majority of b
	return len(a) > (len(b) / 2)
}

func isMajority_Accept(a []*AcceptReply, b []string) bool { // check a is a majority of b
	return len(a) > (len(b) / 2)
}
