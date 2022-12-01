package paxos

// In all data types that represent RPC arguments/reply, field names
// must start with capital letters, otherwise RPC will break.

const (
	OK     = "OK"
	Reject = "Reject"
)

type Response string

type PrepareArgs struct {
	Seq         int
	ProposalNum float64
	Index       int // server index
	Done        int
}

type PrepareReply struct {
	Reject          bool
	ProposalPromise float64 // used for proposer to choose next proposal number
	ProposalAcc     float64
	ValAcc          interface{}
}

type AcceptArgs struct {
	Seq         int
	ProposalNum float64
	V           interface{}
	Index       int // server index
	Done        int
}

type AcceptReply struct {
	Reject          bool
	ProposalPromise float64 // used for proposer to choose next proposal number
}

type DecidedArgs struct {
	Seq         int
	ProposalNum float64
	V           interface{}
	Index       int // server index
	Done        int
}

type DecidedReply struct {
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.prepare_helper(args, reply)
	return nil
}

func (px *Paxos) prepare_helper(args *PrepareArgs, reply *PrepareReply) bool {
	seq, proposal_num, server_index, done_value := args.Seq, args.ProposalNum, args.Index, args.Done
	// log.Printf("Server index with %d's Prepare Receiving proposal num of %f from server_index of %d", px.me, proposal_num, server_index)
	if sharedInfo, ok := px.impl.SeqInfo[seq]; ok {
		np := sharedInfo.Np
		// log.Printf("Prepare Phase: Seq number %d found in SeqInfo, with np equals %f and proposal number of %f", seq, np, proposal_num)
		if proposal_num > np { // succeed note: the proposal_num is greater than np
			sharedInfo.Np = proposal_num
			px.impl.SeqInfo[seq] = sharedInfo
			reply.Reject = false
			reply.ProposalPromise = np
			reply.ProposalAcc = px.impl.AcceptorInfo[seq].ProposalAcc
			reply.ValAcc = px.impl.AcceptorInfo[seq].ValAcc
			return true
		} else { // reject
			// log.Printf("Prepare Phase: Reject Seq number %d with proposal number %f", seq, proposal_num)
			// log.Printf("Prepare Phase: Reject Seq number %d with proposal number %f, proposal_promise is %f", seq, proposal_num, np)
			reply.Reject = true
			reply.ProposalPromise = sharedInfo.Np
		}
	} else {
		// log.Printf("Prepare Phase: Seq number %d not found in SeqInfo, constructing new state on this seq number with proposal number of %f", seq, proposal_num)
		info := SharedInfo{ // construct np
			fate: Pending,
			Np:   proposal_num,
		}
		px.impl.SeqInfo[seq] = info
		reply.Reject = false
		reply.ProposalPromise = proposal_num
	}
	// update done values
	px.impl.DoneValues[server_index] = done_value
	return true
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.accept_helper(args, reply)
	return nil
}

func (px *Paxos) accept_helper(args *AcceptArgs, reply *AcceptReply) bool {
	seq, proposal_num, Value, server_index, done_value := args.Seq, args.ProposalNum, args.V, args.Index, args.Done
	// log.Printf("Server index with %d's Accept Handler Receiving proposal num of %f from server_index of %d", px.me, proposal_num, server_index)
	if sharedInfo, ok := px.impl.SeqInfo[seq]; ok {
		np := sharedInfo.Np
		// log.Printf("Accept Phase: Seq number %d found in SeqInfo, with np equals %f and proposal number of %f", seq, np, proposal_num)
		if proposal_num >= np { // succeed note: the proposal_num is greater than or equal to np
			sharedInfo.Np = proposal_num
			px.impl.SeqInfo[seq] = sharedInfo
			// update acceptor state
			new_acc_state := AcceptorInfo{
				ProposalAcc: proposal_num,
				ValAcc:      Value,
			}
			px.impl.AcceptorInfo[seq] = new_acc_state
			// update reply
			reply.Reject = false
			reply.ProposalPromise = np
			return true
		} else { // reject
			// log.Printf("Accept Phase: Reject Seq number %d with proposal number %f, proposal_promise is %f", seq, proposal_num, np)
			reply.Reject = true
			reply.ProposalPromise = np
		}
	} else {
		// log.Printf("Accept Phase: Seq number %d not found in SeqInfo, constructing new state on this seq number with proposal number of %f", seq, proposal_num)
		info := SharedInfo{ // construct np
			fate: Pending,
			Np:   proposal_num,
		}
		px.impl.SeqInfo[seq] = info
		acc_info := AcceptorInfo{ // construct acceptor state -- directly accepting the valeu proposed
			ProposalAcc: proposal_num,
			ValAcc:      Value,
		}
		// log.Printf("Accept Phase: Seq number %d not found in SeqInfo, constructing initial Accept State", seq)
		px.impl.AcceptorInfo[seq] = acc_info
		reply.ProposalPromise = proposal_num
	}
	// update done values
	px.impl.DoneValues[server_index] = done_value
	return true
}

func (px *Paxos) Learn(args *DecidedArgs, reply *DecidedReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.learn_helper(args, reply)
	return nil
}

func (px *Paxos) learn_helper(args *DecidedArgs, reply *DecidedReply) bool {
	seq, proposal_num, V, server_index, done_value := args.Seq, args.ProposalNum, args.V, args.Index, args.Done
	new_state := SharedInfo{
		fate: Decided,
		Np:   proposal_num,
	}
	// log.Printf("Learning Phase: Seq number %d, updating Seq info, proposal_num %f is Decided! Decided Value is %v", seq, proposal_num, px.impl.AcceptorInfo[seq].ValAcc)
	px.impl.SeqInfo[seq] = new_state
	acc_info := AcceptorInfo{ // construct acceptor state -- directly accepting the value proposed
		ProposalAcc: proposal_num,
		ValAcc:      V,
	}
	px.impl.AcceptorInfo[seq] = acc_info
	// update done values of the peer server and myself !!!
	px.impl.DoneValues[server_index] = done_value
	return true
}

//
// add RPC handlers for any RPCs you introduce.
//
