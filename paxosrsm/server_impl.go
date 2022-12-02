package paxosrsm

import (
	"time"

	"kvstore/paxos"
)

// additions to PaxosRSM state
type PaxosRSMImpl struct {
	seq int
}

// initialize rsm.impl.*
func (rsm *PaxosRSM) InitRSMImpl() {
	rsm.impl.seq = 0
}

// application invokes AddOp to submit a new operation to the replicated log
// AddOp returns only once value v has been decided for some Paxos instance
func (rsm *PaxosRSM) AddOp(v interface{}) {
	// length of log is the next seq number we will try
	to := 10 * time.Millisecond
	prev_seq := rsm.impl.seq - 1
	for {
		for { // find next seq number to propose
			status, decided_val := rsm.px.Status(rsm.impl.seq)
			if status == paxos.Forgotten {
				rsm.impl.seq++
			} else if status == paxos.Decided { // add to the replication log
				rsm.px.Done(rsm.impl.seq)
				rsm.impl.seq++
				rsm.applyOp(decided_val)
				if v == decided_val { // two interface must be exactly the same
					// log.Printf("Comparing %v with %v,  returnning True", v, decided_val)
					return
				}
				// log.Printf("Comparing %v with %v,  returnning False", v, decided_val)
			} else {
				break
			}
		}
		if prev_seq != rsm.impl.seq {
			prev_seq = rsm.impl.seq
			rsm.px.Start(rsm.impl.seq, v) // propose for the seq
		}
		// rsm.px.PrintState()
		time.Sleep(to)
		if to < 100*time.Millisecond {
			to *= 2
		}
	}
}
