package common

// define here any data types that you need to access in two packages without
// creating circular dependencies
type Shard2Server struct {
	GID   int64
	Count int
}

// for new RPCs that you add, declare types for arguments and reply
type SendShardsArgs struct {
	Shard         int
	ReceiverGroup []string
	UUID          int64
}

type SendShardsReply struct {
	Err string
}

type ReceiveShardsArgs struct {
	RequestIds map[int64]int
	Shard      int
	DB         map[string]string
	UUID       int64
}

type ReceiveShardsReply struct {
	Err string
}
