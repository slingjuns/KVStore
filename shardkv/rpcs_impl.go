package shardkv

// Field names must start with capital letters,
// otherwise RPC will break.

// additional state to include in arguments to PutAppend RPC.
type PutAppendArgsImpl struct {
	UUID int64
}

// additional state to include in arguments to Get RPC.
type GetArgsImpl struct {
	UUID int64
}
