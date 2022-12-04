11/27/2022
## Debug:
1. Move RPC does not remove registered server group, but some groups might be still registered but have no shards assigned to it

## 11/28/2022
 Challenge:
1. How to design a correct protocol for handling concurrent operations in the presence of configuration changes.
2. Detect duplicate request
3. For each RPC handler, we must add the operation to the replication log
4. How to know if a shard is not assigned to the current server rather than not catching up
5. Think about how should the shardkv client and server deal with ErrWrongGroup. When the client receives an ErrWrongGroup response to one of its requests and retries its request, 
should the client retain or change the request identifier that it includes in the request? On the server-side, when returning ErrWrongGroup to a client's request, 
should the server add this request's identifier to the cache that the server maintains to detect duplicate requests?

## 12/01/2022
1. Deadlock found in ShardKV.ReceiveShards RPC Handler
2. Found a new bug -- servers in the Shardmaster group are not synced well (inconsistent) -- 
        solved by making operations deterministic (iterating over map is RANDOM!!!, can use sort to make it deterministic)
3. Still failing "oncurrent Leave or Join RPC" sometimes -- solved by including groups with 0 shards assigned
====PASS PART-A=======

## 12/02/2022
1. Duplicate Request != Repeated Request, i.e. Duplicate requests have the same UUID, but Repeated Requests don't
2. RPC Chains Deadlock -- solved by using 1 size buffered chan as trylock (lock may fail but keeps trying) -- see this article [](https://blog.csdn.net/jq0123/article/details/108680243)

## 12/03/2022
1. Write my own test Testing Repeated Joins/Leaves, but when client contacts different server, sometimes the server ignores certain operations -- solved by adding UUID in every operations
        * this is because when the server uses Paxos to catch up the replication log, if no UUID used, then the server may think two joins are the same but actually it is not.

## 12/04/2022
1. Write my own test Testing Concurrent Put/Append/Joins/Leave, found Get() not showing latest Put or Append (Wrong Value) -- solved by removing hardcoded receive request