package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Type      string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate   int // snapshot if log grows this big
	data           map[string]string
	servedRequests map[int64]int64
	resultChan     map[int]chan Op
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	ok, resOp := kv.applyOp(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Value = resOp.Value
	reply.Err = OK

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	ok, _ := kv.applyOp(op)
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}

	reply.Err = OK

}

func (kv *KVServer) applyOp(op Op) (bool, Op) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false, op
	}

	kv.mu.Lock()
	resCh := kv.getOrCreateChan(index)
	kv.mu.Unlock()

	select {
	case appliedOp := <-resCh:
		return kv.validateCommand(op, appliedOp), appliedOp
	case <-time.After(600 * time.Millisecond):
		return false, op
	}
}

// get or create a channel at the given index
func (kv *KVServer) getOrCreateChan(index int) chan Op {
	if _, ok := kv.resultChan[index]; !ok {
		kv.resultChan[index] = make(chan Op, 1)
	}
	return kv.resultChan[index]
}

// check if the applied command is the same as the one sent by the client
func (kv *KVServer) validateCommand(sent Op, applied Op) bool {
	return sent.ClientId == applied.ClientId && sent.RequestId == applied.RequestId
}

// check if the request is duplicate
func (kv *KVServer) isDuplicateRequest(op Op) bool {
	lastServed, ok := kv.servedRequests[op.ClientId]
	if ok {
		return lastServed >= op.RequestId
	}
	return false
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.

func (kv *KVServer) Run() {
	for {
		appliedOp := <-kv.applyCh

		index := appliedOp.CommandIndex
		op := appliedOp.Command.(Op)
		kv.mu.Lock()
		if op.Type == "Get" {
			op.Value = kv.data[op.Key]
		} else {
			if !kv.isDuplicateRequest(op) {
				if op.Type == "Put" {
					kv.data[op.Key] = op.Value
				} else {
					kv.data[op.Key] = kv.data[op.Key] + op.Value
				}
				kv.servedRequests[op.ClientId] = op.RequestId
			}
		}
		resCh := kv.getOrCreateChan(index)
		resCh <- op

		kv.mu.Unlock()
	}
}
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg, 1)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.data = make(map[string]string)
	kv.resultChan = make(map[int]chan Op)
	kv.servedRequests = make(map[int64]int64)

	// You may need initialization code here.
	go kv.Run()
	return kv
}
