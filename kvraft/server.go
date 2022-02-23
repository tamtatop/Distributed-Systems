package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	GetOp    = 0
	PutOp    = 1
	AppendOp = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	OpType   int // Get or Put or Append
	Key      string
	Value    string
	OpId     int
	ClientId int
}

// type OpFullId struct {
// 	OpId     int
// 	ClientId int
// }

type UniqueReply struct {
	CId   int
	OpId  int
	Reply interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	kvData     map[string]string
	replyChans map[int]chan interface{}
	replyCache map[int]UniqueReply

	lastLogIndex int
}

type KVSnapshot struct { // short names to reduce snapshot constant size :)
	Kd map[string]string   // kvData
	Rc map[int]UniqueReply // replyCache
	Li int                 // lastLogIndex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	op := Op{
		OpType:   GetOp,
		Key:      args.Key,
		Value:    "",
		OpId:     args.CommandId,
		ClientId: args.ClientId,
	}
	// DPrintf("try lock in get")
	kv.mu.Lock()
	// DPrintf("did lock in get")
	index, _, isLeader := kv.rf.Start(op)

	DPrintf("isLeader: %+v", isLeader)
	if !isLeader {
		*reply = GetReply{
			Err:         ErrWrongLeader,
			LeaderIndex: kv.rf.GetLeader(),
		}
		kv.mu.Unlock()
		return
	}
	ch := make(chan interface{})
	kv.replyChans[index] = ch
	kv.mu.Unlock()
	lola, ok := <-ch
	if !ok {
		*reply = GetReply{
			Err:         ErrWrongLeader,
			LeaderIndex: kv.rf.GetLeader(),
		}
		return
	}
	DPrintf("GOT putappendreply FROM RAFT: %+v", lola)

	*reply = (lola).(GetReply)
	reply.LeaderIndex = kv.rf.GetLeader()
	// if isLeader

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	DPrintf("ARGS: %+v", args)
	opType := PutOp
	if args.Op == "Append" {
		opType = AppendOp
	}
	op := Op{
		OpType:   opType,
		Key:      args.Key,
		Value:    args.Value,
		OpId:     args.CommandId,
		ClientId: args.ClientId,
	}
	// DPrintf("try lock in putappend")
	kv.mu.Lock()
	// DPrintf("did lock in putappend")
	index, _, isLeader := kv.rf.Start(op)

	DPrintf("isLeader: %+v", isLeader)
	if !isLeader {
		*reply = PutAppendReply{
			Err:         ErrWrongLeader,
			LeaderIndex: kv.rf.GetLeader(),
		}
		kv.mu.Unlock()
		return
	}

	ch := make(chan interface{})
	kv.replyChans[index] = ch
	kv.mu.Unlock()

	lola, ok := <-ch
	if !ok {
		*reply = PutAppendReply{
			Err:         ErrWrongLeader,
			LeaderIndex: kv.rf.GetLeader(),
		}
		return
	}
	DPrintf("GOT putappendreply FROM RAFT: %+v", lola)
	*reply = (lola).(PutAppendReply)
	reply.LeaderIndex = kv.rf.GetLeader()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) sendAndSaveReply(op Op, reply interface{}, msg raft.ApplyMsg) {
	ch, ok := kv.replyChans[msg.CommandIndex]
	if ok {
		// DPrintf("SENDO")
		ch <- reply
		// DPrintf("RECIVO")
	}
	delete(kv.replyChans, msg.CommandIndex)
	uniqueReply := UniqueReply{
		CId:   op.ClientId,
		OpId:  op.OpId,
		Reply: reply,
	}
	kv.replyCache[op.ClientId] = uniqueReply
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(KVSnapshot{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendReply{})
	labgob.Register(UniqueReply{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvData = make(map[string]string)
	kv.replyChans = make(map[int]chan interface{})
	kv.replyCache = make(map[int]UniqueReply)

	// You may need initialization code here.

	go func() {
		cur_term := 0
		for {
			term, isLeader := kv.rf.GetState()
			if isLeader && cur_term != term {
				op := Op{
					OpType: GetOp,
				}
				kv.rf.Start(op)
				cur_term = term
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()

	go func() {
		if kv.maxraftstate == -1 {
			return
		}

		for {
			kv.mu.Lock()
			// DPrintf("%v %v %v", kv.rf.GetStateSize(), kv.maxraftstate, kv.lastLogIndex)
			if 10*kv.rf.GetStateSize() >= 8*kv.maxraftstate {
				kvsnapshot := KVSnapshot{
					Kd: map[string]string{},
					Rc: map[int]UniqueReply{},
					Li: kv.lastLogIndex,
				}
				for k, v := range kv.kvData {
					kvsnapshot.Kd[k] = v
				}
				for k, v := range kv.replyCache {
					kvsnapshot.Rc[k] = v
				}
				DPrintf("%v %v", len(kvsnapshot.Kd), len(kvsnapshot.Rc))

				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kvsnapshot)
				kv.rf.DoSnapshot(kv.lastLogIndex, w.Bytes())
			}
			time.Sleep(15 * time.Millisecond)
			kv.mu.Unlock()
		}
	}()

	go func() {
		cur_term := 0
		for {
			msg := <-kv.applyCh
			// DPrintf("try lock in for")
			kv.mu.Lock()
			kv.lastLogIndex = msg.CommandIndex
			// DPrintf("did lock in for")

			term, _ := kv.rf.GetState()
			if term != cur_term {
				DPrintf("REMOVE ALL CHANS")
				// remove all channels
				for k := range kv.replyChans {
					close(kv.replyChans[k])
					delete(kv.replyChans, k)
				}

				cur_term = term
			}
			if msg.CommandValid == false { // we received snapshot
				// remove all channels
				DPrintf("REMOVE ALL CHANS")
				for k := range kv.replyChans {
					close(kv.replyChans[k])
					delete(kv.replyChans, k)
				}


				var kvsnapshot KVSnapshot

				r := bytes.NewBuffer((msg.Command).(raft.SnapshotApplyCommand).SnapshotData)
				d := labgob.NewDecoder(r)
				if d.Decode(&kvsnapshot) != nil {
					panic("bad snap")
				} 
				kv.lastLogIndex = kvsnapshot.Li
				kv.replyCache = kvsnapshot.Rc
				kv.kvData = kvsnapshot.Kd

				kv.mu.Unlock()
				continue
			}

			op := (msg.Command).(Op)

			lastReply, ok := kv.replyCache[op.ClientId]
			if ok && op.OpId == lastReply.OpId {
				DPrintf("return cached %+v, opID: %v", lastReply.Reply, lastReply.OpId)
				kv.sendAndSaveReply(op, lastReply.Reply, msg)
				// DPrintf("TOC")

				kv.mu.Unlock()
				continue
			}
			DPrintf("GOT OP FROM RAFT: %+v", msg)
			// kv.commitedCommands = append(kv.commitedCommands, msg.Command.CommandId)

			if op.OpType == GetOp {
				value, ok := kv.kvData[op.Key]
				var err Err = OK
				if !ok {
					err = ErrNoKey
				}
				reply := GetReply{
					Err:   err,
					Value: value,
				}
				kv.sendAndSaveReply(op, reply, msg)
			} else if op.OpType == PutOp {
				kv.kvData[op.Key] = op.Value
				reply := PutAppendReply{
					Err: OK,
				}
				kv.sendAndSaveReply(op, reply, msg)
			} else if op.OpType == AppendOp {
				kv.kvData[op.Key] += op.Value
				reply := PutAppendReply{
					Err: OK,
				}
				kv.sendAndSaveReply(op, reply, msg)
			}
			// DPrintf("TOC")

			kv.mu.Unlock()
		}
	}()

	return kv
}
