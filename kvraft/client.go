package kvraft

import (
	"crypto/rand"
	"math/big"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderIndex  int
	commandIndex int // index of the last command sent
	clientIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.leaderIndex = -1
	ck.clientIndex = int(nrand())
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.commandIndex += 1
	args := GetArgs{
		Key:       key,
		Op:        "Get",
		CommandId: ck.commandIndex,
		ClientId:  ck.clientIndex,
	}
	reply := GetReply{}
	ok := false
	DPrintf("GETargs: %+v", args)
	for !ok {
		if ck.leaderIndex == -1 {
			ck.leaderIndex = int(nrand()) % len(ck.servers)
		}
		DPrintf("OKKKKK: %+v", ok)
		ok = ck.servers[ck.leaderIndex].Call("KVServer.Get", &args, &reply)
		DPrintf("ok: %+v, REPLYYY: %+v", ok, reply)

		if ok && reply.Err == ErrWrongLeader {
			ok = false
			ck.leaderIndex = reply.LeaderIndex
		}
		if !ok {
			ck.leaderIndex = -1
		}
	}
	// ck.leaderIndex = reply.LeaderIndex
	if reply.Err == ErrNoKey {
		return ""
	}
	return reply.Value
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.commandIndex += 1
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		CommandId: ck.commandIndex,
		ClientId:  ck.clientIndex,
	}
	reply := PutAppendReply{}
	ok := false
	DPrintf("PUTAPPENDargs: %+v", args)
	for !ok {
		if ck.leaderIndex == -1 {
			ck.leaderIndex = int(nrand()) % len(ck.servers)
		}
		DPrintf("OKKKKK: %+v", ok)
		ok = ck.servers[ck.leaderIndex].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("ok: %+v, REPLYYY: %+v", ok, reply)

		if ok && reply.Err == ErrWrongLeader {
			ok = false
			ck.leaderIndex = reply.LeaderIndex	
		}
		if !ok {
			ck.leaderIndex = -1
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
