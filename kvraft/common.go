package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ENK"
	ErrWrongLeader = "EWL"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandId int
	ClientId  int
}

type PutAppendReply struct { // PutAppendReply
	Err         Err
	LeaderIndex int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Op        string // always "Get"
	CommandId int
	ClientId  int
}

type GetReply struct { // GetReply
	Err      Err
	Value    string
	LeaderIndex int
}
