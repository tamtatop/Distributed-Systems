package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	// "fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"bytes"

	"../labgob"
	"../labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent
	CurrentTerm int //2A
	VotedFor    int //2A
	Log         []LogEntry

	//volatile
	CommitIndex int
	LastApplied int

	//leaders volatile
	NextIndex  []int
	MatchIndex []int

	State           int       //2A
	ElectionTimeout time.Time //2A

	// 3B
	LastSnapshottedIndex int
	LastSnapshottedTerm  int
	SnapshotData         []byte
	leaderID int
}

const Leader = 1
const Candidate = 2
const Follower = 3

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isleader = rf.State == Leader

	return term, isleader
}

func (rf *Raft) GetStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetLeader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.leaderID
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

// must be called from locked context
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastSnapshottedIndex)
	e.Encode(rf.LastSnapshottedTerm)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

type SC struct {
	S interface{}
}

func (rf *Raft) persistRaftAndSnapshot() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.LastSnapshottedIndex)
	e.Encode(rf.LastSnapshottedTerm)

	statedata := w.Bytes()

	// if rf.SnapshotData != nil {
	// 	fmt.Printf("%+v\n", rf.SnapshotData)
	// }

	rf.persister.SaveStateAndSnapshot(statedata, rf.SnapshotData)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshotBytes []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var LastSnapshottedIndex int
	var LastSnapshottedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&LastSnapshottedIndex) != nil ||
		d.Decode(&LastSnapshottedTerm) != nil {
		return
	} else {
		rf.CurrentTerm = currentTerm
		rf.VotedFor = votedFor
		rf.Log = log
		rf.LastSnapshottedIndex = LastSnapshottedIndex
		rf.LastSnapshottedTerm = LastSnapshottedTerm
	}

	if snapshotBytes == nil || len(snapshotBytes) < 1 { // bootstrap without any state?
		return
	}
	rf.SnapshotData = snapshotBytes
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //2A
	CandidateID  int //2A
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.CurrentTerm

	if args.Term > rf.CurrentTerm {
		rf.VotedFor = -1
		rf.State = Follower
	}
	if args.Term >= rf.CurrentTerm && rf.VotedFor == -1 {
		rf.CurrentTerm = args.Term
		rf.ElectionTimeout = generateElectionTimeout()
		if args.LastLogTerm < rf.Log[len(rf.Log)-1].Term {
			return
		}
		if args.LastLogTerm == rf.Log[len(rf.Log)-1].Term && args.LastLogIndex < rf.getRealIndex(len(rf.Log)-1) {
			return
		}
		reply.VoteGranted = true
		rf.VotedFor = args.CandidateID
		rf.State = Follower
	}

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int //2A
	LeaderID     int //2A
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int  //2A
	Success       bool //2A
	ConflictTerm  int  //2C
	ConflictIndex int  //2C
}

// handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	//fmt.Printf("GOT HEARTBEAT, %+v me: %v\n", args, rf.me)
	reply.Term = rf.CurrentTerm
	reply.Success = false

	if args.Term < rf.CurrentTerm { // invalid AE request
		return
	}

	rf.leaderID = args.LeaderID
	if args.Term > rf.CurrentTerm {
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
	}
	rf.ElectionTimeout = generateElectionTimeout()
	rf.State = Follower

	if rf.getRealIndex(len(rf.Log)-1) < args.PrevLogIndex {
		reply.ConflictIndex = rf.getRealIndex(len(rf.Log))
		reply.ConflictTerm = 0
		return
	}

	// 3B
	if args.PrevLogIndex <= rf.LastSnapshottedIndex {
		extras := rf.LastSnapshottedIndex - args.PrevLogIndex
		if len(args.Entries) <= extras {
			reply.Success = true
			return
		}
		args.PrevLogIndex = rf.LastSnapshottedIndex
		args.PrevLogTerm = rf.LastSnapshottedTerm
		args.Entries = args.Entries[extras:]
	}

	if rf.Log[rf.getRelativeIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.Log[rf.getRelativeIndex(args.PrevLogIndex)].Term
		for i, logEntry := range rf.Log {
			if logEntry.Term == reply.ConflictTerm {
				reply.ConflictIndex = rf.getRealIndex(i)
				return
			}
		}
		panic("must return")
	}

	//fmt.Printf("GOT VALID HEARTBEAT, %+v me: %v\n", args, rf.me)

	for i, val := range args.Entries {
		j := args.PrevLogIndex + i + 1
		if j < rf.getRealIndex(len(rf.Log)) {
			if rf.Log[rf.getRelativeIndex(j)].Term != val.Term {
				rf.Log = rf.Log[:rf.getRelativeIndex(j)+1]
			}
			rf.Log[rf.getRelativeIndex(j)] = val
		} else {
			rf.Log = append(rf.Log, val)
		}
	}

	if args.LeaderCommit > rf.CommitIndex {
		lastLogInd := args.PrevLogIndex + len(args.Entries)
		rf.CommitIndex = int(math.Min(float64(args.LeaderCommit), float64(lastLogInd)))
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// fmt.Println("sendiiing", server, "from", rf.me, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Max returns the larger of x or y.
func Max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

// Min returns the smaller of x or y.
func Min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	index = rf.getRealIndex(len(rf.Log))
	term = rf.CurrentTerm
	isLeader = rf.State == Leader

	if isLeader {
		logToSend := LogEntry{command, term}
		rf.Log = append(rf.Log, logToSend)
		rf.MatchIndex[rf.me] = rf.getRealIndex(len(rf.Log) - 1)

		go rf.sendHeartBeat()
	}

	return index, term, isLeader
}

func (rf *Raft) handleAppendEntriesReply(serverInd int, args AppendEntriesArgs, reply AppendEntriesReply) {
	// fmt.Println("KNoCK KNOCK")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// fmt.Println(serverInd)

	// fmt.Println("NEXTINDEX: ", rf.NextIndex, args.PrevLogIndex, rf.me, rf.CurrentTerm)
	// fmt.Println("MATCHINDEX: ", rf.MatchIndex, args.PrevLogIndex)
	// fmt.Printf("%+v, %+v\n", args, reply)

	if rf.CurrentTerm < reply.Term {
		rf.State = Follower
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1

		rf.persist()
		return
	}

	if rf.CurrentTerm != args.Term {
		return
	}

	if reply.Success {
		rf.MatchIndex[serverInd] = Max(rf.MatchIndex[serverInd], args.PrevLogIndex+len(args.Entries))
		rf.NextIndex[serverInd] = rf.MatchIndex[serverInd] + 1

		// fmt.Println("MatchIndex", rf.MatchIndex)

		matchSorted := make([]int, len(rf.MatchIndex))
		copy(matchSorted, rf.MatchIndex)
		sort.Ints(matchSorted)
		median := matchSorted[len(matchSorted)/2]
		if median >= rf.LastSnapshottedIndex && rf.Log[rf.getRelativeIndex(median)].Term == rf.CurrentTerm {
			rf.CommitIndex = median
		}

		rf.persist()
		return
	}

	//rf.NextIndex[serverInd] = (args.PrevLogIndex + rf.MatchIndex[serverInd] + 1) / 2
	rf.NextIndex[serverInd] = reply.ConflictIndex
	if rf.MatchIndex[serverInd]+1 > rf.NextIndex[serverInd] {
		rf.NextIndex[serverInd] = rf.MatchIndex[serverInd] + 1
	}

	if rf.NextIndex[serverInd] == 0{
		panic("ni zero")
	}
	

	if rf.MatchIndex[serverInd] != rf.getRealIndex(len(rf.Log)-1) {
		go rf.sendHeartBeatToServer(serverInd)
	}
}

func (rf *Raft) addLogEntriesToArgs(serverInd int, args *AppendEntriesArgs) {
	startInd := rf.NextIndex[serverInd]
	if startInd <= rf.LastSnapshottedIndex {
		panic("addLogEntriesToArgs startInd in snapshot")
	}
	// fmt.Println("NextIndex", startInd)
	// fmt.Println("MatchIndex", rf.MatchIndex[serverInd])
	// fmt.Println("LOG", rf.Log)
	logEntries := make([]LogEntry, len(rf.Log[rf.getRelativeIndex(startInd):]))
	copy(logEntries, rf.Log[rf.getRelativeIndex(startInd):])
	args.Entries = logEntries
}

func generateElectionTimeout() time.Time {
	return time.Now().Add(time.Millisecond * time.Duration(400+rand.Intn(300))) // 400 - 700 milisec
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendSnapshotToServer(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.CurrentTerm,
		LeaderID:          rf.me,
		LastIncludedIndex: rf.LastSnapshottedIndex,
		LastIncludedTerm:  rf.LastSnapshottedTerm,
		SnapshotData:      rf.SnapshotData,
	}
	reply := InstallSnapshotReply{}
	go func() {
		if rf.sendInstallSnapshot(server, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()

			if rf.CurrentTerm != args.Term {
				return
			}

			if rf.CurrentTerm < reply.Term {
				rf.State = Follower
				rf.CurrentTerm = reply.Term
				rf.VotedFor = -1

				rf.persist()
				return
			}

			// server should have installed snapshot
			rf.NextIndex[server] = Max(rf.MatchIndex[server]+1, args.LastIncludedIndex+1)
			rf.MatchIndex[server] = rf.NextIndex[server] - 1
			go rf.sendHeartBeatToServer(server)
		}
	}()
}

func (rf *Raft) sendHeartBeatToServer(server int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.State == Leader

	if !isLeader {
		return
	}

	if rf.NextIndex[server] <= rf.LastSnapshottedIndex {
		rf.sendSnapshotToServer(server)
		return
	}

	args := AppendEntriesArgs{
		Term:     rf.CurrentTerm,
		LeaderID: rf.me,
		// PrevLogIndex: -1, // to each their own
		// PrevLogTerm:  -1, // to each their own
		// Entries:      []LogEntry{}, // to each their own
		LeaderCommit: rf.CommitIndex,
	}

	reply := AppendEntriesReply{}

	rf.addLogEntriesToArgs(server, &args)
	lastLogIndex := rf.NextIndex[server] - 1
	args.PrevLogIndex = rf.NextIndex[server] - 1
	args.PrevLogTerm = rf.Log[rf.getRelativeIndex(lastLogIndex)].Term

	go func(server int, args AppendEntriesArgs, reply AppendEntriesReply, nextIndex []int) {
		//fmt.Println("SENDING APPEND LOG ENTRY from: ", rf.me, " to: ", i)
		if rf.sendAppendEntries(server, &args, &reply) {
			rf.handleAppendEntriesReply(server, args, reply)
		}
	}(server, args, reply, rf.NextIndex)
}

func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.State == Leader

	if isLeader {
		for i := range rf.peers {
			if i != rf.me {
				go rf.sendHeartBeatToServer(i)
			}
		}
	}
}

func (rf *Raft) doElection() {
	rf.mu.Lock()
	if rf.ElectionTimeout.Before(time.Now()) && rf.State != Leader {
		// fmt.Println("Election STARTED", rf.me)

		rf.leaderID = -1
		rf.State = Candidate
		rf.VotedFor = rf.me
		rf.CurrentTerm++
		rf.ElectionTimeout = generateElectionTimeout()

		args := RequestVoteArgs{}
		args.Term = rf.CurrentTerm
		args.CandidateID = rf.me
		args.LastLogIndex = rf.getRealIndex(len(rf.Log) - 1)
		args.LastLogTerm = rf.Log[len(rf.Log)-1].Term

		electionTerm := rf.CurrentTerm

		rf.persist()
		rf.mu.Unlock()

		reply := RequestVoteReply{}
		replyNum := 1 // self vote

		for i := range rf.peers {
			if i != rf.me {
				go func(i int, args RequestVoteArgs, reply RequestVoteReply) {
					rf.sendRequestVote(i, &args, &reply)
					//fmt.Println("got RequsetVote Answer: total replies: ",  replyNum, "me:", rf.me)
					if reply.VoteGranted {
						rf.mu.Lock()
						replyNum++
						//fmt.Println("got YES RequsetVote Answer: total replies: ",  replyNum, "me:", rf.me)

						if electionTerm == rf.CurrentTerm && replyNum == len(rf.peers)/2+1 {
							rf.State = Leader
							rf.leaderID = rf.me

							// fmt.Println("AM LEADER LOSERRRSSSS: ", rf.me, " term: ", rf.CurrentTerm)
							// update nextIndex
							for i := range rf.NextIndex {
								rf.NextIndex[i] = rf.getRealIndex(len(rf.Log))
								rf.MatchIndex[i] = 0
							}
							rf.MatchIndex[rf.me] = rf.getRealIndex(len(rf.Log) - 1)

							rf.mu.Unlock()
							//rf.Start(nil)

							rf.sendHeartBeat()
						} else {
							rf.mu.Unlock()
						}
					} else {
						rf.mu.Lock()
						if reply.Term > rf.CurrentTerm {
							rf.CurrentTerm = reply.Term
							rf.State = Follower
							rf.VotedFor = -1
							rf.ElectionTimeout = generateElectionTimeout()

							rf.persist()
						}
						rf.mu.Unlock()
					}
				}(i, args, reply)
			}
		}
	} else {
		rf.mu.Unlock()
	}

}

func (rf *Raft) getRelativeIndex(i int) int {
	return i - rf.LastSnapshottedIndex
}

func (rf *Raft) getRealIndex(i int) int {
	return i + rf.LastSnapshottedIndex
}

func (rf *Raft) DoSnapshot(lastLogIndex int, snapshotData []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.LastApplied < lastLogIndex {
		panic("log in snapshot not applied")
	}
	if lastLogIndex <= rf.LastSnapshottedIndex {
		return
	}
	lastLogTerm := rf.Log[rf.getRelativeIndex(lastLogIndex)].Term
	rf.Log = append(make([]LogEntry, 0), rf.Log[rf.getRelativeIndex(lastLogIndex):]...)
	rf.LastSnapshottedTerm =lastLogTerm
	rf.LastSnapshottedIndex = lastLogIndex
	rf.SnapshotData = snapshotData
	rf.persistRaftAndSnapshot()

}


type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	SnapshotData      []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	if args.Term < rf.CurrentTerm {
		return
	}

	if args.Term > rf.CurrentTerm {
		rf.VotedFor = -1
		rf.CurrentTerm = args.Term
	}
	rf.ElectionTimeout = generateElectionTimeout()
	rf.State = Follower // we need to be follower
	rf.leaderID = args.LeaderID

	if args.LastIncludedIndex < rf.LastSnapshottedIndex { // we already have bigger snapshot
		return
	}


	if rf.getRelativeIndex(args.LastIncludedIndex) < len(rf.Log) && rf.Log[rf.getRelativeIndex(args.LastIncludedIndex)].Term == args.LastIncludedTerm {
		// create new array so GC can clean discarded idx's
		rf.Log = append(make([]LogEntry, 0), rf.Log[rf.getRelativeIndex(args.LastIncludedIndex):]...)
	} else {
		rf.Log = append(make([]LogEntry, 0), LogEntry{
			Command: nil,
			Term:    args.LastIncludedTerm,
		})
	}

	rf.CommitIndex = Max(rf.CommitIndex, args.LastIncludedIndex)
	rf.SnapshotData = args.SnapshotData
	rf.LastSnapshottedIndex = args.LastIncludedIndex
	rf.LastSnapshottedTerm = args.LastIncludedTerm

	rf.persistRaftAndSnapshot()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	// fmt.Println("sendiiingi install snapshot", server, "from", rf.me, args)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type SnapshotApplyCommand struct {
	LastSnapshottedIndex int
	LastSnapshottedTerm  int
	SnapshotData         []byte
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.VotedFor = -1
	rf.State = Follower
	rf.Log = append(rf.Log, LogEntry{nil, 0})
	rf.LastSnapshottedTerm = 0
	rf.LastSnapshottedIndex = 0

	rf.NextIndex = make([]int, len(rf.peers))
	rf.MatchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	go func() {
		for {
			if rf.killed() {
				return
			}

			rf.sendHeartBeat()
			time.Sleep(90 * time.Millisecond)
		}
	}()

	go func() {

		rf.ElectionTimeout = generateElectionTimeout()

		for {
			if rf.killed() {
				return
			}

			rf.doElection()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	go func() {
		for {
			rf.mu.Lock()
			for rf.LastApplied < rf.CommitIndex {
				if rf.LastApplied >= rf.LastSnapshottedIndex {
					rf.LastApplied++
					applyMsg := ApplyMsg{
						CommandValid: true,
						Command:      rf.Log[rf.getRelativeIndex(rf.LastApplied)].Command,
						CommandIndex: rf.LastApplied,
					}
					// fmt.Println(rf.me, rf.Log, rf.CommitIndex, rf.LastApplied)
					rf.mu.Unlock()
					//if applyMsg.Command != nil {
					applyCh <- applyMsg
					//}
					rf.mu.Lock()
				} else {
					applyMsg := ApplyMsg{
						CommandValid: false,
						Command: SnapshotApplyCommand{
							LastSnapshottedIndex: rf.LastSnapshottedIndex,
							LastSnapshottedTerm:  rf.LastSnapshottedTerm,
							SnapshotData:         rf.SnapshotData,
						},
						CommandIndex: rf.LastSnapshottedIndex,
					}
					rf.LastApplied = rf.LastSnapshottedIndex
					rf.mu.Unlock()
					applyCh <- applyMsg
					rf.mu.Lock()
				}
			}

			rf.mu.Unlock()

			time.Sleep(10 * time.Millisecond)
		}
	}()

	return rf
}
