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
	//	"bytes"

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	muApply   sync.Mutex
	timeMu    sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state       string // leader, follower, or candidate
	currentTerm int
	votedFor    int
	log         []Log

	snapshot      []byte
	snapshotTerm  int
	snapshotIndex int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	timeout   float64   // 选举超时时长
	lastTime  time.Time // 上次有效消息时间
	voteForMe int       // 向我投票的人
	majority  int       // 大多数
	leaderId  int

	applyCh     chan ApplyMsg
	appendChs   []chan AppendMsg
	appendTimes []time.Time
}
type AppendMsg struct {
	types string // heartbeat log
	index int
	time  time.Time
}

type Log struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == "leader")
	// log.Printf("term=%v,leader=%v,server=%v", term, isleader, rf.me)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist(snapshotNeed bool) {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.snapshotIndex)
	e.Encode(rf.snapshotTerm)
	state := w.Bytes()

	if snapshotNeed {
		rf.persister.SaveStateAndSnapshot(state, rf.snapshot)
	} else {
		rf.persister.SaveRaftState(state)
	}
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []Log
	var snapshotIndex int
	var snapshotTerm int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&snapshotIndex) != nil ||
		d.Decode(&snapshotTerm) != nil {
		panic("readPersist error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.snapshotIndex = snapshotIndex
		rf.snapshotTerm = snapshotTerm
	}

	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// log.Printf("before ... server=%v, log...snapshot, index=%v, rf.lastApplied=%v, rf.snapshotIndex=%v", rf.me, index, rf.lastApplied, rf.snapshotIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// log.Printf("server=%v, log...snapshot, index=%v, rf.lastApplied=%v, rf.snapshotIndex=%v", rf.me, index, rf.lastApplied, rf.snapshotIndex)
	if rf.lastApplied < index {
		return
	}
	if rf.snapshotIndex >= index {
		return
	}
	if snapshot == nil || len(snapshot) <= 0 {
		return
	}
	// log.Printf("server=%v, before snapshot... rf.log=%v, rf.snapshotIndex=%v, rf.snapshotTerm=%v", rf.me, rf.log, rf.snapshotIndex, rf.snapshotTerm)
	snapshotIndex := rf.snapshotIndex
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.log[index-snapshotIndex-1].Term

	// 将snapshot复制
	rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)

	// 截取log
	logs := make([]Log, len(rf.log)+snapshotIndex+1-index-1)
	copy(logs, rf.log[index+1-snapshotIndex-1:])
	rf.log = logs

	// log.Printf("server=%v,after snapshot... rf.log=%v, rf.snapshotIndex=%v, rf.snapshotTerm=%v", rf.me, rf.log, rf.snapshotIndex, rf.snapshotTerm)
	rf.persist(true)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
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
	// log.Printf("server=%v RequestVote...", rf.me)
	// Your code here (2A, 2B).
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.VoteGranted = false
		rf.mu.Unlock()
		return
	}
	turn := false
	if args.Term > currentTerm {
		rf.currentTerm = args.Term
		currentTerm = rf.currentTerm
		if rf.state != "follower" {
			turn = true
		}
		rf.state = "follower"
		rf.votedFor = -1
		rf.persist(false)
	}
	votedFor := rf.votedFor
	var lastLogIndex int
	var lastLogTerm int
	if len(rf.log) == 0 {
		lastLogIndex = rf.snapshotIndex
		lastLogTerm = rf.snapshotTerm
	} else {
		lastLogIndex = len(rf.log) - 1 + rf.snapshotIndex + 1
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}

	rf.mu.Unlock()
	if turn {
		rf.setTimeOut()
	}
	reply.Term = currentTerm
	reply.VoteGranted = false
	if votedFor == -1 || votedFor == args.CandidateId {
		if args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			reply.VoteGranted = true
			rf.mu.Lock()
			rf.votedFor = args.CandidateId
			rf.persist(false)
			rf.mu.Unlock()
			rf.setLastTime()
			// log.Printf("id=%v,votedFor=%v,term=%v", rf.me, rf.votedFor, currentTerm)
			return
		}
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
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term        int
	Success     bool
	PreLogIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// log.Printf("server=%v AppendEntries...", rf.me)
	rf.muApply.Lock()
	rf.mu.Lock()
	reply.Success = false

	// log.Printf("args=%v,log=%v,term=%v,server=%v", args, rf.log, rf.currentTerm, rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		// log.Printf("args=%v,log=%v,term=%v,server=%v, reply=%v", args, rf.log, rf.currentTerm, rf.me, reply)
		rf.mu.Unlock()
		rf.muApply.Unlock()
		// log.Printf("sever %v recevie append entries from server %v, args=%v, reply=%v", rf.me, rf.leaderId, args, reply)
		return
	}
	turn := false
	if rf.state != "follower" {
		turn = true
	}
	rf.state = "follower"
	if args.Term != rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = args.Term
	rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm
	length := len(rf.log) + rf.snapshotIndex + 1
	msgs := make([]ApplyMsg, 0)
	// log.Printf("length-1=%v,args.PreLogIndex=%v,server=%v", length-1, args.PreLogIndex, rf.me)
	if length-1 >= args.PreLogIndex {
		// log.Printf("args.PreLogIndex=%v,rf.snapshotIndex=%v,server=%v", args.PreLogIndex, rf.snapshotIndex, rf.me)
		if args.PreLogIndex >= rf.snapshotIndex {
			var term int
			if args.PreLogIndex == rf.snapshotIndex {
				term = rf.snapshotTerm
			} else {
				term = rf.log[args.PreLogIndex-rf.snapshotIndex-1].Term
			}
			// log.Printf("term=%v,args.PreLogTerm=%v,server=%v", term, args.PreLogTerm, rf.me)
			if term == args.PreLogTerm {
				reply.Success = true
				// appendEntries
				if len(args.Entries) != 0 {
					same := true
					for index, entry := range args.Entries {
						if length-1 < args.PreLogIndex+index+1 {
							rf.log = append(rf.log, entry)
							same = true
						} else if rf.log[args.PreLogIndex+index+1-rf.snapshotIndex-1].Term != entry.Term {
							rf.log[args.PreLogIndex+index+1-rf.snapshotIndex-1] = entry
							same = false
						} else {
							same = true
						}
					}
					if !same {
						rf.log = rf.log[:args.PreLogIndex+len(args.Entries)+1-rf.snapshotIndex-1]
					}
					// log.Printf("server=%v, log=%v", rf.me, rf.log)
				}
				// 计算commmitIndex
				commitIndex := args.PreLogIndex + len(args.Entries)
				if args.LeaderCommit < commitIndex {
					commitIndex = args.LeaderCommit
				}
				// log.Printf("commitIndex=%v", commitIndex)
				if commitIndex > rf.commitIndex {
					rf.commitIndex = commitIndex
					// log.Printf("server=%v, commitIndex=%v, lastApplied=%v", rf.me, rf.commitIndex, rf.lastApplied)
					for rf.lastApplied < rf.commitIndex {
						// 应用到状态机
						if rf.lastApplied < rf.snapshotIndex {
							rf.lastApplied = rf.snapshotIndex
							snapshot := make([]byte, len(rf.snapshot))
							copy(snapshot, rf.snapshot)
							msg := ApplyMsg{SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: rf.snapshotTerm, SnapshotIndex: rf.snapshotIndex}
							msgs = append(msgs, msg)
						} else {
							rf.lastApplied++
							msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied-rf.snapshotIndex-1].Command, CommandIndex: rf.lastApplied}
							// log.Printf("server=%v, msg=%v", rf.me, msg)
							msgs = append(msgs, msg)
						}
					}
				}
				rf.persist(false)
			} else {
				// 这个条件不会满足
				if args.PreLogIndex == 0 {
					reply.PreLogIndex = 0
				} else {
					var preLogIndex int
					isBreak := false
					for preLogIndex = args.PreLogIndex - 1; preLogIndex >= rf.snapshotIndex+1; preLogIndex-- {
						if rf.log[preLogIndex-rf.snapshotIndex-1].Term != term {
							isBreak = true
							break
						}
					}
					if isBreak {
						reply.PreLogIndex = preLogIndex
					} else {
						reply.PreLogIndex = rf.snapshotIndex
					}

				}
			}
		} else {
			reply.PreLogIndex = rf.snapshotIndex
		}
	} else {
		reply.PreLogIndex = length - 1
	}
	if !reply.Success && reply.PreLogIndex < rf.commitIndex {
		reply.PreLogIndex = rf.commitIndex
	}
	// log.Printf("sever %v recevie append entries from server %v, args=%v, reply=%v", rf.me, rf.leaderId, args, reply)
	rf.mu.Unlock()
	// log.Printf("lock ...sever %v recevie append entries from server %v, args=%v, reply=%v, msgs=%v", rf.me, rf.leaderId, args, reply, msgs)
	for _, msg := range msgs {
		rf.applyCh <- msg
	}
	rf.muApply.Unlock()
	// log.Printf("applych ...sever %v recevie append entries from server %v, args=%v, reply=%v", rf.me, rf.leaderId, args, reply)
	if turn {
		rf.setTimeOut()
	} else {
		rf.setLastTime()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotsArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotsReply struct {
	Term int
}

func (rf *Raft) InstallSnapshots(args *InstallSnapshotsArgs, reply *InstallSnapshotsReply) {
	// log.Printf("server=%v, install snapshots", rf.me)
	rf.muApply.Lock()
	rf.mu.Lock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		rf.muApply.Unlock()
		// log.Printf("server=%v, args.Term=%v, rf.currentTerm=%v", rf.me, args.Term, rf.currentTerm)
		return
	}
	turn := false
	if rf.state != "follower" {
		turn = true
	}
	rf.state = "follower"
	if args.Term != rf.currentTerm {
		rf.votedFor = -1
	}
	rf.currentTerm = args.Term
	rf.leaderId = args.LeaderId
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.commitIndex {
		// log.Printf("server=%v, args.LastIncludedIndex=%v, rf.commitIndex=%v", rf.me, args.LastIncludedIndex, rf.commitIndex)
		rf.mu.Unlock()
		rf.muApply.Unlock()
		return
	}
	snapshotIndex := rf.snapshotIndex
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.Term

	// 将snapshot复制
	rf.snapshot = make([]byte, len(args.Data))
	copy(rf.snapshot, args.Data)

	// 截取log
	logLength := len(rf.log) + snapshotIndex + 1
	if logLength-1 < args.LastIncludedIndex || snapshotIndex >= args.LastIncludedIndex || rf.log[args.LastIncludedIndex-snapshotIndex-1].Term != args.LastIncludedTerm {
		rf.log = make([]Log, 0)
	} else {
		log := make([]Log, len(rf.log)+snapshotIndex+1-rf.snapshotIndex-1)
		copy(log, rf.log[rf.snapshotIndex+1-snapshotIndex-1:])
		rf.log = log
	}
	rf.commitIndex = args.LastIncludedIndex

	rf.persist(true)

	// 应用到状态机
	// log.Printf("server=%v install snapshots 应用到状态机之前", rf.me)
	rf.lastApplied = args.LastIncludedIndex
	snapshot := make([]byte, len(rf.snapshot))
	copy(snapshot, rf.snapshot)
	msg := ApplyMsg{SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: rf.snapshotTerm, SnapshotIndex: rf.snapshotIndex}
	rf.mu.Unlock()
	rf.applyCh <- msg
	rf.muApply.Unlock()
	if turn {
		rf.setTimeOut()
	} else {
		rf.setLastTime()
	}
	// log.Printf("server=%v install snapshots finished...", rf.me)

}

func (rf *Raft) sendInstallSnapshots(server int, args *InstallSnapshotsArgs, reply *InstallSnapshotsReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshots", args, reply)
	return ok
}

func (rf *Raft) syncAppendEntries(server int) bool {
	// log.Printf("server=%v syncAppendEntries...", rf.me)
	rf.mu.Lock()
	if rf.state != "leader" {
		rf.mu.Unlock()
		return false
	}
	nextIndex := rf.nextIndex[server]

	if nextIndex <= rf.snapshotIndex {
		snapshotIndex := rf.snapshotIndex
		snapshot := make([]byte, len(rf.snapshot))
		copy(snapshot, rf.snapshot)
		args := InstallSnapshotsArgs{rf.currentTerm, rf.me, snapshotIndex, rf.snapshotTerm, snapshot}
		rf.mu.Unlock()
		reply := InstallSnapshotsReply{}
		ok := rf.sendInstallSnapshots(server, &args, &reply)
		// log.Printf("sever %v send install snapshots to server %v, args=%v, ok=%v, reply=%v", rf.me, server, args, ok, reply)
		if !ok {
			return false
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			turn := false
			if rf.state != "follower" {
				turn = true
			}
			rf.votedFor = -1
			rf.state = "follower"
			rf.currentTerm = reply.Term
			rf.persist(false)
			rf.mu.Unlock()
			if turn {
				rf.setTimeOut()
			}
			return false
		}
		if rf.state != "leader" {
			rf.mu.Unlock()
			return false
		}

		nextIndex = snapshotIndex + 1
		if nextIndex > rf.nextIndex[server] {
			rf.nextIndex[server] = nextIndex
		}
		matchIndex := rf.matchIndex[server]
		if matchIndex < nextIndex-1 {
			rf.matchIndex[server] = nextIndex - 1
		}
		logLength := len(rf.log) + rf.snapshotIndex + 1
		rf.mu.Unlock()
		if nextIndex < logLength {
			// go rf.syncAppendEntries(server)
			rf.appendChs[server] <- AppendMsg{types: "log", index: nextIndex}
		}
		// log.Printf("send install snapshots finished...")
	} else {
		preLogIndex := nextIndex - 1
		var preLogTerm int
		if preLogIndex == rf.snapshotIndex {
			preLogTerm = rf.snapshotTerm
		} else {
			preLogTerm = rf.log[preLogIndex-rf.snapshotIndex-1].Term
		}

		entries := make([]Log, 0)
		logLength := len(rf.log) + rf.snapshotIndex + 1
		if logLength > nextIndex {
			endIndex := logLength
			if logLength > nextIndex+100 {
				endIndex = nextIndex + 100
			}
			entries = rf.log[nextIndex-rf.snapshotIndex-1 : endIndex-rf.snapshotIndex-1]
		}
		args := AppendEntriesArgs{rf.currentTerm, rf.me, preLogIndex, preLogTerm, entries, rf.commitIndex}
		rf.mu.Unlock()
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)

		if !ok {
			// time.Sleep(time.Duration(3) * time.Millisecond)
			// rf.syncAppendEntries(server)
			// log.Printf("send server=%v not ok", server)
			return false
		}

		// log.Printf("reply=%v,term=%v", reply, rf.currentTerm)
		rf.mu.Lock()
		// log.Printf("sever %v send append entries to server %v, args=%v, ok=%v, reply=%v", rf.me, server, args, ok, reply)
		if reply.Term > rf.currentTerm {
			turn := false
			if rf.state != "follower" {
				turn = true
			}
			rf.votedFor = -1
			rf.state = "follower"
			rf.currentTerm = reply.Term
			rf.persist(false)
			rf.mu.Unlock()
			if turn {
				rf.setTimeOut()
			}
			return false
		}

		if rf.state != "leader" {
			rf.mu.Unlock()
			return false
		}

		if reply.Success {
			// log.Printf("nextIndex=%v, entries=%v", nextIndex, entries)
			nextIndex = nextIndex + len(entries)
			if nextIndex > rf.nextIndex[server] {
				rf.nextIndex[server] = nextIndex
			}
			matchIndex := rf.matchIndex[server]
			if matchIndex < nextIndex-1 {
				rf.matchIndex[server] = nextIndex - 1
			}
			logLength := len(rf.log) + rf.snapshotIndex + 1
			rf.mu.Unlock()
			if matchIndex < nextIndex-1 {
				rf.updateCommitInfoOfLeader()
			}
			if nextIndex < logLength {
				// go rf.syncAppendEntries(server)
				rf.appendChs[server] <- AppendMsg{types: "log", index: nextIndex}
			}
		} else {
			if reply.PreLogIndex+1 < rf.nextIndex[server] {
				rf.nextIndex[server] = reply.PreLogIndex + 1
			}
			rf.mu.Unlock()
			// go rf.syncAppendEntries(server)
		}

	}
	return true
}

func (rf *Raft) updateCommitInfoOfLeader() {
	// log.Printf("server=%v updateCommitInfoOfLeader...", rf.me)
	rf.muApply.Lock()
	rf.mu.Lock()

	matchIndice := make([]int, 0)
	// 只提交当前任期的index
	for index, matchIndex := range rf.matchIndex {
		// log.Printf("index=%v, matchIndex=%v, commitIndex=%v", index, matchIndex, rf.commitIndex)
		if index != rf.me && matchIndex > rf.commitIndex && rf.log[matchIndex-rf.snapshotIndex-1].Term == rf.currentTerm {
			matchIndice = append(matchIndice, matchIndex)
		}
	}
	msgs := make([]ApplyMsg, 0)
	if len(matchIndice) >= rf.majority-1 {
		sort.Sort(sort.Reverse(sort.IntSlice(matchIndice)))
		// log.Printf("server=%v, matchIndice=%v", rf.me, matchIndice)
		// log.Printf("majority=%v", rf.majority)
		match := matchIndice[rf.majority-2]
		// log.Printf("match=%v, commitIndex=%v", match, rf.commitIndex)
		if match > rf.commitIndex {
			rf.commitIndex = match
			// log.Printf("server=%v, commitIndex=%v, lastApplied=%v", rf.me, rf.commitIndex, rf.lastApplied)
			for rf.lastApplied < rf.commitIndex {
				// 应用到状态机
				if rf.lastApplied < rf.snapshotIndex {
					rf.lastApplied = rf.snapshotIndex
					snapshot := make([]byte, len(rf.snapshot))
					copy(snapshot, rf.snapshot)
					msg := ApplyMsg{SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: rf.snapshotTerm, SnapshotIndex: rf.snapshotIndex}
					msgs = append(msgs, msg)
				} else {
					rf.lastApplied++
					msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied-rf.snapshotIndex-1].Command, CommandIndex: rf.lastApplied}
					msgs = append(msgs, msg)
				}
			}
		}
	}

	rf.mu.Unlock()
	for _, msg := range msgs {
		rf.applyCh <- msg
	}
	rf.muApply.Unlock()
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
	// log.Printf("server=%v Start...", rf.me)
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	state := rf.state
	isLeader = (state == "leader")
	term = rf.currentTerm
	if isLeader {
		log := Log{term, command}
		rf.log = append(rf.log, log)
		index = len(rf.log) - 1 + rf.snapshotIndex + 1
		rf.persist(false)
	}
	rf.mu.Unlock()
	if isLeader {
		for server := range rf.peers {
			if server != rf.me {
				// go rf.syncAppendEntries(server)
				rf.appendChs[server] <- AppendMsg{types: "log", index: index}
			}
		}
		// times := 0
		// for {
		// 	rf.mu.Lock()
		// 	state := rf.state
		// 	isLeader = (state == "leader")
		// 	currentTerm := rf.currentTerm
		// 	commitIndex := rf.commitIndex
		// 	// log.Printf("server=%v, isLeader=%v, currentTerm=%v, term=%v", rf.me, isLeader, currentTerm, term)
		// 	rf.mu.Unlock()
		// 	if !isLeader || currentTerm != term {
		// 		term = currentTerm
		// 		index = -1
		// 		break
		// 	}
		// 	if times > 5 {
		// 		break
		// 	}
		// 	// log.Printf("commitIndex=%v, index=%v", commitIndex, index)
		// 	if commitIndex < index {
		// 		time.Sleep(time.Duration(200) * time.Millisecond)
		// 		times++
		// 	} else {
		// 		break
		// 	}

		// }
	}
	// log.Printf("server=%v, index=%v, term=%v, isLeader=%v, command=%v", rf.me, index, term, isLeader, command)
	return index, term, isLeader
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		lastTime := rf.lastTime
		timeout := rf.timeout
		state := rf.state
		unlock := false

		if state != "leader" {
			now := time.Now()
			if float64(now.Sub(lastTime).Milliseconds()) > timeout {
				// log.Printf("server=%v,diff=%vtimeout=%v", rf.me, float64(now.Sub(lastTime).Milliseconds()), timeout)
				rf.currentTerm++
				rf.voteForMe = 0
				rf.state = "candidate"
				rf.votedFor = -1
				me := rf.me
				currentTerm := rf.currentTerm
				rf.persist(false)
				rf.mu.Unlock()
				rf.setTimeOut()
				// 先为自己投票
				rf.voteForMyself(currentTerm)
				for index := range rf.peers {
					// log.Printf("rf=%v", rf)
					if index != me {
						go rf.fireRqeuestVote(index, currentTerm)
					}
				}
				unlock = true
			}
		}
		if !unlock {
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
}

func (rf *Raft) heartBeat() {
	for rf.killed() == false {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		if state == "leader" {
			for server := range rf.peers {
				// log.Printf("rf=%v", rf)
				if server != rf.me {
					rf.timeMu.Lock()
					times := rf.appendTimes[server]
					rf.timeMu.Unlock()
					sub := time.Now().Sub(times)
					if sub < time.Duration(60*time.Millisecond) {
						time.Sleep(time.Duration(60*time.Millisecond) - sub)
						continue
					} else {
						go rf.syncAppendEntries(server)
					}

					// rf.appendChs[server] <- AppendMsg{types: "heartbeat", time: time.Now()}
				}
			}
		}
		time.Sleep(time.Duration(60 * time.Millisecond))
	}
}

func (rf *Raft) recevieAppendCh(server int) {
	// times := time.Now().Add(time.Duration(-11 * time.Millisecond))
	for m := range rf.appendChs[server] {
		// log.Printf("server %v msg=%v", server, m)
		// if m.types == "heartbeat" {
		// 	if time.Now().Sub(times) > time.Duration(10*time.Millisecond) {
		// 		// log.Printf("server heart %v msg=%v", server, m)
		// 		go rf.syncAppendEntries(server)
		// 		times = time.Now()
		// 	}

		// } else
		if m.types == "log" {
			rf.mu.Lock()
			matchIndex := rf.matchIndex[server]
			rf.mu.Unlock()
			if matchIndex < m.index {
				success := rf.syncAppendEntries(server)
				if success {
					times := time.Now()
					rf.timeMu.Lock()
					rf.appendTimes[server] = times
					rf.timeMu.Unlock()
				}
			}
		}
	}
}

func (rf *Raft) voteForMyself(currentTerm int) {
	// log.Printf("server=%v voteForMyself...", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	current := rf.currentTerm
	state := rf.state
	me := rf.me
	// 判断是否还是候选者，任期是否仍为当前任期
	if currentTerm != current || state != "candidate" {
		return
	}
	rf.voteForMe++
	rf.votedFor = me
	rf.persist(false)
}

func (rf *Raft) fireRqeuestVote(server int, currentTerm int) {
	// log.Printf("server=%v fireRqeuestVote...", rf.me)
	rf.mu.Lock()
	current := rf.currentTerm
	state := rf.state
	me := rf.me
	var lastLogIndex int
	var lastLogTerm int
	if len(rf.log) == 0 {
		lastLogIndex = rf.snapshotIndex
		lastLogTerm = rf.snapshotTerm
	} else {
		lastLogIndex = len(rf.log) - 1 + rf.snapshotIndex + 1
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()
	// 判断是否还是候选者，任期是否仍为当前任期
	if currentTerm != current || state != "candidate" {
		return
	}
	args := RequestVoteArgs{currentTerm, me, lastLogIndex, lastLogTerm}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)
	// log.Printf("requestvote=%v", ok)
	if ok {
		rf.mu.Lock()
		current = rf.currentTerm
		state = rf.state
		if reply.Term > current {
			rf.currentTerm = reply.Term
			turn := false
			if rf.state != "follower" {
				turn = true
			}
			rf.state = "follower"
			rf.votedFor = -1
			rf.persist(false)
			rf.mu.Unlock()
			if turn {
				rf.setTimeOut()
			}
			return
		}
		// 判断是否还是候选者，任期是否仍为当前任期
		if currentTerm != current || state != "candidate" {
			rf.mu.Unlock()
			return
		}
		if reply.VoteGranted {
			rf.voteForMe++
			// 判断是否刚好大多数投票
			if rf.voteForMe == rf.majority {
				rf.state = "leader"
				rf.leaderId = rf.me
				nextIndex := len(rf.log) + rf.snapshotIndex + 1
				for index := range rf.nextIndex {
					if index != me {
						rf.nextIndex[index] = nextIndex
					}
				}
				for index := range rf.matchIndex {
					if index != me {
						rf.matchIndex[index] = 0
					}
				}
				// 发送一个空消息(后面分析主从线性化)
				// go rf.Start("EmptyCommand")
			}
		}

		rf.mu.Unlock()

	} else {
		time.Sleep(time.Duration(10) * time.Millisecond)
		rf.fireRqeuestVote(server, currentTerm)
	}
}
func (rf *Raft) setTimeOut() {
	random := getRandFloat64(600, 1000)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.timeout = random
	rf.lastTime = time.Now()
}
func (rf *Raft) setLastTime() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastTime = time.Now()
}
func getRandFloat64(min int, max int) float64 {
	rand.Seed(time.Now().UnixNano())
	return rand.Float64()*float64(max-min) + float64(min)
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
	// log.Printf("server%v make", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, Log{0, "NoCommand"})
	rf.snapshotIndex = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.state = "follower"
	rf.voteForMe = 0
	rf.setTimeOut()
	rf.majority = len(rf.peers)/2 + 1
	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	rf.appendChs = make([]chan AppendMsg, len(rf.peers))
	rf.appendTimes = make([]time.Time, len(rf.peers))
	for i := 0; i < len(rf.appendChs); i++ {
		if i != rf.me {
			rf.appendChs[i] = make(chan AppendMsg, 1000)
		}
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	if rf.snapshotIndex != -1 {
		rf.commitIndex = rf.snapshotIndex
		// rf.lastApplied = rf.snapshotIndex
		// snapshot := make([]byte, len(rf.snapshot))
		// copy(snapshot, rf.snapshot)
		// msg := ApplyMsg{SnapshotValid: true, Snapshot: snapshot, SnapshotTerm: rf.snapshotTerm, SnapshotIndex: rf.snapshotIndex}
		// rf.applyCh <- msg
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartBeat()
	times := time.Now().Add(time.Duration(-100 * time.Millisecond))
	for i := 0; i < len(rf.appendChs); i++ {
		if i != rf.me {
			go rf.recevieAppendCh(i)
			rf.appendTimes[i] = times
		}
	}
	return rf
}
