package kvraft

import "sync"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrExecute     = "ErrExecute"
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
	Rand int
}

type PutAppendReply struct {
	Err Err
}

type OkArgs struct {
	Rand int
}

type OkReply struct {
	Err Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type GetStateArgs struct{}

type GetStateReply struct {
	Term     int
	Isleader bool
}

var mu sync.Mutex
var clientId int

func getClientId() int {
	mu.Lock()
	defer mu.Unlock()
	clientId++
	return clientId << 50
}
