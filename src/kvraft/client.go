package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leadId   int
	mu       sync.Mutex
	uuidMu   sync.Mutex
	uuid     int
	clientId int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) getUUID() int {
	ck.uuidMu.Lock()
	ck.uuidMu.Unlock()
	ck.uuid++
	return ck.clientId + ck.uuid
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	// log.Printf("MakeClerk...")
	ck.leadId = ck.getLeaderId(false)
	ck.clientId = getClientId()
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
	// log.Printf("Get...")
	ck.mu.Lock()
	leaderId := ck.leadId
	if leaderId == -1 {
		leaderId = ck.getLeaderId(true)
		ck.leadId = leaderId
	}
	ck.mu.Unlock()
	args := GetArgs{key, ck.getUUID()}
	reply := GetReply{}
	for {
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		// log.Printf("client leadId = %v, Get ok=%v, args=%v reply=%v", leaderId, ok, args, reply)
		if !ok || !(reply.Err == OK || reply.Err == ErrNoKey) {
			ck.mu.Lock()
			leaderId = ck.getLeaderId(true)
			ck.leadId = leaderId
			ck.mu.Unlock()
		} else {
			return reply.Value
		}
	}
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
	// log.Printf("PutAppend...")
	ck.mu.Lock()
	leaderId := ck.leadId
	if leaderId == -1 {
		leaderId = ck.getLeaderId(true)
		ck.leadId = leaderId
	}
	ck.mu.Unlock()
	// start := time.Now()
	args := PutAppendArgs{key, value, op, ck.getUUID()}
	// dur := time.Since(start)
	// log.Printf("uuid cost %v", dur)
	reply := PutAppendReply{}
	for {
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		// log.Printf("client leadId = %v, PutAppend ok=%v, args=%v reply=%v", leaderId, ok, args, reply)
		if !ok || !(reply.Err == OK || reply.Err == ErrNoKey) {
			ck.mu.Lock()
			leaderId = ck.getLeaderId(true)
			ck.leadId = leaderId
			ck.mu.Unlock()
		} else {
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) GetState(server int, args *GetStateArgs, reply *GetStateReply) bool {
	ok := ck.servers[server].Call("KVServer.GetState", args, reply)
	return ok
}

func (ck *Clerk) getLeaderId(internal bool) int {
	times := 0
	for {
		leaderId := -1
		term := -1
		for i := 0; i < len(ck.servers); i++ {
			reply := GetStateReply{}
			ok := ck.GetState(i, &GetStateArgs{}, &reply)
			// log.Printf("i=%v, reply=%v", i, reply)
			if ok {
				if reply.Term >= term {
					if reply.Term > term {
						leaderId = -1
					}
					term = reply.Term
					if reply.Isleader {
						leaderId = i
					}
				}
			}
		}
		if leaderId != -1 {
			// log.Printf("return times learder=%v", leaderId)
			return leaderId
		}
		times++
		if !internal && times > 5 {
			// log.Printf("times return learder=%v", leaderId)
			return -1
		}
		// log.Printf("next loop times learder=%v", leaderId)
		time.Sleep(time.Duration(600) * time.Microsecond)
	}

}
