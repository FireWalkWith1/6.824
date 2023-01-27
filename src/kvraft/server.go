package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"github.com/google/uuid"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpT        string
	Key        string
	Value      string
	Rand       int
	ServerUUID string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kv      map[string]string
	randk   map[int]int8
	chanMap sync.Map
}

type apply struct {
	ch    chan int
	times time.Time
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// log.Printf("server get begin")
	serverUUID := uuid.NewString()
	command := Op{"Get", args.Key, "", 0, serverUUID}
	ch := make(chan int, 1)
	kv.chanMap.Store(serverUUID, apply{ch, time.Now()})
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// log.Printf("server get end")
		kv.chanMap.Delete(serverUUID)
		return
	}
	isExecuted := false
	// for i := 0; i < 20; i++ {
	// 	isExecuted = kv.isExecute(index, args.Rand)
	// 	if isExecuted {
	// 		break
	// 	}
	// 	time.Sleep(time.Duration(10 * time.Millisecond))
	// }
	if _, ok := <-ch; ok {
		isExecuted = true
	}
	if !isExecuted {
		reply.Err = ErrExecute
	} else {
		kv.mu.Lock()
		value, exist := kv.kv[args.Key]
		kv.mu.Unlock()
		if !exist {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Err = OK
			reply.Value = value
		}
	}
	kv.chanMap.Delete(serverUUID)
	// log.Printf("server get end")
}

func (kv *KVServer) Ok(args *OkArgs, reply *OkReply) {
	// Your code here.
	// log.Printf("server get begin")
	serverUUID := uuid.NewString()
	command := Op{"Ok", "", "", args.Rand, serverUUID}
	ch := make(chan int, 1)
	kv.chanMap.Store(serverUUID, apply{ch, time.Now()})
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// log.Printf("server get end")
		kv.chanMap.Delete(serverUUID)
		return
	}
	isExecuted := false
	if _, ok := <-ch; ok {
		isExecuted = true
	}
	if !isExecuted {
		reply.Err = ErrExecute
	} else {
		reply.Err = OK
	}
	kv.chanMap.Delete(serverUUID)
	// log.Printf("server get end")
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// log.Printf("server %v PutAppend begin, time %v, args %v", kv.me, time.Now(), args)
	serverUUID := uuid.NewString()
	command := Op{args.Op, args.Key, args.Value, args.Rand, serverUUID}
	ch := make(chan int, 1)
	kv.chanMap.Store(serverUUID, apply{ch, time.Now()})
	_, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		// log.Printf("server PutAppend end")
		reply.Err = ErrWrongLeader
		kv.chanMap.Delete(serverUUID)
		return
	}
	isExecuted := false
	// for i := 0; i < 20; i++ {
	// 	isExecuted = kv.isExecute(index, args.Rand)
	// 	if isExecuted {
	// 		break
	// 	}
	// 	time.Sleep(time.Duration(10 * time.Millisecond))
	// }
	if _, ok := <-ch; ok {
		isExecuted = true
	}
	if !isExecuted {
		reply.Err = ErrExecute
	} else {
		reply.Err = OK
	}
	kv.chanMap.Delete(serverUUID)
	// log.Printf("server %v PutAppend end, time %v, args %v", kv.me, time.Now(), args)
}

func (kv *KVServer) GetState(args *GetStateArgs, reply *GetStateReply) {
	term, isLeader := kv.rf.GetState()
	reply.Term = term
	reply.Isleader = isLeader
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

// periodically snapshot raft state
func (kv *KVServer) applierSnap() {
	kv.mu.Lock()
	applyCh := kv.applyCh
	kv.mu.Unlock()
	for m := range applyCh {
		// log.Printf("server %v apply begin, time %v, args %v", kv.me, time.Now(), m)
		// start := time.Now()
		if m.SnapshotValid {
			if m.Snapshot == nil || len(m.Snapshot) == 0 {
				log.Fatalf("nil snapshot")
			}
			r := bytes.NewBuffer(m.Snapshot)
			d := labgob.NewDecoder(r)
			var kvs map[string]string
			var rand2timestamp map[int]int8
			if d.Decode(&kvs) != nil ||
				d.Decode(&rand2timestamp) != nil {
				log.Fatalf("snapshot decode error")
			}
			// log.Printf("server %v kvs=%v rand2timestam%v", kv.me, kvs, rand2timestamp)
			kv.mu.Lock()
			kv.kv = kvs
			kv.randk = rand2timestamp
			kv.mu.Unlock()
		} else if m.CommandValid {
			command := m.Command.(Op)

			kv.execute(command)

			v, ok := kv.chanMap.Load(command.ServerUUID)
			if ok {
				a := v.(apply)
				a.ch <- 1
			}
			kv.mu.Lock()
			maxraftstate := kv.maxraftstate
			kv.mu.Unlock()
			if maxraftstate != -1 && kv.rf.RaftStateSize() > maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				kv.mu.Lock()
				keyvalue := kv.kv
				rand2timestamp := kv.randk
				kv.mu.Unlock()
				e.Encode(keyvalue)
				e.Encode(rand2timestamp)
				kv.rf.Snapshot(m.CommandIndex, w.Bytes())
			}
		} else {
			// Ignore other types of ApplyMsg.
			// log.Printf("else")
		}
		// dur := time.Since(start)
		// log.Printf("server %v apply end, time %v, args %v", kv.me, time.Now(), m)
	}
}

func (kv *KVServer) execute(command Op) {
	if command.OpT == "Put" || command.OpT == "Append" {
		isExecuted := kv.isExecuted(command)
		if !isExecuted {
			if command.OpT == "Put" {
				kv.executePut(command)
			} else if command.OpT == "Append" {
				kv.executeAppend(command)
			}
		}
	} else if command.OpT == "Ok" {
		kv.mu.Lock()
		delete(kv.randk, command.Rand)
		kv.mu.Unlock()
	}
}

func (kv *KVServer) executePut(command Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.kv[command.Key] = command.Value
}

func (kv *KVServer) executeAppend(command Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, exist := kv.kv[command.Key]
	if exist {
		value = value + command.Value
	} else {
		value = command.Value
	}
	kv.kv[command.Key] = value
}

func (kv *KVServer) isExecuted(command Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	rand := command.Rand
	// start := time.Now()
	_, executed := kv.randk[rand]
	// dur := time.Since(start)
	// log.Printf("map cost %v", dur)
	if !executed {
		kv.randk[rand] = 0
		// log.Printf("server = %v, command = %v, map size %v", kv.me, command, len(kv.rand2timestamp))
	}

	return executed
}

func (kv *KVServer) channelExpire() {
	for {
		kv.chanMap.Range(func(key, value interface{}) bool {
			a := value.(apply)
			if time.Now().Sub(a.times) > time.Duration(200*time.Millisecond) {
				close(a.ch)
			}
			return true
		})
		time.Sleep(time.Duration(10 * time.Millisecond))
	}

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

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.randk = make(map[int]int8)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	snapshot := kv.rf.GetSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var kvs map[string]string
		var rand2timestamp map[int]int8
		if d.Decode(&kvs) != nil ||
			d.Decode(&rand2timestamp) != nil {
			log.Fatalf("snapshot decode error")
		}
		kv.kv = kvs
		kv.randk = rand2timestamp
	}
	// You may need initialization code here.
	go kv.applierSnap()
	go kv.channelExpire()
	return kv
}
