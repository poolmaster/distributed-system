package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
  "time"
  "fmt"
  //"errors"
)

const Debug = 1

const (
  GET = "Get"
  PUT = "Put"
  APPEND = "Append"
  
  OP_TIMEOUT = 2000 * time.Millisecond
)

type Op struct {
	// Field names must start with capital letters,
	// otherwise RPC will break.
  Name      string
  Key       string
  Value     string
  OpId      int64
  ClientId  int64
}

type RaftKV struct {// {{{
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

  //kv state
  db          map[string]string
  idxCmtOpCh  map[int]chan Op   //channel per index for committed Op 
}// }}}

//invoke an operation and wait for Raft to commit
//return (wrongLeader, err, leaderId)
func (kv *RaftKV) invoke(op Op) (bool, Err, int) {// {{{
  cmtIdx, _, isLeader := kv.rf.Start(op) 
  
  if !isLeader { 
    return true, OK, kv.rf.GetLeaderId()
  }

  kv.mu.Lock()
  cmtOpCh, ok := kv.idxCmtOpCh[cmtIdx] 
  if !ok {
    cmtOpCh = make(chan Op, 1)
    kv.idxCmtOpCh[cmtIdx] = cmtOpCh
  }
  kv.mu.Unlock()
  //two condition for return error:
  //1. term change
  //2. timeout: with dection of term change, timeout will be
  //case where leader is partitioned. So better try different
  //server in this case
  select {
    case <-time.After(OP_TIMEOUT):
      return true, ErrTimeOut, -1
    case  cmtOp := <-cmtOpCh:
      //if it is a different op, it must come from a different raft
      //there for, we wont have cmtOpCh to handle 1-to-many mapping
      if cmtOp.ClientId != op.ClientId || cmtOp.OpId != op.OpId {
        return true, ErrTermChg, -1 
      } else {
        kv.debug("Executing Op")
        return false, OK, -1
      }
  }
}// }}}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {// {{{
  op := Op {
    Name: GET,
    Key: args.Key,
    Value: "",
    ClientId: args.ClientId,
    OpId: args.OpId }
  
  reply.WrongLeader, reply.Err, reply.LeaderId = kv.invoke(op) 
  if !reply.WrongLeader && (reply.Err == OK) {
    kv.mu.Lock()
    value, ok := kv.db[args.Key]
    if ok {
      reply.Value = value
    } else {
      reply.Value = ""
      reply.Err = ErrNoKey
    }
    kv.mu.Unlock()
  }
  return
}// }}}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {// {{{
  kv.debug("get called with PutAppend()\n")
  op := Op {
    Name: args.Op,
    Key: args.Key,
    Value: args.Value,
    ClientId: args.ClientId,
    OpId: args.OpId }

  reply.WrongLeader, reply.Err, reply.LeaderId = kv.invoke(op) 
  if !reply.WrongLeader && (reply.Err == OK) {
    kv.mu.Lock()
    if args.Op == PUT {
      kv.db[args.Key] = args.Value
    } else if args.Op == APPEND {
      kv.db[args.Key] += args.Value
    } else {
      panic("Illegal OpType being provided!")
    }
    kv.mu.Unlock()
  }
  return
}// }}}

// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {// {{{
	kv.rf.Kill()
}// }}}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {// {{{
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

  kv.db = make(map[string]string)
  kv.idxCmtOpCh = make(map[int]chan Op)    
  
  go kv.commitOp()

	return kv
}// }}}

func (kv *RaftKV) commitOp() {// {{{
  for {
    msg := <-kv.applyCh
    kv.mu.Lock()
    kv.idxCmtOpCh[msg.Index]<- msg.Command.(Op)
    kv.mu.Unlock()
  }
}// }}}

func DPrintf(format string, a ...interface{}) (n int, err error) {// {{{
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}// }}}

func (kv *RaftKV) debug(format string, a ...interface{}) (n int, err error) {// {{{
  format = fmt.Sprintf("KV-N%v:\t", kv.me) + format
  //DPrintf(format, a...) 
  if Debug > 0 {
    fmt.Printf(format, a...)
  }
  return 
}// }}}

