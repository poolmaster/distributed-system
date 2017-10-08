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

const Debug = 0

const (
  GET = "Get"
  PUT = "Put"
  APPEND = "Append"
  
  OP_TIMEOUT = 400 * time.Millisecond
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

type Result struct {
  op      Op
  err     Err 
  value   string
}

type RaftKV struct {// {{{
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

  //kv state
  db        map[string]string   //kv storage 
  resCh     map[int]chan Result //channel per index for applied results 
  lastOpId  map[int64]int64     //per client most recent returned request
}// }}}

func (kv *RaftKV) commitOp() {// {{{
  for {
    msg := <-kv.applyCh
    res := Result {op: msg.Command.(Op), err: OK, value: ""} 
    kv.mu.Lock()
    //handle duplicate
    duplicate := false
    temp, ok := kv.lastOpId[res.op.ClientId]
    if ok && temp >= res.op.OpId { 
      kv.debug("Client-%v: Op-%v has been applied. return\n", res.op.ClientId, res.op.OpId)
      duplicate = true
    } else {
      kv.lastOpId[res.op.ClientId] = res.op.OpId
      kv.debug("Ck-%v update last-applied Op to Op-%v", res.op.ClientId, kv.lastOpId[res.op.ClientId])
    }
    //BUG: rf.Start() could be called and return result 
    //before resCh[idx] being created
    if _, ok := kv.resCh[msg.Index]; !ok {
      kv.resCh[msg.Index] = make(chan Result, 1)
    }

    //apply operation
    //access storage
    //BUG: for GET operation, even if with duplicate, we has to sample the data
    //the case is: a request sent to N0, timeout, and retried to N1;
    //N0 came back to finish the request and thus N0 mark that op done. then N1 will not 
    //sample the data and return. The problem is we have to return data @ most
    //recent requested leader
    //for PUT/APPEND, it is different, cos once we got into this stage, we wont
    //have any error.
    if res.op.Name == GET {
      if value, ok := kv.db[res.op.Key]; ok {
        res.value = value
      } else {
        res.err = ErrNoKey
      }
    } else if res.op.Name == PUT && !duplicate {
      kv.db[res.op.Key] = res.op.Value
    } else if res.op.Name == APPEND && !duplicate {
      if _, ok := kv.db[res.op.Key]; !ok {
        kv.db[res.op.Key] = "" 
      }
      kv.db[res.op.Key] += res.op.Value  
    }   
    
    kv.resCh[msg.Index]<- res //notify RPC call
    kv.debug("Op-%v applied (duplicate?: %v)\n", res.op.OpId, duplicate)
    if preResCh, ok := kv.resCh[msg.Index - 1]; ok {
      //close channel for previous index and deallocate resource
      close(preResCh)
      delete(kv.resCh, msg.Index - 1)
    }
    kv.mu.Unlock()
  }
}// }}}

//invoke an operation and wait for Raft to commit
func (kv *RaftKV) invoke(op Op) Result {// {{{
  kv.debug("invoking Op-%v\n", op.OpId)
  res := Result {op: op, err: OK, value: ""}
  cmtIdx, _, isLeader := kv.rf.Start(op) 
 
  if !isLeader { 
    kv.debug("wrong leader for Op-%v\n", op.OpId)
    res.err = ErrWL
    return res 
  }

  kv.mu.Lock()
  if _, ok := kv.resCh[cmtIdx]; !ok {
    kv.resCh[cmtIdx] = make(chan Result, 1)
  }
  resIdxCh := kv.resCh[cmtIdx]
  kv.mu.Unlock()
  //two condition for return error:
  //1. term change
  //2. timeout: with dection of term change, timeout will be
  //case where leader is partitioned. So better try different
  //server in this case
  kv.debug("waiting for Op-%v\n", op.OpId)
  select {
    case <-time.After(OP_TIMEOUT):
      kv.debug("timeout for Op-%v\n", op.OpId)
      res.err = ErrTO
    case res = <-resIdxCh: 
      //if it is a different op, it must come from a different raft
      //therefore, we wont have resIdxCh to handle 1-to-many mapping
      if res.op.ClientId != op.ClientId || res.op.OpId != op.OpId {
        kv.debug("term chagne for Op-%v\n", op.OpId)
        res.err = ErrTC
      } else {
        kv.debug("Executed Op-%v: %v(key=%v, value=%v) from Client-%v\n", 
          op.OpId, op.Name, op.Key, op.Value, op.ClientId)
      }
  }
  return res
}// }}}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {// {{{
  op := Op {
    Name: GET,
    Key: args.Key,
    Value: "",
    ClientId: args.ClientId,
    OpId: args.OpId }
  
  result := kv.invoke(op) 
  reply.WrongLeader = (result.err == ErrWL)
  reply.Err = result.err
  reply.Value = result.value
  
  if (reply.Err == OK || reply.Err == ErrNoKey) && result.op != op {
    fmt.Printf("sanity check: result.opId=%v, original.opId=%v\n", result.op.OpId, op.OpId)
    panic("op mismatch for a valid return!")
  }
}// }}}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {// {{{
  op := Op {
    Name: args.Op,
    Key: args.Key,
    Value: args.Value,
    ClientId: args.ClientId,
    OpId: args.OpId }

  result := kv.invoke(op) 
  reply.WrongLeader = (result.err == ErrWL)
  reply.Err = result.err

  if reply.Err == OK && result.op != op {
    fmt.Printf("sanity check: result.opId=%v, original.opId=%v\n", result.op.OpId, op.OpId)
    panic("op mismatch for a valid return!")
  }
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
  kv.resCh = make(map[int]chan Result)    
  kv.lastOpId = make(map[int64]int64)
  
  go kv.commitOp()
  
  return kv
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

