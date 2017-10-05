package raftkv

import (
  "labrpc"
  "crypto/rand"
  "math/big"
  "fmt"
)


type Clerk struct {
	servers   []*labrpc.ClientEnd
  me        int64   //client id
  opId      int64   //id for next op to be finished
  leaderId  int     //store most recent Id of leader
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

  //initialization
  ck.me = nrand()  
  ck.opId = 0
  ck.leaderId = 0 
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {// {{{
  ck.debug("Initializing Get(key=%v)\n", key)
  args := GetArgs {
    Key: key,
    ClientId: ck.me,
    OpId: ck.opId}
  for {
    var reply GetReply
    ok := ck.servers[ck.leaderId].Call("RaftKV.Get", &args, &reply)

    //no reponse
    if !ok {
      ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
      continue
    } 
    //retry different servers
    if reply.WrongLeader {
      if reply.LeaderId == -1 {
        ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
      } else {
        ck.leaderId = reply.LeaderId
      }
      continue
    } 
    //error handlign
    //success
    if reply.Err == OK || reply.Err == ErrNoKey {
      ck.debug("Finished Get(key=%v)\n\n", key)
	    return reply.Value
    }
  }
}// }}}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {// {{{
  ck.debug("Initializing %v(key=%v, value=%v)\n", op, key, value)
  args := PutAppendArgs {
    Key: key,
    Value: value,
    Op: op,
    ClientId: ck.me,
    OpId: ck.opId}
  for {
    var reply PutAppendReply  //has to be inside the loop
    ok := ck.servers[ck.leaderId].Call("RaftKV.PutAppend", &args, &reply)
    old := ck.leaderId
    //no reponse
    if !ok {
      ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
      ck.debug("no-response: old=%v new=%v, total=%v\n", old, ck.leaderId, len(ck.servers))
      continue
    } 

    //retry different servers
    if reply.WrongLeader {
      if reply.LeaderId == -1 {
        ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
      } 
      //else {
      //  ck.leaderId = reply.LeaderId
      //}
      ck.debug("wrong-leader: old=%v new=%v, total=%v\n", old, ck.leaderId, len(ck.servers))
      continue
    } 
    //success
    if reply.Err == OK {
      ck.debug("Finished %v(%v, %v)\n", op, key, value)
      return
    }
  }
}// }}}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) debug(format string, a ...interface{}) (n int, err error) {// {{{
  format = fmt.Sprintf("client-%v:\t", ck.me) + format
  //DPrintf(format, a...) 
  if Debug > 0 {
    fmt.Printf(format, a...)
  }
  return 
}// }}}

