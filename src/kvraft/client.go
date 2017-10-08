package raftkv

import (
  "labrpc"
  "crypto/rand"
  "math/big"
  "fmt"
  "time"
)

const (
  REQ_INTERVAL = 50 * time.Millisecond //after try all servers, client stops for a bit
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	me        int64   //client id
	opId      int64   //id for next op to be finished
	leaderId  int     //store most recent Id of leader
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {// {{{
	ck := new(Clerk)
	ck.servers = servers

  //initialization
	ck.me = nrand()  
	ck.opId = 0
	ck.leaderId = 0 
	return ck
}// }}}

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
  ck.debug("Start Op-%v: Get(key=%v)\n", ck.opId, key)
  args := GetArgs {
    Key: key,
    ClientId: ck.me,
    OpId: ck.opId}
  ck.opId++
  for {
    for i := range(ck.servers) {
      serverId := (ck.leaderId + i) % len(ck.servers)
      ck.debug("Op-%v sent to server-port-%v\n", args.OpId, serverId)
      var reply GetReply
      ok := ck.servers[serverId].Call("RaftKV.Get", &args, &reply)

      //no reponse or wrongLeader
      if !ok || reply.WrongLeader { continue } 
      //error handlign
      //success
      if reply.Err == OK || reply.Err == ErrNoKey {
        ck.debug("Finished Op-%v Get(key=%v)=%v\n\n\n", args.OpId, key, reply.Value)
        ck.leaderId = serverId
	      return reply.Value
      }
    }
    ck.debug("No Leader: waiting for election of Raft\n\n")
    <-time.After(REQ_INTERVAL)
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
  ck.debug("Start Op-%v: %v(key=%v, value=%v)\n", ck.opId, op, key, value)
  args := PutAppendArgs {
    Key: key,
    Value: value,
    Op: op,
    ClientId: ck.me,
    OpId: ck.opId}
  ck.opId++
  for {
    for i := range (ck.servers) {
      serverId := (ck.leaderId + i) % len(ck.servers)
      ck.debug("Op-%v sent to server-port-%v\n", args.OpId, serverId)
      var reply PutAppendReply  //has to be inside the loop
      ok := ck.servers[serverId].Call("RaftKV.PutAppend", &args, &reply)

      //no reponse or wrongLeader
      if !ok || reply.WrongLeader { continue } 

      //success
      if reply.Err == OK {
        ck.debug("Finished Op-%v %v(key=%v, value=%v)\n\n\n", args.OpId, op, key, value)
        ck.leaderId = serverId
        return
      }
    }
    ck.debug("No Leader: waiting for election of Raft\n\n")
    <-time.After(REQ_INTERVAL)
  }
}// }}}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

//helper functions
func nrand() int64 {// {{{
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}// }}}

func (ck *Clerk) debug(format string, a ...interface{}) (n int, err error) {// {{{
  format = fmt.Sprintf("client-%v:\t", ck.me) + format
  //DPrintf(format, a...) 
  if Debug > 0 {
    fmt.Printf(format, a...)
  }
  return 
}// }}}

