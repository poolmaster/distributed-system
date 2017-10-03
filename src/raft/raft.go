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
  "fmt"
  "sync"
  "labrpc"
  "bytes"
  "encoding/gob"
  "math/rand"
  "time"
)

const (
  FOLLOWER = iota
  CANDIDATE
  LEADER
  
  TIMEOUT_MIN = 500 //Millisecond   
  TIMEOUT_MAX = 800 
  
  HB_INTERVAL = 50 * time.Millisecond
  
  NUM_LOG_RPC = 10   //number of log entries sent per RPC
  MAX_CHAN_DEPTH = 100
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
  Term  int
  Cmd   interface{} 
}

// A Go object implementing a single Raft peer.
type Raft struct {// {{{
	// state a Raft server must maintain.
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
  
  cmtChan     chan bool           //send commit signal 
  applyChan   chan ApplyMsg       //chan to handle committed message
  electTimer  *time.Timer         //timer for election cycle
  state       int 
  voteCnt     int 

  //persistent state of servers
  curTerm     int                 //init to 0
  votedFor    int                 //index for candidate that received vote in current Term
  log         []LogEntry
  //volatile state of servers
  cmtIdx      int                 //index of highest logEntry committed 
  applyIdx    int                 //index of highest logEntry applied to SM
  //volatile state of leader
  nextIdx     []int               //index of next logEntry to be sent to followers
  matchIdx    []int               //index of highest logEntry known to be replicated for each follower
}// }}}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {// {{{
  rf.mu.Lock()
  term := rf.curTerm
  isleader := (rf.state == LEADER)
  rf.mu.Unlock() 
	return term, isleader
}// }}}

//Helper Functions// {{{
func (rf *Raft) getRandTimeout() (time.Duration) {
  return (time.Duration(rand.Int63() % (TIMEOUT_MAX - TIMEOUT_MIN) + TIMEOUT_MIN) *
          time.Millisecond)
}

func (rf *Raft) getLastLogTerm() int {
  return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) getFirstLogIdxOfTerm(term int) int {
  //binary search
  start, end := 0, len(rf.log) - 1
  for start + 1 < end {
    mid := start + (end - start) / 2
    if rf.log[mid].Term >= term {
      end = mid
    } else {
      start = mid
    }
  }
  if rf.log[start].Term == term {
    return start
  } else if rf.log[end].Term == term {
    return end
  } else {
    return -1
  }
}
// }}}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() { // {{{
	writeBuffer := new(bytes.Buffer)
  encoder := gob.NewEncoder(writeBuffer)
  encoder.Encode(rf.curTerm)
  encoder.Encode(rf.votedFor)
  encoder.Encode(rf.log)
	data := writeBuffer.Bytes()
	rf.persister.SaveRaftState(data)
}// }}}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {// {{{
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	readBuffer := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(readBuffer)
  decoder.Decode(&rf.curTerm)
  decoder.Decode(&rf.votedFor)
  decoder.Decode(&rf.log)
}// }}}

// RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
  Term        int
  CandId      int 
  LastLogIdx  int
  LastLogTerm int
}

// RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
  Term  int
  Vote  bool
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {// {{{
  rf.mu.Lock()  
  defer rf.mu.Unlock()

  if args.Term < rf.curTerm {
    reply.Term = rf.curTerm
    reply.Vote = false
    rf.debug("request-vote from N%vT%v: Obsolete Term\n", args.CandId, args.Term)
    return 
  }
  
  if args.Term > rf.curTerm {
    rf.curTerm = args.Term
    rf.votedFor = -1  //clear obsolete vote info
    rf.state = FOLLOWER
    rf.persist()
  }

  reply.Term = rf.curTerm
  reply.Vote = false
  if rf.votedFor == -1 {
    lastLogTerm := rf.getLastLogTerm()
    rf.debug("request vote received from N%vT%v cand.LLT,LLI=%v,%v, node.LLT=%v,%v\n", 
      args.CandId, args.Term, args.LastLogTerm, args.LastLogIdx, lastLogTerm, len(rf.log) - 1)
    if (args.LastLogTerm < lastLogTerm) {
      rf.debug("request vote received from N%vT%v: Less up-to-date log. No Vote\n", args.CandId, args.Term)
    } else if (args.LastLogTerm == lastLogTerm &&
               args.LastLogIdx < len(rf.log) - 1) {
      rf.debug("request vote received from N%vT%v: Less up-to-date log. No Vote.\n", args.CandId, args.Term)
    } else {
      reply.Vote = true  
      rf.votedFor = args.CandId
      rf.state = FOLLOWER
      rf.electTimer.Reset(rf.getRandTimeout())
      rf.persist()
    }
  }
}// }}}

// Comments: example code to send a RequestVote RPC to a server.// {{{
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
//// }}}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {// {{{
  //rf.debug("requesting vote from Node %v\n", server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply) 
  rf.mu.Lock()
  defer rf.mu.Unlock()
  
  if !ok {
    rf.debug("No response from N%vT%v\n", server, reply.Term)
    return false
  }

  if reply.Term > rf.curTerm {
    rf.debug("vote fail from N%vT%v: convert to FOLLOWER\n", server, reply.Term)
    rf.curTerm = reply.Term
    rf.votedFor = -1
    rf.state = FOLLOWER
    rf.persist()
  }
  //revoke because of new term or heartbeat received 
  if rf.state == FOLLOWER {
    return false
  }

  if reply.Term < rf.curTerm {
    rf.debug("vote fail from N%vT%v: obsolete vote\n", server, reply.Term)
    return false 
  }

  if reply.Vote {
    rf.debug("vote success from N%vT%v\n", server, reply.Term)
    rf.voteCnt++
    if rf.state == CANDIDATE && rf.voteCnt > len(rf.peers) / 2 {
      rf.debug("leader elected\n") 
      rf.state = LEADER
      for i := range(rf.peers) { 
        //last log entry index + 1
        //prevLogIdx always send, so heartBeat withouth entry
        //will match exisiting entries for followers anyway
        rf.nextIdx[i] = len(rf.log) 
        rf.matchIdx[i] = 0 
      }
      //trick to wake runAsCandidate() thread, no need for another channel
      rf.electTimer.Reset(1)  
      rf.persist()
    }
  }
  return true
}// }}}

// AppendEntries RPC
type AppendEntryArgs struct {
  Term          int
  LeaderId      int  //for redirection, not needed in this project
  PrevLogIdx    int  
  PrevLogTerm   int
  Entries       []LogEntry
  LeaderCmtIdx  int
}

type AppendEntryReply struct {
  Term        int
  Success     bool
  NextIdx     int
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {// {{{
  rf.mu.Lock()  
  defer rf.mu.Unlock()  
  defer rf.persist()

  //obsolete rpc
  if args.Term < rf.curTerm { 
    reply.Term = rf.curTerm
    reply.Success = false
    reply.NextIdx = -1 //mark as -1 to distinguish with other fail
    rf.debug("heartbeat from N%vT%v: Obsolete Term\n", args.LeaderId, args.Term)
    return
  }
  //valid heart beat (maybe with cmds)
  if args.Term == rf.curTerm && rf.state == LEADER {
    err := fmt.Sprintf("heartbeat from (N%vT%v): leader conflict", args.LeaderId, args.Term)
    panic(err)
  }

  rf.curTerm = args.Term
  rf.state = FOLLOWER
  rf.electTimer.Reset(rf.getRandTimeout())
  
  reply.Term = rf.curTerm
  //term mismatch on prevlog
  if args.PrevLogIdx >= len(rf.log) {
    reply.Success = false 
    reply.NextIdx = len(rf.log) 
    rf.debug("heartbeat from N%vT%v: PLI out of range: LogSize=%v\n", args.LeaderId, args.Term, reply.NextIdx)
    return 
  }
  if rf.log[args.PrevLogIdx].Term != args.PrevLogTerm {
    reply.Success = false   
    //bypass all entries in the conflicting term
    //find the first entry with same term as conflicting entry
    //and
    reply.NextIdx = rf.getFirstLogIdxOfTerm(rf.log[args.PrevLogIdx].Term)
    //reply.NextIdx = args.PrevLogIdx
    rf.debug("heartbeat from N%vT%v: Term mismatch on log@PLI (leader=%v,follower=%v\n", 
      args.LeaderId, args.Term, args.PrevLogTerm, rf.log[args.PrevLogIdx].Term)
    return 
  }
  //update logs
  for i := range args.Entries {  
    logIdx := args.PrevLogIdx + 1 + i
    if logIdx >= len(rf.log) {
      rf.log = append(rf.log, args.Entries[i])
    } else if rf.log[logIdx].Term != args.Entries[i].Term {
      rf.log[logIdx] = args.Entries[i] 
      rf.log = rf.log[:logIdx + 1]
    }
  }
  //update cmtIdx
  lastNewIdx := args.PrevLogIdx + len(args.Entries)
  if args.LeaderCmtIdx > rf.cmtIdx {
    //oldCmtIdx := rf.cmtIdx
    if args.LeaderCmtIdx < lastNewIdx {
      rf.cmtIdx = args.LeaderCmtIdx
    } else if rf.cmtIdx < lastNewIdx { 
      //possibly no log updates causing lastNewIdx < cmtIdx ?
      rf.cmtIdx = lastNewIdx
    }
    rf.cmtChan<- true
    rf.debug("(Follower) New log being committed. Leader.cmtIdx=%v\n", rf.cmtIdx)
    //for i := oldCmtIdx + 1; i <= rf.cmtIdx; i++ {
    //  rf.debug("\tCommit: idx=%v, term=%v cmd=%v\n", i, rf.log[i].Term, rf.log[i].Cmd)
    //}
  }
  reply.Success = true  
  reply.NextIdx = lastNewIdx + 1
  if len(args.Entries) > 0 {
    rf.debug("heartbeat from N%vT%v: NumLogAdded=%v.\n", args.LeaderId, args.Term, len(args.Entries))
    //cmdList := "["
    //for i := range rf.log {
    //  cmdList += fmt.Sprintf("%v/", rf.log[i].Term)
    //  cmdList += fmt.Sprintf("%v, ", rf.log[i].Cmd)
    //}
    //rf.debug("\t\t%v\n", cmdList)
  }
  return
}// }}}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {// {{{
  //rf.debug("sending heartbeat to Node %v\n", server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply) 
  rf.mu.Lock()
  defer rf.mu.Unlock()

  if !ok {
    //rf.debug("no response from Node %v\n", server)
    return false
  }
  //obsolete term + prevlog mismatch
  if !reply.Success || rf.state != LEADER {   
    if reply.Term > rf.curTerm { 
      rf.curTerm = reply.Term
      rf.state = FOLLOWER
      rf.persist()
      rf.debug("return heartbeat from N%vT%v: failed (newer term)\n", server, reply.Term)
    } else if rf.state == LEADER && reply.NextIdx > 0 { //BUG: could accidentally update next to -1/0
      rf.nextIdx[server] = reply.NextIdx //BUG: only success could update matchIdx
      rf.debug("return heartbeat from N%vT%v: Log@PLI mismatch: updated NextIdx=%v\n", server, reply.Term, reply.NextIdx)
    }
    return false
  }

  //log applied success
  rf.matchIdx[server] = reply.NextIdx - 1
  //only need to apply when matchidx change bigger than cmtIdx
  if rf.matchIdx[server] > rf.cmtIdx {
    rf.debug("return heartbeat from N%vT%v: log replicated: matchIdx=%v > cmtIdx=%v\n",
      server, reply.Term, reply.NextIdx - 1, rf.cmtIdx)
    //oldCmtIdx := rf.cmtIdx
    for i := reply.NextIdx - 1; i > rf.cmtIdx; i-- {
      if rf.log[i].Term < rf.curTerm {
        //BUG: prolem of Figure 8 in paper
        //only commit log in current term by counting
        break
      } else if rf.log[i].Term > rf.curTerm { 
        fmt.Println("Leader gets log newer than its term: logTerm=%v", rf.log[i].Term)
        panic("Leader gets log newer than its term!")
      }
      applyCnt := 1
      for j := range rf.peers {
        if rf.matchIdx[j] >= i {
          applyCnt++
        }
      }
      if applyCnt > len(rf.peers) / 2 {
        rf.cmtIdx = i
        rf.cmtChan<- true
        rf.debug("(Leader) New log being committed. Leader.cmtIdx=%v\n", rf.cmtIdx)
        //for j := range (rf.peers) {
        //  rf.debug("follower's matchIdx[%v]=%v\n", j, rf.matchIdx[j])
        //}
        break //older logs are committed automatically
      }
    }
    //for i := oldCmtIdx + 1; i <= rf.cmtIdx; i++ {
    //  rf.debug("\tCommit: idx=%v, term=%v cmd=%v\n", i, rf.log[i].Term, rf.log[i].Cmd)
    //}
  }
  return true
}// }}}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {// {{{
  //rf.debug("Start a command\n")
  index := -1
  term, isLeader := rf.GetState()
  if isLeader {
    rf.mu.Lock()
	  index = len(rf.log)
    rf.log = append(rf.log, LogEntry{Term: term, Cmd: command})
    //rf.matchIdx[rf.me] = len(rf.log) - 1
    rf.persist()
    rf.debug("Command appended to Log: idx=%v, cmd=%v\n", index, command)
    rf.mu.Unlock()
  }
	return index, term, isLeader
}// }}}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {// {{{
  // could disable debug message.
}// }}}

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {// {{{
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
  rf.cmtChan = make(chan bool, MAX_CHAN_DEPTH)
  rf.applyChan = applyCh

	//initialization
  rf.electTimer = time.NewTimer(TIMEOUT_MAX * time.Millisecond)
  rf.state = FOLLOWER
  rf.voteCnt = 0
  rf.cmtIdx = 0
  rf.applyIdx = 0
  rf.nextIdx = make([]int, len(rf.peers)) //initialize when become leader
  rf.matchIdx = make([]int, len(rf.peers))
  for i := range rf.matchIdx {
    rf.matchIdx[i] = 0
  }
	// initialize from state persisted before a crash
  rf.curTerm = 0
  rf.votedFor = -1
  rf.log = make([]LogEntry, 0)
  rf.log = append(rf.log, LogEntry{Term: 0})
	rf.readPersist(persister.ReadRaftState())

  go rf.writeApplyCh()
  go rf.run()

	return rf
}// }}}

// write applied message to channel
func (rf *Raft) writeApplyCh() {// {{{
  for {
    <-rf.cmtChan 
    rf.mu.Lock()
    for i := rf.applyIdx + 1; i <= rf.cmtIdx; i++ {
      var newApplyMsg ApplyMsg
      newApplyMsg.Index = i 
      newApplyMsg.Command = rf.log[i].Cmd
      rf.applyChan<- newApplyMsg 
      rf.applyIdx = i 
      rf.debug("Applied committed msg: cmtIdx=%v, applyIdx=%v\n", rf.cmtIdx, rf.applyIdx)
    }
    rf.mu.Unlock()
  }
}// }}}

// run always 
func (rf *Raft) run() {// {{{
  for {
    rf.mu.Lock()
    state := rf.state
    rf.mu.Unlock()
    switch state {
    case FOLLOWER:
      rf.runAsFollower()
    case CANDIDATE:
      rf.runAsCandidate()
    case LEADER:
      rf.runAsLeader()
    }
  }
}// }}}

func (rf *Raft) runAsFollower() {// {{{
  //not using select-case-chan to avoid efficiency problem
  //since temp timer cannot garbage collected until expire
  rf.mu.Lock()
  rf.electTimer.Reset(rf.getRandTimeout())
  rf.mu.Unlock()
  //timer will be reset when receiving reply
  <-rf.electTimer.C
  rf.mu.Lock()
  rf.debug("election timeout\n")
  rf.state = CANDIDATE
  rf.mu.Unlock()
}// }}}

func (rf *Raft) broadcastRequestVote() {// {{{
  rf.mu.Lock()
  var voteArgs RequestVoteArgs 
  voteArgs.Term = rf.curTerm
  voteArgs.CandId = rf.me
  voteArgs.LastLogTerm = rf.getLastLogTerm() 
  voteArgs.LastLogIdx = len(rf.log) - 1
  rf.electTimer.Reset(rf.getRandTimeout())  //reset timer once broadcast
  rf.mu.Unlock()
  for i := range rf.peers {
    if i != rf.me && rf.state == CANDIDATE {
      go func (i int) {
        var voteReply RequestVoteReply
        rf.sendRequestVote(i, &voteArgs, &voteReply)
      } (i)
    }
  }
}// }}}

func (rf *Raft) runAsCandidate() {// {{{
  rf.mu.Lock()
  rf.curTerm++
  rf.voteCnt = 1
  rf.votedFor = rf.me
  rf.persist()
  rf.mu.Unlock()
  rf.broadcastRequestVote() 
  <-rf.electTimer.C  //wake by split-vote/network-fail OR leader elected
}// }}}

func (rf *Raft) broadcastAppendEntries() {// {{{
  var appendArgs AppendEntryArgs
  rf.mu.Lock()
  appendArgs.Term = rf.curTerm
  appendArgs.LeaderId = rf.me
  appendArgs.LeaderCmtIdx = rf.cmtIdx
  rf.mu.Unlock()
  for i := range rf.peers {
    if i != rf.me && rf.state == LEADER {
      rf.mu.Lock()
      appendArgs.PrevLogIdx = rf.nextIdx[i] - 1
      appendArgs.Entries = make([]LogEntry, 0)
      appendArgs.PrevLogTerm = rf.log[appendArgs.PrevLogIdx].Term
      logCnt := 0
      for j := 0; j < NUM_LOG_RPC; j++ {
        if rf.nextIdx[i] + j >= len(rf.log) { break }
        appendArgs.Entries = append(appendArgs.Entries, rf.log[rf.nextIdx[i] + j]) 
        logCnt++
      }
      rf.nextIdx[i] += logCnt
      if (logCnt > 0) { 
        //rf.debug("NextIdx updated for server%v, nextIdx=%v\n", i, rf.nextIdx[i])
      }
      rf.mu.Unlock()
      go func (i int, appendArgs AppendEntryArgs) {
        var appendReply AppendEntryReply 
        rf.sendAppendEntries(i, &appendArgs, &appendReply)
      } (i, appendArgs) 
    }
  }
}// }}}

func (rf *Raft) runAsLeader() {// {{{
  _, isLeader := rf.GetState()
  //leader state could be changed due to obsolete term
  //add condition to reduce RPC messages 
  if isLeader {
    rf.broadcastAppendEntries()
  }
  time.Sleep(HB_INTERVAL)
}// }}}

func (rf *Raft) debug(format string, a ...interface{}) (n int, err error) {// {{{
  format = fmt.Sprintf("N%vT%v:\t", rf.me, rf.curTerm) + format
  //DPrintf(format, a...) 
  if Debug > 0 {
    fmt.Printf(format, a...)
  }
  return 
}// }}}
