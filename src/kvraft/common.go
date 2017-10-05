package raftkv

const (
	OK          = "OK"
	ErrNoKey    = "ErrNoKey"
  ErrTimeOut  = "ErrTimeOut"
  ErrTermChg  = "ErrTermChg"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// Field names must start with capital letters,
	Key       string
	Value     string
	Op        string // "Put" or "Append"
  ClientId  int64
  OpId      int64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
  LeaderId    int
}

type GetArgs struct {
	Key       string
  ClientId  int64
  OpId      int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
  LeaderId    int
}
