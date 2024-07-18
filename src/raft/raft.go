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
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int
	votedFor    int // candidateId that received vote in current term(or null if none)
	logs        []LogEntry

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed
	lastApplied int

	// Volatile state on leaders
	// reinitialized after election
	nextIndex  []int
	matchIndex []int

	// self config
	currentState RaftState
	heartBeat    time.Duration
	electionTime time.Time
}

type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command string // command
	Term    int    // the term when this log entry is received by leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentState == Leader {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm

	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
// handler for the call of SendRequestVote, done by other raft peers
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// handle the request vote rpc
	DPrintf("follower %d start to grab lock before handling requestVote from %d\n", rf.me, args.CandidateId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("follower %d grabed lock before handling requestVote from %d\n", rf.me, args.CandidateId)
	// 如果发送request的term比当前follower的term小
	// 设置reply参数，直接返回
	if args.Term < rf.currentTerm {
		DPrintf("follower %d receive requestVote with smaller term from candidate %d\n", rf.me, args.CandidateId)
		reply.Term = args.Term
		reply.VoteGranted = false
		return
	}
	// 如果发送request的term比当前follower的term大
	// 设置当前follower状态
	if args.Term > rf.currentTerm {
		DPrintf("follower %d receive requestVote with larger term from candidate %d\n", rf.me, args.CandidateId)
		rf.currentTerm = args.Term
		rf.transferToFollower() // votedFor = -1, currentState = Follower
	}

	// votedFor: candidateId that received vote in current term 重点是 current term
	// 所以，term改变了，votedFor也要重置1
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		DPrintf("follower %d vote for candidate %d\n", rf.me, args.CandidateId)
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		rf.votedFor = args.CandidateId
		rf.resetElectionTimer()
	}
	//DPrintf("follower %d did nothing, retreived\n", rf.me)
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// handler for the call of sendAppendEntries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		DPrintf("HeatBeat: follower %d receive requestVote with smaller term from leader %d\n", rf.me, args.LeaderId)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	if rf.currentTerm < args.Term {
		DPrintf("HeatBeat: follower %d receive requestVote with larger term from leader %d\n", rf.me, args.LeaderId)
		rf.currentTerm = reply.Term
		rf.transferToFollower() //votedFor = -1, currentState = Follower
	}

	DPrintf("HeatBeat: follower %d reset electionTime\n", rf.me)
	rf.resetElectionTimer()
	reply.Success = true
	reply.Term = rf.currentTerm
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentState = Follower
	rf.currentTerm = 0 //TODO should initialize ?
	rf.votedFor = -1

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	DPrintf("Start a new peer, total peers: %d\n", len(rf.peers))
	go rf.ticker()

	return rf
}

// 周期性地进行
// 如果没有在规定时间内，接收到其他request，就发起election requestVote
func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		if rf.currentState == Leader {
			rf.mu.Unlock()
			rf.leaderAppendEntries(true)
			continue

		} else if time.Now().After(rf.electionTime) {
			rf.mu.Unlock()
			rf.leaderElection()
		}
	}
}

// start leader election
func (rf *Raft) leaderElection() {
	// no need to Lock() 因为一定是lock()状态进入这个函数的
	// transfer to candidate state
	DPrintf("peer %d start leaderElection\n", rf.me)
	rf.currentTerm++
	rf.currentState = Candidate
	rf.votedFor = rf.me
	rf.resetElectionTimer()

	count := 1
	finished := 1
	var mu sync.Mutex
	cond := sync.NewCond(&mu)
	var wg sync.WaitGroup // 保证全部goroutine退出，election 线程再退出
	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			continue
		}
		wg.Add(1)
		go rf.candidateRequestVote(peerId, &count, &finished, &mu, cond, &wg)
	}

	// when check the condition, first grab the lock
	// always check the condition in a loop
	n_peer := len(rf.peers)
	mu.Lock()
	for count <= n_peer/2 && finished != n_peer && rf.currentState == Candidate {
		// the condition is false, then go into the loop
		// Wait() is only called while you are holding the lock
		// when Wait(), it atomically release the lock and put itself into
		// a list of waiting threads
		// When return from Wait() and go back to the top of for loop, it will
		// reacquire the lock, so the check only happen when holding the lock
		DPrintf("peer %d current count: %d, finished: %d, currentState: %v", rf.me, count, finished, rf.currentState)
		cond.Wait()
	}
	// so outside here we still have the lock until we hav done doing whatever
	// we need to do before mu.Unlock()
	DPrintf("current count: %d, finished: %d, currentState: %v", count, finished, rf.currentState)
	if rf.currentState != Candidate {
		DPrintf("Peer %d's election is aborted due to the small termIdx\n", rf.me)
		mu.Unlock()
		wg.Wait()
		return
	} else if count > n_peer/2 {
		DPrintf("Peer %d received %d votes, which is enough\n", rf.me, count)
		rf.transferToLeader()
	} else {
		DPrintf("Peer %d received %d votes, which is not enough", rf.me, count)
		//TODO 票数不够，被动等待appendEntry的到来，再转换身份？ 或者等electionTimeout后再开启选举
	}
	mu.Unlock()
}

func (rf *Raft) resetElectionTimer() {
	t := time.Now()
	electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}

func (rf *Raft) candidateRequestVote(peerId int, count *int, finished *int, mu *sync.Mutex, cond *sync.Cond, wg *sync.WaitGroup) {
	defer wg.Done()
	args := RequestVoteArgs{rf.currentTerm, rf.me, -1, -1} //TODO for last two args
	reply := RequestVoteReply{}

	// rf.mu防止互斥访问rf的一些数据 与rpc handler竞争rf.mu锁
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	// 可能是其他子线程通过对比term，改变了当前candidate的身份->follower 或者已经是leader了
	// 先检查现在还是不是candidate，不是的话直接返回，不发送requestVote了
	if rf.currentState != Candidate {
		return
	} else {
		rf.mu.Unlock()
	}

	// 保证这里开始，rf.mu解锁，因为要处理其他peer的requestVote
	DPrintf("Candidate %d start to send request vote to peer %d\n", rf.me, peerId)
	succ := rf.sendRequestVote(peerId, &args, &reply)
	if !succ {
		log.Fatalf("Cannot send request vote to %d\n", peerId)
	} else {
		DPrintf("Candidate %d finshed send request vote to peer %d\n", rf.me, peerId)
	}

	// 主要是和leaderelection中的检查的部分竞争mu锁
	mu.Lock()
	defer mu.Unlock()
	// 当前term小于reply.term，transfer to follower，直接return
	if reply.Term > rf.currentTerm {
		DPrintf("Peer %d's termIdx is smaller, switch to follower\n", rf.me)
		rf.currentTerm = reply.Term
		rf.transferToFollower()
		return
	}
	if reply.VoteGranted {
		DPrintf("Candidate %d receive a vote from %d\n", rf.me, peerId)
		*count++
	}
	*finished++
	// Broadcast must be called while holding the lock
	cond.Broadcast()
}

func (rf *Raft) transferToLeader() {
	rf.votedFor = -1
	rf.currentState = Leader
	// 立即发送心跳
	rf.leaderAppendEntries(true)
}

func (rf *Raft) transferToFollower() {
	rf.votedFor = -1
	rf.currentState = Follower
	// rf.resetElectionTimer() // candidate 因为term小而变为follower，暂时不resettimer
}

func (rf *Raft) leaderAppendEntries(sendHeartBeat bool) {
	DPrintf("Leader %d start send heartbeat\n", rf.me)
	for peerId, _ := range rf.peers {
		if peerId == rf.me { // 注意leader本人也需要resetTimer
			rf.resetElectionTimer()
		}
		DPrintf("Leader %d start send heartbeat to follower %d\n", rf.me, peerId)
		args := AppendEntriesArgs{}
		if sendHeartBeat {
			args = AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, []LogEntry{}, -1}
		} else {
			args = AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, rf.logs, -1}
		}
		reply := AppendEntriesReply{}
		succ := rf.sendAppendEntries(peerId, &args, &reply)
		if !succ {
			DPrintf("Leader %d send heartbeat failed\n", rf.me)
			log.Fatalf("Cannot send AppendEntries to %d\n", peerId)
		}

		// check term
		if reply.Term > rf.currentTerm {
			DPrintf("Leader %d's termIdx is smaller, switch to follower\n", rf.me)
			rf.currentTerm = reply.Term
			rf.transferToFollower() // move to the next term, reset votedFor and currentState
		}
	}
}
