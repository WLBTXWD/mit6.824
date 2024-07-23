package raft

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
	// DPrintf("follower %d start to grab lock before handling requestVote from %d\n", rf.me, args.CandidateId)
	//DPrintf("follower %d try to grab lock", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("follower %d grabbed lock", rf.me)
	// DPrintf("follower %d grabed lock before handling requestVote from %d\n", rf.me, args.CandidateId)
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
		rf.votedFor = -1
		rf.currentState = Follower
	}

	// 前面的term的检查，是每个收到的requestVote，都必须handle的
	// 接下来进行能否投票的检查 只有是follower的时候，并且还没投过票，才能够投票
	if rf.currentState == Follower && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf_last_idx := len(rf.logs) - 1
		rf_last_term := rf.logs[rf_last_idx].Term
		// 这个判断，保证了只有拥有最新log（term最大，同样大，更长）的candidate才能成为leader
		if args.LastLogTerm > rf_last_term || (args.LastLogTerm == rf_last_term && args.LastLogIndex >= rf_last_idx) {
			DPrintf("follower %d vote for candidate %d\n", rf.me, args.CandidateId)
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
			rf.electionTimer.Reset(rf.RandomizedElectionTimeout())
		}
	}
}

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
		DPrintf("Follower %d receive requestVote with smaller term from leader %d\n", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if rf.currentTerm < args.Term {
		DPrintf("Follower %d receive requestVote with larger term from leader %d\n", rf.me, args.LeaderId)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.currentState = Follower
	}

	rf.electionTimer.Reset(rf.RandomizedElectionTimeout())
	reply.Term = rf.currentTerm

	// consistency check
	// args.prev_log_idx，有没有这个idx
	last_idx := len(rf.logs) - 1
	if last_idx < args.PrevLogIndex {
		// false
		DPrintf("Follower %d's last log index %d is smaller than args.PrevLogIndex %d", rf.me, last_idx, args.PrevLogIndex)
		reply.Success = false
		return
	} else if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		// false
		DPrintf("Follower %d's PrevLogTerm %d is not equal to args.PrevLogTerm %d", rf.me, rf.logs[args.PrevLogIndex], args.PrevLogTerm)
		reply.Success = false
		return
	}
	// pass consistency check, 处理new ones
	// 说明 在这个follower中找到了与prevLogIndex一致的entry，两者对齐了
	// new ones 是 leader记录的 nextIndex[peerId]保存的entry
	// 遍历所有要加入的 args.Entries, 与rf.logs的 prevLoIdnedx + 1 处开始比较
	DPrintf("Follower %d passes consistency check for prev_log_idx %d\n", rf.me, args.PrevLogIndex)
	for idx, entry := range args.Entries {
		last_idx = len(rf.logs) - 1
		if last_idx < args.PrevLogIndex+idx+1 {
			rf.logs = append(rf.logs, entry) //append any new entry not already in the log
			DPrintf("Follower %d add a new command %v\n", rf.me, entry)
		}
		if rf.logs[args.PrevLogIndex+idx+1].Term != entry.Term {
			// delete this and all that follow it
			rf.logs = rf.logs[:args.PrevLogIndex+idx+1] // 左闭右开
			rf.logs = append(rf.logs, entry)
			DPrintf("Follower %d delete a old command %v and re-add it\n", rf.me, entry)
		}
	}

	// commitIdx更新，并更新lastApplied
	last_idx = len(rf.logs) - 1
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, last_idx)
		//DPrintf("")
	}

	// 如果lastApplied 比 commitIndex 小
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{true, rf.logs[i].Command, i}
	}
	// 如果lastApplied 比 commitIndex小，等于commitIndex
	rf.lastApplied = max(rf.lastApplied, rf.commitIndex)

	reply.Success = true

}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if a < b {
		return b
	} else {
		return a
	}
}
