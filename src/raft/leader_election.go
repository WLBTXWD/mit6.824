package raft

import "log"

// start leader election
func (rf *Raft) leaderElection() {
	// no need to Lock() 因为一定是lock()状态进入这个函数的
	// transfer to candidate state
	DPrintf("peer %d start leaderElection\n", rf.me)
	rf.currentTerm++
	rf.currentState = Candidate
	rf.votedFor = rf.me

	grantedVoteCnt := 1
	last_log_index := len(rf.logs) - 1
	last_log_term := rf.logs[last_log_index].Term
	args := RequestVoteArgs{rf.currentTerm, rf.me, last_log_index, last_log_term}
	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			continue
		}
		go func(peerId int) {
			reply := RequestVoteReply{}
			DPrintf("Candidate %d start to send voteRequest to %d\n", rf.me, peerId)
			succ := rf.sendRequestVote(peerId, &args, &reply)
			if !succ {
				// log.Fatalf("Cannot send request vote to %d\n", peerId)
				DPrintf("Cnadidate %d Cannot send request vote to %d\n", rf.me, peerId)
			} else {
				DPrintf("Candidate %d receives voteResponse from %d\n", rf.me, peerId)
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			//检查reply之前，先确认当前处于什么term，处于什么状态
			// 有可能已经满足投票数变成leader了，或者收到其他人的心跳，变成follower了
			if rf.currentState != Candidate {
				DPrintf("Peer %d is no longer candidate, now is %v\n", rf.me, rf.currentState)
				return
			}
			// check term with reply
			if reply.Term > rf.currentTerm {
				DPrintf("Candidate %d's termIdx is smaller, switch to follower\n", rf.me)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.currentState = Follower
				return
			}
			if reply.VoteGranted {
				DPrintf("Candidate %d receive a vote from %d\n", rf.me, peerId)
				grantedVoteCnt++
				if grantedVoteCnt > len(rf.peers)/2 {
					DPrintf("Candidate %d receive enough votes in term %v\n", rf.me, rf.currentTerm)
					rf.transferToLeader()
				}
			}
		}(peerId)

	}
}

func (rf *Raft) leaderAppendEntries(sendHeartBeat bool) {
	if sendHeartBeat {
		DPrintf("Leader %d start send heartbeat\n", rf.me)
	} else {
		DPrintf("Leader %d start send AppendEntries\n", rf.me)
	}
	for peerId, _ := range rf.peers {
		if peerId == rf.me {
			rf.electionTimer.Reset(rf.RandomizedElectionTimeout())
			continue
		}
		// 不管是不是sendHeartbeat，都发送
		// 只有在last log indx >= nextIndex[peerId]才会发送  Fig 2 Leaders:3
		// if len(rf.logs)-1 < rf.nextIndex[peerId] {
		// 	continue
		// }
		// DPrintf("Outer: Leader %d start send appendEntry to follower %d, bcuz it's nextIdx is %d which <= last log idx %d\n",
		// 	rf.me, peerId, rf.nextIndex[peerId], len(rf.logs)-1)
		if !sendHeartBeat {
			// 只有在last log indx >= nextIndex[peerId]才会发送  Fig 2 Leaders:3
			if len(rf.logs)-1 < rf.nextIndex[peerId] {
				continue
			}
			DPrintf("Outer: Leader %d start send appendEntry to follower %d, bcuz it's nextIdx is %d which <= last log idx %d\n",
				rf.me, peerId, rf.nextIndex[peerId], len(rf.logs)-1)
		}
		// 向peerId发送heartBeat
		go rf.leaderAppendEntry(peerId, sendHeartBeat)
	}
}

func (rf *Raft) leaderAppendEntry(peerId int, sendHeartBeat bool) {
	// 下面的代码是，如果已经不是candidate了，提前终止发送heartbeat，
	// 这是一种优化，但是需要获取两次锁
	rf.mu.Lock()
	// 获取锁后，如果不是leader了，就返回
	if rf.currentState != Leader {
		rf.mu.Unlock()
		DPrintf("peer %d is not leader any longer\n", rf.me)
		return
	}
	if sendHeartBeat {
		DPrintf("Leader %d start send heartbeat to follower %d\n", rf.me, peerId)
	} else {
		DPrintf("Leader %d start send appendEntries to follower %d\n", rf.me, peerId)
	}
	// Fig 2 Leaders:3
	// consistency check就是检查 prev_log_idx，也就是检查rf.nextIndex[peerId]记录的对不对
	// 如果不对，return false 那么leader就会decrement nextIndex[peerId]
	prev_log_idx := rf.nextIndex[peerId] - 1
	prev_log_term := rf.logs[prev_log_idx].Term
	record_n_log := len(rf.logs)
	args := AppendEntriesArgs{rf.currentTerm, rf.me, prev_log_idx, prev_log_term, []LogEntry{}, rf.commitIndex}
	reply := AppendEntriesReply{}
	// follower i 应该增加的条目是:从 nextIndex[peerId]开始，到last log index结束的所有logentry

	if !sendHeartBeat {
		args.Entries = append(args.Entries, rf.logs[rf.nextIndex[peerId]:]...)
		DPrintf("Leader %d fill in the sendEntry args for follower %d, prev_idx is %d\n", rf.me, peerId, prev_log_idx)
	} else {
		args.Entries = append(args.Entries, rf.logs[rf.nextIndex[peerId]:]...)
		DPrintf("Leader %d fill in the sendHeartbeat args for follower %d, prev_idx is %d\n", rf.me, peerId, prev_log_idx)
	}
	rf.mu.Unlock()
	// 发送rpc时候不能加锁
	succ := rf.sendAppendEntries(peerId, &args, &reply)

	rf.mu.Lock() // 打印要在拿到lock后再打印，这样更统一
	if !succ {
		if sendHeartBeat {
			DPrintf("Leader %d send heartbeat failed to follower %d\n", rf.me, peerId)
		} else {
			DPrintf("Leader %d send appendentry failed to follower %d\n", rf.me, peerId)
		}
		rf.mu.Unlock()
		return // 没有发送成功，那就直接返回？
		// log.Fatalf("Cannot send AppendEntries to %d\n", peerId)
	}

	// check term 转换身份了，需要提前终止，否则后面要进入if !Success的分支
	if reply.Term > rf.currentTerm {
		DPrintf("Leader %d's termIdx is smaller, switch to follower\n", rf.me)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.currentState = Follower
		// 一定要手动unlock
		rf.mu.Unlock()
		return
	}
	// check succ Fig 2 Leaders:4
	// 如果成功，说明完全同步了（任何不同步的log entry，以及所有之后的entry都删掉了
	if reply.Success {
		// success, update nextIndex and matchIndex for follower
		rf.nextIndex[peerId] = record_n_log      // 就是当前leader的log最大idx + 1
		rf.matchIndex[peerId] = record_n_log - 1 // 就是当前leader的log最大idx
		if sendHeartBeat {
			DPrintf("Leader %d update the follower %d via heartbeat, now it's nextIndex is %d\n", rf.me, peerId, rf.nextIndex[peerId])
		} else {
			DPrintf("Leader %d update the follower %d via appendEntry, now it's nextIndex is %d\n", rf.me, peerId, rf.nextIndex[peerId])
		}
		// 接下来更新commitIndex
		for N := len(rf.logs) - 1; N > rf.commitIndex; N-- {
			if rf.logs[N].Term != rf.currentTerm {
				continue // leader只能更新在自己任期内的log entry 到 commited状态？
			}
			cnt := 1
			for peer := 0; peer < len(rf.peers); peer++ {
				if rf.matchIndex[peer] >= N {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = N
				break // 因为是从大到小的，所以第一次找到最大的，就返回了
			}
		}
		// 更新完commitIndex之后，就更新lastApplied
		// 如果lastApplied 比 commitIndex 小
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.applyCh <- ApplyMsg{true, rf.logs[i].Command, i}
		}
		// 如果lastApplied 比 commitIndex小，等于commitIndex
		rf.lastApplied = max(rf.lastApplied, rf.commitIndex)
	} else {
		rf.nextIndex[peerId]--
		DPrintf("peer %d's nextIndex decrement to %d\n", peerId, rf.nextIndex[peerId])
		if rf.nextIndex[peerId] < 0 {
			log.Fatalf("peer %d nextIndex in leader %d is smaller than 0\n", peerId, rf.me)
		}
		DPrintf("peer %d append entry failed, retry\n", peerId)
		go rf.leaderAppendEntry(peerId, false)
	}
	rf.mu.Unlock()
}

func (rf *Raft) transferToLeader() {
	rf.votedFor = -1
	rf.currentState = Leader
	// reinitialize nextIndex[] and matchIndex[]
	// TODO reinitialize every idx in nextIndex[] to leader last log index + 1
	// DPrintf("rf.nextIndex is: %d\n", len(rf.logs))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.logs) //如果leader有0条，就是1，有1条，就是2
	}
	// initialize matchIndex[] to 0
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.logs) - 1 // 自己的matchIndex初始化为目前最大的index
	rf.leaderAppendEntries(true)
	rf.heartbeatTimer.Reset(rf.StableHeartbeatTimeout()) //首次发送完后，立即reset时间
}
