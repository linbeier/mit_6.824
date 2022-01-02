package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

const (
	Leader    int = 0
	Candidate     = 1
	Follower      = 2
)

type LogEntry struct {
	Term    int
	Command string
	Key     string
	Value   string
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate term
	CandidateId  int //candidate Id
	LastLogIndex int //index of candidate's last log entry
	LastLogTerm  int //term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //current term, for candidate to update itself
	VoteGranted bool //True means candidate receives vote
}

type AppendEtryArgs struct {
	Term         int        //leader's term
	LeaderId     int        // for followers to redirect client's request
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader's commit index
}

type AppendEtryReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) ResetTimeOut() {
	if rf.electTimer == nil {
		rf.electTimer = time.NewTimer(time.Duration((rand.Intn(200))+150) * time.Millisecond)
	} else {
		rf.electTimer.Reset(time.Duration((rand.Intn(200))+150) * time.Millisecond)
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
