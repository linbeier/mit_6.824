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
	//	"bytes"

	"sync"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"

	"github.com/sirupsen/logrus"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	//persistent state, update before responding to RPCs
	currentState int //current state of this server
	currentTerm  int //last term server has seen ----------- is integer enough for Term?
	votedFor     int //candidateId that receives vote in current term
	// newTerm      bool
	log []LogEntry //log entires; each entry contains command for state machines and term when entry was received by leader

	//variables about log states
	commitIndex int //index of highest log entry to be commited
	lastApplied int //index of highest log entry applied by state machine

	//leader feature, reinitialize after election
	nextIndex  []int //for each server, index of the next log entry to send to that server
	matchIndex []int //for each server, index of highest log entry known to be replicated on server

	electTimer *time.Timer

	revAppendEntry  chan int
	revRequestVote  chan int
	convertFollower chan int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.currentState == Leader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) AppendEntry(args *AppendEtryArgs, reply *AppendEtryReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else if args.Term == rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = true
		rf.revAppendEntry <- args.Term
	} else {
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.Success = true
		if rf.currentState != Follower {
			rf.convertFollower <- args.Term
		}
		rf.revAppendEntry <- args.Term
	}
	logrus.WithFields(logrus.Fields{
		"term":    rf.currentTerm,
		"me":      rf.me,
		"request": *args,
		"reply":   *reply,
	}).Info("AppendEntry Received")
	rf.mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		if rf.currentState != Follower {
			rf.convertFollower <- args.Term
		}
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else if args.Term == rf.currentTerm && rf.votedFor == args.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	rf.mu.Unlock()
	logrus.WithFields(logrus.Fields{
		"term":    rf.currentTerm,
		"me":      rf.me,
		"request": *args,
		"reply":   *reply,
	}).Info("RequestVote Received")
	rf.revRequestVote <- args.Term
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	logrus.WithFields(logrus.Fields{
		"received": ok,
		"server":   server,
		"request":  *args,
		"reply":    *reply,
	}).Info("send RequestVote")
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.convertFollower <- reply.Term
	}
	rf.mu.Unlock()
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEtryArgs, reply *AppendEtryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	logrus.WithFields(logrus.Fields{
		"received": ok,
		"server":   server,
		"request":  *args,
		"reply":    *reply,
	}).Info("send AppendEntry")
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.convertFollower <- reply.Term
	}
	rf.mu.Unlock()
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rf.ResetTimeOut()
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.revAppendEntry:
			rf.ResetTimeOut()
		case <-rf.revRequestVote:
			rf.ResetTimeOut()
		case <-rf.electTimer.C:
			rf.electTimer.Stop()
			go rf.Candidate()
			return
		}

	}
}

//when candidate receives AppendEntry RPC, it convert to follower
func (rf *Raft) Candidate() {
	rf.mu.Lock()
	rf.currentState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	logrus.WithFields(logrus.Fields{
		"Term": rf.currentTerm,
		"me":   rf.me,
	}).Info("Follower timeout, transit to candidate")
	rf.mu.Unlock()

	VoteNum := 1
	convertLeader := make(chan bool)
	// toFollower := make(chan bool)

	var mutex sync.Mutex

	rf.ResetTimeOut()

	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				rf.mu.Lock()
				reply := &RequestVoteReply{}
				args := &RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastApplied, //not sure, still confused
					LastLogTerm:  0,              //not sure
				}
				rf.mu.Unlock()
				ok := rf.sendRequestVote(i, args, reply)
				if !ok {
					logrus.WithFields(logrus.Fields{
						"Term":         args.Term,
						"CandidateId":  args.CandidateId,
						"LastLogIndex": args.LastLogIndex,
						"LastLogTerm":  args.LastLogTerm,
					}).Warn("No respnse in sendRequestVote!")
				}
				// if reply.Term > rf.currentTerm {
				// 	toFollower <- true
				// 	return
				// }
				if reply.VoteGranted == true {
					mutex.Lock()
					VoteNum++
					if VoteNum > len(rf.peers)/2 {
						convertLeader <- true
					}
					mutex.Unlock()
				}
			}(i)
		}
	}

	for rf.killed() == false {
		select {
		case <-rf.revAppendEntry:
			mutex.Lock()
			rf.mu.Lock()
			logrus.WithFields(logrus.Fields{
				"me":      rf.me,
				"term":    rf.currentTerm,
				"voteNum": VoteNum,
			}).Info("Transit to follower")
			rf.mu.Unlock()
			mutex.Unlock()
			go rf.Follower()
			return
		case <-rf.electTimer.C:
			//re-elect when timeout
			mutex.Lock()
			rf.mu.Lock()
			logrus.WithFields(logrus.Fields{
				"me":      rf.me,
				"term":    rf.currentTerm,
				"voteNum": VoteNum,
			}).Info("Election timeout, re-elect")
			rf.mu.Unlock()
			mutex.Unlock()
			go rf.Candidate()
			return
		case <-rf.convertFollower:
			rf.mu.Lock()
			mutex.Lock()
			logrus.WithFields(logrus.Fields{
				"me":      rf.me,
				"term":    rf.currentTerm,
				"voteNum": VoteNum,
			}).Info("Transit to follower")
			mutex.Unlock()
			rf.mu.Unlock()
			go rf.Follower()
			return
		case <-convertLeader:
			mutex.Lock()
			rf.mu.Lock()
			logrus.WithFields(logrus.Fields{
				"me":      rf.me,
				"term":    rf.currentTerm,
				"voteNum": VoteNum,
			}).Info("Transit to Leader")
			rf.mu.Unlock()
			mutex.Unlock()
			go rf.Leader()
			return
		}
	}
}

func (rf *Raft) Follower() {
	rf.mu.Lock()
	rf.currentState = Follower
	// rf.newTerm = false
	rf.mu.Unlock()

	go rf.ticker()
}

func (rf *Raft) Leader() {
	rf.mu.Lock()
	rf.currentState = Leader
	rf.mu.Unlock()

	var mutex sync.Mutex
	close := false
	quit := make(chan bool)
	go func(close *bool, quit chan bool) {
		for {
			select {
			case <-rf.convertFollower:
				mutex.Lock()
				rf.mu.Lock()
				*close = true
				logrus.WithFields(logrus.Fields{
					"me":    rf.me,
					"State": rf.currentState,
					"term":  rf.currentTerm,
				}).Info("Transit to follower")
				go rf.Follower()
				rf.mu.Unlock()
				mutex.Unlock()
				return
			// case <-rf.revRequestVote:
			// 	mutex.Lock()
			// 	rf.mu.Lock()
			// 	if rf.newTerm {
			// 		*close = true
			// 		logrus.WithFields(logrus.Fields{
			// 			"me":    rf.me,
			// 			"State": rf.currentState,
			// 			"term":  rf.currentTerm,
			// 		}).Info("Transit to follower")
			// 		go rf.Follower()
			// 		rf.mu.Unlock()
			// 		mutex.Unlock()
			// 		return
			// 	}
			// 	rf.mu.Unlock()
			// 	mutex.Unlock()
			case <-quit:
				return
			}

		}
	}(&close, quit)

	defer func() {
		quit <- true
	}()
	//Heartbeat
	for rf.killed() == false {
		mutex.Lock()
		if close {
			mutex.Unlock()
			return
		}
		mutex.Unlock()

		rf.mu.Lock()
		args := &AppendEtryArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
		}
		rf.mu.Unlock()
		reply := &AppendEtryReply{}

		for i, _ := range rf.peers {
			if i != rf.me {
				ok := rf.sendAppendEntry(i, args, reply)
				if !ok {
					logrus.WithFields(logrus.Fields{
						"Term":     args.Term,
						"LeaderId": args.LeaderId,
					}).Warn("Heart Beat no Response")
				} else {
					logrus.WithFields(logrus.Fields{
						"Term":     args.Term,
						"LeaderId": args.LeaderId,
					}).Info("Heart Beat Response:", *reply)
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

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
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	// rf.newTerm = false

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.revAppendEntry = make(chan int)
	rf.revRequestVote = make(chan int)
	rf.convertFollower = make(chan int)

	// logfile, _ := os.Open("log")
	// println(ok)
	// logrus.SetOutput(logfile)
	// defer logfile.Close()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// // start ticker goroutine to start elections
	// go rf.ticker()
	go rf.Follower()

	return rf
}
