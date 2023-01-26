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

	"math/rand"
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
	currentState uint32 //current state of this server
	currentTerm  uint32 //last term server has seen ----------- is integer enough for Term?
	votedFor     int    //candidateId that receives vote in current term
	// newTerm      bool
	log []LogEntry //log entires; each entry contains command for state machines and term when entry was received by leader

	//variables about log states
	commitIndex int //index of highest log entry to be commited
	lastApplied int //index of highest log entry applied by state machine

	//leader feature, reinitialize after election
	nextIndex  []int //for each server, index of the next log entry to send to that server
	matchIndex []int //for each server, index of highest log entry known to be replicated on server

	electTimer *time.Timer

	revAppendEntry chan bool
	revRequestVote chan bool
	//convertFollower chan int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term uint32
	var isleader bool
	// Your code here (2A).
	term = rf.getTerm()
	isleader = rf.getState() == Leader
	return int(term), isleader
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
	term := rf.getTerm()
	if args.Term < term {
		reply.Term = term
		reply.Success = false
	} else {
		rf.setTerm(args.Term)
		reply.Term = term
		reply.Success = true
		if rf.getState() != Follower {
			//follower will reset timer
			rf.setState(Follower)
		}
		rf.revAppendEntry <- true
	}
	logrus.WithFields(logrus.Fields{
		"me":      rf.me,
		"term":    term,
		"request": *args,
		"reply":   *reply,
	}).Info("AppendEntry Received")
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	term := rf.getTerm()
	if args.Term >= term {
		rf.votedFor = args.CandidateId
		rf.setTerm(args.Term)
		if rf.currentState != Follower {
			rf.setState(Follower)
		}
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.revRequestVote <- true
	} else {
		reply.Term = term
		reply.VoteGranted = false
	}
	logrus.WithFields(logrus.Fields{
		"me":      rf.me,
		"term":    term,
		"request": *args,
		"reply":   *reply,
	}).Info("RequestVote Received")

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
	//logrus.WithFields(logrus.Fields{
	//	"me":       rf.me,
	//	"received": ok,
	//	"server":   server,
	//	"request":  *args,
	//	"reply":    *reply,
	//}).Info("send RequestVote")

	if reply.Term > rf.getTerm() {
		//rf.convertFollower <- reply.Term
		rf.setState(Follower)
		rf.setTerm(reply.Term)
	}
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEtryArgs, reply *AppendEtryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	//logrus.WithFields(logrus.Fields{
	//	"me":       rf.me,
	//	"received": ok,
	//	"server":   server,
	//	"request":  *args,
	//	"reply":    *reply,
	//}).Info("send AppendEntry")
	if reply.Term > rf.getTerm() {
		//rf.convertFollower <- reply.Term
		rf.setState(Follower)
		rf.setTerm(reply.Term)
	}
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
	for rf.killed() == false && rf.getState() == Follower {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.revAppendEntry:
			rf.ResetTimeOut()
		case <-rf.revRequestVote:
			rf.ResetTimeOut()
		case <-rf.electTimer.C:
			//rf.electTimer.Stop()
			go rf.Candidate()
			return
		}

	}
}

func (rf *Raft) startElect(VoteNum *int, mutex *sync.Mutex) {

	rf.incTerm()
	rf.votedFor = rf.me

	rf.ResetTimeOut()

	for i := range rf.peers {
		if i != rf.me {
			go func(i int) {
				term := rf.getTerm()
				reply := &RequestVoteReply{}
				args := &RequestVoteArgs{
					Term:         term,
					CandidateId:  rf.me,
					LastLogIndex: rf.lastApplied, //not sure, still confused
					LastLogTerm:  0,              //not sure
				}
				ok := rf.sendRequestVote(i, args, reply)
				if !ok {
					logrus.WithFields(logrus.Fields{
						"me":           rf.me,
						"argsTerm":     args.Term,
						"CandidateId":  args.CandidateId,
						"LastLogIndex": args.LastLogIndex,
						"LastLogTerm":  args.LastLogTerm,
					}).Warn("No respnse in sendRequestVote!")
					return
				}

				if reply.VoteGranted == true {
					mutex.Lock()
					*VoteNum++
					// only one routine will send to convert leader channel
					if *VoteNum > len(rf.peers)/2 && rf.getState() == Candidate {
						rf.setState(Leader)
					}
					mutex.Unlock()
				}
			}(i)
		}
	}
}

//when candidate receives AppendEntry RPC, it convert to follower
func (rf *Raft) Candidate() {
	rf.setState(Candidate)

	term := rf.getTerm()
	logrus.WithFields(logrus.Fields{
		"me":   rf.me,
		"Term": term,
	}).Info("Follower timeout, transit to candidate")

	VoteNum := 1

	var mutex sync.Mutex

	rf.startElect(&VoteNum, &mutex)

	for rf.killed() == false && rf.getState() == Candidate {
		select {
		case <-rf.revAppendEntry:
			logrus.WithFields(logrus.Fields{
				"me":   rf.me,
				"term": rf.getTerm(),
			}).Info("Received AppendEntry")
		case <-rf.revRequestVote:

		case <-rf.electTimer.C:
			//re-elect when timeout
			logrus.WithFields(logrus.Fields{
				"me":      rf.me,
				"term":    rf.getTerm(),
				"voteNum": VoteNum,
			}).Info("Election timeout, re-elect")

			VoteNum = 1
			rf.startElect(&VoteNum, &mutex)

			//case <-rf.convertFollower:
			//	go rf.Follower()
			//
			//	rf.mu.Lock()
			//	mutex.Lock()
			//	logrus.WithFields(logrus.Fields{
			//		"me":      rf.me,
			//		"term":    rf.currentTerm,
			//		"voteNum": VoteNum,
			//	}).Info("Transit to follower")
			//	mutex.Unlock()
			//	rf.mu.Unlock()
			//	return
			//case <-convertLeader:
			//	go rf.Leader()
			//
			//	mutex.Lock()
			//	rf.mu.Lock()
			//	logrus.WithFields(logrus.Fields{
			//		"me":      rf.me,
			//		"term":    rf.currentTerm,
			//		"voteNum": VoteNum,
			//	}).Info("Transit to Leader")
			//	rf.mu.Unlock()
			//	mutex.Unlock()
			//	return
		}
	}

	switch rf.getState() {
	case Leader:
		rf.Leader()
	case Candidate:
		rf.Candidate()
	case Follower:
		rf.Follower()
	}
}

func (rf *Raft) Follower() {
	rf.setState(Follower)

	rf.ResetTimeOut()
	for rf.killed() == false && rf.getState() == Follower {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.revAppendEntry:
			rf.ResetTimeOut()
		case <-rf.revRequestVote:
			rf.ResetTimeOut()
		case <-rf.electTimer.C:
			rf.setState(Candidate)
			break
		}
	}

	switch rf.getState() {
	case Leader:
		rf.Leader()
	case Candidate:
		rf.Candidate()
	case Follower:
		rf.Follower()
	}
}

func (rf *Raft) Leader() {

	rf.setState(Leader)

	tiker := time.NewTicker(200 * time.Millisecond)
	defer tiker.Stop()

	args := &AppendEtryArgs{
		Term:     rf.getTerm(),
		LeaderId: rf.me,
	}
	reply := &AppendEtryReply{}

	for rf.killed() == false && rf.getState() == Leader {
		select {
		case <-tiker.C:
			for i, _ := range rf.peers {
				if i != rf.me {
					go func(i int, args *AppendEtryArgs, reply *AppendEtryReply) {
						ok := rf.sendAppendEntry(i, args, reply)
						if !ok {
							logrus.WithFields(logrus.Fields{
								"to":       i,
								"Term":     args.Term,
								"LeaderId": args.LeaderId,
							}).Warn("Heart Beat no Response")
						} else {
							logrus.WithFields(logrus.Fields{
								"to":       i,
								"Term":     args.Term,
								"LeaderId": args.LeaderId,
							}).Info("Heart Beat Response:", *reply)
						}
					}(i, args, reply)
				}
			}
		case <-rf.revAppendEntry:
			//rf.mu.Lock()
			//logrus.WithFields(logrus.Fields{
			//	"me":   rf.me,
			//	"term": rf.currentTerm,
			//}).Info("Received AppendEntry")
			//rf.mu.Unlock()
		case <-rf.revRequestVote:
			//
			//case <-rf.convertFollower:
			//	rf.mu.Lock()
			//	logrus.WithFields(logrus.Fields{
			//		"me":    rf.me,
			//		"State": rf.currentState,
			//		"term":  rf.currentTerm,
			//	}).Info("Transit to follower")
			//	rf.mu.Unlock()
			//	go rf.Follower()
			//	return
		}
	}
	switch rf.getState() {
	case Leader:
		rf.Leader()
	case Candidate:
		rf.Candidate()
	case Follower:
		rf.Follower()
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
	rand.Seed(time.Now().UnixNano())

	// Your initialization code here (2A, 2B, 2C).
	rf.currentState = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	// rf.newTerm = false

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.revAppendEntry = make(chan bool)
	rf.revRequestVote = make(chan bool)

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
