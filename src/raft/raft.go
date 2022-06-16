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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

// TODO: keep it empty for now for 2a
type Data struct {
}

type AppendEntryRequest struct {
	Term     int
	LeaderID int
	Data     *Data
}

type AppendEntryResponse struct {
	Term int
}

type RequestVoteRequest struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteResponse struct {
	// Your data here (2A).
	// TODO: why reply need to have a term? Why need to update candidate's term?
	Term      int
	GrantVote bool
}

type RaftState string

const (
	LEADER    RaftState = "leader"
	FOLLOWER  RaftState = "follower"
	CANDIDATE RaftState = "candidate"
)

const (
	// TODO: 1. change the timeout to pass the lab test
	// 2. do i have to change election timeout in every new term?
	MIN_ELECTION_TIMEOUT = 300
	MAX_ELECTION_TIMEOUT = 500
	HEARTBEAT_INTERVAL   = 100 * time.Millisecond // 0.1 s
)

// TODO: encapsulate all shared data here.
type LockedData struct {
	mu sync.Mutex
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	// TODO: group locked data together in a struct and put mu there.
	mu    sync.Mutex          // Lock to protect shared access to this peer's state
	peers []*labrpc.ClientEnd // RPC end points of all peers
	// Wha does this field do?
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]
	leader    int        // index of leader into peers[]
	dead      int32      // set by Kill()
	state     RaftState

	lastHeartbeatTime time.Time
	electionTimeout   time.Duration // election time out is unique in every peer within min and max

	// 	for each server, index of the next log entry
	// to send to that server (initialized to leader
	// last log index + 1)
	nextIndex []int

	// for each server, index of highest log entry
	// known to be replicated on server
	// (initialized to 0, increases monotonically)
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm     int
	votedFor        int
	votesReceived   int
	voteTimeElapsed time.Duration
	voteResponses   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// TODO: confirm whether an uninitialized array is nil.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm

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

//
// example RequestVoteHandler RPC handler.
//
// TODO: change term to req term and convert to follower if req's term > currentTerm.
func (rf *Raft) RequestVoteHandler(req *RequestVoteRequest, resp *RequestVoteResponse) {
	resp.GrantVote = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	resp.Term = rf.currentTerm

	// the peer that send request is out of date and need to update term.
	if req.Term < rf.currentTerm {
		fmt.Printf("Peer %v outdated, new term: %v, peer term: %v\n ", req.CandidateID, rf.currentTerm, req.Term)
		return
	}

	// TODO: should i skip voting or vote imediately vote for this round?
	// if req.Term > rf.currentTerm {
	// 	rf.state = FOLLOWER
	// 	rf.currentTerm = req.Term
	// 	rf.votedFor = -1
	// 	return
	// }
	// TODO: should i have the second condition?
	if rf.votedFor == -1 || rf.votedFor == req.CandidateID {
		resp.GrantVote = true
		rf.votedFor = req.CandidateID
		fmt.Printf("I'm peer %v and i grand vote to %v \n", rf.me, req.CandidateID)
	}
	// Your code here (2A, 2B).
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
func (rf *Raft) sendRequestVote(server int, req *RequestVoteRequest, reply *RequestVoteResponse) bool {
	fmt.Printf("SendRequestVote")
	ok := rf.peers[server].Call("Raft.RequestVoteHandler", req, reply)
	return ok
}

//
// example RequestVoteHandler RPC handler.
//
func (rf *Raft) AppendEntryHandler(req *AppendEntryRequest, resp *AppendEntryResponse) {
	// Your code here (2A, 2B).
	if req.Data == nil {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		resp.Term = rf.currentTerm

		fmt.Printf("im %v %v and i got heartbeeat from %v, term : %v \n", rf.state, rf.me, req.LeaderID, req.Term)
		// the stale leader should then update term and convert to follower.
		if rf.currentTerm > req.Term {
			return
		}

		rf.lastHeartbeatTime = time.Now()

		// TODO: idealy the request term should only be current term or currentTerm + 1. Need to add that check later.
		// This peer is out of date. Update term and turn into follower.
		if rf.currentTerm < req.Term {
			rf.convertToState(FOLLOWER, req.Term)
		}

		return
	}

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
	fmt.Printf("peer %v is killed \n", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// fmt.Printf("me: %v\n", rf)
	// fmt.Printf("my election timeout: %v \n", rf.electionTimeout)
	for rf.killed() == false {
		var currentState RaftState
		var currentTerm int
		var votedFor int

		rf.mu.Lock()
		currentState = rf.state
		currentTerm = rf.currentTerm
		votedFor = rf.votedFor
		rf.mu.Unlock()

		switch currentState {
		case LEADER:
			// fmt.Printf("Im server %v and im leader ! \n", rf.me)
			cond := sync.NewCond(&rf.mu)
			for server, client := range rf.peers {
				if server == rf.me {
					continue
				}
				go func(client *labrpc.ClientEnd, server int) {
					for true {
						var isLeader bool
						rf.mu.Lock()
						isLeader = rf.state == LEADER
						rf.mu.Unlock()

						if !isLeader {
							return
						}

						fmt.Printf("im leader %v and im sending hearbeat to %v\n", rf.me, server)
						req := &AppendEntryRequest{
							Term:     currentTerm,
							LeaderID: rf.me,
							Data:     nil,
						}
						resp := &AppendEntryResponse{}
						ok := client.Call("Raft.AppendEntryHandler", req, resp)
						if !ok {
							fmt.Printf("leader %v failed to get heartbeat response from peer %v\n", rf.me, server)
						}
						// This means "me" is a stale leader, turns into a follower.
						if resp.Term > currentTerm {
							rf.mu.Lock()
							rf.convertToState(FOLLOWER, resp.Term)
							rf.mu.Unlock()
							return
						}
						time.Sleep(HEARTBEAT_INTERVAL)
					}

				}(client, server)
			}

			rf.mu.Lock()
			for rf.state == LEADER {
				cond.Wait()
			}
			rf.mu.Unlock()

		case FOLLOWER:
			now := time.Now()
			// fmt.Printf("peer %v going to sleep for election time out %v\n", rf.me, rf.electionTimeout)
			// TODO: change this into a gorountine.
			time.Sleep(rf.electionTimeout)
			rf.mu.Lock()

			if rf.lastHeartbeatTime.Before(now) {
				fmt.Printf("peer %v, Last heart beet: %v, now: %v\n", rf.me, rf.lastHeartbeatTime, now)

				rf.state = CANDIDATE
				rf.currentTerm++
				currentTerm = rf.currentTerm
				// TODO: Can't send Vote Request, since that needs to lock the mu, which is already locked above.
				// May need a better way to vote for self.
				if rf.votedFor == -1 {
					rf.votedFor = rf.me
				}
			}
			rf.mu.Unlock()

		case CANDIDATE:
			// TODO: only start one round of vote now.
			// fmt.Printf("peer %v is candidate now \n", rf.me)
			// var voteResponses []*RequestVoteResponse
			// voteComplete := make(chan string, 1)

			// start a gorountine to send votes and wait for rpc.
			// TODO: change to use one gorountine per vove, and boradcast when the votes are done.

			rf.mu.Lock()
			if votedFor == rf.me {
				rf.votesReceived++
			}
			rf.mu.Unlock()

			voteStartTime := time.Now()
			cond := sync.NewCond(&rf.mu)

			for server, client := range rf.peers {
				if server == rf.me {
					continue
				}
				go func(client *labrpc.ClientEnd, server int) {
					req := &RequestVoteRequest{
						Term:        currentTerm,
						CandidateID: rf.me,
						// TODO: add last log index here to check if the candidate's logs are update to date.
					}
					resp := &RequestVoteResponse{}
					ok := client.Call("Raft.RequestVoteHandler", req, resp)
					if !ok {
						fmt.Printf("peer %v failed to get vote responses from peer %v\n", rf.me, server)
						return
					}
					rf.mu.Lock()
					defer rf.mu.Unlock()

					// this peer already won the election, so don't need to proceed.
					if rf.state == LEADER {
						return
					}

					if resp.Term > currentTerm {
						rf.convertToState(FOLLOWER, resp.Term)
						return
					}
					if resp.GrantVote {
						rf.votesReceived++
					}
					rf.voteResponses++
					elaspedTime := time.Now().Sub(voteStartTime).Milliseconds()
					if elaspedTime > int64(rf.voteTimeElapsed) {
						rf.voteTimeElapsed = time.Duration(elaspedTime)
					}
					fmt.Printf("peer %v got %v responses and got %v votes \n", rf.me, rf.voteResponses, rf.votesReceived)
					cond.Broadcast()
				}(client, server)
			}

			rf.mu.Lock()
			// TODO: there's chance that this peer is already leader but some responses hasn't return,
			// since it's only waiting for the marjority not all. Need to deal with those responses as well.

			// TODO: may need to double check the order of these statements.
			for rf.votesReceived < len(rf.peers)/2+1 &&
				rf.voteResponses < len(rf.peers) &&
				rf.state == CANDIDATE &&
				rf.voteTimeElapsed < rf.electionTimeout {
				cond.Wait()
			}
			if rf.state != CANDIDATE {
				rf.mu.Unlock()
				continue
			} else if rf.votesReceived >= len(rf.peers)/2+1 {
				rf.convertToState(LEADER, rf.currentTerm)
			} else if rf.voteResponses == len(rf.peers)-1 || rf.voteTimeElapsed > rf.electionTimeout {
				rf.convertToState(CANDIDATE, rf.currentTerm+1)
				rf.votedFor = rf.me
			}
			rf.mu.Unlock()

			// TODO: start next round of votes.

		default:
			log.Fatalf("Invalid state")
		}
	}
}

// This mutation is done without lock, so the caller must lock before calling it.
func (rf *Raft) convertToState(state RaftState, term int) {
	fmt.Printf("peer %v convert from %v to %v\n", rf.me, rf.state, state)
	rf.currentTerm = term
	rf.votedFor = -1
	rf.voteResponses = 0
	rf.state = state
	rf.votesReceived = 0
	rf.lastHeartbeatTime = time.Now()
	rf.voteTimeElapsed = 0
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
	rf.currentTerm = 0
	rf.leader = -1
	rf.votedFor = -1
	rf.state = FOLLOWER
	rf.electionTimeout = getElectionTimeout()
	rf.lastHeartbeatTime = time.Now()

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	// Modify Make() to create a background goroutine that will kick off leader
	// election periodically by sending out RequestVote RPCs when it hasn't heard
	// from another peer for a while. This way a peer will learn who is the leader,
	// if there is already a leader, or become the leader itself.

	return rf
}

func getElectionTimeout() time.Duration {
	randInterval := (float32(MAX_ELECTION_TIMEOUT) - float32(MIN_ELECTION_TIMEOUT)) * rand.Float32()
	return time.Duration((MIN_ELECTION_TIMEOUT + int(randInterval))) * time.Millisecond
}
