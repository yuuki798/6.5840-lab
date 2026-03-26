package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// election segment
	currentTerm int // currentTerm is better to understood, but it's called "currentTerm" in the paper
	votedFor    int

	electionTimeout time.Duration
	lastResetTime   time.Time

	state int // Follower, Candidate, or Leader, means role, however "state" is more intuitive

	log         []LogEntry // this is the core data structure of Raft, the log entries; log[0] is a dummy entry to simplify indexing
	commitIndex int        // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int        // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	applyCh chan raftapi.ApplyMsg // it means the channel to send committed log entries to the service (or tester) on the same server, through which the tester checks correctness of Raft implementation.
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

func (rf *Raft) lastLogInfo() (int, int) {
	idx := len(rf.log) - 1
	return idx, rf.log[idx].Term
}

func (rf *Raft) resetElectionTimer() {
	rf.lastResetTime = time.Now()
	rf.electionTimeout = time.Duration(200+rand.Intn(200)) * time.Millisecond
}

// return currentTerm and whether this server believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

func (rf *Raft) persist() {
	// Your code here (3C).
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

// ==================== Apply ====================

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

// ==================== RequestVote ====================

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		reply.Term = rf.currentTerm
	}

	// Election restriction (Section 5.4.1)
	lastIdx, lastTerm := rf.lastLogInfo()
	logOk := args.LastLogTerm > lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIdx)

	if !logOk {
		return
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	rf.votedFor = args.CandidateId
	rf.resetElectionTimer()
	reply.VoteGranted = true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// ==================== AppendEntries ====================

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = 0

	if args.Term < rf.currentTerm {
		return
	}

	rf.resetElectionTimer()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		reply.Term = rf.currentTerm
	}
	rf.state = Follower

	// Log too short
	if args.PrevLogIndex >= len(rf.log) {
		reply.ConflictIndex = len(rf.log)
		return
	}

	// Term mismatch
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for idx > 0 && rf.log[idx-1].Term == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	// Append entries
	for i, entry := range args.Entries {
		pos := args.PrevLogIndex + 1 + i
		if pos < len(rf.log) {
			if rf.log[pos].Term != entry.Term {
				rf.log = rf.log[:pos]
				rf.log = append(rf.log, args.Entries[i:]...)
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
	}

	// Update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		ci := args.LeaderCommit
		if ci > len(rf.log)-1 {
			ci = len(rf.log) - 1
		}
		rf.commitIndex = ci
	}

	reply.Success = true
}

// ==================== Leader: replication ====================

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntriesHandler", args, reply)
	return ok
}

// replicateTo sends entries to one peer; returns true if state may have changed.
// Called without holding rf.mu.
func (rf *Raft) replicateTo(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	nextIdx := rf.nextIndex[server]
	prevIdx := nextIdx - 1
	prevTerm := rf.log[prevIdx].Term
	entries := make([]LogEntry, len(rf.log[nextIdx:]))
	copy(entries, rf.log[nextIdx:])
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	savedTerm := rf.currentTerm
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.resetElectionTimer()
		return
	}

	if rf.state != Leader || rf.currentTerm != savedTerm {
		return
	}

	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatch
			rf.nextIndex[server] = newMatch + 1
		}
		rf.advanceCommitIndex()
	} else {
		// Fast back-off using conflict info
		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			found := -1
			for i := len(rf.log) - 1; i > 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					found = i
					break
				}
			}
			if found >= 0 {
				rf.nextIndex[server] = found + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}
}

func (rf *Raft) advanceCommitIndex() {
	// Gather matchIndex, sort, pick median
	matches := make([]int, len(rf.peers))
	copy(matches, rf.matchIndex)
	sort.Ints(matches)
	// majority position: for 5 peers, index 2; for 3 peers, index 1
	majorityIdx := matches[len(rf.peers)/2]

	if majorityIdx > rf.commitIndex && rf.log[majorityIdx].Term == rf.currentTerm {
		rf.commitIndex = majorityIdx
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateTo(i)
	}
}

// ==================== Start ====================

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	term := rf.currentTerm
	rf.log = append(rf.log, LogEntry{Term: term, Command: command})
	index := len(rf.log) - 1
	rf.matchIndex[rf.me] = index
	// Replicate to followers immediately (non-blocking)
	go rf.broadcastAppendEntries()
	return index, term, true
}

// ==================== Election ====================

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.resetElectionTimer()
	term := rf.currentTerm
	lastIdx, lastTerm := rf.lastLogInfo()
	rf.mu.Unlock()

	votes := 1
	var mu sync.Mutex

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				rf.votedFor = -1
				rf.resetElectionTimer()
				return
			}

			if rf.state != Candidate || rf.currentTerm != term {
				return
			}

			if reply.VoteGranted {
				mu.Lock()
				votes++
				won := votes > len(rf.peers)/2
				mu.Unlock()
				if won {
					rf.becomeLeader()
				}
			}
		}(i)
	}
}

func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}
	rf.state = Leader
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1

	// Send initial heartbeats immediately (while holding lock, goroutines will acquire later)
	go rf.broadcastAppendEntries()
}

// ==================== Ticker ====================

func (rf *Raft) ticker() {
	for {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Leader {
			// Periodic heartbeat
			go rf.broadcastAppendEntries()
			rf.mu.Unlock()
			// Sleep additional ~90ms so heartbeats go out roughly every 100ms total
			time.Sleep(90 * time.Millisecond)
			continue
		}
		// Check election timeout
		if time.Since(rf.lastResetTime) > rf.electionTimeout {
			rf.mu.Unlock()
			rf.startElection()
			continue
		}
		rf.mu.Unlock()
	}
}

// ==================== Make ====================

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.readPersist(persister.ReadRaftState())
	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = 0
	rf.resetElectionTimer()

	rf.log = []LogEntry{{Term: 0}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	go rf.ticker()
	go rf.applier()

	return rf
}
