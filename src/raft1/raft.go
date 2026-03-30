package raft

import (
	"bytes"
	"math/rand"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *tester.Persister
	me        int

	currentTerm int
	votedFor    int

	electionTimeout time.Duration
	lastResetTime   time.Time

	state int

	log         []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	applyCh chan raftapi.ApplyMsg

	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte

	pendingSnapshot      []byte
	pendingSnapshotIndex int
	pendingSnapshotTerm  int
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

func (rf *Raft) lastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) lastLogInfo() (int, int) {
	idx := rf.lastLogIndex()
	return idx, rf.log[len(rf.log)-1].Term
}

func (rf *Raft) resetElectionTimer() {
	rf.lastResetTime = time.Now()
	rf.electionTimeout = time.Duration(200+rand.Intn(200)) * time.Millisecond
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// ==================== Persist ====================

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
}

func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// ==================== Snapshot ====================

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.lastLogIndex() {
		return
	}

	phys := index - rf.lastIncludedIndex
	rf.lastIncludedTerm = rf.log[phys].Term

	newLog := make([]LogEntry, len(rf.log)-phys)
	copy(newLog, rf.log[phys:])
	newLog[0].Command = nil
	rf.log = newLog

	rf.lastIncludedIndex = index
	rf.snapshot = snapshot
	rf.persist()
}

// ==================== InstallSnapshot RPC ====================

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = Follower
	rf.resetElectionTimer()
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.persist()
		return
	}

	lastIdx := rf.lastLogIndex()
	if args.LastIncludedIndex < lastIdx {
		phys := args.LastIncludedIndex - rf.lastIncludedIndex
		if rf.log[phys].Term == args.LastIncludedTerm {
			newLog := make([]LogEntry, len(rf.log)-phys)
			copy(newLog, rf.log[phys:])
			newLog[0].Command = nil
			rf.log = newLog
		} else {
			rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
		}
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm}}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapshot = args.Data

	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	if args.LastIncludedIndex > rf.lastApplied {
		rf.lastApplied = args.LastIncludedIndex
		rf.pendingSnapshot = args.Data
		rf.pendingSnapshotIndex = args.LastIncludedIndex
		rf.pendingSnapshotTerm = args.LastIncludedTerm
	}

	rf.persist()
}

// ==================== Apply ====================

func (rf *Raft) applier() {
	for {
		rf.mu.Lock()

		if rf.pendingSnapshot != nil {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.pendingSnapshot,
				SnapshotTerm:  rf.pendingSnapshotTerm,
				SnapshotIndex: rf.pendingSnapshotIndex,
			}
			rf.pendingSnapshot = nil
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
		}

		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			phys := rf.lastApplied - rf.lastIncludedIndex
			if phys < 1 || phys >= len(rf.log) {
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
				continue
			}
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[phys].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			rf.applyCh <- msg
			continue
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
		rf.persist()
		rf.state = Follower
		reply.Term = rf.currentTerm
	}

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
	rf.persist()
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
		rf.persist()
		reply.Term = rf.currentTerm
	}
	rf.state = Follower

	if args.PrevLogIndex < rf.lastIncludedIndex {
		skipCount := rf.lastIncludedIndex - args.PrevLogIndex
		if skipCount >= len(args.Entries) {
			reply.Success = true
			if args.LeaderCommit > rf.commitIndex {
				ci := args.LeaderCommit
				lastIdx := rf.lastLogIndex()
				if ci > lastIdx {
					ci = lastIdx
				}
				rf.commitIndex = ci
			}
			return
		}
		args.Entries = args.Entries[skipCount:]
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
	}

	lastIdx := rf.lastLogIndex()

	if args.PrevLogIndex > lastIdx {
		reply.ConflictIndex = lastIdx + 1
		return
	}

	phys := args.PrevLogIndex - rf.lastIncludedIndex
	if rf.log[phys].Term != args.PrevLogTerm {
		reply.ConflictTerm = rf.log[phys].Term
		idx := args.PrevLogIndex
		for idx > rf.lastIncludedIndex && rf.log[idx-rf.lastIncludedIndex-1].Term == reply.ConflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	for i, entry := range args.Entries {
		pos := args.PrevLogIndex + 1 + i
		physPos := pos - rf.lastIncludedIndex
		if physPos < len(rf.log) {
			if rf.log[physPos].Term != entry.Term {
				rf.log = rf.log[:physPos]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		ci := args.LeaderCommit
		newLastIdx := rf.lastLogIndex()
		if ci > newLastIdx {
			ci = newLastIdx
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

func (rf *Raft) replicateTo(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	nextIdx := rf.nextIndex[server]

	if nextIdx <= rf.lastIncludedIndex {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshot,
		}
		savedTerm := rf.currentTerm
		rf.mu.Unlock()

		reply := &InstallSnapshotReply{}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = Follower
			rf.votedFor = -1
			rf.persist()
			rf.resetElectionTimer()
			return
		}

		if rf.state != Leader || rf.currentTerm != savedTerm {
			return
		}

		rf.nextIndex[server] = args.LastIncludedIndex + 1
		rf.matchIndex[server] = args.LastIncludedIndex
		return
	}

	prevIdx := nextIdx - 1
	prevPhys := prevIdx - rf.lastIncludedIndex
	prevTerm := rf.log[prevPhys].Term
	nextPhys := nextIdx - rf.lastIncludedIndex
	entries := make([]LogEntry, len(rf.log[nextPhys:]))
	copy(entries, rf.log[nextPhys:])
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
		rf.persist()
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
		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			found := -1
			for i := len(rf.log) - 1; i > 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					found = i + rf.lastIncludedIndex
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
	matches := make([]int, len(rf.peers))
	copy(matches, rf.matchIndex)
	sort.Ints(matches)
	majorityIdx := matches[len(rf.peers)/2]

	phys := majorityIdx - rf.lastIncludedIndex
	if majorityIdx > rf.commitIndex && phys > 0 && phys < len(rf.log) && rf.log[phys].Term == rf.currentTerm {
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
	rf.persist()
	index := rf.lastLogIndex()
	rf.matchIndex[rf.me] = index
	go rf.broadcastAppendEntries()
	return index, term, true
}

// ==================== Election ====================

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.persist()
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
				rf.persist()
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
	lastIdx := rf.lastLogIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = lastIdx + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = lastIdx

	go rf.broadcastAppendEntries()
}

// ==================== Ticker ====================

func (rf *Raft) ticker() {
	for {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()
		if rf.state == Leader {
			go rf.broadcastAppendEntries()
			rf.mu.Unlock()
			time.Sleep(90 * time.Millisecond)
			continue
		}
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

	rf.votedFor = -1
	rf.state = Follower
	rf.currentTerm = 0
	rf.log = []LogEntry{{Term: 0}}
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()

	rf.resetElectionTimer()
	rf.commitIndex = rf.lastIncludedIndex
	rf.lastApplied = rf.lastIncludedIndex

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	go rf.ticker()
	go rf.applier()

	return rf
}
