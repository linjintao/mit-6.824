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
  //  "bytes"
  "bytes"
  "fmt"
  "log"
  "math/rand"
  "sync"
  "sync/atomic"
  "time"

  //  "6.824/labgob"
  "6.824/labgob"
  "6.824/labrpc"
)

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

type Role int

const (
  Follower  Role = 0
  Candidate Role = 1
  Leader    Role = 2
)

const (
  ElectionTimeout = time.Millisecond * 150
  HeartbeatTimeout = time.Millisecond * 150
  MaxLockTime = time.Millisecond * 10
)

type LogEntry struct {
  Term int
  Idx int
  Command interface{}
}

func Max(x, y int) int {
  if x < y {
    return y
  }
  return x
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

  // Your data here (2A, 2B, 2C).
  // Look at the paper's Figure 2 for a description of what
  // state a Raft server must maintain.
  applyCh        chan ApplyMsg
  applyCond      *sync.Cond
  replicatorCond []*sync.Cond
  role          Role

  currentTerm    int
  votedFor       int
  logEntries     []LogEntry

  commitIndex       int

  lastApplied       int
  nextIndex         []int  //index of next log entry will be sent
  matchIndex        []int  //index of log has been replicated, used to determine if the log can be committed when majority requirement archieve

  electionTimer  *time.Timer
  heartbeatTimer *time.Timer

  lockName  string
  lockStart time.Time
  lockEnd   time.Time
  DebugLog  bool
}

func (rf *Raft) lock(m string) {
  rf.mu.Lock()
  rf.lockStart = time.Now()
  rf.lockName = m
}

func (rf *Raft) log(format string, a ...interface{}) {
  if rf.DebugLog == false {
    return
  }
  //term, idx := rf.lastLogTermIndex()
  r := fmt.Sprintf(format, a...)
  //s := fmt.Sprintf("gid:%d, me: %d, role:%v,term:%d, commitIdx: %v, snidx:%d, apply:%v, matchidx: %v, nextidx:%+v, lastlogterm:%d,idx:%d",
  //  rf.gid, rf.me, rf.role, rf.term, rf.commitIndex, rf.lastSnapshotIndex, rf.lastApplied, rf.matchIndex, rf.nextIndex, term, idx)
  log.Printf("log:%s\n", r)
}

func (rf *Raft) unlock(m string) {
  rf.lockEnd = time.Now()
  rf.lockName = ""
  duration := rf.lockEnd.Sub(rf.lockStart)

  if rf.lockName != "" && duration > MaxLockTime {
    rf.log("Lock too long:%s:%s:isKill:%v", m, duration, rf.killed())
  }
  rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

  // Your code here (2A).
  rf.lock("get state")
  defer rf.unlock("get state")

  return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) getPersistData() []byte {

  w := new(bytes.Buffer)
  e := labgob.NewEncoder(w)
  e.Encode(rf.currentTerm)
  e.Encode(rf.votedFor)
  e.Encode(rf.commitIndex)
  e.Encode(rf.lastApplied)
  lastIncludedIndex := rf.logEntries[0].Idx
  e.Encode(lastIncludedIndex)

  myName := "linjintao"
  e.Encode(myName)

  e.Encode(rf.logEntries)
  var name string
  if rf.role == Leader {
    name = "Leader"
  } else {
    name = "Follower"
  }
  rf.log("%v %v getPersist, currentTerm = %v, votedFor = %v, commitIndex = %v, lasstApplied = %v, logs = %v", 
         name, rf.me, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, rf.logEntries)
  data := w.Bytes()
  return data
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
  // Your code here (2C).
  var name string
  if rf.role == Leader {
    name = "Leader"
  } else {
    name = "Follower"
  }
  rf.log("%v %v do persisting", name, rf.me)
  data := rf.getPersistData()
  rf.persister.SaveRaftState(data)
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
  r := bytes.NewBuffer(data)
  d := labgob.NewDecoder(r)
  d.Decode(&rf.currentTerm)
  d.Decode(&rf.votedFor)
  d.Decode(&rf.commitIndex)
  d.Decode(&rf.lastApplied)
  var lastIncludedIndex int
  d.Decode(&lastIncludedIndex)
  rf.lastApplied = lastIncludedIndex


  var myName string
  d.Decode(&myName)

  d.Decode(&rf.logEntries)
  var name string
  if rf.role == Leader {
    name = "Leader"
  } else {
    name = "Follower"
  }
  rf.log("%v %v read persist, currentTerm = %v, votedFor = %v, commitIndex = %v, lastApplied = %v, myName = %v, logs = %v", name,
         rf.me, rf.currentTerm, rf.votedFor, rf.commitIndex, rf.lastApplied, myName, rf.logEntries)
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

type InstallSnapshotRequest struct {
  Term int
  LeaderId int
  LastIncludedIndex int
  LastIncludedTerm int
  Offset int
  Data []byte
  Done bool
}

type InstallSnapshotResponse struct {
  Term int
}

func (rf *Raft) genInstallSnapshotRequest(peer int) *InstallSnapshotRequest {

  rf.mu.Lock()
  defer rf.mu.Unlock()
  lastIncludedIndex, lastIncludedTerm := rf.getFirstLog().Idx, rf.getFirstLog().Term
  rf.log("Leader %v sending snapshot to %v lastIncludedTerm %v, lastIncludedIndex %v", rf.me, peer, lastIncludedTerm, lastIncludedIndex)

  return &InstallSnapshotRequest {
    Term: rf.currentTerm,
    LeaderId: rf.me,
    LastIncludedIndex: lastIncludedIndex,
    LastIncludedTerm: lastIncludedTerm,
    Offset: 0,
    Data: rf.persister.ReadSnapshot(),
    Done: true,
  }
}

func (rf *Raft) handleInstallSnapshot(peer int, request *InstallSnapshotRequest, response *InstallSnapshotResponse) {

  rf.mu.Lock()
  defer rf.mu.Unlock()
  if rf.currentTerm < response.Term {
    rf.currentTerm = response.Term
    rf.ChangeRole(Follower)
    rf.electionTimer.Reset(RandomizedElectionTimeOut())
  } else {
    rf.matchIndex[peer] = request.LastIncludedIndex
    rf.nextIndex[peer] = request.LastIncludedIndex + 1
  }
}

func (rf *Raft) InstallSnapshot(request *InstallSnapshotRequest, response *InstallSnapshotResponse) {

  rf.mu.Lock()
  defer rf.mu.Unlock()
  rf.log("Follower %v receive snapshot, lastIncludedTerm %v, lastIncludedIndex %v", rf.me, request.LastIncludedTerm, request.LastIncludedIndex)
  rf.log("Follower %v, currentTerm %v, commitIndex %v, lastApplied %v, logs %v", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logEntries)

  response.Term = rf.currentTerm

  if request.Term < rf.currentTerm {
    return
  }

  if request.Term > rf.currentTerm {
    rf.currentTerm, rf.votedFor = request.Term, -1
    rf.persist()
  }

  rf.ChangeRole(Follower)
  rf.electionTimer.Reset(RandomizedElectionTimeOut())

  if request.LastIncludedIndex <= rf.commitIndex || request.LastIncludedIndex <= rf.getFirstLog().Idx {
    return
  }

  //rf.logEntries[0].Idx = request.LastIncludedIndex
  //rf.logEntries[0].Term = request.LastIncludedTerm
  //rf.persister.SaveStateAndSnapshot(rf.getPersistData(), request.Data)

  //rf.lastApplied = request.LastIncludedIndex
  //rf.commitIndex = request.LastIncludedIndex
  go func() {
    rf.applyCh <- ApplyMsg{
      SnapshotValid: true,
      Snapshot: request.Data,
      SnapshotTerm: request.LastIncludedTerm,
      SnapshotIndex: request.LastIncludedIndex,
    }
  }()
}

func shrinkEntriesArray(logEntries []LogEntry) []LogEntry {

  return append([]LogEntry{}, logEntries...)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

  // Your code here (2D).

  rf.log("%v %v receive request CondInstallSnapshot lastIncludedTerm %v, lastIncludedIndex %v, log %v", rf.GetRoleName(), rf.me, lastIncludedTerm, lastIncludedIndex, rf.logEntries)
  rf.log("%v %v, before CondInstallSnapshot currentTerm %v, commitIndex %v, lastApplied %v, logs %v", rf.GetRoleName(), rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logEntries)

  rf.mu.Lock()
  defer rf.mu.Unlock()

  if lastIncludedIndex <= rf.commitIndex {
    return false
  }

  if lastIncludedIndex > rf.getLastLog().Idx {
    rf.logEntries = make([]LogEntry, 1)
  } else {
    //newLogEntries := make([]LogEntry, 1)
    //newLogEntries = append(newLogEntries, rf.logEntries[lastIncludedIndex - rf.getFirstLog().Idx:]...)
    rf.log("%v %v key log %v", rf.GetRoleName(), rf.me, rf.logEntries[lastIncludedIndex - rf.getFirstLog().Idx])
    rf.logEntries = shrinkEntriesArray(rf.logEntries[lastIncludedIndex - rf.getFirstLog().Idx:])
    rf.log("%v %v after shrinking logEntriesArray %v", rf.GetRoleName(), rf.me, rf.logEntries)
    rf.logEntries[0].Command = nil
  }

  rf.logEntries[0].Term = lastIncludedTerm
  rf.logEntries[0].Idx = lastIncludedIndex
  rf.lastApplied = Max(lastIncludedIndex, rf.lastApplied)
  rf.commitIndex = Max(lastIncludedIndex, rf.commitIndex)

  rf.log("%v %v after shrinking logEntries %v", rf.GetRoleName(), rf.me, rf.logEntries)
  rf.log("%v %v, after CondInstallSnapshot currentTerm %v, commitIndex %v, lastApplied %v, logs %v", rf.GetRoleName(), rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logEntries)

  rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)

  return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
  // Your code here (2D).
  rf.mu.Lock()
  defer rf.mu.Unlock()

  rf.log("%v %v snapshoting index %v, commitIndex %v, lastApplied %v, log %v", rf.GetRoleName(), rf.me, index, rf.commitIndex, rf.lastApplied, rf.logEntries)
  snapshotIndex := rf.getFirstLog().Idx
  if index <=snapshotIndex || index > rf.commitIndex {
    return
  }

  //index = rf.commitIndex
  remainLogs := rf.logEntries[index - snapshotIndex + 1:]
  lastIncludedIndex := rf.logEntries[index - snapshotIndex].Idx
  lastIncludedTerm:= rf.logEntries[index - snapshotIndex].Term

  rf.lastApplied = index
  rf.logEntries = make([]LogEntry, 1)
  rf.logEntries[0].Idx = lastIncludedIndex
  rf.logEntries[0].Term = lastIncludedTerm
  rf.logEntries = append(rf.logEntries, remainLogs...)
  rf.log("%v %v commitIndex %v, lastApplied %v, after snapshot shrinking log %v", rf.GetRoleName(), rf.me, rf.commitIndex, rf.lastApplied, rf.logEntries)
  rf.persister.SaveStateAndSnapshot(rf.getPersistData(), snapshot)
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
  // Your data here (2A, 2B).
  Term         int
  CandidateId  int
  LastLogIndex int
  LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
  // Your data here (2A).
  Term        int
  VoteGranted bool
}


type RequestAppendEntriesArgs struct {
  Term         int
  LeaderId     int
  PrevLogIndex int
  PrevLogTerm  int
  Entries      []LogEntry
  LeaderCommit int
}

type RequestAppendEntriesReply struct {
  Term    int
  Success bool
}

func (rf *Raft) lastLogTermIndex() (int, int) {
  term := rf.logEntries[len(rf.logEntries) - 1].Term
  index := rf.logEntries[len(rf.logEntries) - 1].Idx
  return term, index
}


func (rf *Raft) isLogUpToDate(lastLogTerm int, lastLogIndex int) bool {

  lastLog := rf.getLastLog()
  if lastLog.Term < lastLogTerm {
    return true
  } else if lastLog.Term == lastLogTerm {
    if lastLog.Idx <= lastLogIndex {
      return true
    }
  }

  return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
  // Your code here (2A, 2B).
  rf.mu.Lock()
  defer rf.mu.Unlock()
  defer rf.persist()
  reply.Term = rf.currentTerm;
  reply.VoteGranted = false;

  rf.log("%v %v, currentTerm %v received voting request from %v, term %v", rf.GetRoleName(), rf.me, rf.currentTerm, args.CandidateId, args.Term)

  if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
    return
  }

  if args.Term > rf.currentTerm {
    rf.ChangeRole(Follower)
    rf.currentTerm, rf.votedFor = args.Term, -1
  }

  // check if requester log is more update to date
  if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
    reply.VoteGranted = false
    return
  }

  rf.currentTerm, rf.votedFor = args.Term, args.CandidateId
  rf.ChangeRole(Follower)
  reply.VoteGranted = true;
  rf.log("%v %v grant vote to %v", rf.GetRoleName(), rf.me, rf.votedFor)
  rf.electionTimer.Reset(RandomizedElectionTimeOut())
}

func (rf *Raft) AppendEntries(request *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {

  rf.mu.Lock()
  defer rf.mu.Unlock()
  defer rf.persist()

  rf.log("Follower %v received Leader %v, term %v,request logs %v", rf.me, request.LeaderId, request.Term, request.Entries)
  rf.log("Follower %v currentTerm %v, commitIndex %v, lastApplied %v, logs %v", rf.me, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.logEntries)

  // save a copy of currentTerm
  curTerm := rf.currentTerm
  reply.Term = curTerm
  
  // Rules for Servers - All Servers - rule 2
  if request.Term > rf.currentTerm {
    rf.currentTerm = request.Term
    rf.ChangeRole(Follower)
  }

  // implementation note 1
  if request.Term < curTerm {
    reply.Success = false
    return
  }
  // implementation note 2
  lastLog := rf.getLastLog();
  if request.PrevLogIndex > lastLog.Idx {
    reply.Success = false
    return
  }

  rf.ChangeRole(Follower)
  rf.electionTimer.Reset(RandomizedElectionTimeOut())

  i := len(rf.logEntries) - 1
  for ; i >= 0; i-- {
    if rf.logEntries[i].Idx == request.PrevLogIndex {
      break
    }
  }

  if i < 0 {
    firstLog := rf.getFirstLog()
    rf.log("Follower %v firstLog %v, lastLog %v, received request PrevLogIndex %v", rf.me, firstLog, lastLog, request.PrevLogIndex) 

    if firstLog.Idx > request.PrevLogIndex  {
      if len(request.Entries) > 0 && request.Entries[len(request.Entries) - 1].Idx > lastLog.Idx {
        lenToAppended := request.Entries[len(request.Entries) - 1].Idx - lastLog.Idx
        logsToAppended := request.Entries[len(request.Entries) - lenToAppended + 1:]
        rf.logEntries = append(rf.logEntries, logsToAppended...)
      }
    } else {

      reply.Success = false
      return
    }
  } else {

    // implementation note 3 & 4
    // delete confict and append new
    if len(rf.logEntries) > 0 {
      rf.logEntries = rf.logEntries[:i + 1]
      rf.logEntries = append(rf.logEntries, request.Entries...)
    }
  }
  lastIdx := rf.getLastLog().Idx
  rf.log("Follower %v after AppendEntries logs %v", rf.me, rf.logEntries)
  if request.LeaderCommit > rf.commitIndex {
    if lastIdx < request.LeaderCommit {
      rf.commitIndex = lastIdx
    } else {
      rf.commitIndex = request.LeaderCommit
    }

    rf.applyCond.Signal()
  }
  reply.Success = true
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
  return ok
}

func (rf *Raft) sendAppendEntries(server int, args *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) bool {
  ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
  return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotRequest, reply *InstallSnapshotResponse) bool {
  ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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
  isLeader := true

  // Your code here (2B).
  rf.mu.Lock()
  defer rf.mu.Unlock()

  if rf.role != Leader {
    return -1, -1, false
  }
  
  _, lastIndex := rf.lastLogTermIndex()
  index = lastIndex + 1;

  rf.logEntries = append(rf.logEntries,
    LogEntry{
      Term: rf.currentTerm,
      Command: command,
      Idx: index,
    })
  rf.BrocastHeartbeat(false)
  
  rf.log("Leader %v committing one command %v", rf.me, command) 
  return index, rf.currentTerm, isLeader
}

func (rf *Raft) BrocastHeartbeat(isHeartBeat bool) {

  for peer := range rf.peers {
    if peer == rf.me {
      continue
    }
    if isHeartBeat {
      rf.log("Leader %v sending heartbeat to %v", rf.me, peer)
      go rf.replicateOneRound(peer)
    } else {
      rf.replicatorCond[peer].Signal()
    }
  }
}

// generate append entries rpc request which include log starting from prevLogIndex
func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *RequestAppendEntriesArgs {

  rf.mu.Lock()
  rf.mu.Unlock()
  prevLogTerm := 0
  i := 0
  for i = len(rf.logEntries) - 1; i > 0; i-- {
    if rf.logEntries[i - 1].Idx == prevLogIndex {
      prevLogTerm = rf.logEntries[i].Term
      break
    }
  }

  var logsCopied []LogEntry
  if prevLogTerm != 0 {
    logs := rf.logEntries[i:]
    logsCopied = append(logsCopied, logs...)
  } 

  return &RequestAppendEntriesArgs {
    Term:         rf.currentTerm,
    LeaderId:     rf.me,
    PrevLogIndex: prevLogIndex,
    PrevLogTerm:  prevLogTerm,
    LeaderCommit: rf.commitIndex,
    Entries:      logsCopied,
  }
}

func (rf *Raft) replicateOneRound(peer int) {

  if rf.role != Leader {
    return
  }
  defer rf.persist()

  rf.mu.Lock()
  prevLogIndex := rf.nextIndex[peer] - 1
  rf.mu.Unlock()


  if prevLogIndex < rf.getFirstLog().Idx {
    rf.log("Leader %v send snapshot to Follower %v, prevLogIndex %v, firstIndex %v, leader logs = %v", rf.me, peer, prevLogIndex, rf.getFirstLog().Idx, rf.logEntries)
    snapshotRequest := rf.genInstallSnapshotRequest(peer)
    snapshotResponse := new(InstallSnapshotResponse)
    if rf.sendInstallSnapshot(peer, snapshotRequest, snapshotResponse) {
      rf.handleInstallSnapshot(peer, snapshotRequest, snapshotResponse)
    }
  } else {

    request := rf.genAppendEntriesRequest(prevLogIndex)
    rf.log("Leader %v sending Follower %v, log len = %v prevLogIndex = %v, LeaderCommit = %v, leader logs = %v", rf.me, peer, len(request.Entries), prevLogIndex, request.LeaderCommit, rf.logEntries)
    reply := &RequestAppendEntriesReply{}
    if rf.sendAppendEntries(peer, request, reply) {
      rf.handleAppendAppendEntriesRespone(peer, request, reply)
    }
  }
}

func (rf *Raft) handleAppendAppendEntriesRespone(peer int, request *RequestAppendEntriesArgs, reply *RequestAppendEntriesReply) {

  rf.mu.Lock()
  defer rf.mu.Unlock()

  if reply.Term > rf.currentTerm {
    rf.ChangeRole(Follower)
    return
  }

  rf.log("Leader %v handling Follower %v reply %v len %v", rf.me, peer, reply.Success, len(request.Entries))

  if reply.Success == false {
    rf.nextIndex[peer] -= 1
    return
  }

  if reply.Success  && len(request.Entries) > 0 {

    newMatchIndex := request.PrevLogIndex + len(request.Entries)
    newNextIndex := newMatchIndex + 1

    rf.log("Leader %v update Follower %v matchIndex %v --> %v", rf.me, peer, rf.matchIndex[peer], newMatchIndex)
    rf.log("Leader %v update Follower %v nextIndex %v --> %v", rf.me, peer, rf.nextIndex[peer], newNextIndex)


    rf.matchIndex[peer] = newMatchIndex
    rf.nextIndex[peer] = newNextIndex
  }

  for logLen := len(rf.logEntries); logLen >= 1; logLen-- {
    logIndex := rf.logEntries[logLen - 1].Idx
    //logTerm := rf.logEntries[logLen - 1].Term
    if logIndex <= rf.commitIndex {
      break
    }

    count := 1
    //if logTerm == rf.currentTerm {
      for i := 0; i < len(rf.peers); i++ {
        if i != rf.me && rf.matchIndex[i] >= logIndex {
          count += 1
        }
      }
    //}

    if count > len(rf.peers) / 2 {
      rf.commitIndex = logIndex
      rf.applyCond.Signal()
      break
    }
  }
}

func (rf *Raft) replicator(peer int) {
  rf.replicatorCond[peer].L.Lock()
  defer rf.replicatorCond[peer].L.Unlock()

  for rf.killed() == false {

    for !rf.needReplicating(peer) {
      rf.replicatorCond[peer].Wait()
    }
    rf.heartbeatTimer.Reset(StableHeartbeatTimeOut())
    rf.electionTimer.Reset(RandomizedElectionTimeOut())
    rf.replicateOneRound(peer)
  }

}

func (rf *Raft) needReplicating(peer int) bool {
  rf.mu.Lock()
  defer rf.mu.Unlock()
  return rf.role == Leader && rf.matchIndex[peer] < rf.getLastLog().Idx
}

func (rf *Raft) getLastLog() *LogEntry {
  size := len(rf.logEntries)
  return &(rf.logEntries[size - 1])
}

func (rf *Raft) getFirstLog() *LogEntry {
  return &(rf.logEntries[0])
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

func (rf *Raft) ChangeRole(role Role) {
  rf.role = role
}

func (rf *Raft) GetRoleName() string {
  if rf.role == Leader {
    return "Leader"
  } else {
    return "Follower"
  }
}

func (rf *Raft) StartElection() {
  lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
  request := RequestVoteArgs {
    Term:         rf.currentTerm,
    CandidateId:  rf.me,
    LastLogTerm:  lastLogTerm,
    LastLogIndex: lastLogIndex,
  }
  grantedVotes := 1
  rf.votedFor = rf.me
  rf.persist()

  for peer := range rf.peers {
    if peer == rf.me {
      continue
    }

    go func(peer int) {
      response := RequestVoteReply{}
      if rf.sendRequestVote(peer, &request, &response) {
        rf.mu.Lock()
        defer rf.mu.Unlock();
        // currentTerm may have changed, means the election is finished 
        if rf.currentTerm == request.Term && rf.role == Candidate {
          if response.VoteGranted {
            grantedVotes += 1
            if grantedVotes > len(rf.peers) / 2 {
              rf.ChangeRole(Leader)
              rf.BrocastHeartbeat(true)
              rf.log("New leader %v", rf.me)
            }
          } else if response.Term > rf.currentTerm {
            rf.ChangeRole(Follower)
            rf.currentTerm, rf.votedFor = response.Term, -1
            rf.persist()
          }
        }
      }

    }(peer)
  }
}

func (rf *Raft) applier() {

  for rf.killed() == false {
    rf.mu.Lock()
    for rf.lastApplied >= rf.commitIndex {
      rf.applyCond.Wait()
    }
    var name string
    if rf.role == Leader {
      name = "Leader"
    } else {
      name = "Follower"
    }
    rf.log("%v %v, applier with commitIndex %v, lastApplied %v, logs %v", name, rf.me, rf.commitIndex, rf.lastApplied, rf.logEntries)

    firstIndex, commitIndex, lastApplied := rf.getFirstLog().Idx, rf.commitIndex, rf.lastApplied
    entries := make([]LogEntry, commitIndex - lastApplied)
    copy(entries, rf.logEntries[lastApplied + 1 - firstIndex: commitIndex + 1 - firstIndex])
    rf.mu.Unlock()

    rf.log("%v %v appling logs %v", name, rf.me, entries)
    for _, entry := range entries {
      rf.applyCh <- ApplyMsg {
        CommandValid : true,
        Command: entry.Command,
        CommandIndex: entry.Idx,
      }
    }
    rf.mu.Lock()
    rf.lastApplied = Max(rf.lastApplied, commitIndex)
    rf.persist()
    rf.mu.Unlock()
  }
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

  // Your code here to check if a leader election should
  // be started and to randomize sleeping time using
  // time.Sleep().
  for rf.killed() == false {
    select {
    case <- rf.electionTimer.C:
      rf.mu.Lock()
      if rf.role == Leader {
        rf.electionTimer.Reset(RandomizedElectionTimeOut())
        rf.mu.Unlock()
        continue
      }
      rf.ChangeRole(Candidate)
      rf.currentTerm += 1
      rf.StartElection()
      rf.log("Start election, rf %v, term %v", rf.me, rf.currentTerm)
      rf.electionTimer.Reset(RandomizedElectionTimeOut())
      rf.mu.Unlock()
    case <- rf.heartbeatTimer.C:
      rf.mu.Lock()
      if rf.role == Leader {
        rf.log("Leader %v heartbeat timeout, term = %v, commitIndex %v, log last index = %v", rf.me, rf.currentTerm, rf.commitIndex, rf.getLastLog().Idx)
        rf.BrocastHeartbeat(true)
      }
      rf.heartbeatTimer.Reset(StableHeartbeatTimeOut())
      rf.mu.Unlock()
    }
  }
}

func RandomizedElectionTimeOut() time.Duration {
  r := time.Duration(rand.Int63()) % ElectionTimeout
  return ElectionTimeout + r
}


func StableHeartbeatTimeOut() time.Duration {
  return HeartbeatTimeout
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
  rf := &Raft{
    peers:          peers,
    persister:      persister,
    me:             me,
    dead:           0,
    applyCh:        applyCh,
    replicatorCond: make([]*sync.Cond, len(peers)),
    role:           Follower,
    currentTerm:    0,
    votedFor:       -1,
    logEntries:     make([]LogEntry, 1),
    lastApplied:    0,
    nextIndex:      make([]int, len(peers)),
    matchIndex:     make([]int, len(peers)),
    heartbeatTimer: time.NewTimer(StableHeartbeatTimeOut()),
    electionTimer:  time.NewTimer(RandomizedElectionTimeOut()),
    DebugLog:       false,
  }
  // Your initialization code here (2A, 2B, 2C).

  // initialize from state persisted before a crash
  rf.readPersist(persister.ReadRaftState())
  rf.applyCond = sync.NewCond(&rf.mu)
  
  lastLog := rf.getLastLog()

  for i := 0; i < len(peers); i++ {
    rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Idx + 1
    if i != rf.me {
      rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
      go rf.replicator(i)
    }
  }

  // start ticker goroutine to start elections
  go rf.ticker()
  go rf.applier()

  return rf
}
