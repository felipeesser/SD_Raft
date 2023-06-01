package raft

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// Tempos de eleicao //
const (
	DefaultElectionTimeoutMin   = 250
	DefaultElectionTimeoutRange = 150
	DefaultHeartbeatInterval    = 50
	DefaultChannelBufferSize    = 20
)

type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const(
	LEADER=1
	FOLLOWER=2
	CANDIDATE=3
)

type LogEntry struct {
	LogIndex   int
	LogTerm    int
	LogCommand interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	
	state			int
	votes int
	requestVoteReplied chan bool
	winner chan bool
	appendEntriesRec chan bool
	commandApplied chan ApplyMsg

	currentTerm int
	votedFor int 
	
}
type AppendEntriesReply struct {
	Term int
	Success bool

}
type AppendEntriesArgs struct {
	Term int
	LeaderId int
}

func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool= rf.state==LEADER
	term= rf.currentTerm
	return term, isleader
}

// func (rf *Raft) persist() {
// }
// func (rf *Raft) readPersist(data []byte) {
// 	if data == nil || len(data) < 1 { // bootstrap without any state?
// 		return
// 	}
// }

type RequestVoteArgs struct {
	Term			int
	CandidateId		int 
}

type RequestVoteReply struct {
	Term			int
	VoteGranted		bool
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//servidor destinatário tem term maior que remetente 
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}else if args.Term > rf.currentTerm {//servidor destinatário tem term menor que remetente
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.requestVoteReplied <- true
	}else {//servidor destinatário tem term igual ao remetente
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	
}

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {//atualiza servidor remetente com term menor que destinatário
			rf.currentTerm = reply.Term
			fmt.Printf("%d deixou de ser %d\n", rf.me,rf.state)
			rf.state= FOLLOWER
			rf.votedFor = -1
			rf.votes=0
			return ok

		}
		if rf.state == CANDIDATE {
			if reply.VoteGranted {
				rf.votes++
				if rf.votes > (len(rf.peers) / 2){
					rf.winner <- true
				}
			}

		} 
		return ok
	}
	return ok
}
func (rf *Raft) bcastRequestVote() {
	var args RequestVoteArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	rf.mu.Unlock()

	for i := range rf.peers {
		if i != rf.me && rf.state == CANDIDATE {
			go func(i int) {
				var reply RequestVoteReply
				rf.sendRequestVote(i, args, &reply)
			}(i)
		}
	}
}
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.appendEntriesRec <- true
	if args.Term > rf.currentTerm {
		fmt.Printf("%d deixou de ser %d\n", rf.me,rf.state)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1 // resetar pois tem novo termo //
	}
	reply.Term = args.Term
}


func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.state = FOLLOWER
			rf.votedFor = -1
			return ok
		}
		}
	return ok
}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

func (rf *Raft) Kill() {
}

func (rf *Raft) actionFollower(){
	electionTimeout := rand.Intn(DefaultElectionTimeoutRange) + DefaultElectionTimeoutMin

	select {
	case <- time.After(time.Duration(electionTimeout) * time.Millisecond)://não recebeu heartbeats ou não votou em determinado intervalo
		rf.state = CANDIDATE
	case <-rf.requestVoteReplied://votou
	case <-rf.appendEntriesRec://recebeu heartbeat
	
	}

}
func (rf *Raft) actionCandidate(){
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votes = 1
	fmt.Printf("%d se candidatou com %d votos\n", rf.me,rf.votes)
	rf.mu.Unlock()


	go func() {

		var args RequestVoteArgs

		rf.mu.Lock()
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		rf.mu.Unlock()

		for i := range rf.peers {
			if i != rf.me {
				var reply RequestVoteReply
				go rf.sendRequestVote(i, args, &reply)
		
			}
		}
	}()

	electionTimeout := rand.Intn(DefaultElectionTimeoutRange) + DefaultElectionTimeoutMin
	//(a) it wins the election, (b) another server establishes itself as leader, or (c) a period of time goes by with no winner.
	select {
		case <-rf.winner:
			rf.mu.Lock()
			fmt.Printf("%d ganhou com %d votos\n", rf.me,rf.votes)
			rf.votes=0
			rf.state = LEADER
			rf.mu.Unlock()
		case <-rf.appendEntriesRec:
			rf.state = FOLLOWER
		case <- time.After(time.Duration(electionTimeout) * time.Millisecond):
	}

}
func (rf *Raft) actionLeader(){
	
	for i := range rf.peers {
		if i != rf.me {
			var args AppendEntriesArgs
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			var reply AppendEntriesReply
			go rf.sendAppendEntries(i, args, &reply)

		}
	}
	time.Sleep(DefaultHeartbeatInterval * time.Millisecond)

}


func (rf *Raft) doLoop() {
	for {
		switch rf.state {
		case FOLLOWER:
			rf.actionFollower()
		case CANDIDATE:
			rf.actionCandidate()
		case LEADER:
			rf.actionLeader()
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.state=FOLLOWER
	rf.currentTerm=0
	rf.votedFor = -1
	rf.votes = 0
	
	rf.appendEntriesRec = make(chan bool, DefaultChannelBufferSize)
	rf.requestVoteReplied = make(chan bool, DefaultChannelBufferSize)
	rf.winner = make(chan bool, DefaultChannelBufferSize)

	rf.commandApplied = applyCh



	go rf.doLoop()
	
	return rf
}