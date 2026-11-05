use std::collections::{HashMap, HashSet};

use rand::{RngExt, SeedableRng, rngs::StdRng};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Term(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct LogIndex(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct NodeId(pub u64);

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub term : Term,
    pub command: Vec<u8> // This is the actual content of the LOG as raw bytes
}

#[derive(Debug, Clone, PartialEq)]
pub enum Role {
    Follower,
    Candidate { 
        voters: HashSet<NodeId> // We choose the set because no single YES would be stored twice in a SET
    },
    Leader {
        next_index: HashMap<NodeId,LogIndex>,
        match_index: HashMap<NodeId,LogIndex> // This will be used to count how many nodes has a particular entry, so that it can decide to commit it or not
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    RequestVote {
        term: Term,
        candidate_id:NodeId,
        last_log_index: LogIndex,
        last_log_term: Term
    },

    RequestVoteReply {
        term: Term,
        vote_granted:bool
    },

    AppendEntries {
        term:Term,
        leader_id:NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    },

    AppendEntriesReply {
        term: Term,
        success: bool,
        match_index: LogIndex,
    },
}


#[derive(Debug, Clone, PartialEq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub msg: Message
}


#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub election_timeout_min: u64,
    pub election_timeout_max: u64,
    pub heartbeat_interval: u64
}




pub struct RaftNode {
    pub id: NodeId,
    pub peers: Vec<NodeId>, // everyone else except this node
    pub config: Config,

    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    pub log: Vec<LogEntry>,

    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub role: Role,

    pub election_elapsed: u64,     
    pub election_timeout: u64,     
    pub heartbeat_elapsed: u64,

    rng: StdRng,   
}



