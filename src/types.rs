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

impl Default for Config {
    fn default() -> Self {
        Config {
            election_timeout_max: 20,
            election_timeout_min: 10,
            heartbeat_interval: 3
        }
    }
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

impl RaftNode {
    pub fn new(id: NodeId, peers: Vec<NodeId>, config: Config, seed: u64) -> Self {
        let mut rng = StdRng::seed_from_u64(seed);
        let election_timeout = rng.random_range(config.election_timeout_min..=config.election_timeout_max);

        RaftNode { 
            id, 
            peers, 
            config, 
            current_term: Term(0), 
            voted_for: None, 
            log: Vec::new(), 
            commit_index: LogIndex(0), 
            last_applied: LogIndex(0), 
            role: Role::Follower, 
            election_elapsed: 0, 
            election_timeout, 
            heartbeat_elapsed: 0, 
            rng
        }

        
    }

    fn last_log_index(&self) -> LogIndex {
        self.commit_index + 1
    }

    fn last_log_term(&self) -> Term  {
        self.current_term
    }


    fn random_election_timeout(&mut self) -> u64 {
        self.rng.random_range(self.config.election_timeout_min..=self.config.election_timeout_max)
    }
}





#[cfg(test)]
mod tests {
    use super::*;

    fn node_with_log(terms: &[u64]) -> RaftNode {
        let mut n = RaftNode::new(NodeId(1), vec![NodeId(2), NodeId(3)], Config::default(), 42);
        for &t in terms {
            n.log.push(LogEntry { term: Term(t), command: vec![] });
        }
        n
    }

    #[test]
    fn new_node_starts_as_follower() { /* check role, term, empty log */ }

    #[test]
    fn up_to_date_check() {
        let n = node_with_log(&[1, 1, 2, 3, 3]); // my log: 5 entries, last term 3
        // case 1: last term 4, any length  -> true
        // case 2: last term 3, length 4    -> false
        // case 3: last term 3, length 5    -> true
        // case 4: last term 2, length 100  -> false
    }
}