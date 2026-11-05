use std::collections::HashSet;

use rand::{RngExt, SeedableRng, rngs::StdRng};

use crate::types::{Config, Envelope, LogEntry, LogIndex, Message, NodeId, RaftNode, Role, Term};

pub mod types;

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote { term, .. }
            | Message::RequestVoteReply { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::AppendEntriesReply { term, .. } => *term,
        }
    }
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


    fn send(&self, to: NodeId, msg: Message) -> Envelope {
        Envelope { from: self.id, to, msg }
    }


    fn append_entries_for(&self, peer: NodeId) -> Envelope {
        let last = self.last_log_index().0;
        let next = match &self.role {
            Role::Leader { next_index, .. } => next_index.get(&peer).map(|i| i.0).unwrap_or(last + 1),
            _ => last + 1,
        };
        let next = next.clamp(1, last + 1);

        let prev_log_index = LogIndex(next - 1);
        let prev_log_term = self.term_at(prev_log_index).unwrap_or(Term(0));
        let entries = self.log[(next as usize - 1)..].to_vec();

        self.send(peer, Message::AppendEntries {
            term: self.current_term,
            leader_id: self.id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit: self.commit_index,
        })
    }


    fn broadcast_append_entries(&self) -> Vec<Envelope> {
        self.peers.iter().map(|&p| self.append_entries_for(p)).collect()
    }

    pub fn start_election(&mut self) -> Vec<Envelope> {
        self.current_term = Term(self.current_term.0 + 1);
        self.voted_for = Some(self.id);

        let mut voters = HashSet::new();
        voters.insert(self.id);
        self.role = Role::Candidate { voters };

        self.election_elapsed = 0;
        self.election_timeout = self.random_election_timeout();

        // Single-node cluster: my own vote is already a majority
        if self.majority() == 1 {
            return self.become_leader();
        }

        self.peers
            .iter()
            .map(|&peer| Envelope {
                from: self.id,
                to: peer,
                msg: Message::RequestVote {
                    term: self.current_term,
                    candidate_id: self.id,
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
                },
            })
            .collect()
    }

    fn become_leader(&mut self) -> Vec<Envelope> {
        let next = LogIndex(self.last_log_index().0 + 1);
        let next_index = self.peers.iter().map(|&p| (p, next)).collect();
        let match_index = self.peers.iter().map(|&p| (p, LogIndex(0))).collect();

        self.role = Role::Leader { next_index, match_index };
        self.heartbeat_elapsed = 0;
        self.broadcast_append_entries()
    }


    pub fn handle_message(
    &mut self,
    from: NodeId,
    msg: Message
) -> Vec<Envelope> {

    // Check whether the incoming message has a higher term.
    let incoming_term = msg.term();

    if incoming_term > self.current_term {
        self.become_follower(incoming_term);
    }

    match msg {

        Message::RequestVote {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        } => {
            self.handle_request_vote(
                from,
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            )
        }

        Message::RequestVoteReply {
            term,
            vote_granted,
        } => {
            self.handle_request_vote_reply(
                from,
                term,
                vote_granted,
            )
        }

        Message::AppendEntries {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        } => {
            self.handle_append_entries(
                from,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            )
        }

        Message::AppendEntriesReply {
            term,
            success,
            match_index,
        } => {
            self.handle_append_entries_reply(
                from,
                term,
                success,
                match_index,
            )
        }
    }

}

    fn handle_append_entries_reply(
    &mut self,
    from: NodeId,
    term: Term,
    success: bool,
    match_index: LogIndex,
) -> Vec<Envelope> {

    // Ignore stale responses.
    if term != self.current_term {
        return Vec::new();
    }

    // Only a leader should process replication responses.
    if !matches!(self.role, Role::Leader { .. }) {
        return Vec::new();
    }

    if success {

        if let Role::Leader {
            next_index,
            match_index: replicated,
        } = &mut self.role {

            let matched = replicated.entry(from)
                .or_insert(LogIndex(0));

            // Replication progress must never move backwards.
            *matched = LogIndex(
                matched.0.max(match_index.0)
            );

            next_index.insert(
                from,
                LogIndex(matched.0 + 1)
            );
        }

        Vec::new()

    } else {

        if let Role::Leader {
            next_index,
            ..
        } = &mut self.role {

            if let Some(next) = next_index.get_mut(&from) {
                next.0 = next.0.saturating_sub(1).max(1);
            }
        }

        // Retry with the previous log position.
        vec![self.append_entries_for(from)]
    }
}

    fn handle_request_vote_reply(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
    ) -> Vec<Envelope> {

        // Ignore responses from older terms.
        if term != self.current_term {
            return Vec::new();
        }

        // We only count votes while we're a candidate.
        if !matches!(self.role, Role::Candidate { .. }) {
            return Vec::new();
        }

        if !vote_granted {
            return Vec::new();
        }

        let required_votes = self.majority();

        let won_election = match &mut self.role {

            Role::Candidate { voters } => {
                voters.insert(from);

                voters.len() >= required_votes
            }

            _ => false,
        };

        if won_election {
            return self.become_leader();
        }

        Vec::new()
    }


    fn handle_append_entries(
        &mut self,
        from: NodeId,
        term: Term,
        _leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: LogIndex,
    ) -> Vec<Envelope> {

        // 1. Reject requests from older terms.
        if term < self.current_term {
            return vec![
                self.send(
                    from,
                    Message::AppendEntriesReply {
                        term: self.current_term,
                        success: false,
                        match_index: LogIndex(0),
                    }
                )
            ];
        }

        // 2. Recognize the leader.
        if !matches!(self.role, Role::Follower) {
            self.become_follower(term);
        }

        self.election_elapsed = 0;

        // 3. Verify the preceding log entry.
        if self.term_at(prev_log_index) != Some(prev_log_term) {
            return vec![
                self.send(
                    from,
                    Message::AppendEntriesReply {
                        term: self.current_term,
                        success: false,
                        match_index: LogIndex(0),
                    }
                )
            ];
        }

        // 4. Resolve conflicting entries and append missing entries.
        for (offset, entry) in entries.iter().enumerate() {

            let index = LogIndex(
                prev_log_index.0 + offset as u64 + 1
            );

            if index <= self.last_log_index() {

                if self.term_at(index) != Some(entry.term) {

                    // Remove the conflicting entry and everything after it.
                    self.log.truncate(index.0 as usize - 1);

                    // Append the new entries from this position onward.
                    self.log.extend(entries[offset..].iter().cloned());

                    break;
                }

            } else {
                self.log.push(entry.clone());
            }
        }

        // 5. Update commit index.
        if leader_commit > self.commit_index {

            self.commit_index = LogIndex(
                leader_commit.0.min(self.last_log_index().0)
            );
        }

        // 6. Respond successfully.
        let match_index = LogIndex(
            prev_log_index.0 + entries.len() as u64
        );

        vec![
            self.send(
                from,
                Message::AppendEntriesReply {
                    term: self.current_term,
                    success: true,
                    match_index,
                }
            )
        ]
    }
    fn handle_request_vote(
        &mut self,
        from: NodeId,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    ) -> Vec<Envelope> {

        let mut vote_granted = false;
        
        if term >= self.current_term {
            let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate_id);
            let log_is_up_to_date =
            self.candidate_log_is_up_to_date(
                last_log_term,
                last_log_index,
            );

            if can_vote && log_is_up_to_date {
                self.voted_for = Some(candidate_id);

                self.election_elapsed = 0;
                self.election_timeout = self.random_election_timeout();

                vote_granted = true;
            }

        }

        vec![
            self.send(
                from,
                Message::RequestVoteReply {
                    term: self.current_term,
                    vote_granted,
                },
            )
    ]
        
    }
    pub fn tick(&mut self) -> Vec<Envelope> {
        if matches!(self.role, Role::Leader { .. }) {
            self.heartbeat_elapsed += 1;
            if self.heartbeat_elapsed >= self.config.heartbeat_interval {
                self.heartbeat_elapsed = 0;
                return self.broadcast_append_entries();
            }
        } else {
            self.election_elapsed += 1;
            if self.election_elapsed >= self.election_timeout {
                return self.start_election();
            }
        }
        Vec::new()
    }

    fn majority(&self) -> usize {
        let cluster_size = self.peers.len() + 1;
        cluster_size / 2 + 1
    }

    fn become_follower(&mut self, term: Term) {
        self.current_term = term;
        self.voted_for = None;          // new term, so I haven't voted in it yet
        self.role = Role::Follower;
        self.election_elapsed = 0;
        self.election_timeout = self.random_election_timeout();
    }

    fn last_log_index(&self) -> LogIndex {
        LogIndex(self.log.len() as u64)
    }

    fn last_log_term(&self) -> Term  {
        self.log.last().map(|e| e.term).unwrap_or(Term(0))
    }

    fn term_at(&self, index: LogIndex) -> Option<Term> {
        if index.0 == 0{
            return Some(Term(0));
        }
        self.log.get(index.0 as usize - 1).map(|e| e.term)
    }

    fn candidate_log_is_up_to_date(&self, last_term: Term, last_index: LogIndex) -> bool {
        (last_term, last_index) >= (self.last_log_term(), self.last_log_index())
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
fn new_node_starts_as_follower() {
    let n = node_with_log(&[]);
    assert_eq!(n.role, Role::Follower);
    assert_eq!(n.current_term, Term(0));
    assert_eq!(n.voted_for, None);
    assert!(n.log.is_empty());
    assert!(n.election_timeout >= 10 && n.election_timeout <= 20);
}

#[test]
fn same_seed_gives_same_timeout() {
    let a = RaftNode::new(NodeId(1), vec![], Config::default(), 7);
    let b = RaftNode::new(NodeId(1), vec![], Config::default(), 7);
    assert_eq!(a.election_timeout, b.election_timeout);
}

#[test]
fn log_helpers() {
    let n = node_with_log(&[1, 1, 2]);
    assert_eq!(n.last_log_index(), LogIndex(3));
    assert_eq!(n.last_log_term(), Term(2));
    assert_eq!(n.term_at(LogIndex(0)), Some(Term(0)));
    assert_eq!(n.term_at(LogIndex(3)), Some(Term(2)));
    assert_eq!(n.term_at(LogIndex(4)), None);

    let empty = node_with_log(&[]);
    assert_eq!(empty.last_log_index(), LogIndex(0));
    assert_eq!(empty.last_log_term(), Term(0));
}

#[test]
fn up_to_date_check() {
    let n = node_with_log(&[1, 1, 2, 3, 3]); // 5 entries, last term 3
    assert!(n.candidate_log_is_up_to_date(Term(4), LogIndex(1)));    // higher last term
    assert!(!n.candidate_log_is_up_to_date(Term(3), LogIndex(4)));   // same term, shorter
    assert!(n.candidate_log_is_up_to_date(Term(3), LogIndex(5)));    // same term, same length
    assert!(!n.candidate_log_is_up_to_date(Term(2), LogIndex(100))); // long but old
}
}