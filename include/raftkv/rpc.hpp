#pragma once
#include "raftkv/types.hpp"

namespace raftkv {

// RequestVote 
struct RequestVoteArgs {
    Term     term;            // candidate's term
    NodeId   candidate_id;    // who is asking
    LogIndex last_log_index;  // index of candidate's last log entry  
    Term     last_log_term;   // term  of candidate's last log entry 
};
struct RequestVoteReply {
    Term term;          // receiver's currentTerm, so candidate can step down if stale
    bool vote_granted;  // true iff the candidate received this vote
};

// AppendEntries
struct AppendEntriesArgs {
    Term                  term;           // leader's term
    NodeId                leader_id;      // so followers can redirect clients
    LogIndex              prev_log_index; // index immediately preceding new ones
    Term                  prev_log_term;  // term of prev_log_index entry (consistency check)
    std::vector<LogEntry> entries;        // new entries (empty == heartbeat)
    LogIndex              leader_commit;  // leader's commitIndex
};
struct AppendEntriesReply {
    Term     term;             // receiver's currentTerm, for leader to update itself
    bool     success;          // true iff follower had matching prev_log_index/term
    LogIndex conflict_index = 0;
    Term     conflict_term  = 0;
};

// InstallSnapshot catch up a follower that's too far behind 
struct InstallSnapshotArgs {
    Term        term;                 // leader's term
    NodeId      leader_id;
    LogIndex    last_included_index;  // snapshot replaces all entries up through here
    Term        last_included_term;   // term of last_included_index
    std::string data;                 // serialized state-machine snapshot
};
struct InstallSnapshotReply {
    Term term;  // for leader to step down if it's stale
};

} 