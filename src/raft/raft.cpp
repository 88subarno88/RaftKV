#include "raftkv/raft.hpp"
#include <algorithm>

namespace raftkv {

Raft::Raft(Config cfg, Storage* s, Transport* t, StateMachine* sm)
    : cfg_(std::move(cfg)), storage_(s), transport_(t), sm_(sm) {
    
    // Register RPC handlers. 
    // In a fully single-threaded async engine would wrap these in a lambda
    // that posts the execution to the main event loop queue to maintain the 
    // threading invariant.
    transport_->set_handlers({
        [this](const RequestVoteArgs& a) { return handle_request_vote(a); },
        [this](const AppendEntriesArgs& a) { return handle_append_entries(a); },
        [this](const InstallSnapshotArgs& a) { return handle_install_snapshot(a); }
    });
}

void Raft::start() {
    // Recover state from durable storage
    auto rec = storage_->recover();
    hard_ = rec.hard;
    
    //Restore snapshot if one exists
    if (!rec.snapshot_data.empty()) {
        sm_->restore(rec.snapshot_data);
        log_.compact(rec.snapshot_index, rec.snapshot_term);
        last_applied_ = rec.snapshot_index;
        commit_index_ = rec.snapshot_index;
    }
    
    // Rebuild the inmemory log tail
    log_.merge_from(rec.snapshot_index, rec.log);
    
    // Boot as follower and start the election timer
    become_follower(hard_.current_term, std::nullopt);
}

// Role transitions

void Raft::become_follower(Term term, std::optional<NodeId> leader) {
    if (term > hard_.current_term) {
        hard_.current_term = term;
        hard_.voted_for.reset(); 
        persist_hard_state();
    }
    role_ = Role::Follower;
    leader_id_ = leader.value_or(-1);
    reset_election_timer();
}

void Raft::become_candidate() {
    role_ = Role::Candidate;
    leader_id_ = -1;
    ++hard_.current_term;
    hard_.voted_for = cfg_.self_id; // Vote for self
    persist_hard_state();           // Durable before requesting votes
    votes_received_ = 1; 
    reset_election_timer();
    // Broadcast RequestVote
    RequestVoteArgs args;
    args.term = hard_.current_term;
    args.candidate_id = cfg_.self_id;
    args.last_log_index = log_.last_index();
    args.last_log_term = log_.last_term();
    for (NodeId peer : cfg_.cluster) {
        if (peer == cfg_.self_id) continue;
        
        transport_->send_request_vote(peer, args, [this, term = hard_.current_term](const RequestVoteReply& reply) {
            // Drop stale replies
            if (role_ != Role::Candidate || hard_.current_term != term) return;
            if (reply.term > hard_.current_term) {
                become_follower(reply.term, std::nullopt);
                return;
            }
            if (reply.vote_granted) {
                votes_received_++;
                if (votes_received_ >= quorum()) {
                    become_leader();
                }
            }
        });
    }
}

void Raft::become_leader() {
    role_ = Role::Leader;
    leader_id_ = cfg_.self_id;
    for (NodeId p : cfg_.cluster) {
        next_index_[p] = log_.last_index() + 1;
        match_index_[p] = 0;
    }
    // Assert leadership immediately to prevent follower timeouts
    on_heartbeat_tick();
}

// Commit & apply

void Raft::advance_commit_index() {
    for (LogIndex n = log_.last_index(); n > commit_index_; --n) {
        // Only commit entries from our current term by counting replicas
        if (log_.term_at(n) != hard_.current_term) continue;
        int count = 1; // Count ourselves
        for (NodeId p : cfg_.cluster) {
            if (p != cfg_.self_id && match_index_[p] >= n) {
                count++;
            }
        }
        if (count >= quorum()) {
            commit_index_ = n;
            apply_committed();
            break; // Stop searching downwards
        }
    }
}

void Raft::apply_committed() {
    while (last_applied_ < commit_index_) {
        ++last_applied_;
        // Deduplication/Linearizability is handled inside sm_->apply() 
        // as implemented in our KVStateMachine.
        sm_->apply(log_.at(last_applied_).cmd);
        // Notify any client waiting on this last_applied_ index...
    }
    
    maybe_snapshot();
}

void Raft::maybe_snapshot() {
    if (last_applied_ - (log_.first_index() - 1) >= cfg_.snapshot_threshold) {
        auto blob = sm_->snapshot();
        storage_->save_snapshot(last_applied_, log_.term_at(last_applied_), blob);
        log_.compact(last_applied_, log_.term_at(last_applied_));
    }
}

bool Raft::log_is_up_to_date(LogIndex idx, Term term) const {
    // Election restriction logic
    return term > log_.last_term() ||
           (term == log_.last_term() && idx >= log_.last_index());
}

void Raft::persist_hard_state() {
    storage_->save_hard_state(hard_);
}

void Raft::reset_election_timer() {
    // In a real async runtime (like ASIO or libuv), would draw a random duration
    // uniform_int_distribution<>(cfg_.election_min, cfg_.election_max)
    // and push a delayed event onto the loop.
}

ProposeResult Raft::propose(const Command& cmd) {
    if (role_ != Role::Leader) {
        return {false, leader_id_, 0, hard_.current_term};
    }

    //Append to inmemory log
    LogIndex i = log_.append(hard_.current_term, cmd);
    
    // Persist locally (MUST fsync before  count ourselves in the quorum)
    storage_->append_entries({ log_.at(i) });
    
    // Update leader's own match state
    match_index_[cfg_.self_id] = i;
    next_index_[cfg_.self_id]  = i + 1;
    
    // Trigger replication immediately
    on_heartbeat_tick();
    
    return {true, cfg_.self_id, i, hard_.current_term};
}

} 