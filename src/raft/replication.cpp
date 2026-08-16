#include "raftkv/raft.hpp"
#include <algorithm>

namespace raftkv {

void Raft::on_heartbeat_tick() {
    // Only leaders send heartbeats
    if (role_ != Role::Leader) {
        return;
    }

    // Broadcast to every peer
    for (NodeId p : cfg_.cluster) {
        if (p == cfg_.self_id) {
            continue;
        }

        LogIndex ni = next_index_[p];

        // Is the follower too far behind? 
        if (ni <= log_.first_index() - 1) {
            InstallSnapshotArgs snap_args;
            snap_args.term = hard_.current_term;
            snap_args.leader_id = cfg_.self_id;
            snap_args.last_included_index = log_.first_index() - 1;
            snap_args.last_included_term = log_.term_at(snap_args.last_included_index);
            snap_args.data = sm_->snapshot();

            transport_->send_install_snapshot(p, snap_args, [this, p, req_term = hard_.current_term](const InstallSnapshotReply& reply) {
                if (reply.term > hard_.current_term) {
                    become_follower(reply.term, std::nullopt);
                    return;
                }
                if (role_ != Role::Leader || hard_.current_term != req_term) return; // Stale callback
                
                // Jump the follower's next index forward
                next_index_[p] = log_.first_index();
            });
            continue;
        }

        // Build normal AppendEntries
        AppendEntriesArgs a;
        a.term           = hard_.current_term;
        a.leader_id      = cfg_.self_id;
        a.prev_log_index = ni - 1;
        a.prev_log_term  = log_.term_at(a.prev_log_index);
        a.entries        = log_.slice_from(ni); // empty => pure heartbeat
        a.leader_commit  = commit_index_;

        // Fire the RPC asynchronously
        transport_->send_append_entries(p, a, [this, p, req_term = a.term, req_prev_idx = a.prev_log_index, req_entries_size = (LogIndex)a.entries.size()](const AppendEntriesReply& reply) {
            
            // Step down if they have a higher term
            if (reply.term > hard_.current_term) {
                become_follower(reply.term, std::nullopt);
                return;
            }
            // Ignore stale replies
            if (role_ != Role::Leader || hard_.current_term != req_term) {
                return;
            }

            if (reply.success) {
                // Success: advance our knowledge of what they have
                match_index_[p] = std::max(match_index_[p], req_prev_idx + req_entries_size);
                next_index_[p]  = match_index_[p] + 1;
                
                // Try to commit newly replicated entries
                advance_commit_index();
            } else {
                // Failure ->Fast backup optimization 
                // Instead of decrementing next_index by 1, jump back past the whole conflicting term
                next_index_[p] = log_.find_conflict_backup(reply.conflict_index, reply.conflict_term);
            }
        });
    }
}

AppendEntriesReply Raft::handle_append_entries(const AppendEntriesArgs& a) {
    // Step down if they have a newer term
    if (a.term > hard_.current_term) {
        become_follower(a.term, a.leader_id);
    }

    // Initialize default failure reply
    AppendEntriesReply r;
    r.term    = hard_.current_term;
    r.success = false;

    //Reject stale leader
    if (a.term < hard_.current_term) {
        return r;
    }

    // Valid leader heard -> extend lease and ensure we are a follower
    reset_election_timer();
    leader_id_ = a.leader_id;
    if (role_ == Role::Candidate) {
        become_follower(a.term, a.leader_id);
    }

    //CONSISTENCY CHECK
    // We don't even have an entry at prev_log_index (we are missing entries)
    if (a.prev_log_index > log_.last_index()) {
        r.conflict_index = log_.last_index() + 1;
        r.conflict_term  = 0;
        return r;
    }

    // We have an entry at prev_log_index, but terms don't match (diverging history)
    if (a.prev_log_index >= log_.first_index()) {
        Term actual_term = log_.term_at(a.prev_log_index);
        if (actual_term != a.prev_log_term) {
            r.conflict_term = actual_term;
            
            // Fast backup optimization ->find the *first* index of this conflicting term
            LogIndex c_idx = a.prev_log_index;
            while (c_idx >= log_.first_index() && log_.term_at(c_idx) == actual_term) {
                c_idx--;
            }
            r.conflict_index = c_idx + 1;
            return r;
        }
    }

    // Log perfectly matches up to prev_log_index! 
    // Append/repair suffix (merge_from is idempotent and only truncates on real conflicts)
    log_.merge_from(a.prev_log_index, a.entries);
    
    // Persist new entries to disk (must fsync before returning success)
    if (!a.entries.empty()) {
        storage_->truncate_suffix(a.prev_log_index + 1);
        storage_->append_entries(a.entries);
    }
    // Advance commit index
    if (a.leader_commit > commit_index_) {
        // We cap it at our last log index in case the leader is committing 
        // entries we haven't fully received yet.
        commit_index_ = std::min(a.leader_commit, log_.last_index());
        apply_committed();
    }
    r.success = true;
    return r;
}

} // namespace raftkv