#include "raftkv/raft.hpp"
#include <algorithm>

namespace raftkv {

InstallSnapshotReply Raft::handle_install_snapshot(const InstallSnapshotArgs& a) {
    //Step down if the leader's term is newer
    if (a.term > hard_.current_term) {
        become_follower(a.term, a.leader_id);
    }
    // Initialize reply
    InstallSnapshotReply r;
    r.term = hard_.current_term;
    // Reject stale leaders
    if (a.term < hard_.current_term) {
        return r;
    }
    // Acknowledge valid leader
    reset_election_timer();
    leader_id_ = a.leader_id;
    if (role_ == Role::Candidate) {
        become_follower(a.term, a.leader_id);
    }

    // Ignore stale snapshots 
    // If the snapshot is older than or equal to what  already committed, 
    // it's a delayed network packet. Ignore it.
    if (a.last_included_index <= commit_index_) {
        return r;
    }

    // Do we have a matching entry at the snapshot boundary?
    // If we do, we keep the log entries that follow it. If we don't, our entire 
    // log is divergent and must be completely discarded.
    bool match = false;
    if (a.last_included_index >= log_.first_index() - 1 && a.last_included_index <= log_.last_index()) {
        if (log_.term_at(a.last_included_index) == a.last_included_term) {
            match = true;
        }
    }
    if (match) {
        // Log matches! We only discard the prefix (up to last_included_index), 
        // keeping any uncommitted tail entries intact.
        log_.compact(a.last_included_index, a.last_included_term);
    } else {
        // Divergent log
        // We can safely reset the in-memory Log object and then advance its 
        // base boundary to match the incoming snapshot.
        log_ = Log(); 
        log_.compact(a.last_included_index, a.last_included_term);
        
        // Also inform the storage layer to clear out any conflicting WAL entries
        storage_->truncate_suffix(1); 
    }

    // Apply the snapshot data to the State Machine
    sm_->restore(a.data);

    // Persist the snapshot durably to disk
    storage_->save_snapshot(a.last_included_index, a.last_included_term, a.data);

    // Fast-forward our state tracking
    last_applied_ = a.last_included_index;
    commit_index_ = a.last_included_index;

    return r;
}

}