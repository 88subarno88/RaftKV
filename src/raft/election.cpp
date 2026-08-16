#include "raftkv/raft.hpp"

namespace raftkv {

void Raft::on_election_timeout() {
    //Leaders don't time out (they send heartbeats)
    if (role_ == Role::Leader) {
        return;
    }
    // Become candidate (bumps term, self-votes, persists, resets timer)
    become_candidate();
    //Build args once
    RequestVoteArgs args;
    args.term           = hard_.current_term;
    args.candidate_id   = cfg_.self_id;
    args.last_log_index = log_.last_index();
    args.last_log_term  = log_.last_term();
    // Capture the term  campaign in. If a callback arrives late after 
    // already moved on to a higher term,  must ignore it.
    Term campaign_term = hard_.current_term;
    // Solicit votes from all peers
    for (NodeId peer : cfg_.cluster) {
        if (peer == cfg_.self_id) {
            continue;
        }
        transport_->send_request_vote(peer, args, [this, campaign_term](const RequestVoteReply& reply) {
            // If the peer has a higher term, are obsolete immediately.
            if (reply.term > hard_.current_term) {
                become_follower(reply.term, std::nullopt);
                return;
            }
            // Stale callback check ->Did  already win, step down, or start a new election?
            if (role_ != Role::Candidate || hard_.current_term != campaign_term) {
                return; 
            }
            // Count the vote
            if (reply.vote_granted) {
                votes_received_++;
                if (votes_received_ >= quorum()) {
                    become_leader();
                }
            }
        });
    }
}

RequestVoteReply Raft::handle_request_vote(const RequestVoteArgs& a) {
    // Step down immediately if we see a higher term
    if (a.term > hard_.current_term) {
        become_follower(a.term, std::nullopt);
    }
    // Initialize reply
    RequestVoteReply r;
    r.term = hard_.current_term;
    r.vote_granted = false;
    // Reject stale candidates outright
    if (a.term < hard_.current_term) {
        return r;
    }
    // Can we vote for them? ( haven't voted yet, or already voted for them)
    bool can_vote = !hard_.voted_for.has_value() || hard_.voted_for == a.candidate_id;

    // Check-> Are they at least as up-to-date as us?
    if (can_vote && log_is_up_to_date(a.last_log_index, a.last_log_term)) {
        hard_.voted_for = a.candidate_id;
        persist_hard_state();      // must be durable before replying yes
        reset_election_timer();    // saw a valid candidate, defer own election
        r.vote_granted = true;
    }
    return r;
}

} 