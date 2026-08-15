#pragma once
#include "raftkv/types.hpp"
#include "raftkv/rpc.hpp"
#include "raftkv/log.hpp"
#include "raftkv/storage.hpp"
#include "raftkv/transport.hpp"
#include "raftkv/state_machine.hpp"
#include <unordered_map>
#include <chrono>
#include <random>

namespace raftkv {

struct Config {
    NodeId self_id;
    std::vector<NodeId> cluster;               // all node ids incl. self
    std::chrono::milliseconds heartbeat{50};   // leader -> follower ping period
    std::chrono::milliseconds election_min{150};  // randomized election timeout
    std::chrono::milliseconds election_max{300};  // pick uniformly in [min,max]
    LogIndex snapshot_threshold = 10000;       // take a snapshot every N applied entries
};

struct ProposeResult {
    bool     is_leader;   // false => client should redirect to `leader_hint`
    NodeId   leader_hint; // best guess at who the leader is
    LogIndex index;       // the log index the command was assigned (if leader)
    Term     term;        // term at proposal time (to detect leader change on wait)
};

class Raft {
public:
    Raft(Config cfg, Storage* storage, Transport* transport, StateMachine* sm);
    void start();
    ProposeResult propose(const Command& cmd);

    //Inbound RPC handlers
    RequestVoteReply     handle_request_vote(const RequestVoteArgs& a);
    AppendEntriesReply   handle_append_entries(const AppendEntriesArgs& a);
    InstallSnapshotReply handle_install_snapshot(const InstallSnapshotArgs& a);

    // Timer callbacks 
    void on_election_timeout();
    void on_heartbeat_tick();

    // Introspection 
    Role     role()         const { return role_; }
    Term     current_term() const { return hard_.current_term; }
    NodeId   leader_hint()  const { return leader_id_; }
    LogIndex commit_index() const { return commit_index_; }

private:
    //  Role transitions 
    void become_follower(Term term, std::optional<NodeId> leader);
    void become_candidate();
    void become_leader();

    //  Helpers 
    void persist_hard_state();
    void advance_commit_index();
    void apply_committed();
    void maybe_snapshot();
    bool log_is_up_to_date(LogIndex idx, Term term) const;
    int  quorum() const { return (int)cfg_.cluster.size() / 2 + 1; }
    void reset_election_timer();
    
    // Internal RPC senders
    void send_append_entries_to(NodeId peer);

    Config        cfg_;
    Storage*      storage_;
    Transport*    transport_;
    StateMachine* sm_;
    HardState hard_;
    Log       log_;
    Role     role_         = Role::Follower;
    NodeId   leader_id_    = -1;
    LogIndex commit_index_ = 0;
    LogIndex last_applied_ = 0;

    std::unordered_map<NodeId, LogIndex> next_index_;
    std::unordered_map<NodeId, LogIndex> match_index_;
    int votes_received_ = 0;
    
    // RNG for election timeouts
    std::mt19937 rng_;
};

}