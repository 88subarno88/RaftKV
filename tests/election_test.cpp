#include <gtest/gtest.h>
#include "raftkv/raft.hpp"
#include "raftkv/storage.hpp"
#include "raftkv/transport.hpp"
#include "raftkv/state_machine.hpp"
#include <memory>
#include <vector>
#include <random>

namespace raftkv {
namespace {
struct NodeData {
    Config cfg;
    std::unique_ptr<MemoryStorage> storage;
    std::unique_ptr<MockTransport> transport;
    std::unique_ptr<KVStateMachine> sm;
    std::unique_ptr<Raft> raft;
    int next_election_time = 0;
};

class TestCluster {
public:
    Network net;
    std::vector<std::unique_ptr<NodeData>> nodes;
    std::mt19937 rng{1337}; // Fixed seed for 100% deterministic tests
    int clock_ms = 0;

    explicit TestCluster(int num_nodes) {
        std::vector<NodeId> peers;
        for (int i = 0; i < num_nodes; ++i) peers.push_back(i);

        for (int i = 0; i < num_nodes; ++i) {
            auto n = std::make_unique<NodeData>();
            n->cfg.self_id = i;
            n->cfg.cluster = peers;
            n->cfg.heartbeat = std::chrono::milliseconds(50);
            n->cfg.election_min = std::chrono::milliseconds(150);
            n->cfg.election_max = std::chrono::milliseconds(300);
            n->storage = std::make_unique<MemoryStorage>();
            n->sm = std::make_unique<KVStateMachine>();
            n->transport = std::make_unique<MockTransport>(i, peers, &net);
            n->raft = std::make_unique<Raft>(n->cfg, n->storage.get(), n->transport.get(), n->sm.get());
            n->raft->start();
            
            // Randomize initial timer
            n->next_election_time = random_election_timeout();
            nodes.push_back(std::move(n));
        }
    }

    // Drives the virtual clock forward and fires Raft timers deterministically
    void run_for(int ms) {
        for (int i = 0; i < ms; ++i) {
            clock_ms++;
            net.step(); // Deliver any pending mock network packets
            for (auto& n : nodes) {
                if (n->raft->role() == Role::Leader) {
                    if (clock_ms % n->cfg.heartbeat.count() == 0) {
                        n->raft->on_heartbeat_tick();
                    }
                } else {
                    if (clock_ms >= n->next_election_time) {
                        n->raft->on_election_timeout();
                        // Reset timer after it fires
                        n->next_election_time = clock_ms + random_election_timeout();
                    }
                }
            }
        }
    }

    int random_election_timeout() {
        return 150 + (rng() % 150); // [150, 300)
    }
};


TEST(Election, ElectsExactlyOneLeaderInAQuietCluster) {
    TestCluster c(3);
    c.run_for(500); 
    int leaders = 0;
    Term winning_term = 0;
    for (const auto& n : c.nodes) {
        if (n->raft->role() == Role::Leader) {
            leaders++;
            winning_term = n->raft->current_term();
        }
    }

    EXPECT_EQ(leaders, 1) << "Cluster must elect exactly one leader";
    
    for (const auto& n : c.nodes) {
        EXPECT_EQ(n->raft->current_term(), winning_term) << "All nodes should converge on the same term";
    }
}


TEST(Election, ReElectsAfterLeaderCrash) {
    TestCluster c(3);
    c.run_for(500);

    // Find the current leader
    NodeId old_leader = -1;
    Term old_term = 0;
    for (const auto& n : c.nodes) {
        if (n->raft->role() == Role::Leader) {
            old_leader = n->cfg.self_id;
            old_term = n->raft->current_term();
            break;
        }
    }
    ASSERT_NE(old_leader, -1);
    // Isolate the leader from the rest of the network
    NodeId p1 = (old_leader + 1) % 3;
    NodeId p2 = (old_leader + 2) % 3;
    c.net.partition({{old_leader}, {p1, p2}});

    // Give the remaining two nodes time to notice the missing heartbeats and elect a new leader
    c.run_for(500);

    int new_leaders = 0;
    for (const auto& n : c.nodes) {
        if (n->cfg.self_id != old_leader && n->raft->role() == Role::Leader) {
            new_leaders++;
            EXPECT_GT(n->raft->current_term(), old_term) << "New leader must have a strictly greater term";
        }
    }
    EXPECT_EQ(new_leaders, 1) << "The surviving minority should elect exactly one new leader";
}


TEST(Election, LonelyNodeNeverBecomesLeader) {
    TestCluster c(5);
    // Isolate Node 0 entirely before anything starts
    c.net.partition({{0}, {1, 2, 3, 4}});
    // Run for a long time (many election timeouts)
    c.run_for(2000);
    auto& lonely_node = c.nodes[0]->raft;
    EXPECT_NE(lonely_node->role(), Role::Leader) << "Isolated node cannot reach quorum, cannot become leader";
    EXPECT_GT(lonely_node->current_term(), 1) << "Isolated node should have bumped its term campaigning";

    // Verify the rest of the cluster functioned normally and elected a leader
    int active_leaders = 0;
    for (int i = 1; i < 5; ++i) {
        if (c.nodes[i]->raft->role() == Role::Leader) {
            active_leaders++;
        }
    }
    EXPECT_EQ(active_leaders, 1);
}


TEST(Election, HigherTermCausesStepDown) {
    TestCluster c(3);
    c.run_for(500); // Elect a leader

    // Raw pointer, not a reference: we need to rebind it as we scan for the leader.
    Raft* leader_raft = c.nodes[0]->raft.get();
    // Force find the leader
    for (auto& n : c.nodes) {
        if (n->raft->role() == Role::Leader) leader_raft = n->raft.get();
    }
    Term current = leader_raft->current_term();
    // Fabricate an RPC from some rogue node with a massively higher term
    AppendEntriesArgs rogue_args;
    rogue_args.term = current + 10;
    rogue_args.leader_id = 99;
    
    leader_raft->handle_append_entries(rogue_args);

    EXPECT_EQ(leader_raft->role(), Role::Follower) << "Leader must step down immediately upon seeing a higher term";
    EXPECT_EQ(leader_raft->current_term(), rogue_args.term) << "Leader must adopt the newly observed higher term";
}


TEST(Election, RejectsVoteForStaleLog) {
    TestCluster c(3);
    c.run_for(500); // Wait for quiet cluster

    // The proposal only takes effect on the leader, and node 0 is not
    // necessarily the one elected, so find the real leader before proposing.
    Raft* node = nullptr;
    for (auto& n : c.nodes) {
        if (n->raft->role() == Role::Leader) node = n->raft.get();
    }
    ASSERT_NE(node, nullptr) << "Cluster should have elected a leader";

    Term current = node->current_term();
    // 1. Give it a log entry so its log is officially "longer/newer"
    Command dummy;
    dummy.op = Op::PUT;
    ASSERT_TRUE(node->propose(dummy).is_leader); // last_log_index is now 1, last_log_term is `current`

    // 2. Fabricate a RequestVote from a Candidate (Node 1) with a HIGHER term, 
    //    but an EMPTY log.
    RequestVoteArgs args;
    args.term = current + 1;
    args.candidate_id = 1;
    args.last_log_index = 0; // Stale!
    args.last_log_term = 0;  // Stale!
    RequestVoteReply reply = node->handle_request_vote(args);
    // 3. Verify Raft Safety §5.4.1
    EXPECT_FALSE(reply.vote_granted) << "Must reject vote: candidate's log is shorter/older than ours";
    // Even though it rejected the vote, the candidate had a higher term, so 
    // the node MUST still step down and adopt the term to avoid getting stuck.
    EXPECT_EQ(node->role(), Role::Follower);
    EXPECT_EQ(node->current_term(), args.term);
}

}
} 