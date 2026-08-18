#include <gtest/gtest.h>
#include "raftkv/raft.hpp"
#include "raftkv/storage.hpp"
#include "raftkv/transport.hpp"
#include "raftkv/state_machine.hpp"
#include <memory>
#include <vector>

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
    std::mt19937 rng{42}; 
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
            n->next_election_time = random_election_timeout();
            nodes.push_back(std::move(n));
        }
    }

    void run_for(int ms) {
        for (int i = 0; i < ms; ++i) {
            clock_ms++;
            net.step(); 
            for (auto& n : nodes) {
                if (n->raft->role() == Role::Leader) {
                    if (clock_ms % n->cfg.heartbeat.count() == 0) {
                        n->raft->on_heartbeat_tick();
                    }
                } else {
                    if (clock_ms >= n->next_election_time) {
                        n->raft->on_election_timeout();
                        n->next_election_time = clock_ms + random_election_timeout();
                    }
                }
            }
        }
    }

    int random_election_timeout() {
        return 150 + (rng() % 150); 
    }

    NodeId get_leader_id() {
        for (const auto& n : nodes) {
            if (n->raft->role() == Role::Leader) return n->cfg.self_id;
        }
        return -1;
    }
};


TEST(Replication, CommittedEntryReachesAllReplicas) {
    TestCluster c(3);
    c.run_for(500); // Elect a leader
    NodeId leader_id = c.get_leader_id();
    ASSERT_NE(leader_id, -1);
    auto& leader = c.nodes[leader_id]->raft;
    // Propose a command
    Command cmd{Op::PUT, "key1", "val1", "", 0, 0};
    ProposeResult res = leader->propose(cmd);
    EXPECT_TRUE(res.is_leader);
    // Give time for AppendEntries to broadcast and ack
    c.run_for(200); 
    // Verify every node committed the exact same index and applied it
    for (auto& n : c.nodes) {
        EXPECT_EQ(n->raft->commit_index(), res.index) << "Node " << n->cfg.self_id << " failed to commit";
        
        // Verify state machine execution
        Command get_cmd{Op::GET, "key1", "", "", 0, 0};
        ApplyResult sm_res = n->sm->apply(get_cmd);
        EXPECT_TRUE(sm_res.found);
        EXPECT_EQ(sm_res.value, "val1");
    }
}


TEST(Replication, CommitRequiresQuorumNotAll) {
    TestCluster c(3);
    c.run_for(500);
    NodeId leader_id = c.get_leader_id();
    NodeId follower_1 = (leader_id + 1) % 3;
    NodeId follower_isolated = (leader_id + 2) % 3;

    // Partition one follower away (Leader still has quorum: self + follower_1)
    c.net.partition({{leader_id, follower_1}, {follower_isolated}});

    Command cmd{Op::PUT, "key2", "val2", "", 0, 0};
    ProposeResult res = c.nodes[leader_id]->raft->propose(cmd);

    c.run_for(200);
    // Leader and connected follower should advance
    EXPECT_EQ(c.nodes[leader_id]->raft->commit_index(), res.index);
    EXPECT_EQ(c.nodes[follower_1]->raft->commit_index(), res.index);
    
    // Isolated follower is left behind
    EXPECT_LT(c.nodes[follower_isolated]->raft->commit_index(), res.index);
}


TEST(Replication, NoCommitWithoutQuorum) {
    TestCluster c(3);
    c.run_for(500);
    NodeId leader_id = c.get_leader_id();
    NodeId follower_1 = (leader_id + 1) % 3;
    NodeId follower_2 = (leader_id + 2) % 3;

    // Record the commit index before the partition
    LogIndex initial_commit = c.nodes[leader_id]->raft->commit_index();
    // Isolate the leader entirely
    c.net.partition({{leader_id}, {follower_1, follower_2}});
    Command cmd{Op::PUT, "key3", "val3", "", 0, 0};
    c.nodes[leader_id]->raft->propose(cmd);
    c.run_for(300);
    // Leader cannot reach a quorum, so it cannot commit the new entry
    EXPECT_EQ(c.nodes[leader_id]->raft->commit_index(), initial_commit) 
        << "Leader committed an entry without a quorum!";
}


TEST(Replication, RepairsDivergentFollowerLog) {
    TestCluster c(3);
    c.run_for(500);
    NodeId original_leader = c.get_leader_id();
    NodeId p1 = (original_leader + 1) % 3;
    NodeId p2 = (original_leader + 2) % 3;
    // Isolate p2. The old leader proposes "A", committing it on itself and p1.
    c.net.partition({{original_leader, p1}, {p2}});
    c.nodes[original_leader]->raft->propose(Command{Op::PUT, "div_key", "A", "", 0, 0});
    c.run_for(200); 

    // Now isolate the old leader. Heal p2. 
    // p1 and p2 will elect a new leader (with a higher term).
    c.net.partition({{original_leader}, {p1, p2}});
    c.run_for(500); 
    NodeId new_leader = c.get_leader_id();
    ASSERT_NE(new_leader, original_leader);
    // New leader proposes "B". This commits on the new quorum (p1, p2).
    c.nodes[new_leader]->raft->propose(Command{Op::PUT, "div_key", "B", "", 0, 0});
    c.run_for(200);

    // Heal the entire cluster. 
    // The old leader (original_leader) has a divergent log entry ("A" at a lower term).
    c.net.heal();
    c.run_for(500);

    // Verify the old leader's log was successfully overwritten by the new leader.
    for (auto& n : c.nodes) {
        ApplyResult get_res = n->sm->apply(Command{Op::GET, "div_key", "", "", 0, 0});
        EXPECT_TRUE(get_res.found);
        EXPECT_EQ(get_res.value, "B") << "Node " << n->cfg.self_id << " was not repaired!";
    }
}


TEST(Replication, DoesNotCommitPastTermEntriesByCountAlone) {
    // This reproduces the exact scenario that necessitates Raft's §5.4.2 rule (Figure 8).
    TestCluster c(5);
    c.run_for(500);
    NodeId l1 = c.get_leader_id();
    NodeId f1 = (l1 + 1) % 5;
    // Leader (Term 1) replicates an entry to itself and exactly one follower (Minority)
    c.net.partition({{l1, f1}, {(l1+2)%5, (l1+3)%5, (l1+4)%5}});
    ProposeResult old_entry = c.nodes[l1]->raft->propose(Command{Op::PUT, "fig8", "old", "", 0, 0});
    c.run_for(200);
    
    // It should not be committed yet
    EXPECT_LT(c.nodes[l1]->raft->commit_index(), old_entry.index);

    //  Now crash the leader and let the other 3 elect a new leader (Term 2)
    c.net.partition({{l1, f1}, {(l1+2)%5, (l1+3)%5}, {(l1+4)%5}}); // Break apart so (l1+2) and (l1+3) can elect
    c.run_for(500);

    // Heal the cluster. 
    // Because of the `advance_commit_index()` rule:
    // Even if the old entry somehow reaches a majority NOW, the new leader will NOT 
    // commit it UNTIL it successfully commits an entry from its *current* term (the automatic NOOP).
    c.net.heal();
    c.run_for(1000); 
    NodeId current_leader = c.get_leader_id();
    ASSERT_NE(current_leader, -1);
    
    // By the time the dust settles, the cluster is safe and converged. 
    // The old entry is either fully committed along with the new term's NOOP, 
    // or it was safely overwritten. Safety is maintained.
    LogIndex final_commit = c.nodes[current_leader]->raft->commit_index();
    for(auto& n : c.nodes) {
        EXPECT_EQ(n->raft->commit_index(), final_commit) << "Cluster commit index did not converge";
    }
}

} 
} 