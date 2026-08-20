#include <gtest/gtest.h>
#include "raftkv/raft.hpp"
#include "raftkv/storage.hpp"
#include "raftkv/transport.hpp"
#include "raftkv/state_machine.hpp"
#include <vector>
#include <string>
#include <functional>
#include <iostream>
#include <random>

namespace raftkv {
namespace {

struct Operation {
    int id;
    Op op;
    std::string key;
    std::string val;
    std::string exp;
    bool ok;
    std::string res_val;
    int start_time;
    int end_time;
};

// Checks if a concurrent history is linearizable.
// A history is linearizable if there exists SOME sequential permutation of the
// operations such that:
// The sequential execution is valid for a KV store.
// It respects real-time ordering: If Op A finishes before Op B starts, 
//    Op A MUST appear before Op B in the permutation.
bool is_linearizable(const std::vector<Operation>& ops) {
    int n = ops.size();
    if (n > 31) {
        throw std::runtime_error("Brute-force checker only supports up to 31 ops");
    }

    // Build the Real-Time constraint DAG
    // If op[i] ends before op[j] starts, op[i] MUST happen before op[j].
    std::vector<std::vector<int>> adj(n);
    std::vector<int> indegree(n, 0);
    for (int i = 0; i < n; ++i) {
        for (int j = 0; j < n; ++j) {
            if (ops[i].end_time < ops[j].start_time) {
                adj[i].push_back(j);
                indegree[j]++;
            }
        }
    }

    // DFS Backtracking to find a valid topological sort
    std::function<bool(uint32_t, std::string)> dfs = [&](uint32_t mask, std::string current_val) {
        if (mask == (1u << n) - 1) return true; // All operations successfully linearized!

        for (int i = 0; i < n; ++i) {
            // We can only execute an operation if it hasn't been executed yet, 
            // and all its real-time prerequisites have already been executed.
            if (!(mask & (1 << i)) && indegree[i] == 0) {
                
                bool valid = true;
                std::string next_val = current_val;
                
                // Simulate the KV register
                if (ops[i].op == Op::PUT) {
                    next_val = ops[i].val;
                } else if (ops[i].op == Op::GET) {
                    if (ops[i].res_val != current_val) valid = false;
                } else if (ops[i].op == Op::CAS) {
                    if (ops[i].ok) {
                        if (current_val != ops[i].exp) valid = false;
                        next_val = ops[i].val;
                    } else {
                        if (current_val == ops[i].exp) valid = false;
                    }
                }
                // If this move is sequentially valid, recurse deeper
                if (valid) {
                    // Temporarily remove this node's constraints from the DAG
                    for (int neighbor : adj[i]) indegree[neighbor]--;
                    
                    if (dfs(mask | (1 << i), next_val)) return true;
                    
                    // Backtrack
                    for (int neighbor : adj[i]) indegree[neighbor]++;
                }
            }
        }
        return false;
    };

    return dfs(0, "");
}


TEST(Linearizability, DetectsAnInjectedBug) {
    std::vector<Operation> history;

    // We fabricate a history that represents a "Dirty Read" or "Stale Read" (Split Brain).
    // Op 0: PUT "1" (Starts at 0, Ends at 5)
    history.push_back({0, Op::PUT, "k", "1", "", true, "", 0, 5});
    
    // PUT "2" (Starts at 10, Ends at 15)
    // Because 10 > 5, Op 1 strictly happens AFTER Op 0 in real-time.
    history.push_back({1, Op::PUT, "k", "2", "", true, "", 10, 15});
    
    // GET -> "1" (Starts at 20, Ends at 25)
    // Because 20 > 15, Op 2 strictly happens AFTER Op 1 in real-time.
    // BUT IT READS "1"! This violates linearizability because the register should be "2".
    history.push_back({2, Op::GET, "k", "", "", true, "1", 20, 25});

    // The checker MUST reject this history. If it returns true, the checker is broken.
    EXPECT_FALSE(is_linearizable(history)) 
        << "Checker failed to reject a blatantly non-linearizable history!";
}


// Lightweight Mock Infrastructure
struct NodeData {
    Config cfg;
    std::unique_ptr<MemoryStorage> storage;
    std::unique_ptr<MockTransport> transport;
    std::unique_ptr<KVStateMachine> sm;
    std::unique_ptr<Raft> raft;
};

TEST(Linearizability, HoldsUnderRandomPartitions) {
    Network net;
    std::vector<std::unique_ptr<NodeData>> nodes;
    std::vector<NodeId> peers = {0, 1, 2, 3, 4}; // 5-node cluster

    for (int i = 0; i < 5; ++i) {
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
        nodes.push_back(std::move(n));
    }

    std::mt19937 rng(42);
    std::vector<Operation> history;
    int clock_ms = 0;

    int next_client_id = 100;
    int client_seq = 1;

    // Election timers must be independent and randomized per node. Firing them
    // all on the same tick makes every node a candidate in the same term, the
    // vote splits every round, and no leader is ever elected - which used to
    // spin the retry loop below forever.
    auto random_timeout = [&rng]() { return 150 + (int)(rng() % 150); };
    std::vector<int> next_election(nodes.size());
    for (size_t i = 0; i < nodes.size(); ++i) next_election[i] = random_timeout();

    auto tick = [&]() {
        clock_ms++;
        net.step();
        for (size_t i = 0; i < nodes.size(); ++i) {
            auto& node = nodes[i];
            if (node->raft->role() == Role::Leader) {
                if (clock_ms % 50 == 0) node->raft->on_heartbeat_tick();
            } else if (clock_ms >= next_election[i]) {
                node->raft->on_election_timeout();
                next_election[i] = clock_ms + random_timeout();
            }
        }
    };

    // Simulate 20 concurrent client requests under heavy duress
    for (int req = 0; req < 20; ++req) {
        int t_start = clock_ms;

        // Formulate a random command. Each op gets its own client_id, so the
        // state machine's session cache holds exactly this op's result.
        Command cmd;
        cmd.client_id = next_client_id++;
        cmd.seq_no = client_seq++;
        cmd.key = "k";
        int r = rng() % 3;
        if (r == 0) {
            cmd.op = Op::PUT;
            cmd.value = std::to_string(rng() % 100);
        } else if (r == 1) {
            cmd.op = Op::GET;
        } else {
            cmd.op = Op::CAS;
            cmd.expected = std::to_string(rng() % 100);
            cmd.value = std::to_string(rng() % 100);
        }
        bool applied = false;
        ApplyResult result;

        // Keep retrying against the cluster until someone commits it. Bounded,
        // so a cluster that cannot make progress fails the test instead of hanging.
        for (int attempt = 0; attempt < 60 && !applied; ++attempt) {
            // Randomly scramble the network partitions between attempts
            if (rng() % 2 == 0) {
                net.heal();
            } else {
                // Isolate Node 0 and 1 from 2, 3, and 4
                net.partition({{0, 1}, {2, 3, 4}});
            }

            // Find someone who claims to be leader and propose
            for (auto& n : nodes) {
                if (n->raft->role() != Role::Leader) continue;
                ProposeResult pr = n->raft->propose(cmd);
                if (!pr.is_leader) continue;

                for (int wait = 0; wait < 200 && !applied; ++wait) {
                    tick();
                    // Only trust the commit if this node still holds the same
                    // leadership it proposed under. A node that stepped down may
                    // have had its entry at pr.index overwritten by a new leader,
                    // in which case commit_index says nothing about our command.
                    if (n->raft->role() == Role::Leader &&
                        n->raft->current_term() == pr.term &&
                        n->raft->commit_index() >= pr.index) {
                        // Raft already applied it. Re-applying the identical
                        // command returns the cached result rather than
                        // executing it a second time.
                        result = n->sm->apply(cmd);
                        applied = true;
                    }
                }
                if (applied) break;
            }

            // If nothing committed, step time to let elections run
            if (!applied) {
                for (int wait = 0; wait < 300; ++wait) tick();
            }
        }

        ASSERT_TRUE(applied) << "Operation " << req << " never committed";

        int t_end = clock_ms;

        // Log the operation to our history
        history.push_back({
            req, cmd.op, cmd.key, cmd.value, cmd.expected,
            result.ok, result.value, t_start, t_end
        });
    }

    // After surviving network partitions, leader crashes, and retries, 
    // the recorded history MUST be perfectly linearizable.
    EXPECT_TRUE(is_linearizable(history)) 
        << "Raft violated linearizability under partition stress!";
}

}
} 