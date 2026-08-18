#include <gtest/gtest.h>
#include "raftkv/storage.hpp"
#include "raftkv/raft.hpp"
#include "raftkv/state_machine.hpp"
#include "raftkv/transport.hpp"
#include <filesystem>
#include <fstream>

namespace raftkv {

// Factory function implemented in wal.cpp
extern std::unique_ptr<Storage> make_file_storage(const std::string& dir);

namespace {

class PersistenceTest : public ::testing::Test {
protected:
    std::string test_dir = "./test_data_persistence";

    void SetUp() override {
        // Ensure a clean slate before every test
        std::filesystem::remove_all(test_dir);
        std::filesystem::create_directories(test_dir);
    }

    void TearDown() override {
        // Clean up after the test completes
        std::filesystem::remove_all(test_dir);
    }
};


TEST_F(PersistenceTest, HardStateSurvivesRestart) {
    // Boot, write, and "Crash" (scope destruction simulates process exit)
    {
        auto storage = make_file_storage(test_dir);
        HardState hs;
        hs.current_term = 42;
        hs.voted_for = 3;
        storage->save_hard_state(hs);
    } 
    // Restart and recover
    {
        auto storage = make_file_storage(test_dir);
        RecoveredState rs = storage->recover();
        
        EXPECT_EQ(rs.hard.current_term, 42);
        ASSERT_TRUE(rs.hard.voted_for.has_value());
        EXPECT_EQ(rs.hard.voted_for.value(), 3);
    }
}

TEST_F(PersistenceTest, AckedEntriesSurviveRestart) {
    // Write and Crash
    {
        auto storage = make_file_storage(test_dir);
        std::vector<LogEntry> entries;
        entries.push_back({1, 1, Command{Op::PUT, "k1", "v1", "", 0, 0}});
        entries.push_back({1, 2, Command{Op::PUT, "k2", "v2", "", 0, 0}});
        storage->append_entries(entries); // This calls fsync
    }

    // Restart and recover
    {
        auto storage = make_file_storage(test_dir);
        RecoveredState rs = storage->recover();
        ASSERT_EQ(rs.log.size(), 2);
        EXPECT_EQ(rs.log[0].index, 1);
        EXPECT_EQ(rs.log[0].cmd.key, "k1");
        EXPECT_EQ(rs.log[1].index, 2);
        EXPECT_EQ(rs.log[1].cmd.key, "k2");
    }
}


TEST_F(PersistenceTest, TornTrailingRecordIsDiscarded) {
    // Write valid entries and close cleanly
    {
        auto storage = make_file_storage(test_dir);
        std::vector<LogEntry> entries;
        entries.push_back({1, 1, Command{Op::PUT, "k1", "v1", "", 0, 0}});
        storage->append_entries(entries);
    }

    // Simulate a torn write (power loss exactly halfway through writing a record)
    // We do this by manually appending garbage bytes to the wal.log file
    {
        std::ofstream wal(test_dir + "/wal.log", std::ios::binary | std::ios::app);
        wal.write("HALF_WRITTEN_GARBAGE", 20);
        wal.flush();
    }

    // Restart and recover
    {
        auto storage = make_file_storage(test_dir);
        RecoveredState rs = storage->recover();
        
        // The recovery logic should detect the bad CRC/EOF, discard the torn tail,
        // and safely preserve the perfectly written entry that preceded it.
        ASSERT_EQ(rs.log.size(), 1);
        EXPECT_EQ(rs.log[0].index, 1);
        EXPECT_EQ(rs.log[0].cmd.key, "k1");
    }
}


TEST_F(PersistenceTest, TruncateSuffixRemovesConflicts) {
    // Write 10 entries, then truncate
    {
        auto storage = make_file_storage(test_dir);
        std::vector<LogEntry> entries;
        for (int i = 1; i <= 10; ++i) {
            entries.push_back({1, (LogIndex)i, Command{Op::PUT, "k", "v", "", 0, 0}});
        }
        storage->append_entries(entries);
        
        // Simulating the follower receiving an AppendEntries that conflicts at index 6
        storage->truncate_suffix(6);
    }

    // Restart and recover
    {
        auto storage = make_file_storage(test_dir);
        RecoveredState rs = storage->recover();
        ASSERT_EQ(rs.log.size(), 5) << "Should only contain indices 1 through 5";
        EXPECT_EQ(rs.log.front().index, 1);
        EXPECT_EQ(rs.log.back().index, 5);
    }
}


// Lightweight dependency mock cluster specifically for file storage testing
class CrashableNode {
public:
    Config cfg;
    std::string dir;
    std::unique_ptr<Storage> storage;
    std::unique_ptr<MockTransport> transport;
    std::unique_ptr<KVStateMachine> sm;
    std::unique_ptr<Raft> raft;
    void boot(Network* net, const std::vector<NodeId>& peers) {
        cfg.self_id = peers.back(); 
        cfg.cluster = peers;
        cfg.heartbeat = std::chrono::milliseconds(50);
        cfg.election_min = std::chrono::milliseconds(150);
        cfg.election_max = std::chrono::milliseconds(300);
        std::filesystem::create_directories(dir);
        storage = make_file_storage(dir);
        sm = std::make_unique<KVStateMachine>();
        transport = std::make_unique<MockTransport>(cfg.self_id, peers, net);
        raft = std::make_unique<Raft>(cfg, storage.get(), transport.get(), sm.get());
        raft->start();
    }
};

TEST_F(PersistenceTest, NoAcknowledgedWriteLostOnLeaderKill) {
    Network net;
    std::vector<std::unique_ptr<CrashableNode>> cluster;
    std::vector<NodeId> peers = {0, 1, 2};

    // Boot up cluster using actual FileStorage
    for (int i = 0; i < 3; ++i) {
        auto node = std::make_unique<CrashableNode>();
        node->cfg.self_id = i;
        node->dir = test_dir + "/n" + std::to_string(i);
        node->boot(&net, peers);
        cluster.push_back(std::move(node));
    }
    // Let them elect a leader
    for (int i = 0; i < 500; ++i) {
        net.step();
        for (auto& n : cluster) {
            if (n->raft->role() == Role::Leader && i % 50 == 0) n->raft->on_heartbeat_tick();
            if (n->raft->role() != Role::Leader && i % 200 == 0) n->raft->on_election_timeout();
        }
    }
    NodeId leader_id = -1;
    for (auto& n : cluster) {
        if (n->raft->role() == Role::Leader) leader_id = n->cfg.self_id;
    }
    ASSERT_NE(leader_id, -1);
    // Propose a write and wait for it to commit
    Command cmd{Op::PUT, "vital_data", "safe", "", 0, 0};
    ProposeResult prop = cluster[leader_id]->raft->propose(cmd);
    for (int i = 0; i < 200; ++i) {
        net.step();
        if (i % 50 == 0) cluster[leader_id]->raft->on_heartbeat_tick();
    }
    // Verify it committed before the crash
    ASSERT_EQ(cluster[leader_id]->raft->commit_index(), prop.index);
    // SIGKILL THE LEADER (Destroy all its volatile memory)
    std::string leader_dir = cluster[leader_id]->dir;
    cluster[leader_id].reset(); // The object is destroyed. Memory is wiped.
    // Let the survivors elect a new leader
    for (int i = 0; i < 500; ++i) {
        net.step();
        for (auto& n : cluster) {
            if (n) {
                if (n->raft->role() == Role::Leader && i % 50 == 0) n->raft->on_heartbeat_tick();
                if (n->raft->role() != Role::Leader && i % 200 == 0) n->raft->on_election_timeout();
            }
        }
    }
    // RESTART THE DEAD NODE from its disk directory
    auto revived_node = std::make_unique<CrashableNode>();
    revived_node->cfg.self_id = leader_id;
    revived_node->dir = leader_dir;
    revived_node->boot(&net, peers);
    // It should recover the state machine from the disk WAL automatically
    ApplyResult res = revived_node->sm->apply(Command{Op::GET, "vital_data", "", "", 0, 0});
    // The Ultimate Proof
    EXPECT_TRUE(res.found);
    EXPECT_EQ(res.value, "safe") << "The acknowledged write survived the exact-moment crash!";
}

} 
} 