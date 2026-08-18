#include "raftkv/raft.hpp"
#include "raftkv/storage.hpp"
#include "raftkv/state_machine.hpp"
#include "raftkv/transport.hpp"
#include <iostream>
#include <string>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <future>
#include <map>
#include <chrono>

namespace raftkv {

// Stub factories to make the file compile 
extern std::unique_ptr<Storage> make_file_storage(const std::string& dir);
extern std::unique_ptr<Transport> make_grpc_transport(NodeId self, const std::map<NodeId, std::string>& peer_addrs);
// The Core Event Loop 
class EventLoop {
public:
    using Task = std::function<void()>;

    // PUSH
    void post(Task task) {
        std::lock_guard<std::mutex> lock(mu_);
        queue_.push(std::move(task));
        cv_.notify_one();
    }

    //Called ONLY by the single main thread
    Task pop_blocking_with_timeout(std::chrono::steady_clock::time_point deadline) {
        std::unique_lock<std::mutex> lock(mu_);
        if (cv_.wait_until(lock, deadline, [this]() { return !queue_.empty() || !running_; })){
            if (!running_ && queue_.empty()) return nullptr;
            Task t = std::move(queue_.front());
            queue_.pop();
            return t;
        }
        return nullptr; // Timeout reached
    }

    void stop() {
        std::lock_guard<std::mutex> lock(mu_);
        running_ = false;
        cv_.notify_all();
    }
    bool is_running() const { return running_; }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    std::queue<Task> queue_;
    bool running_ = true;
};

// State Machine Wrapper
// Wraps your KVStateMachine to intercept apply() calls and wake up waiting clients.
class NotifyingStateMachine : public StateMachine {
public:
    explicit NotifyingStateMachine(std::unique_ptr<StateMachine> inner) 
        : inner_(std::move(inner)) {}

    ApplyResult apply(const Command& cmd) override {
        ApplyResult res = inner_->apply(cmd);
        // Notify any waiting client thread
        auto it = waiters_.find(applied_count_ + 1);
        if (it != waiters_.end()) {
            it->second.set_value(res);
            waiters_.erase(it);
        }
        applied_count_++;
        return res;
    }
    std::string snapshot() const override { return inner_->snapshot(); }
    void restore(const std::string& data) override { inner_->restore(data); }
    // Called by the client HTTP thread to wait for a specific log index
    std::future<ApplyResult> wait_for(LogIndex index) {
        return waiters_[index].get_future();
    }

private:
    std::unique_ptr<StateMachine> inner_;
    LogIndex applied_count_ = 0;
    std::map<LogIndex, std::promise<ApplyResult>> waiters_;
};

}


using namespace raftkv;
using namespace std::chrono;

int main(int argc, char** argv) {
    // Parse CLI Flags 
    // Example: raftkv-server --id 0 --peers 0=localhost:9000,1=localhost:9001 --data ./data/n0
    Config cfg;
    cfg.self_id = 0; 
    cfg.cluster = {0, 1, 2};
    std::string data_dir = "./data/n0";
    std::map<NodeId, std::string> peer_addrs = {
        {0, "localhost:9000"}, {1, "localhost:9001"}, {2, "localhost:9002"}
    };
    // Construct Dependencies
    auto storage = make_file_storage(data_dir);
    auto sm = std::make_unique<NotifyingStateMachine>(std::make_unique<KVStateMachine>());
    auto net = make_grpc_transport(cfg.self_id, peer_addrs);
    // Initialize Raft
    Raft raft(cfg, storage.get(), net.get(), sm.get());
    // Boot from disk
    raft.start();
    // Setup the Event Loop
    EventLoop loop;
    // Wire up the Transport so incoming RPCs POST to the event loop
    // This ensures Raft methods are NEVER executed directly on gRPC background threads
    RpcHandlers safe_handlers;
    safe_handlers.on_request_vote = [&](const RequestVoteArgs& args) {
        std::promise<RequestVoteReply> p;
        loop.post([&]() { p.set_value(raft.handle_request_vote(args)); });
        return p.get_future().get(); // Block gRPC thread until main thread processes it
    };
    safe_handlers.on_append_entries = [&](const AppendEntriesArgs& args) {
        std::promise<AppendEntriesReply> p;
        loop.post([&]() { p.set_value(raft.handle_append_entries(args)); });
        return p.get_future().get();
    };
    safe_handlers.on_install_snapshot = [&](const InstallSnapshotArgs& args) {
        std::promise<InstallSnapshotReply> p;
        loop.post([&]() { p.set_value(raft.handle_install_snapshot(args)); });
        return p.get_future().get();
    };
    net->set_handlers(safe_handlers);

    // Mock Client-Facing HTTP Endpoint 
    // In a real app, this runs on a background HTTP server thread.
    auto handle_client_request = [&](Command cmd) -> ApplyResult {
        std::promise<ProposeResult> prop_promise;
        // Push the proposal onto the Raft event loop safely
        loop.post([&]() {
            prop_promise.set_value(raft.propose(cmd));
        });
        ProposeResult prop_res = prop_promise.get_future().get();
        if (!prop_res.is_leader) {
            // Leader Redirect (Client must try another node)
            return {false, "REDIRECT TO NODE " + std::to_string(prop_res.leader_hint), false};
        }
        // Wait for the state machine to actually apply this index
        return sm->wait_for(prop_res.index).get();
    };

    // Run the Event Loop ;The Main Thread
    std::cout << "Node " << cfg.self_id << " started. Entering main event loop..." << std::endl;
    auto next_heartbeat = steady_clock::now() + cfg.heartbeat;
    auto next_election  = steady_clock::now() + cfg.election_max; // Assume randomized in reality
    while (loop.is_running()) {
        auto next_timer = std::min(next_heartbeat, next_election);
        // Block until a task arrives OR a timer is due
        auto task = loop.pop_blocking_with_timeout(next_timer);
        
        if (task) {
            task(); // Handle RPC, client request, etc.
        }

        auto now = steady_clock::now();
        // Fire Heartbeat (if Leader)
        if (now >= next_heartbeat) {
            raft.on_heartbeat_tick();
            next_heartbeat = now + cfg.heartbeat;
        }
        // Fire Election (if Follower/Candidate)
        if (now >= next_election) {
            raft.on_election_timeout();
            // Randomize next election timeout to prevent split votes
            // std::uniform_int_distribution<>(cfg.election_min, cfg.election_max)(rng);
            next_election = now + cfg.election_max; 
        }
    }
    return 0;
}