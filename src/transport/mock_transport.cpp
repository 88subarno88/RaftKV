#include "raftkv/transport.hpp"
#include <map>
#include <vector>
#include <functional>
#include <random>

namespace raftkv {

class Network {
public:
    Network() 
        : current_time_(0), drop_rate_(0.0), lat_min_(0), lat_max_(0), rng_(42) {}

    // Register a node's inbound handlers under its id
    void attach(NodeId id, RpcHandlers h) {
        handlers_[id] = std::move(h);
    }

    // Fault injection knobs the tests toggle 
    
    // Isolate groups. Nodes can only talk to others in the same sub-vector
    void partition(const std::vector<std::vector<NodeId>>& groups) {
        partitions_.clear();
        int group_id = 1;
        for (const auto& group : groups) {
            for (NodeId id : group) {
                partitions_[id] = group_id;
            }
            group_id++;
        }
    } 

    // Full connectivity
    void heal() {
        partitions_.clear();
    }                                              

    // Random loss (0.0 to 1.0)
    void set_drop_rate(double p) {
        drop_rate_ = p;
    }                                               
    
    // Random delay range in ms
    void set_latency(int ms_min, int ms_max) {
        lat_min_ = ms_min;
        lat_max_ = ms_max;
    }                                       

    // Partition check
    bool reachable(NodeId from, NodeId to) const {
        if (partitions_.empty()) return true; // Healed state
        
        auto it_from = partitions_.find(from);
        auto it_to = partitions_.find(to);
        
        // If they are explicitly in the same partition group, they can talk
        if (it_from != partitions_.end() && it_to != partitions_.end()) {
            return it_from->second == it_to->second;
        }
        
        // If partitions are active, unlisted nodes are isolated
        return false;
    }                                               

    // Deliver queued messages 
    void step(uint64_t ms = 10) {
        current_time_ += ms;
        // Pop and execute all messages due at or before current_time_
        while (!queue_.empty() && queue_.begin()->first <= current_time_) {
            auto task = std::move(queue_.begin()->second);
            queue_.erase(queue_.begin());
            task();
        }
    }

    // Helper to simulate the 2-way RPC trip with delays, drops, and partitions
    template<typename Args, typename Reply>
    void dispatch_rpc(NodeId from, NodeId to, const Args& args,
                      std::function<Reply(const Args&)> rpc_handler,
                      std::function<void(const Reply&)> callback) {
        
        if (!reachable(from, to) || should_drop()) return;

        uint64_t req_time = current_time_ + get_latency();
        
        // Schedule Request Delivery
        queue_.insert({req_time, [this, from, to, args, rpc_handler, callback]() {
            // Re-check partitions at delivery time
            if (!reachable(from, to)) return;

            // Execute on target node
            Reply reply = rpc_handler(args);

            // Schedule Reply Delivery
            if (!reachable(to, from) || should_drop()) return;
            
            uint64_t rep_time = current_time_ + get_latency();
            queue_.insert({rep_time, [this, from, to, reply, callback]() {
                // Re-check partitions at reply delivery time
                if (!reachable(to, from)) return;
                callback(reply);
            }});
        }});
    }

    RpcHandlers& get_handlers(NodeId id) { return handlers_[id]; }

private:
    bool should_drop() {
        if (drop_rate_ <= 0.0) return false;
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        return dist(rng_) < drop_rate_;
    }

    uint64_t get_latency() {
        if (lat_min_ >= lat_max_) return lat_min_;
        std::uniform_int_distribution<uint64_t> dist(lat_min_, lat_max_);
        return dist(rng_);
    }

    std::map<NodeId, RpcHandlers> handlers_;
    std::map<NodeId, int> partitions_;
    
    double drop_rate_;
    uint64_t lat_min_;
    uint64_t lat_max_;
    
    std::mt19937 rng_;
    uint64_t current_time_;
    
    // Priority queue of pending closures sorted by due-time
    std::multimap<uint64_t, std::function<void()>> queue_;
};

// The per-node Transport that talks to the shared Network.
class MockTransport : public Transport {
public:
    MockTransport(NodeId self, std::vector<NodeId> peers, Network* net)
        : self_(self), peers_(std::move(peers)), net_(net) {}

    void set_handlers(RpcHandlers h) override {
        net_->attach(self_, std::move(h));
    }

    void send_request_vote(NodeId to, const RequestVoteArgs& a,
                           std::function<void(const RequestVoteReply&)> cb) override {
        net_->dispatch_rpc<RequestVoteArgs, RequestVoteReply>(
            self_, to, a,
            [this, to](const RequestVoteArgs& args) { return net_->get_handlers(to).on_request_vote(args); },
            cb
        );
    }
    
    void send_append_entries(NodeId to, const AppendEntriesArgs& a,
                             std::function<void(const AppendEntriesReply&)> cb) override { 
        net_->dispatch_rpc<AppendEntriesArgs, AppendEntriesReply>(
            self_, to, a,
            [this, to](const AppendEntriesArgs& args) { return net_->get_handlers(to).on_append_entries(args); },
            cb
        );
    }
    
    void send_install_snapshot(NodeId to, const InstallSnapshotArgs& a,
                               std::function<void(const InstallSnapshotReply&)> cb) override { 
        net_->dispatch_rpc<InstallSnapshotArgs, InstallSnapshotReply>(
            self_, to, a,
            [this, to](const InstallSnapshotArgs& args) { return net_->get_handlers(to).on_install_snapshot(args); },
            cb
        );
    }

    const std::vector<NodeId>& peers() const override { return peers_; }

private:
    NodeId self_;
    std::vector<NodeId> peers_;
    Network* net_;
};

} 