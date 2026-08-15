#pragma once
#include "raftkv/rpc.hpp"
#include <functional>
#include <vector>
#include <map>
#include <memory>

namespace raftkv {

// The Raft core registers handlers here; the transport calls them when a message
// arrives FROM a peer. (Incoming direction.)
struct RpcHandlers {
    std::function<RequestVoteReply(const RequestVoteArgs&)>        on_request_vote;
    std::function<AppendEntriesReply(const AppendEntriesArgs&)>    on_append_entries;
    std::function<InstallSnapshotReply(const InstallSnapshotArgs&)> on_install_snapshot;
};

// The Abstract Interface
class Transport {
public:
    virtual ~Transport() = default;
    virtual void set_handlers(RpcHandlers handlers) = 0;
    virtual void send_request_vote(NodeId to, const RequestVoteArgs& args,
                                   std::function<void(const RequestVoteReply&)> cb) = 0;
    virtual void send_append_entries(NodeId to, const AppendEntriesArgs& args,
                                     std::function<void(const AppendEntriesReply&)> cb) = 0;
    virtual void send_install_snapshot(NodeId to, const InstallSnapshotArgs& args,
                                       std::function<void(const InstallSnapshotReply&)> cb) = 0;
    virtual const std::vector<NodeId>& peers() const = 0;
};



class MockTransport; // Forward declaration

// The central "Switch" that connects all mock transports together.
// In  tests, you  tell this network to drop packets, delay them, or 
// sever connections between specific nodes (network partitions).
class MockNetwork {
public:
    void register_node(NodeId id, MockTransport* transport) {
        endpoints_[id] = transport;
    }

    // Network manipulation for tests
    void isolate_node(NodeId id, bool isolated) {
        isolated_[id] = isolated;
    }

    // Routing functions called by the sender's MockTransport
    void route_request_vote(NodeId from, NodeId to, const RequestVoteArgs& args,
                            std::function<void(const RequestVoteReply&)> cb);
    void route_append_entries(NodeId from, NodeId to, const AppendEntriesArgs& args,
                              std::function<void(const AppendEntriesReply&)> cb);      
    void route_install_snapshot(NodeId from, NodeId to, const InstallSnapshotArgs& args,
                                std::function<void(const InstallSnapshotReply&)> cb);

private:
    std::map<NodeId, MockTransport*> endpoints_;
    std::map<NodeId, bool> isolated_; // true if node is partitioned
    
    bool can_communicate(NodeId from, NodeId to) {
        return !isolated_[from] && !isolated_[to];
    }
};

// The endpoint that lives inside each Raft node.
class MockTransport : public Transport {
public:
    MockTransport(NodeId me, MockNetwork* network, std::vector<NodeId> peers)
        : me_(me), network_(network), peers_(std::move(peers)) {
        network_->register_node(me_, this);
    }

    void set_handlers(RpcHandlers handlers) override {
        handlers_ = std::move(handlers);
    }

    void send_request_vote(NodeId to, const RequestVoteArgs& args,
                           std::function<void(const RequestVoteReply&)> cb) override {
        // Delegate routing to the central network
        network_->route_request_vote(me_, to, args, std::move(cb));
    }

    void send_append_entries(NodeId to, const AppendEntriesArgs& args,
                             std::function<void(const AppendEntriesReply&)> cb) override {
        network_->route_append_entries(me_, to, args, std::move(cb));
    }

    void send_install_snapshot(NodeId to, const InstallSnapshotArgs& args,
                               std::function<void(const InstallSnapshotReply&)> cb) override {
        network_->route_install_snapshot(me_, to, args, std::move(cb));
    }

    const std::vector<NodeId>& peers() const override {
        return peers_;
    }

    // Called by the network to deliver an incoming RPC to this node
    RpcHandlers& get_handlers() { return handlers_; }

private:
    NodeId me_;
    MockNetwork* network_;
    std::vector<NodeId> peers_;
    RpcHandlers handlers_;
};

//MockNetwork Routing Implementation

inline void MockNetwork::route_request_vote(NodeId from, NodeId to, const RequestVoteArgs& args,
                                            std::function<void(const RequestVoteReply&)> cb) {
    if (!can_communicate(from, to)) return; // Drop packet silently (simulating network partition)
    
    auto receiver_it = endpoints_.find(to);
    if (receiver_it != endpoints_.end() && receiver_it->second->get_handlers().on_request_vote) {
        // Execute receiver's handler and immediately invoke the sender's callback.
        // In a true async simulator,  would push this into an event queue 
        // rather than executing it synchronously.
        RequestVoteReply reply = receiver_it->second->get_handlers().on_request_vote(args);
        
        if (can_communicate(to, from)) { // Check if reply route is also open
            cb(reply);
        }
    }
}

inline void MockNetwork::route_append_entries(NodeId from, NodeId to, const AppendEntriesArgs& args,
                                              std::function<void(const AppendEntriesReply&)> cb) {
    if (!can_communicate(from, to)) return; 
    auto receiver_it = endpoints_.find(to);
    if (receiver_it != endpoints_.end() && receiver_it->second->get_handlers().on_append_entries) {
        AppendEntriesReply reply = receiver_it->second->get_handlers().on_append_entries(args);
        if (can_communicate(to, from)) {
            cb(reply);
        }
    }
}

inline void MockNetwork::route_install_snapshot(NodeId from, NodeId to, const InstallSnapshotArgs& args,
                                                std::function<void(const InstallSnapshotReply&)> cb) {
    if (!can_communicate(from, to)) return;
    
    auto receiver_it = endpoints_.find(to);
    if (receiver_it != endpoints_.end() && receiver_it->second->get_handlers().on_install_snapshot) {
        InstallSnapshotReply reply = receiver_it->second->get_handlers().on_install_snapshot(args);
        if (can_communicate(to, from)) {
            cb(reply);
        }
    }
}

} 