#include "raftkv/transport.hpp"
#include <grpcpp/grpcpp.h>
// #include "raft.grpc.pb.h"  // generated from proto/raft.proto
#include <future>
#include <memory>
#include <chrono>

namespace raftkv {

namespace pb { 
    // Mocking the pb namespace to represent generated protobuf classes
    struct RequestVoteArgs {}; struct RequestVoteReply {};
    struct AppendEntriesArgs {}; struct AppendEntriesReply {};
    struct InstallSnapshotArgs {}; struct InstallSnapshotReply {};
    
    class RaftService {
    public:
        class Service {
        public:
            virtual ~Service() = default;
            virtual grpc::Status RequestVote(grpc::ServerContext*, const pb::RequestVoteArgs*, pb::RequestVoteReply*)
             { return grpc::Status::OK; }
            virtual grpc::Status AppendEntries(grpc::ServerContext*, const pb::AppendEntriesArgs*, pb::AppendEntriesReply*)
             { return grpc::Status::OK; }
            virtual grpc::Status InstallSnapshot(grpc::ServerContext*, const pb::InstallSnapshotArgs*, pb::InstallSnapshotReply*)
             { return grpc::Status::OK; }
        };
        class Stub {
        public:
            virtual ~Stub() = default;
            // Modern gRPC callback API representations
            struct AsyncStub {
                virtual void RequestVote(grpc::ClientContext*, const pb::RequestVoteArgs*, pb::RequestVoteReply*, std::function<void(grpc::Status)>) {}
                virtual void AppendEntries(grpc::ClientContext*, const pb::AppendEntriesArgs*, pb::AppendEntriesReply*, std::function<void(grpc::Status)>) {}
                virtual void InstallSnapshot(grpc::ClientContext*, const pb::InstallSnapshotArgs*, pb::InstallSnapshotReply*, std::function<void(grpc::Status)>) {}
            };
            virtual AsyncStub* async() { return nullptr; }
        };
        static std::unique_ptr<Stub> NewStub(std::shared_ptr<grpc::Channel>) { return std::make_unique<Stub>(); }
    };
} 

// Example conversion stubs  populate these with actual protobuf setters/getters
static pb::RequestVoteArgs to_pb(const RequestVoteArgs& a) { return {}; }
static RequestVoteReply from_pb(const pb::RequestVoteReply& p) { return {}; }
static pb::AppendEntriesArgs to_pb(const AppendEntriesArgs& a) { return {}; }
static AppendEntriesReply from_pb(const pb::AppendEntriesReply& p) { return {}; }
static pb::InstallSnapshotArgs to_pb(const InstallSnapshotArgs& a) { return {}; }
static InstallSnapshotReply from_pb(const pb::InstallSnapshotReply& p) { return {}; }
static RequestVoteArgs from_pb_args(const pb::RequestVoteArgs& p) { return {}; }
static pb::RequestVoteReply to_pb_reply(const RequestVoteReply& p) { return {}; }
static AppendEntriesArgs from_pb_args(const pb::AppendEntriesArgs& p) { return {}; }
static pb::AppendEntriesReply to_pb_reply(const AppendEntriesReply& p) { return {}; }
static InstallSnapshotArgs from_pb_args(const pb::InstallSnapshotArgs& p) { return {}; }
static pb::InstallSnapshotReply to_pb_reply(const InstallSnapshotReply& p) { return {}; }

class RaftServiceImpl final : public pb::RaftService::Service {
public:
    using PostTaskFn = std::function<void(std::function<void()>)>;
    RaftServiceImpl(PostTaskFn post_to_loop) : post_to_loop_(std::move(post_to_loop)) {}
    void set_handlers(RpcHandlers h) { handlers_ = std::move(h); }
    grpc::Status RequestVote(grpc::ServerContext* context, const pb::RequestVoteArgs* req, pb::RequestVoteReply* reply) override {
        return process_rpc<RequestVoteArgs, RequestVoteReply, pb::RequestVoteArgs, pb::RequestVoteReply>(
            req, reply, [this](const RequestVoteArgs& a) { return handlers_.on_request_vote(a); }
        );
    }
    grpc::Status AppendEntries(grpc::ServerContext* context, const pb::AppendEntriesArgs* req, pb::AppendEntriesReply* reply) override {
        return process_rpc<AppendEntriesArgs, AppendEntriesReply, pb::AppendEntriesArgs, pb::AppendEntriesReply>(
            req, reply, [this](const AppendEntriesArgs& a) { return handlers_.on_append_entries(a); }
        );
    }
    grpc::Status InstallSnapshot(grpc::ServerContext* context, const pb::InstallSnapshotArgs* req, pb::InstallSnapshotReply* reply) override {
        return process_rpc<InstallSnapshotArgs, InstallSnapshotReply, pb::InstallSnapshotArgs, pb::InstallSnapshotReply>(
            req, reply, [this](const InstallSnapshotArgs& a) { return handlers_.on_install_snapshot(a); }
        );
    }
private:
    template <typename CppArgs, typename CppReply, typename PbArgs, typename PbReply>
    grpc::Status process_rpc(const PbArgs* req, PbReply* reply, std::function<CppReply(const CppArgs&)> handler) {
        if (!handlers_.on_request_vote) return grpc::Status(grpc::UNAVAILABLE, "Handlers not set");
        std::promise<CppReply> p;
        CppArgs args = from_pb_args(*req);
        
        // Push work to the deterministic Raft event loop and wait for it
        post_to_loop_([&p, args, handler]() {
            p.set_value(handler(args));
        });

        // Block this gRPC thread until the loop finishes processing
        *reply = to_pb_reply(p.get_future().get());
        return grpc::Status::OK;
    }

    PostTaskFn post_to_loop_;
    RpcHandlers handlers_;
};

class GrpcTransport : public Transport {
public:
    using PostTaskFn = std::function<void(std::function<void()>)>;
    GrpcTransport(NodeId self, 
                  std::vector<NodeId> peers,
                  std::map<NodeId, std::string> addrs, /* id -> host:port */
                  PostTaskFn post_to_loop)
        : self_(self), peers_(std::move(peers)), post_to_loop_(std::move(post_to_loop)) 
    {
        // Create client stubs for all peers
        for (NodeId peer : peers_) {
            auto channel = grpc::CreateChannel(addrs[peer], grpc::InsecureChannelCredentials());
            stubs_[peer] = pb::RaftService::NewStub(channel);
        }
        // Start our own gRPC server
        service_ = std::make_unique<RaftServiceImpl>(post_to_loop_);
        grpc::ServerBuilder builder;
        builder.AddListeningPort(addrs[self_], grpc::InsecureServerCredentials());
        builder.RegisterService(service_.get());
        server_ = builder.BuildAndStart();
    }
    ~GrpcTransport() {
        if (server_) {
            server_->Shutdown();
        }
    }
    void set_handlers(RpcHandlers h) override {
        service_->set_handlers(std::move(h));
    }
    void send_request_vote(NodeId to, const RequestVoteArgs& a,
     std::function<void(const RequestVoteReply&)> cb) override {
        dispatch_async(to, a, cb, 
            [](pb::RaftService::Stub::AsyncStub* async_stub, grpc::ClientContext* ctx, const pb::RequestVoteArgs* req, pb::RequestVoteReply* reply, auto callback) {
                async_stub->RequestVote(ctx, req, reply, callback);
            });
    }

    void send_append_entries(NodeId to, const AppendEntriesArgs& a,
    std::function<void(const AppendEntriesReply&)> cb) override {
        dispatch_async(to, a, cb, 
            [](pb::RaftService::Stub::AsyncStub* async_stub, grpc::ClientContext* ctx, const pb::AppendEntriesArgs* req, pb::AppendEntriesReply* reply, auto callback) {
                async_stub->AppendEntries(ctx, req, reply, callback);
            });
    }

    void send_install_snapshot(NodeId to, const InstallSnapshotArgs& a,
    std::function<void(const InstallSnapshotReply&)> cb) override {
        dispatch_async(to, a, cb, 
            [](pb::RaftService::Stub::AsyncStub* async_stub, grpc::ClientContext* ctx, const pb::InstallSnapshotArgs* req, pb::InstallSnapshotReply* reply, auto callback) {
                async_stub->InstallSnapshot(ctx, req, reply, callback);
            });
    }
    const std::vector<NodeId>& peers() const override { return peers_; }

private:
    // Generic dispatcher for async gRPC calls
    template <typename CppArgs, typename CppReply, typename PbArgs, typename PbReply, typename RpcInvoker>
    void dispatch_async(NodeId to, const CppArgs& a, std::function<void(const CppReply&)> cb, RpcInvoker invoker) {
        if (stubs_.find(to) == stubs_.end()) return;

        // Allocate state that must survive until the callback fires
        auto ctx = new grpc::ClientContext();
        ctx->set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(200)); // ~election timeout
        
        auto pb_req = new PbArgs(to_pb(a));
        auto pb_reply = new PbReply();
        auto* async_stub = stubs_[to]->async();
        invoker(async_stub, ctx, pb_req, pb_reply, [this, cb, ctx, pb_req, pb_reply](grpc::Status status) {
            if (status.ok()) {
                auto reply = from_pb(*pb_reply);
                // CRITICAL THREADING RULE: Post to loop, never call Raft inline
                post_to_loop_([cb, reply]() { cb(reply); });
            }
            
            // Cleanup heap-allocated state
            delete pb_reply;
            delete pb_req;
            delete ctx;
        });
    }
    NodeId self_;
    std::vector<NodeId> peers_;
    PostTaskFn post_to_loop_;
    std::map<NodeId, std::unique_ptr<pb::RaftService::Stub>> stubs_;
    std::unique_ptr<RaftServiceImpl> service_;
    std::unique_ptr<grpc::Server> server_;
};

} 