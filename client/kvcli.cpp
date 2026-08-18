#include "raftkv/types.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <random>
#include <chrono>
#include <thread>
#include <sstream>

using namespace raftkv;

// Network Stub 
// In a real implementation, this wraps your gRPC stub or HTTP client.
struct RpcResponse {
    enum Status { OK, REDIRECT, TIMEOUT_OR_ERROR };
    Status      status;
    NodeId      leader_hint; // Populated if status == REDIRECT
    ApplyResult result;      // Populated if status == OK
};

RpcResponse send_rpc(const std::string& node_addr, const Command& cmd) {
    // await the response, and deserialize it into an RpcResponse.
    // For now, we simulate a network failure to force compiler correctness.
    return {RpcResponse::TIMEOUT_OR_ERROR, -1, {}}; 
}

// Parse comma-separated nodes 
std::vector<std::string> parse_nodes(const std::string& nodes_str) {
    std::vector<std::string> nodes;
    std::stringstream ss(nodes_str);
    std::string item;
    while (std::getline(ss, item, ',')) {
        nodes.push_back(item);
    }
    return nodes;
}

// Main
int main(int argc, char** argv) {
    if (argc < 4) {
        std::cerr << "USAGE: kvcli --nodes host0:910,host1:910 put foo bar\n";
        std::cerr << "       kvcli --nodes host0:910,host1:910 get foo\n";
        std::cerr << "       kvcli --nodes host0:910,host1:910 del foo\n";
        std::cerr << "       kvcli --nodes host0:910,host1:910 cas foo expected_val new_val\n";
        return 1;
    }

    // Parse flags
    std::string nodes_flag = argv[1];
    std::string nodes_list = argv[2];
    if (nodes_flag != "--nodes") {
        std::cerr << "Error: Expected --nodes flag.\n";
        return 1;
    }
    std::vector<std::string> nodes = parse_nodes(nodes_list);
    if (nodes.empty()) {
        std::cerr << "Error: No nodes provided.\n";
        return 1;
    }
    std::string op_str = argv[3];
    Command cmd;
    if (op_str == "put" && argc == 6) {
        cmd.op = Op::PUT;
        cmd.key = argv[4];
        cmd.value = argv[5];
    } else if (op_str == "get" && argc == 5) {
        cmd.op = Op::GET;
        cmd.key = argv[4];
    } else if (op_str == "del" && argc == 5) {
        cmd.op = Op::DELETE;
        cmd.key = argv[4];
    } else if (op_str == "cas" && argc == 7) {
        cmd.op = Op::CAS;
        cmd.key = argv[4];
        cmd.expected = argv[5];
        cmd.value = argv[6];
    } else {
        std::cerr << "Error: Invalid command or arguments.\n";
        return 1;
    }

    // Linearizability Setup: Generate a unique Client ID
    // use a cryptographically strong(ish) random 64-bit int for this session.
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint64_t> dist(1, std::numeric_limits<uint64_t>::max());
    cmd.client_id = dist(gen);
    cmd.seq_no    = 1; // For a CLI tool that runs one command and exits, this is always 1.

    //  The Retry Loop
    int max_retries = 15;
    int current_node_idx = 0; // Start at the first node in the list
    
    for (int attempt = 0; attempt < max_retries; ++attempt) {
        std::string target_addr = nodes[current_node_idx];
        std::cout << "[Attempt " << attempt + 1 << "] Sending to " << target_addr << "...\n";

        //  send the EXACT SAME cmd (same seq_no). 
        // If the previous attempt actually committed but the network dropped the reply, 
        // the Raft leader will see the same seq_no, skip applying it twice, 
        // and return the cached result.
        RpcResponse resp = send_rpc(target_addr, cmd);

        if (resp.status == RpcResponse::OK) {
            // Success, Print result and exit.
            if (cmd.op == Op::GET) {
                if (resp.result.found) {
                    std::cout << "Value: " << resp.result.value << "\n";
                } else {
                    std::cout << "Key not found.\n";
                }
            } else if (cmd.op == Op::CAS) {
                if (resp.result.ok) {
                    std::cout << "CAS successful.\n";
                } else {
                    std::cout << "CAS failed. Current value was: " << resp.result.value << "\n";
                }
            } else {
                std::cout << "OK.\n";
            }
            return 0; // Exit successfully
        } 
        else if (resp.status == RpcResponse::REDIRECT) {
            std::cout << "-> Redirected to node " << resp.leader_hint << ".\n";
            // Update current node to the leader hint. 
            // (Assuming leader_hint maps directly to our array index for simplicity here)
            if (resp.leader_hint >= 0 && resp.leader_hint < static_cast<int>(nodes.size())) {
                current_node_idx = resp.leader_hint;
            } else {
                // Invalid hint, fallback to round-robin
                current_node_idx = (current_node_idx + 1) % nodes.size();
            }
        } 
        else if (resp.status == RpcResponse::TIMEOUT_OR_ERROR) {
            std::cout << "-> Timeout or Error. Leader might be down or electing.\n";
            // Try the next node in the list (Round-Robin)
            current_node_idx = (current_node_idx + 1) % nodes.size();
            
            // Backoff slightly to avoid spamming a cluster during an election
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
    }

    std::cerr << "Error: Max retries exceeded. Cluster unavailable.\n";
    return 1;
}