#include "raftkv/types.hpp"
#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <algorithm>
#include <iomanip>
#include <random>

using namespace raftkv;
using namespace std::chrono;

// Network Stub 
struct RpcResponse {
    bool ok;
    NodeId leader_hint;
};

RpcResponse send_rpc(const std::string& node_addr, const Command& cmd) {
    std::this_thread::sleep_for(std::chrono::microseconds(2000));
    return {true, 0}; 
}

// Metrics Collection 
// Aligned to 64 bytes to prevent False Sharing across CPU cache lines
struct alignas(64) ThreadMetrics {
    uint64_t ops = 0;
    uint64_t errors = 0;
    std::vector<uint64_t> latencies_ns;
    ThreadMetrics() {
        // Pre-allocate to avoid reallocation latency spikes during the benchmark
        latencies_ns.reserve(2000000); 
    }
};

// Benchmark Configuration
struct BenchConfig {
    std::vector<std::string> nodes;
    int threads = 8;
    int duration_sec = 10;
    int warmup_sec = 3;
    int value_size_bytes = 256;
};
// Generate a random string payload
std::string make_payload(int size) {
    std::string s(size, 'x');
    for (int i = 0; i < size; ++i) s[i] = 'A' + (rand() % 26);
    return s;
}
// Parse CLI
BenchConfig parse_args(int argc, char** argv) {
    BenchConfig cfg;
    cfg.nodes = {"127.0.0.1:9000", "127.0.0.1:9001", "127.0.0.1:9002"}; // Default 3 nodes
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--threads" && i + 1 < argc) cfg.threads = std::stoi(argv[++i]);
        else if (arg == "--duration" && i + 1 < argc) cfg.duration_sec = std::stoi(argv[++i]);
        else if (arg == "--value-size" && i + 1 < argc) cfg.value_size_bytes = std::stoi(argv[++i]);
    }
    return cfg;
}

// Worker Thread
void worker_loop(int thread_id, const BenchConfig& cfg, ThreadMetrics& metrics, 
                 std::atomic<bool>& is_warmup, std::atomic<bool>& is_running) {
    
    std::string payload = make_payload(cfg.value_size_bytes);
    int current_node = 0; // Simple round-robin or sticky routing

    // Pre-build command to minimize allocation overhead in the hot loop
    Command cmd;
    cmd.op = Op::PUT;
    cmd.value = payload;
    cmd.client_id = 10000 + thread_id; 

    uint64_t seq_no = 1;
    while (is_running.load(std::memory_order_relaxed)) {
        cmd.key = "key_" + std::to_string(thread_id) + "_" + std::to_string(seq_no);
        cmd.seq_no = seq_no++;

        auto t_start = steady_clock::now();
        
        RpcResponse res = send_rpc(cfg.nodes[current_node], cmd);

        auto t_end = steady_clock::now();
        uint64_t latency = duration_cast<nanoseconds>(t_end - t_start).count();

        // Handle Redirects
        if (!res.ok) {
            current_node = res.leader_hint % cfg.nodes.size();
            metrics.errors++;
            continue; 
        }

        // Only record metrics if the warmup phase is over
        if (!is_warmup.load(std::memory_order_relaxed)) {
            metrics.ops++;
            metrics.latencies_ns.push_back(latency);
        }
    }
}

// Main
int main(int argc, char** argv) {
    BenchConfig cfg = parse_args(argc, argv);

    std::cout << "=================================================\n";
    std::cout << " RaftKV Benchmark \n";
    std::cout << "=================================================\n";
    std::cout << " Nodes      : " << cfg.nodes.size() << "\n";
    std::cout << " Threads    : " << cfg.threads << "\n";
    std::cout << " Value Size : " << cfg.value_size_bytes << " B\n";
    std::cout << " Duration   : " << cfg.duration_sec << "s (" << cfg.warmup_sec << "s warmup)\n";
    std::cout << "=================================================\n";
    std::vector<ThreadMetrics> metrics(cfg.threads);
    std::vector<std::thread> threads;
    std::atomic<bool> is_warmup{true};
    std::atomic<bool> is_running{true};
    // Launch threads
    for (int i = 0; i < cfg.threads; ++i) {
        threads.emplace_back(worker_loop, i, std::ref(cfg), std::ref(metrics[i]), 
                             std::ref(is_warmup), std::ref(is_running));
    }
    // Warmup Phase
    std::cout << "[*] Warming up cluster for " << cfg.warmup_sec << " seconds...\n";
    std::this_thread::sleep_for(seconds(cfg.warmup_sec));
    // Benchmark Phase
    std::cout << "[*] Warmup complete. Starting benchmark for " << cfg.duration_sec << " seconds...\n";
    is_warmup.store(false, std::memory_order_release);
    auto bench_start = steady_clock::now();
    std::this_thread::sleep_for(seconds(cfg.duration_sec));
    // Stop and Join
    is_running.store(false, std::memory_order_release);
    auto bench_end = steady_clock::now();
    for (auto& t : threads) t.join();
    double actual_duration_sec = duration_cast<duration<double>>(bench_end - bench_start).count();

    // Aggregate Results
    uint64_t total_ops = 0;
    uint64_t total_errors = 0;
    std::vector<uint64_t> all_latencies;
    // Pre-calculate total size to avoid multiple re-allocations during merge
    size_t total_latency_count = 0;
    for (const auto& m : metrics) total_latency_count += m.latencies_ns.size();
    all_latencies.reserve(total_latency_count);
    for (const auto& m : metrics) {
        total_ops += m.ops;
        total_errors += m.errors;
        all_latencies.insert(all_latencies.end(), m.latencies_ns.begin(), m.latencies_ns.end());
    }
    if (all_latencies.empty()) {
        std::cerr << "Error: No successful operations recorded.\n";
        return 1;
    }

    // Sort for percentiles
    std::sort(all_latencies.begin(), all_latencies.end());

    double ops_per_sec = total_ops / actual_duration_sec;
    
    // Convert to milliseconds for human-readable output
    auto to_ms = [](uint64_t ns) { return static_cast<double>(ns) / 1000000.0; };

    double p50 = to_ms(all_latencies[all_latencies.size() * 0.50]);
    double p90 = to_ms(all_latencies[all_latencies.size() * 0.90]);
    double p99 = to_ms(all_latencies[all_latencies.size() * 0.99]);
    double p999 = to_ms(all_latencies[all_latencies.size() * 0.999]);
    double max_lat = to_ms(all_latencies.back());

    // Print Report Table
    std::cout << "\n=================================================\n";
    std::cout << " RESULTS\n";
    std::cout << "=================================================\n";
    std::cout << " Throughput     : " << std::fixed << std::setprecision(2) << ops_per_sec << " ops/sec\n";
    std::cout << " Total Ops      : " << total_ops << "\n";
    std::cout << " Redirects/Errs : " << total_errors << "\n";
    std::cout << "-------------------------------------------------\n";
    std::cout << " Latency (ms)\n";
    std::cout << "   p50 (Median) : " << p50 << " ms\n";
    std::cout << "   p90          : " << p90 << " ms\n";
    std::cout << "   p99 (Tail)   : " << p99 << " ms\n";
    std::cout << "   p99.9        : " << p999 << " ms\n";
    std::cout << "   Max          : " << max_lat << " ms\n";
    std::cout << "=================================================\n";

    return 0;
}