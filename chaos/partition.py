#!/usr/bin/env python3
# ============================================================================
# partition.py — Jepsen-lite chaos harness (drives real multi-process cluster)
# ============================================================================

import argparse
import json
import os
import random
import signal
import subprocess
import time
import threading
import shutil
import atexit

# Configuration
BINARY_SERVER = "./raftkv-server"
BINARY_CLI = "./kvcli"
BASE_PORT = 9000
DATA_DIR = "./chaos_data"

class Cluster:
    def __init__(self, num_nodes, base_port, data_dir):
        self.num_nodes = num_nodes
        self.base_port = base_port
        self.data_dir = data_dir
        self.procs = {}  # node_id -> subprocess.Popen
        self.peers_str = ",".join([f"{i}=127.0.0.1:{base_port + i}" for i in range(num_nodes)])
        # Ensure cleanup on script exit (Ctrl+C or crash)
        atexit.register(self.teardown)
    def start_all(self):
        print(f"[*] Starting cluster of {self.num_nodes} nodes...")
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir)
        for i in range(self.num_nodes):
            self.start_node(i)
        time.sleep(2) # Give them a moment to elect an initial leader

    def start_node(self, node_id):
        node_dir = os.path.join(self.data_dir, f"n{node_id}")
        os.makedirs(node_dir, exist_ok=True)
        
        cmd = [
            BINARY_SERVER,
            "--id", str(node_id),
            "--peers", self.peers_str,
            "--data", node_dir
        ]
        
        # Open output to devnull to prevent console spam, or pipe to log files
        log_file = open(os.path.join(node_dir, "stdout.log"), "w")
        proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
        self.procs[node_id] = proc
        print(f"    -> Started Node {node_id} (PID: {proc.pid})")
    def kill_node(self, node_id):
        if node_id in self.procs and self.procs[node_id].poll() is None:
            print(f"[!] SIGKILL Node {node_id}")
            self.procs[node_id].kill()
            self.procs[node_id].wait()

    def pause_node(self, node_id):
        if node_id in self.procs and self.procs[node_id].poll() is None:
            print(f"[!] SIGSTOP (Pause) Node {node_id}")
            os.kill(self.procs[node_id].pid, signal.SIGSTOP)

    def resume_node(self, node_id):
        if node_id in self.procs and self.procs[node_id].poll() is None:
            print(f"[*] SIGCONT (Resume) Node {node_id}")
            os.kill(self.procs[node_id].pid, signal.SIGCONT)

    def find_leader(self):
        # found the leader by executing a dummy GET via our CLI and parsing the redirect.
        # This tests the actual end-to-end client routing experience.
        cmd = [BINARY_CLI, "--nodes", self.peers_str, "get", "__dummy_leader_check__"]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
            for line in res.stdout.split('\n'):
                if "Redirected to node" in line:
                    # Parse "-> Redirected to node 2."
                    leader_id = int(line.strip().split()[-1].strip('.'))
                    return leader_id
                if "OK" in line or "Key not found" in line:
                    # If it didn't redirect, the node we hit first (usually 0) is the leader
                    return 0 
        except subprocess.TimeoutExpired:
            return None
        return None

    def partition(self, groups):
        # Emulates network partitions using local iptables (Requires sudo!)
        # Groups is a list of lists: e.g. [[0,1], [2,3,4]]
        print(f"[!] Partitioning network into groups: {groups}")
        self.heal() # Clear existing rules first
        
        # Drop traffic between nodes in different groups
        for i, group_a in enumerate(groups):
            for group_b in groups[i+1:]:
                for node_a in group_a:
                    for node_b in group_b:
                        port_a = self.base_port + node_a
                        port_b = self.base_port + node_b
                        
                        # Block A -> B
                        subprocess.run(["sudo", "iptables", "-A", "INPUT", "-p", "tcp", "--dport", str(port_b), "-m", "tcp", "--sport", str(port_a), "-j", "DROP"], check=False)
                        # Block B -> A
                        subprocess.run(["sudo", "iptables", "-A", "INPUT", "-p", "tcp", "--dport", str(port_a), "-m", "tcp", "--sport", str(port_b), "-j", "DROP"], check=False)

    def heal(self):
        print("[*] Healing network partitions (flushing iptables)...")
        # Flushes all rules. WARNING: In a real CI environment, you'd want a dedicated custom iptables chain to avoid clearing unrelated host rules.
        subprocess.run(["sudo", "iptables", "-F"], check=False)

    def teardown(self):
        print("\n[*] Tearing down cluster...")
        self.heal()
        for node_id, proc in self.procs.items():
            if proc.poll() is None:
                proc.kill()

# Workload Generator


def workload_writes(cluster, duration, history_file):
    print(f"[*] Starting background workload for {duration} seconds...")
    end_time = time.time() + duration
    
    with open(history_file, 'w') as f:
        req_id = 0
        while time.time() < end_time:
            req_id += 1
            op = random.choice(["put", "get", "cas"])
            key = f"key_{random.randint(1, 5)}"
            val = f"val_{random.randint(1, 100)}"
            exp = f"val_{random.randint(1, 100)}"
            
            cmd = [BINARY_CLI, "--nodes", cluster.peers_str, op, key]
            if op == "put":
                cmd.append(val)
            elif op == "cas":
                cmd.extend([exp, val])

            t_start = time.time()
            
            # Execute CLI
            ok = False
            res_val = ""
            try:
                # second timeout. If it hangs longer, the cluster has lost quorum.
                res = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
                output = res.stdout
                
                if "OK" in output or "CAS successful" in output:
                    ok = True
                elif "Value:" in output:
                    ok = True
                    res_val = output.split("Value:")[1].strip()
                elif "Key not found" in output:
                    ok = True
                    res_val = ""
                else:
                    ok = False # Timeout, max retries, or CAS failure
            except subprocess.TimeoutExpired:
                ok = False

            t_end = time.time()

            # Log to history for the linearizability checker
            record = {
                "id": req_id,
                "op": op.upper(),
                "key": key,
                "value": val if op in ["put", "cas"] else "",
                "expected": exp if op == "cas" else "",
                "ok": ok,
                "res_val": res_val,
                "start_time": t_start,
                "end_time": t_end
            }
            f.write(json.dumps(record) + "\n")
            f.flush()
            
            # Throttle slightly
            time.sleep(0.05)

# Scenarios


def scenario_leader_kill(cluster, duration):
    """The headline GIF scenario: steady writes + kill_leader, measure stall, recover."""
    worker = threading.Thread(target=workload_writes, args=(cluster, duration, "history.jsonl"))
    worker.start()

    time.sleep(5)
    
    leader = cluster.find_leader()
    if leader is not None:
        print(f"\n[SCENARIO] Identified Leader: Node {leader}. Assassinating...")
        t_kill = time.time()
        cluster.kill_node(leader)
        
        # Measure how long re-election takes
        new_leader = None
        while new_leader is None or new_leader == leader:
            new_leader = cluster.find_leader()
            time.sleep(0.1)
        
        t_recover = time.time()
        print(f"[SCENARIO] Re-election completed in {t_recover - t_kill:.2f} seconds! New Leader: Node {new_leader}")
        
        time.sleep(5)
        print(f"[SCENARIO] Restarting dead Node {leader} to test disk recovery...")
        cluster.start_node(leader)
    
    worker.join()

def scenario_partition_flap(cluster, duration):
    """Repeatedly partition and heal under load."""
    worker = threading.Thread(target=workload_writes, args=(cluster, duration, "history.jsonl"))
    worker.start()

    end_time = time.time() + duration
    while time.time() < end_time - 5: # Leave 5 seconds of peace at the end
        time.sleep(3)
        
        # Split a 5 node cluster into a 2-node and 3-node partition
        nodes = list(range(cluster.num_nodes))
        random.shuffle(nodes)
        split = 2
        cluster.partition([nodes[:split], nodes[split:]])
        
        time.sleep(4)
        cluster.heal()

    worker.join()


def scenario_kill_restart(cluster, duration):
    """Randomly kill and restart minority nodes forever."""
    worker = threading.Thread(target=workload_writes, args=(cluster, duration, "history.jsonl"))
    worker.start()

    end_time = time.time() + duration
    while time.time() < end_time - 3:
        time.sleep(2)
        
        # Pick a minority (e.g., 2 nodes out of 5)
        minority_size = (cluster.num_nodes // 2)
        victims = random.sample(range(cluster.num_nodes), minority_size)
        
        for v in victims:
            cluster.kill_node(v)
            
        time.sleep(3)
        
        for v in victims:
            cluster.start_node(v)

    worker.join()

# Main Entry Point


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RaftKV Jepsen-Lite Chaos Harness")
    parser.add_argument("--scenario", choices=["leader_kill", "partition_flap", "kill_restart"], required=True)
    parser.add_argument("--nodes", type=int, default=5, help="Number of nodes in the cluster")
    parser.add_argument("--duration", type=int, default=30, help="Duration of the test in seconds")
    
    args = parser.parse_args()

    # Pre-flight check
    if not os.path.exists(BINARY_SERVER) or not os.path.exists(BINARY_CLI):
        print(f"Error: Could not find {BINARY_SERVER} or {BINARY_CLI}.")
        print("Please compile the project first.")
        exit(1)

    cluster = Cluster(args.nodes, BASE_PORT, DATA_DIR)
    cluster.start_all()

    try:
        if args.scenario == "leader_kill":
            scenario_leader_kill(cluster, args.duration)
        elif args.scenario == "partition_flap":
            scenario_partition_flap(cluster, args.duration)
        elif args.scenario == "kill_restart":
            scenario_kill_restart(cluster, args.duration)
            
        print("\n[*] Scenario completed successfully.")
        print("[*] History written to history.jsonl. Feed this to your linearizability checker!")
        
    except KeyboardInterrupt:
        print("\n[*] Interrupted by user. Exiting...")
    finally:
        # Ate-exit handler will clean up cluster
        pass