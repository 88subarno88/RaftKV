#pragma once
#include "raftkv/types.hpp"
#include <map>
#include <string>
#include <cstdint>
#include <stdexcept>

namespace raftkv {

// The Abstract Interface 
class StateMachine {
public:
    virtual ~StateMachine() = default;

    // Apply one committed command and return its result.
    virtual ApplyResult apply(const Command& cmd) = 0;

    // Serialize the ENTIRE current state to bytes (for a snapshot).
    virtual std::string snapshot() const = 0;

    // Replace current state with the deserialized snapshot bytes.
    virtual void restore(const std::string& snapshot_bytes) = 0;
};


// Key-Value Store
class KVStateMachine : public StateMachine {
public:
    ApplyResult apply(const Command& cmd) override {
        // Deduplication (Exactly-Once Semantics / Linearizability)
        // If a client timed out and retried a command that was actually committed,
        // we must not apply it twice. We return the cached result instead.
        if (cmd.client_id != 0) {
            auto session_it = client_sessions_.find(cmd.client_id);
            if (session_it != client_sessions_.end()) {
                if (cmd.seq_no < session_it->second.last_seq_no) {
                    return {false, "Stale sequence number", false};
                }
                if (cmd.seq_no == session_it->second.last_seq_no) {
                    return session_it->second.last_result; // Return cached result!
                }
            }
        }

        // Execute the state machine logic
        ApplyResult res;
        res.ok = true;

        switch (cmd.op) {
            case Op::NOOP:
            case Op::CONFIG:
                // Do nothing, just return success.
                break;
                
            case Op::GET: {
                auto it = kv_store_.find(cmd.key);
                if (it != kv_store_.end()) {
                    res.value = it->second;
                    res.found = true;
                } else {
                    res.found = false;
                }
                break;
            }
            case Op::PUT:
                kv_store_[cmd.key] = cmd.value;
                break;
                
            case Op::DELETE:
                kv_store_.erase(cmd.key);
                break;
                
            case Op::CAS: {
                auto it = kv_store_.find(cmd.key);
                std::string current_val = (it != kv_store_.end()) ? it->second : "";
                
                if (current_val == cmd.expected) {
                    kv_store_[cmd.key] = cmd.value;
                    res.ok = true;
                } else {
                    res.ok = false;
                    res.value = current_val; // Let client know what it actually was
                }
                break;
            }
        }

        //  Update the client session cache with the latest result
        if (cmd.client_id != 0) {
            client_sessions_[cmd.client_id] = SessionState{cmd.seq_no, res};
        }

        return res;
    }

    std::string snapshot() const override {
        std::string out;
        
        // Serialize Client Sessions
        encode_u32(out, static_cast<uint32_t>(client_sessions_.size()));
        for (const auto& [client_id, session] : client_sessions_) {
            encode_u64(out, client_id);
            encode_u64(out, session.last_seq_no);
            out.push_back(session.last_result.ok ? 1 : 0);
            out.push_back(session.last_result.found ? 1 : 0);
            encode_str(out, session.last_result.value);
        }

        // Serialize KV Store
        encode_u32(out, static_cast<uint32_t>(kv_store_.size()));
        for (const auto& [k, v] : kv_store_) {
            encode_str(out, k);
            encode_str(out, v);
        }

        return out;
    }

    void restore(const std::string& data) override {
        kv_store_.clear();
        client_sessions_.clear();
        size_t offset = 0;

        // Restore Client Sessions
        uint32_t num_sessions = 0;
        if (!decode_u32(data, offset, num_sessions)) throw std::runtime_error("Corrupt snapshot");
        
        for (uint32_t i = 0; i < num_sessions; ++i) {
            uint64_t client_id, seq_no;
            if (!decode_u64(data, offset, client_id)) throw std::runtime_error("Corrupt snapshot");
            if (!decode_u64(data, offset, seq_no)) throw std::runtime_error("Corrupt snapshot");
            
            if (offset + 2 > data.size()) throw std::runtime_error("Corrupt snapshot");
            bool ok = data[offset++] != 0;
            bool found = data[offset++] != 0;
            
            std::string val;
            if (!decode_str(data, offset, val)) throw std::runtime_error("Corrupt snapshot");
            
            client_sessions_[client_id] = SessionState{seq_no, ApplyResult{ok, val, found}};
        }

        // Restore KV Store
        uint32_t num_kv = 0;
        if (!decode_u32(data, offset, num_kv)) throw std::runtime_error("Corrupt snapshot");
        
        for (uint32_t i = 0; i < num_kv; ++i) {
            std::string k, v;
            if (!decode_str(data, offset, k)) throw std::runtime_error("Corrupt snapshot");
            if (!decode_str(data, offset, v)) throw std::runtime_error("Corrupt snapshot");
            kv_store_[k] = v;
        }
    }

private:
    struct SessionState {
        uint64_t last_seq_no;
        ApplyResult last_result;
    };

    //  use std::map (Red-Black Tree), NOT std::unordered_map (Hash Table).
    // Iterating over a hash table yields non-deterministic order across different nodes 
    // and process restarts. Using std::map guarantees the snapshot() byte output is 
    // identically ordered bit-for-bit on every single Raft replica.
    std::map<std::string, std::string> kv_store_;
    std::map<uint64_t, SessionState> client_sessions_;

    // Serialization Helpers

    static void encode_u32(std::string& out, uint32_t val) {
        for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((val >> (i * 8)) & 0xFF));
    }
    static void encode_u64(std::string& out, uint64_t val) {
        for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((val >> (i * 8)) & 0xFF));
    }
    static void encode_str(std::string& out, const std::string& str) {
        encode_u32(out, static_cast<uint32_t>(str.size()));
        out.append(str);
    }

    static bool decode_u32(const std::string& data, size_t& offset, uint32_t& val) {
        if (offset + 4 > data.size()) return false;
        val = 0;
        for (int i = 0; i < 4; ++i) val |= (static_cast<uint32_t>(static_cast<unsigned char>(data[offset++])) << (i * 8));
        return true;
    }
    static bool decode_u64(const std::string& data, size_t& offset, uint64_t& val) {
        if (offset + 8 > data.size()) return false;
        val = 0;
        for (int i = 0; i < 8; ++i) val |= (static_cast<uint64_t>(static_cast<unsigned char>(data[offset++])) << (i * 8));
        return true;
    }
    static bool decode_str(const std::string& data, size_t& offset, std::string& str) {
        uint32_t len;
        if (!decode_u32(data, offset, len)) return false;
        if (offset + len > data.size()) return false;
        str = data.substr(offset, len);
        offset += len;
        return true;
    }
};

} 