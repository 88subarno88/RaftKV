#pragma once
#include <cstdint>
#include <string>
#include <optional>
#include <vector>

namespace raftkv {

//Logical identifiers 
using NodeId= int;       // small integer id per node (0..N-1). Simple & printable.
using Term= uint64_t;  // Raft term -> a monotonically increasing logical clock.
using LogIndex=uint64_t;  // 1-based position in the replicated log. Index 0 == "empty".

// This is the payload that gets replicated
// A config-change command (add/remove node) also travels through the log as a
// special op, which is why membership changes are just another log entry.
enum class Op : uint8_t { NOOP, PUT, GET, DELETE, CAS, CONFIG };

struct Command {
    Op          op = Op::NOOP;
    std::string key;
    std::string value;      // for PUT / CAS(new)
    std::string expected;   // for CAS(old) swap only if current == expected
    uint64_t    client_id = 0; // for dedup of retried commands (linearizable clients)
    uint64_t    seq_no = 0;    // per-client monotonically increasing request id

    
    // Serializes the command into a deterministic byte string
    std::string serialize() const {
        std::string out;
        
        //op (1 byte)
        out.push_back(static_cast<char>(op));
        
        //client_id & seq_no (8 bytes each, little-endian)
        encode_u64(out, client_id);
        encode_u64(out, seq_no);
        
        //strings (4 bytes length prefix + data)
        encode_str(out, key);
        encode_str(out, value);
        encode_str(out, expected);
        
        return out;
    }

    // Deserializes from a byte string. Returns false if the data is corrupted/incomplete.
    bool deserialize(const std::string& data) {
        size_t offset = 0;
        //fail: Minimum possible size (1 byte op+2x 8-byte ints+3x 4-byte lengths)
        if (data.size() < 1 + 16 + 12) return false;
        op = static_cast<Op>(data[offset++]);
        if (!decode_u64(data, offset, client_id)) return false;
        if (!decode_u64(data, offset, seq_no)) return false;
        if (!decode_str(data, offset, key)) return false;
        if (!decode_str(data, offset, value)) return false;
        if (!decode_str(data, offset, expected)) return false;
        return true;
    }

private:
    //Helpers

    static void encode_u64(std::string& out, uint64_t val) {
        for (int i = 0; i < 8; ++i) {
            out.push_back(static_cast<char>((val >> (i * 8)) & 0xFF));
        }
    }

    static bool decode_u64(const std::string& data, size_t& offset, uint64_t& val) {
        if (offset + 8 > data.size()) return false;
        val = 0;
        for (int i = 0; i < 8; ++i) {
            val |= (static_cast<uint64_t>(static_cast<unsigned char>(data[offset++])) << (i * 8));
        }
        return true;
    }

    static void encode_str(std::string& out, const std::string& str) {
        uint32_t len = static_cast<uint32_t>(str.size());
        for (int i = 0; i < 4; ++i) {
            out.push_back(static_cast<char>((len >> (i * 8)) & 0xFF));
        }
        out.append(str);
    }

    static bool decode_str(const std::string& data, size_t& offset, std::string& str) {
        if (offset + 4 > data.size()) return false;
        
        uint32_t len = 0;
        for (int i = 0; i < 4; ++i) {
            len |= (static_cast<uint32_t>(static_cast<unsigned char>(data[offset++])) << (i * 8));
        }
        
        if (offset + len > data.size()) return false;
        str = data.substr(offset, len);
        offset += len;
        
        return true;
    }
};

// One entry in the replicated log
// INVARIANT : if two logs contain an entry with the same
// index AND term, the entries are identical AND all preceding entries match.
struct LogEntry {
    Term       term  = 0;   // term in which the leader created this entry
    LogIndex   index = 0;   // its position in the log (1-based)
    Command    cmd;         // the actual command to apply once committed
};

//Result of applying a command to the state machine
struct ApplyResult {
    bool        ok = true;
    std::string value;      // GET returns the value here; empty + found=false if missing
    bool        found = false;
};

// Role of a node at any instant .
enum class Role : uint8_t { Follower, Candidate, Leader };

inline const char* to_string(Role r) {
    switch (r) {
        case Role::Follower:  return "Follower";
        case Role::Candidate: return "Candidate";
        case Role::Leader:    return "Leader";
    }
    return "?";
}

} 