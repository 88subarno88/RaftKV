#include "raftkv/state_machine.hpp"
#include <unordered_map>
#include <sstream>
#include <cstdint>

namespace raftkv {
class KVStateMachine : public StateMachine {
public:
    ApplyResult apply(const Command& c) override {
        // Check if have already applied this exact command for this client
        // This makes retried writes idempotent -> linearizable clients
        // NOOP and CONFIG commands skip deduplication.
        if (c.op != Op::NOOP && c.op != Op::CONFIG) {
            auto it = last_seq_.find(c.client_id);
            if (it != last_seq_.end() && it->second >= c.seq_no) {
                return cached_[c.client_id];
            }
        }
        ApplyResult result{false, "", false};
        // Execute the deterministic state machine transition
        switch (c.op) {
            case Op::PUT: {
                store_[c.key] = c.value;
                result = {true, "", true};
                break;
            }
            case Op::GET: {
                auto it = store_.find(c.key);
                if (it != store_.end()) {
                    result = {true, it->second, false};
                } else {
                    // Key not found is still a successful execution, just returns empty
                    result = {true, "", false}; 
                }
                break;
            }
            case Op::DELETE: {
                store_.erase(c.key);
                result = {true, "", true};
                break;
            }
            case Op::CAS: { // Compare-And-Swap
                auto it = store_.find(c.key);
                if (it != store_.end() && it->second == c.expected) {
                    it->second = c.value;
                    result = {true, "", true};
                } else {
                    // Failed condition. Return false, and optionally the current value.
                    result = {false, (it != store_.end() ? it->second : ""), false};
                }
                break;
            }
            case Op::CONFIG:
            case Op::NOOP: {
                result = {true, "", false};
                break;
            }
        }

        // After computing result, record it to prevent duplicate execution 
        if (c.op != Op::NOOP && c.op != Op::CONFIG) {
            last_seq_[c.client_id] = c.seq_no;
            cached_[c.client_id] = result;
        }

        return result;
    }

    std::string snapshot() const override {
        // Serialize store_ and the dedup table to bytes.
        // Done via a simple binary serializer using std::ostringstream.
        std::ostringstream os(std::ios::binary);
        // Serialize the KV store
        uint64_t store_size = store_.size();
        write_pod(os, store_size);
        for (const auto& [k, v] : store_) {
            write_string(os, k);
            write_string(os, v);
        }
        // Serialize the client session table (last_seq_ and cached_)
        uint64_t sessions_size = last_seq_.size();
        write_pod(os, sessions_size);
        for (const auto& [client_id, seq] : last_seq_) {
            write_pod(os, client_id);
            write_pod(os, seq);
            const ApplyResult& res = cached_.at(client_id);
            write_pod(os, res.success);
            write_string(os, res.value);
            write_pod(os, res.mutated);
        }

        return os.str();
    }
    void restore(const std::string& bytes) override {
        // Clear + deserialize store_ and session table from bytes.
        store_.clear();
        last_seq_.clear();
        cached_.clear();

        if (bytes.empty()) return;
        std::istringstream is(bytes, std::ios::binary);
        // Deserialize the KV store
        uint64_t store_size = 0;
        read_pod(is, store_size);
        for (uint64_t i = 0; i < store_size; ++i) {
            std::string k = read_string(is);
            std::string v = read_string(is);
            store_[k] = v;
        }
        // Deserialize the client session table
        uint64_t sessions_size = 0;
        read_pod(is, sessions_size);
        for (uint64_t i = 0; i < sessions_size; ++i) {
            uint64_t client_id = 0;
            uint64_t seq = 0;
            read_pod(is, client_id);
            read_pod(is, seq);
            ApplyResult res;
            read_pod(is, res.success);
            res.value = read_string(is);
            read_pod(is, res.mutated);
            last_seq_[client_id] = seq;
            cached_[client_id] = res;
        }
    }

private:
    std::unordered_map<std::string, std::string> store_;
    // Client session tables for deduplication:
    std::unordered_map<uint64_t /*client_id*/, uint64_t /*last_seq*/> last_seq_;
    std::unordered_map<uint64_t /*client_id*/, ApplyResult> cached_;

    //Helper Serialization Functions 
    template<typename T>
    void write_pod(std::ostream& os, const T& val) const {
        os.write(reinterpret_cast<const char*>(&val), sizeof(T));
    }
    template<typename T>
    void read_pod(std::istream& is, T& val) {
        is.read(reinterpret_cast<char*>(&val), sizeof(T));
    }
    void write_string(std::ostream& os, const std::string& str) const {
        uint64_t len = str.size();
        write_pod(os, len);
        if (len > 0) {
            os.write(str.data(), len);
        }
    }

    std::string read_string(std::istream& is) {
        uint64_t len = 0;
        read_pod(is, len);
        if (len == 0) return "";
        std::string str(len, '\0');
        is.read(str.data(), len);
        return str;
    }
};

} 