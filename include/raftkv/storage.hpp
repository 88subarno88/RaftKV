#pragma once
#include "raftkv/types.hpp"
#include <vector>
#include <string>
#include <optional>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <memory>

// For fsync() in POSIX systems. (On Windows,  use FlushFileBuffers)
#if defined(__unix__) || defined(__APPLE__)
#include <unistd.h>
#include <fcntl.h>
#else
// Mock for non-POSIX
inline int fsync(int fd) { return 0; }
#endif

namespace raftkv {

// What must be durable across restarts
struct HardState {
    Term                  current_term = 0;
    std::optional<NodeId> voted_for;
};

// The result of reading everything back on startup.
struct RecoveredState {
    HardState             hard;
    std::vector<LogEntry> log;             // entries after the snapshot boundary
    LogIndex              snapshot_index = 0;
    Term                  snapshot_term  = 0;
    std::string           snapshot_data;   // empty if none
};

class Storage {
public:
    virtual ~Storage() = default;

    virtual void save_hard_state(const HardState& hs) = 0;
    virtual void append_entries(const std::vector<LogEntry>& entries) = 0;
    virtual void truncate_suffix(LogIndex from_index) = 0;
    virtual void save_snapshot(LogIndex last_included_index, Term last_included_term, const std::string& data) = 0;
    virtual RecoveredState recover() = 0;
};

class MemoryStorage : public Storage {
public:
    void save_hard_state(const HardState& hs) override {
        hard_ = hs;
    }

    void append_entries(const std::vector<LogEntry>& entries) override {
        log_.insert(log_.end(), entries.begin(), entries.end());
    }

    void truncate_suffix(LogIndex from_index) override {
        if (from_index <= snapshot_index_) {
            log_.clear();
            return;
        }
        size_t physical_keep = from_index - snapshot_index_ - 1;
        if (physical_keep < log_.size()) {
            log_.erase(log_.begin() + physical_keep, log_.end());
        }
    }

    void save_snapshot(LogIndex last_included_index, Term last_included_term, const std::string& data) override {
        if (last_included_index <= snapshot_index_) return; // Ignore stale snapshots

        // Discard the portion of the log that is now in the snapshot
        size_t remove_count = last_included_index - snapshot_index_;
        if (remove_count >= log_.size()) {
            log_.clear();
        } else {
            log_.erase(log_.begin(), log_.begin() + remove_count);
        }

        snapshot_index_ = last_included_index;
        snapshot_term_  = last_included_term;
        snapshot_data_  = data;
    }

    RecoveredState recover() override {
        return {hard_, log_, snapshot_index_, snapshot_term_, snapshot_data_};
    }

private:
    HardState             hard_;
    std::vector<LogEntry> log_;
    LogIndex              snapshot_index_ = 0;
    Term                  snapshot_term_  = 0;
    std::string           snapshot_data_;
};

// Durable, crash-safe storage backed by a CRC32-checksummed write-ahead log.
// The implementation lives in src/storage/wal.cpp (it needs POSIX fsync/ftruncate
// and torn-write recovery), so it is constructed through this factory rather
// than being exposed as a class here.
std::unique_ptr<Storage> make_file_storage(const std::string& dir);

} 