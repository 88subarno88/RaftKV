#pragma once
#include "raftkv/types.hpp"
#include <vector>
#include <string>
#include <optional>
#include <filesystem>
#include <fstream>
#include <stdexcept>

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

class FileStorage : public Storage {
public:
    explicit FileStorage(const std::string& dir) : dir_(dir) {
        std::filesystem::create_directories(dir_);
        
    }

    void save_hard_state(const HardState& hs) override {
        // Write to a temporary file first
        std::string tmp_path = dir_ + "/hardstate.tmp";
        std::string final_path = dir_ + "/hardstate.bin";
        
        std::ofstream out(tmp_path, std::ios::binary);
        // serialize hs.current_term and hs.voted_for using encode_u64
        out.flush();
        
        // OS-level sync of the file descriptor to ensure bytes are physically on the drive
        fsync(get_fd(out)); 
        out.close();

        // POSIX rename is atomic. If the machine crashes during this call,
        // we either have the old file intact, or the new file intact. No half-states.
        std::filesystem::rename(tmp_path, final_path);
    }

    void append_entries(const std::vector<LogEntry>& entries) override {
        if (entries.empty()) return;
        
        // Append to the WAL (Write-Ahead Log)
        std::ofstream wal(dir_ + "/wal.bin", std::ios::binary | std::ios::app);
        for (const auto& entry : entries) {
            // ... (serialize entry) ...
        }
        wal.flush();
        fsync(get_fd(wal)); // CRITICAL: Must be on disk before returning!
    }

    void truncate_suffix(LogIndex from_index) override {
        // In a real WAL, truncation is usually handled by writing a special "TRUNCATE X" 
        // marker to the end of the log. When the node recovers, it reads the log, sees 
        // the marker, and ignores any entries physically present after X. 
        // This is vastly faster and safer than trying to physically shrink the file.
        
        std::ofstream wal(dir_ + "/wal.bin", std::ios::binary | std::ios::app);
        // write a truncate marker containing from_index
        wal.flush();
        fsync(get_fd(wal));
    }

    void save_snapshot(LogIndex last_included_index, Term last_included_term, const std::string& data) override {
        // Write the snapshot securely using the Temp + Rename pattern
        std::string tmp_path = dir_ + "/snapshot.tmp";
        std::string final_path = dir_ + "/snapshot.bin";
        
        std::ofstream out(tmp_path, std::ios::binary);
        // serialize index, term, and data
        out.flush();
        fsync(get_fd(out));
        out.close();

        std::filesystem::rename(tmp_path, final_path);

        // Now that the snapshot is safe,  can safely delete the old WAL
        // and start a fresh one, because the snapshot contains everything we need.
        std::filesystem::remove(dir_ + "/wal.bin");
    }

    RecoveredState recover() override {
        RecoveredState state;
        // Read hardstate.bin (if exists)
        // Read snapshot.bin (if exists)
        // Read wal.bin sequentially. If a TRUNCATE marker is found, 
        // discard the in-memory tail and continue.
        return state;
    }

private:
    std::string dir_;
    int get_fd(std::ofstream& stream) {
        // Platform specific logic omitted for brevity. 
        // In purely POSIX C++, use standard POSIX I/O.
        return 0; 
    }
};

} 