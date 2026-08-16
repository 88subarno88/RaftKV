#include "raftkv/storage.hpp"
#include <filesystem>
#include <map>
#include <stdexcept>
#include <cstring>
// POSIX APIs for direct file control (fsync, ftruncate, open, read, write)
#if defined(__unix__) || defined(__APPLE__)
#include <unistd.h>
#include <fcntl.h>
#else
#error "This WAL implementation requires POSIX I/O. Windows/MSVC requires FlushFileBuffers."
#endif

namespace raftkv {

namespace {

// Simple, Zero-Dependency CRC32 
uint32_t calculate_crc32(const std::string& data) {
    uint32_t crc = 0xFFFFFFFF;
    for (char c : data) {
        crc ^= static_cast<uint8_t>(c);
        for (int j = 0; j < 8; ++j) {
            crc = (crc & 1) ? (crc >> 1) ^ 0xEDB88320 : (crc >> 1);
        }
    }
    return ~crc;
}

// Deterministic Encoding Helpers
void encode_u32(std::string& out, uint32_t val) {
    for (int i = 0; i < 4; ++i) out.push_back(static_cast<char>((val >> (i * 8)) & 0xFF));
}
void encode_u64(std::string& out, uint64_t val) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((val >> (i * 8)) & 0xFF));
}
bool decode_u32(const std::string& data, size_t& offset, uint32_t& val) {
    if (offset + 4 > data.size()) return false;
    val = 0;
    for (int i = 0; i < 4; ++i) val |= (static_cast<uint32_t>(static_cast<unsigned char>(data[offset++])) << (i * 8));
    return true;
}
bool decode_u64(const std::string& data, size_t& offset, uint64_t& val) {
    if (offset + 8 > data.size()) return false;
    val = 0;
    for (int i = 0; i < 8; ++i) val |= (static_cast<uint64_t>(static_cast<unsigned char>(data[offset++])) << (i * 8));
    return true;
}

// OS-level write block (throws on IO failure)
void exact_write(int fd, const void* buf, size_t count) {
    const char* ptr = static_cast<const char*>(buf);
    while (count > 0) {
        ssize_t written = ::write(fd, ptr, count);
        if (written < 0) throw std::runtime_error("WAL write failed");
        ptr += written;
        count -= written;
    }
}

// OS-level read block (returns false on EOF)
bool exact_read(int fd, void* buf, size_t count) {
    char* ptr = static_cast<char*>(buf);
    while (count > 0) {
        ssize_t r = ::read(fd, ptr, count);
        if (r < 0) throw std::runtime_error("WAL read failed");
        if (r == 0) return false; // Unexpected EOF (Torn Write)
        ptr += r;
        count -= r;
    }
    return true;
}

// Sync directory to ensure atomic renames are durably recorded in the filesystem tree
void sync_dir(const std::string& dir) {
    int fd = ::open(dir.c_str(), O_RDONLY);
    if (fd >= 0) {
        ::fsync(fd);
        ::close(fd);
    }
}

}

class FileStorage : public Storage {
public:
    explicit FileStorage(const std::string& dir) : dir_(dir), wal_fd_(-1) {
        std::filesystem::create_directories(dir_);
        // Open WAL file for read and append
        std::string wal_path = dir_ + "/wal.log";
        wal_fd_ = ::open(wal_path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
        if (wal_fd_ < 0) throw std::runtime_error("Failed to open wal.log");
    }

    ~FileStorage() override {
        if (wal_fd_ >= 0) {
            ::close(wal_fd_);
        }
    }

    void save_hard_state(const HardState& hs) override {
        std::string tmp_path = dir_ + "/hardstate.tmp";
        std::string final_path = dir_ + "/hardstate.bin";
        std::string data;
        encode_u64(data, hs.current_term);
        data.push_back(hs.voted_for.has_value() ? 1 : 0);
        encode_u64(data, hs.voted_for.value_or(0));

        int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) throw std::runtime_error("Failed to open hardstate.tmp");
        exact_write(fd, data.data(), data.size());
        ::fsync(fd);
        ::close(fd);
        std::filesystem::rename(tmp_path, final_path);
        sync_dir(dir_); // Flush the directory entry
    }

    void append_entries(const std::vector<LogEntry>& entries) override {
        if (entries.empty()) return;
        for (const auto& entry : entries) {
            std::string record_data;
            encode_u64(record_data, entry.term);
            encode_u64(record_data, entry.index);
            record_data.append(entry.cmd.serialize());
            uint32_t len = record_data.size();
            uint32_t crc = calculate_crc32(record_data);
            std::string header;
            encode_u32(header, len);
            encode_u32(header, crc);
            // Record the offset before writing
            off_t current_offset = ::lseek(wal_fd_, 0, SEEK_CUR);
            offsets_[entry.index] = current_offset;
            exact_write(wal_fd_, header.data(), header.size());
            exact_write(wal_fd_, record_data.data(), record_data.size());
        }
        // ONE single fsync for the entire batch
        ::fsync(wal_fd_);
    }

    void truncate_suffix(LogIndex from_index) override {
        auto it = offsets_.lower_bound(from_index);
        if (it != offsets_.end()) {
            off_t truncate_at = it->second;
            // Physically truncate the file
            if (::ftruncate(wal_fd_, truncate_at) != 0) {
                throw std::runtime_error("Failed to ftruncate WAL");
            }
            ::fsync(wal_fd_);
            // Remove from memory
            offsets_.erase(it, offsets_.end());
            // Reset file pointer
            ::lseek(wal_fd_, 0, SEEK_END);
        }
    }

    void save_snapshot(LogIndex last_index, Term last_term, const std::string& data) override {
        std::string tmp_path = dir_ + "/snapshot.tmp";
        std::string final_path = dir_ + "/snapshot.bin";
        std::string header;
        encode_u64(header, last_index);
        encode_u64(header, last_term);
        encode_u32(header, data.size());
        int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) throw std::runtime_error("Failed to open snapshot.tmp");
        exact_write(fd, header.data(), header.size());
        exact_write(fd, data.data(), data.size());
        ::fsync(fd);
        ::close(fd);
        std::filesystem::rename(tmp_path, final_path);
        sync_dir(dir_);
        // WAL Compaction ;Discard covered entries physically
        truncate_suffix(last_index + 1);
    }

    RecoveredState recover() override {
        RecoveredState rs;

        //  Read Hard State
        std::string hs_path = dir_ + "/hardstate.bin";
        if (std::filesystem::exists(hs_path)) {
            int fd = ::open(hs_path.c_str(), O_RDONLY);
            if (fd >= 0) {
                std::string data(32, '\0');
                ssize_t r = ::read(fd, data.data(), data.size());
                ::close(fd);
                if (r > 0) {
                    size_t offset = 0;
                    decode_u64(data, offset, rs.hard.current_term);
                    if (data[offset++] == 1) {
                        uint64_t v = 0;
                        decode_u64(data, offset, v);
                        rs.hard.voted_for = static_cast<NodeId>(v);
                    }
                }
            }
        }

        //Read Snapshot
        std::string snap_path = dir_ + "/snapshot.bin";
        if (std::filesystem::exists(snap_path)) {
            int fd = ::open(snap_path.c_str(), O_RDONLY);
            if (fd >= 0) {
                std::string header(20, '\0');
                if (exact_read(fd, header.data(), 20)) {
                    size_t offset = 0;
                    decode_u64(header, offset, rs.snapshot_index);
                    decode_u64(header, offset, rs.snapshot_term);
                    uint32_t len = 0;
                    decode_u32(header, offset, len);

                    rs.snapshot_data.resize(len);
                    exact_read(fd, rs.snapshot_data.data(), len);
                }
                ::close(fd);
            }
        }

        //Scan WAL
        ::lseek(wal_fd_, 0, SEEK_SET);
        off_t valid_length = 0;
        
        while (true) {
            off_t start_offset = ::lseek(wal_fd_, 0, SEEK_CUR);
            std::string header(8, '\0');
            if (!exact_read(wal_fd_, header.data(), 8)) break; // EOF (Clean)
            size_t off = 0;
            uint32_t len = 0, crc = 0;
            decode_u32(header, off, len);
            decode_u32(header, off, crc);
            std::string data(len, '\0');
            if (!exact_read(wal_fd_, data.data(), len)) break; // Torn Write (Middle of data)
            if (calculate_crc32(data) != crc) break; // Corrupt data! Stop reading.
            valid_length = ::lseek(wal_fd_, 0, SEEK_CUR); // Commit the read
            // Deserialize LogEntry
            LogEntry entry;
            size_t data_off = 0;
            decode_u64(data, data_off, entry.term);
            decode_u64(data, data_off, entry.index);
            std::string cmd_bytes = data.substr(data_off);
            entry.cmd.deserialize(cmd_bytes);
            offsets_[entry.index] = start_offset;
            if (entry.index > rs.snapshot_index) {
                rs.log.push_back(entry);
            }
        }

        // Repair torn writes (truncate uncommitted garbage at the end of the file)
        ::ftruncate(wal_fd_, valid_length);
        ::lseek(wal_fd_, 0, SEEK_END);

        return rs;
    }

private:
    std::string dir_;
    int wal_fd_;
    std::map<LogIndex, off_t> offsets_;
};

// Factory injection
std::unique_ptr<Storage> make_file_storage(const std::string& dir) {
    return std::make_unique<FileStorage>(dir);
}

} 