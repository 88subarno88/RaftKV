#include "raftkv/storage.hpp"
#include <filesystem>
#include <stdexcept>
#include <cstring>
// POSIX APIs for direct file control (fsync, open, read, write)
#if defined(__unix__) || defined(__APPLE__)
#include <unistd.h>
#include <fcntl.h>
#else
#error "This implementation requires POSIX I/O for fsync."
#endif

namespace raftkv {

namespace {
// encoding helpers
void encode_u64(std::string& out, uint64_t val) {
    for (int i = 0; i < 8; ++i) out.push_back(static_cast<char>((val >> (i * 8)) & 0xFF));
}
bool decode_u64(const std::string& data, size_t& offset, uint64_t& val) {
    if (offset + 8 > data.size()) return false;
    val = 0;
    for (int i = 0; i < 8; ++i) val |= (static_cast<uint64_t>(static_cast<unsigned char>(data[offset++])) << (i * 8));
    return true;
}
// OS-level exact read/write to prevent partial IO operations
void exact_write(int fd, const void* buf, size_t count) {
    const char* ptr = static_cast<const char*>(buf);
    while (count > 0) {
        ssize_t written = ::write(fd, ptr, count);
        if (written < 0) throw std::runtime_error("Snapshot write failed");
        ptr += written;
        count -= written;
    }
}
bool exact_read(int fd, void* buf, size_t count) {
    char* ptr = static_cast<char*>(buf);
    while (count > 0) {
        ssize_t r = ::read(fd, ptr, count);
        if (r < 0) throw std::runtime_error("Snapshot read failed");
        if (r == 0) return false; // Unexpected EOF
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
struct SnapshotHeader {
    LogIndex last_included_index = 0;
    Term     last_included_term  = 0;
    uint64_t len = 0;
};


// Atomically writes the snapshot using the Temp + Rename + Fsync pattern
void write_snapshot_atomic(const std::string& path, const SnapshotHeader& h, const std::string& data) {
    std::string tmp_path = path + ".tmp";
    // Open temporary file
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        throw std::runtime_error("Failed to open snapshot temp file: " + tmp_path);
    }
    //Serialize header deterministically (3 * 8 bytes = 24 bytes total)
    std::string header_bytes;
    encode_u64(header_bytes, h.last_included_index);
    encode_u64(header_bytes, h.last_included_term);
    encode_u64(header_bytes, h.len);
    //  Write exactly
    exact_write(fd, header_bytes.data(), header_bytes.size());
    exact_write(fd, data.data(), data.size());
    // Force bytes to physical disk before closing
    if (::fsync(fd) != 0) {
        ::close(fd);
        throw std::runtime_error("fsync failed on snapshot temp file");
    }
    ::close(fd);
    // Atomic rename (POSIX guarantee: overwrites existing destination safely)
    std::filesystem::rename(tmp_path, path);
    // Fsync the parent directory so the rename survives a power loss
    std::filesystem::path p(path);
    sync_dir(p.parent_path().string());
}


// Reads the snapshot safely, returning false if it doesn't exist or is corrupted
bool read_snapshot(const std::string& path, SnapshotHeader& h, std::string& data) {
    if (!std::filesystem::exists(path)) {
        return false;
    }
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        return false;
    }
    // Read the 24-byte header
    std::string header_bytes(24, '\0');
    if (!exact_read(fd, header_bytes.data(), 24)) {
        ::close(fd);
        return false; // File too small
    }
    // Decode the header
    size_t offset = 0;
    decode_u64(header_bytes, offset, h.last_included_index);
    decode_u64(header_bytes, offset, h.last_included_term);
    decode_u64(header_bytes, offset, h.len);

    // Read the payload blob
    data.resize(h.len);
    if (!exact_read(fd, data.data(), h.len)) {
        ::close(fd);
        return false; // Payload truncated (Torn Write)
    }
    ::close(fd);
    return true;
}

} 