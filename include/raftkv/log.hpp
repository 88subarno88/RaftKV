#pragma once
#include "raftkv/types.hpp"
#include <vector>
#include <stdexcept>
#include <algorithm>

namespace raftkv {

class Log {
public:
    // Boundaries
    inline LogIndex first_index() const {
        return last_included_index_ + 1;
    }

    inline LogIndex last_index() const {
        return last_included_index_ + entries_.size();
    }

    inline Term last_term() const {
        return empty() ? last_included_term_ : entries_.back().term;
    }

    inline bool empty() const {
        return entries_.empty();
    }

    // Returns term at a given logical index
    inline Term term_at(LogIndex index) const {
        if (index == 0) {
            return 0; // Base case, simplifies consistency checks
        }
        if (index == last_included_index_) {
            return last_included_term_;
        }
        if (index < last_included_index_) {
            throw std::out_of_range("Log::term_at - index truncated by snapshot");
        }
        if (index > last_index()) {
            throw std::out_of_range("Log::term_at - index out of bounds");
        }
        return entries_[index - last_included_index_ - 1].term;
    }

    // Fetch entry by logical index
    inline const LogEntry& at(LogIndex index) const {
        if (index < first_index() || index > last_index()) {
            throw std::out_of_range("Log::at - index out of bounds");
        }
        return entries_[index - last_included_index_ - 1];
    }

    // Slice for AppendEntries
    inline std::vector<LogEntry> slice_from(LogIndex from) const {
        if (from > last_index()) {
            return {};
        }
        if (from < first_index()) {
            throw std::out_of_range("Log::slice_from - request requires sending a snapshot");
        }
        size_t physical_start = from - last_included_index_ - 1;
        return std::vector<LogEntry>(entries_.begin() + physical_start, entries_.end());
    }

    // Mutation

    // Appends a new command from the leader
    inline LogIndex append(Term term, const Command& cmd) {
        LogIndex new_index = last_index() + 1;
        entries_.push_back({term, new_index, cmd});
        return new_index;
    }

    // Follower-side reconciliation
    inline void merge_from(LogIndex prev_index, const std::vector<LogEntry>& entries) {
        for (const auto& entry : entries) {
            LogIndex logical_idx = entry.index;

            // If  already have an entry at this index
            if (logical_idx <= last_index()) {
                // If terms match, it's identical. Skip (idempotency rule).
                if (term_at(logical_idx) == entry.term) {
                    continue;
                }
                
                // CONFLICT! Truncate the log from this index onward
                size_t physical_idx = logical_idx - last_included_index_ - 1;
                entries_.erase(entries_.begin() + physical_idx, entries_.end());
            }

            // Append the new/conflicting entry
            entries_.push_back(entry);
        }
    }

    // Compaction (Snapshotting)
    inline void compact(LogIndex up_to_index, Term up_to_term) {
        // Ignore stale compaction requests
        if (up_to_index <= last_included_index_) {
            return;
        }

        // If snapshot encompasses our entire log (or extends beyond it, e.g., from InstallSnapshot)
        if (up_to_index >= last_index()) {
            entries_.clear();
        } else {
            // Otherwise, discard the prefix we no longer need
            size_t remove_count = up_to_index - last_included_index_;
            entries_.erase(entries_.begin(), entries_.begin() + remove_count);
        }

        last_included_index_ = up_to_index;
        last_included_term_  = up_to_term;
    }

    // Fast backup optimization logic for Leader
    inline LogIndex find_conflict_backup(LogIndex conflict_index, Term conflict_term) const {
        // If follower's log was too short, they send conflict_term == 0.
        // Jump back directly to the index they requested.
        if (conflict_term == 0) {
            return conflict_index;
        }

        // Search  log for the follower's conflicting term
        //  look for the LAST entry in our log that matches this term
        for (LogIndex idx = last_index(); idx >= first_index(); --idx) {
            if (term_at(idx) == conflict_term) {
                return idx + 1; // Back up to exactly one past the last entry of this term
            }
            if (term_at(idx) < conflict_term) {
                break; // Since terms increase monotonically, we can stop early
            }
        }

        //  don't have the conflicting term at all. Fall back to the index the follower provided.
        return conflict_index;
    }

private:
    std::vector<LogEntry> entries_;
    LogIndex last_included_index_ = 0;
    Term     last_included_term_  = 0;
};

} 