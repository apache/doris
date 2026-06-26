#pragma once

#include <unistd.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

#include "snii/common/slice.h"
#include "snii/common/status.h"
#include "snii/io/local_file.h"
#include "snii/writer/memory_reporter.h"
#include "snii/writer/temp_dir.h"

namespace snii::writer {

// A tiered append buffer for one build-time section. While resident it holds the
// bytes as a CHAIN OF CHUNKS (one per append) rather than a single growing vector:
// each append owns a right-sized allocation, so there is NO geometric-doubling
// realloc transient and NO power-of-two capacity slack -- the resident cost is
// exactly the bytes appended, for any section size. Once the running size crosses
// `cap_bytes` the buffer SPILLS to a temp file (resolve_temp_dir()) and routes later
// appends there, so a huge section stays RSS-bounded at ~cap_bytes while a small one
// is RAM-only (zero disk, spill-only build). append order/bytes are identical
// wherever they land; stream_into() reproduces the section in order. RAII-removes the
// temp. (cap_bytes == UINT64_MAX disables spilling -> always RAM.)
class SpillableByteBuffer {
public:
    // `reporter` is an OPTIONAL writer-level build-RAM reporter (null off-Doris /
    // unit tests). When non-null, every change to ram_bytes_ (the RESIDENT tier) is
    // mirrored to it as a signed delta: a positive delta per RAM append, and a single
    // negative delta == prior ram_bytes_ when the buffer spills (the resident chunks
    // are dropped and the bytes move to disk, so they must NOT be counted as RSS).
    // Spilled bytes live on disk and are never reported.
    SpillableByteBuffer(uint64_t cap_bytes, std::string tag, MemoryReporter* reporter = nullptr)
            : cap_bytes_(cap_bytes), tag_(std::move(tag)), reporter_(reporter) {}
    ~SpillableByteBuffer() {
        // Balance the reporter: on the common un-spilled path the resident ram_bytes_ was
        // reported as positive on append but never released, so release it now (a missed
        // negative would leak into Doris's MemTracker). After a spill, spill_to_disk()
        // already reported the negative and ram_bytes_ no longer counts as resident.
        if (reporter_ && !spilled_ && ram_bytes_ > 0) {
            reporter_->report(-static_cast<int64_t>(ram_bytes_));
        }
        if (!temp_path_.empty()) std::remove(temp_path_.c_str());
    }
    SpillableByteBuffer(const SpillableByteBuffer&) = delete;
    SpillableByteBuffer& operator=(const SpillableByteBuffer&) = delete;

    // Total bytes appended so far (the offset basis for callers recording sub-offsets).
    uint64_t size() const { return spilled_ ? spilled_bytes_ : ram_bytes_; }

    // Copying append (the Slice bytes are copied into a fresh chunk).
    Status append(Slice bytes) {
        if (spilled_) {
            SNII_RETURN_IF_ERROR(temp_.append(bytes));
            spilled_bytes_ += bytes.size();
            return Status::OK();
        }
        if (!bytes.empty()) {
            chunks_.emplace_back(bytes.data(), bytes.data() + bytes.size());
            ram_bytes_ += bytes.size();
            if (reporter_) reporter_->report(static_cast<int64_t>(bytes.size()));
        }
        if (over_cap()) return spill_to_disk();
        return Status::OK();
    }

    // Move append: the section ADOPTS the caller's vector (no copy, no slack). The
    // common dict path -- each flushed block is handed off by move.
    Status append_move(std::vector<uint8_t>&& v) {
        if (spilled_) {
            SNII_RETURN_IF_ERROR(temp_.append(Slice(v)));
            spilled_bytes_ += v.size();
            return Status::OK();
        }
        if (!v.empty()) {
            ram_bytes_ += v.size();
            if (reporter_) reporter_->report(static_cast<int64_t>(v.size()));
            chunks_.push_back(std::move(v));
        }
        if (over_cap()) return spill_to_disk();
        return Status::OK();
    }

    // Must be called once after the last append, before stream_into(): flushes the temp
    // (if spilled) so it can be read back. A no-op for a RAM-resident buffer.
    Status seal() {
        if (spilled_ && !sealed_) {
            SNII_RETURN_IF_ERROR(temp_.finalize());
            sealed_ = true;
        }
        return Status::OK();
    }

    // Streams the whole section (RAM chunks or sealed temp) into `out`, in append order.
    Status stream_into(snii::io::FileWriter* out) const {
        if (!spilled_) {
            for (const auto& c : chunks_) {
                if (!c.empty()) SNII_RETURN_IF_ERROR(out->append(Slice(c)));
            }
            return Status::OK();
        }
        snii::io::LocalFileReader r;
        SNII_RETURN_IF_ERROR(r.open(temp_path_));
        constexpr uint64_t kChunk = 1u << 20; // fixed copy window (no whole-section reload)
        std::vector<uint8_t> buf;
        for (uint64_t off = 0; off < spilled_bytes_; off += kChunk) {
            const uint64_t n = std::min(kChunk, spilled_bytes_ - off);
            SNII_RETURN_IF_ERROR(r.read_at(off, n, &buf));
            SNII_RETURN_IF_ERROR(out->append(Slice(buf)));
        }
        return Status::OK();
    }

    bool spilled() const { return spilled_; }

private:
    // Gate-2 spill condition (UNIFIED): spill when the writer's TOTAL build RAM crosses
    // the one shared cap (reporter_->over_cap()), with the local cap_bytes_ kept only as
    // a defensive per-buffer hard ceiling (e.g. when no reporter is attached).
    bool over_cap() const {
        return (reporter_ != nullptr && reporter_->over_cap()) || ram_bytes_ >= cap_bytes_;
    }
    Status spill_to_disk() {
        temp_path_ = resolve_temp_dir() + "/snii_" + tag_ + "_" + std::to_string(::getpid()) + "_" +
                     std::to_string(reinterpret_cast<uintptr_t>(this)) + ".tmp";
        SNII_RETURN_IF_ERROR(temp_.open(temp_path_));
        for (const auto& c : chunks_) {
            if (!c.empty()) SNII_RETURN_IF_ERROR(temp_.append(Slice(c)));
        }
        spilled_bytes_ = ram_bytes_;
        // The resident tier is freed: report the full negative delta == prior ram_bytes_
        // so the writer-level RAM counter (and Doris's LOAD tracker) no longer counts
        // these bytes as RSS -- they now live on disk. This single negative balances the
        // sum of all prior positive append deltas (net-zero RAM after spill).
        if (reporter_) reporter_->report(-static_cast<int64_t>(ram_bytes_));
        std::vector<std::vector<uint8_t>>().swap(chunks_); // reclaim the RAM immediately
        spilled_ = true;
        return Status::OK();
    }

    uint64_t cap_bytes_;
    std::string tag_;
    MemoryReporter* reporter_ = nullptr;       // optional build-RAM reporter (null off-Doris)
    std::vector<std::vector<uint8_t>> chunks_; // resident tier: one chunk per append
    uint64_t ram_bytes_ = 0;
    bool spilled_ = false;
    bool sealed_ = false;
    snii::io::LocalFileWriter temp_;
    std::string temp_path_;
    uint64_t spilled_bytes_ = 0;
};

} // namespace snii::writer
