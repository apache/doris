#pragma once

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "snii/common/slice.h"
#include "common/status.h"
#include "snii/io/file_writer.h"
#include "snii/writer/memory_reporter.h"
#include "snii/writer/temp_dir.h"
#include "util/slice.h"

namespace snii::writer {
using doris::Status; // RETURN_IF_ERROR expands to bare Status

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
        // Release the Doris temp writer before unlinking: a sealed writer is already CLOSED
        // (no-op here); an un-sealed one (error path) is aborted, which closes the fd and
        // unlinks the scratch file. Then best-effort remove the path (RAII delete on success).
        temp_writer_.reset();
        if (!temp_path_.empty()) std::remove(temp_path_.c_str());
    }
    SpillableByteBuffer(const SpillableByteBuffer&) = delete;
    SpillableByteBuffer& operator=(const SpillableByteBuffer&) = delete;

    // Total bytes appended so far (the offset basis for callers recording sub-offsets).
    uint64_t size() const { return spilled_ ? spilled_bytes_ : ram_bytes_; }

    // Copying append (the Slice bytes are copied into a fresh chunk).
    doris::Status append(Slice bytes) {
        if (spilled_) {
            const doris::Slice s(bytes.data(), bytes.size());
            RETURN_IF_ERROR(to_snii(temp_writer_->appendv(&s, 1)));
            spilled_bytes_ += bytes.size();
            return doris::Status::OK();
        }
        if (!bytes.empty()) {
            chunks_.emplace_back(bytes.data(), bytes.data() + bytes.size());
            ram_bytes_ += bytes.size();
            if (reporter_) reporter_->report(static_cast<int64_t>(bytes.size()));
        }
        if (over_cap()) return spill_to_disk();
        return doris::Status::OK();
    }

    // Move append: the section ADOPTS the caller's vector (no copy, no slack). The
    // common dict path -- each flushed block is handed off by move.
    doris::Status append_move(std::vector<uint8_t>&& v) {
        if (spilled_) {
            const doris::Slice s(v.data(), v.size());
            RETURN_IF_ERROR(to_snii(temp_writer_->appendv(&s, 1)));
            spilled_bytes_ += v.size();
            return doris::Status::OK();
        }
        if (!v.empty()) {
            ram_bytes_ += v.size();
            if (reporter_) reporter_->report(static_cast<int64_t>(v.size()));
            chunks_.push_back(std::move(v));
        }
        if (over_cap()) return spill_to_disk();
        return doris::Status::OK();
    }

    // Must be called once after the last append, before stream_into(): flushes the temp
    // (if spilled) so it can be read back. A no-op for a RAM-resident buffer.
    doris::Status seal() {
        if (spilled_ && !sealed_) {
            RETURN_IF_ERROR(to_snii(temp_writer_->close()));
            sealed_ = true;
        }
        return doris::Status::OK();
    }

    // Streams the whole section (RAM chunks or sealed temp) into `out`, in append order.
    doris::Status stream_into(snii::io::FileWriter* out) const {
        if (!spilled_) {
            for (const auto& c : chunks_) {
                if (!c.empty()) RETURN_IF_ERROR(out->append(Slice(c)));
            }
            return doris::Status::OK();
        }
        doris::io::FileReaderSPtr reader;
        RETURN_IF_ERROR(
                to_snii(doris::io::global_local_filesystem()->open_file(temp_path_, &reader)));
        constexpr uint64_t kChunk = 1u << 20; // fixed copy window (no whole-section reload)
        std::vector<uint8_t> buf;
        for (uint64_t off = 0; off < spilled_bytes_; off += kChunk) {
            const uint64_t n = std::min(kChunk, spilled_bytes_ - off);
            buf.resize(static_cast<size_t>(n));
            size_t bytes_read = 0;
            RETURN_IF_ERROR(to_snii(reader->read_at(
                    off, doris::Slice(buf.data(), static_cast<size_t>(n)), &bytes_read)));
            if (bytes_read != n) {
                return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>("short read from spill scratch file");
            }
            RETURN_IF_ERROR(out->append(Slice(buf.data(), static_cast<size_t>(n))));
        }
        return doris::Status::OK();
    }

    bool spilled() const { return spilled_; }

private:
    // Gate-2 spill condition (UNIFIED): spill when the writer's TOTAL build RAM crosses
    // the one shared cap (reporter_->over_cap()), with the local cap_bytes_ kept only as
    // a defensive per-buffer hard ceiling (e.g. when no reporter is attached).
    bool over_cap() const {
        return (reporter_ != nullptr && reporter_->over_cap()) || ram_bytes_ >= cap_bytes_;
    }
    // Bridge a Doris IO doris::Status into SNII's doris::Status. R01 (status migration) is not done yet,
    // so this buffer still returns doris::Status; this mirrors snii_doris_adapter's
    // to_snii_status (ok -> OK, otherwise IoError carrying the Doris message).
    static doris::Status to_snii(const doris::Status& s) {
        if (s.ok()) return doris::Status::OK();
        return doris::Status::Error<doris::ErrorCode::IO_ERROR, false>(s.to_string_no_stack());
    }
    doris::Status spill_to_disk() {
        temp_path_ = resolve_temp_dir() + "/snii_" + tag_ + "_" + std::to_string(::getpid()) + "_" +
                     std::to_string(reinterpret_cast<uintptr_t>(this)) + ".tmp";
        RETURN_IF_ERROR(to_snii(
                doris::io::global_local_filesystem()->create_file(temp_path_, &temp_writer_)));
        for (const auto& c : chunks_) {
            if (!c.empty()) {
                const doris::Slice s(c.data(), c.size());
                RETURN_IF_ERROR(to_snii(temp_writer_->appendv(&s, 1)));
            }
        }
        spilled_bytes_ = ram_bytes_;
        // The resident tier is freed: report the full negative delta == prior ram_bytes_
        // so the writer-level RAM counter (and Doris's LOAD tracker) no longer counts
        // these bytes as RSS -- they now live on disk. This single negative balances the
        // sum of all prior positive append deltas (net-zero RAM after spill).
        if (reporter_) reporter_->report(-static_cast<int64_t>(ram_bytes_));
        std::vector<std::vector<uint8_t>>().swap(chunks_); // reclaim the RAM immediately
        spilled_ = true;
        return doris::Status::OK();
    }

    uint64_t cap_bytes_;
    std::string tag_;
    MemoryReporter* reporter_ = nullptr;       // optional build-RAM reporter (null off-Doris)
    std::vector<std::vector<uint8_t>> chunks_; // resident tier: one chunk per append
    uint64_t ram_bytes_ = 0;
    bool spilled_ = false;
    bool sealed_ = false;
    doris::io::FileWriterPtr temp_writer_; // Doris local writer for the spill scratch file
    std::string temp_path_;
    uint64_t spilled_bytes_ = 0;
};

} // namespace snii::writer
