// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <limits>
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
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/temp_dir.h"
#include "util/slice.h"

namespace doris::snii::writer {

// A tiered append buffer for one build-time section. While resident it holds the
// bytes as a CHAIN OF CHUNKS (one per append) rather than a single growing vector.
// Copying appends own a right-sized allocation. Move appends preserve the caller's
// allocation, including capacity slack, and account that retained capacity as resident
// memory. Once the resident capacity crosses `cap_bytes` the buffer SPILLS to a temp
// file (resolve_temp_dir()) and routes later appends there, so a huge section stays
// RSS-bounded at ~cap_bytes while a small one is RAM-only (zero disk, spill-only build).
// Append order/bytes are identical wherever they land; stream_into() reproduces the
// logical section bytes in order. RAII-removes the temp. (cap_bytes == UINT64_MAX
// disables spilling -> always RAM.)
class SpillableByteBuffer {
public:
    // `reporter` is an OPTIONAL writer-level build-RAM reporter (null off-Doris /
    // unit tests). Resident chunk capacities are hard-reserved before adoption or
    // allocation. If the shared cap cannot admit the next chunk, the buffer spills
    // its resident prefix first and writes that chunk directly to disk. Spilled
    // bytes are not resident and hold no reservation.
    SpillableByteBuffer(uint64_t cap_bytes, std::string tag, MemoryReporter* reporter = nullptr)
            : cap_bytes_(cap_bytes),
              tag_(std::move(tag)),
              reporter_(reporter),
              reservation_(reporter == nullptr ? MemoryReporter::Reservation()
                                               : reporter->make_reservation()) {}
    ~SpillableByteBuffer() { release_storage(); }
    SpillableByteBuffer(const SpillableByteBuffer&) = delete;
    SpillableByteBuffer& operator=(const SpillableByteBuffer&) = delete;

    // Total bytes appended so far (the offset basis for callers recording sub-offsets).
    uint64_t size() const {
        return consumed_ ? consumed_size_ : (spilled_ ? spilled_bytes_ : ram_bytes_);
    }

    // Copying append (the Slice bytes are copied into a fresh chunk).
    Status append(Slice bytes) {
        if (consumed_) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spillable buffer: append after stream");
        }
        if (spilled_) {
            const ::doris::Slice s(bytes.data(), bytes.size());
            RETURN_IF_ERROR(to_snii(temp_writer_->appendv(&s, 1)));
            spilled_bytes_ += bytes.size();
            return Status::OK();
        }
        if (!bytes.empty()) {
            bool keep_resident = true;
            RETURN_IF_ERROR(reserve_resident_capacity(bytes.size(), &keep_resident));
            if (!keep_resident) {
                const ::doris::Slice s(bytes.data(), bytes.size());
                RETURN_IF_ERROR(to_snii(temp_writer_->appendv(&s, 1)));
                spilled_bytes_ += bytes.size();
                return Status::OK();
            }
            std::vector<uint8_t> chunk;
            chunk.reserve(bytes.size());
            DCHECK_EQ(chunk.capacity(), bytes.size());
            chunk.insert(chunk.end(), bytes.data(), bytes.data() + bytes.size());
            chunks_.push_back(std::move(chunk));
            ram_bytes_ += bytes.size();
            ram_capacity_bytes_ += chunks_.back().capacity();
        }
        if (over_cap()) return spill_to_disk();
        return Status::OK();
    }

    // Move append: the section ADOPTS the caller's vector without copying. The common
    // dict path hands off each flushed block this way. Logical bytes remain v.size(),
    // while resident accounting uses the capacity retained by the adopted vector.
    Status append_move(std::vector<uint8_t>&& v) {
        if (consumed_) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spillable buffer: append after stream");
        }
        if (spilled_) {
            const ::doris::Slice s(v.data(), v.size());
            RETURN_IF_ERROR(to_snii(temp_writer_->appendv(&s, 1)));
            spilled_bytes_ += v.size();
            return Status::OK();
        }
        if (!v.empty()) {
            const size_t logical_bytes = v.size();
            const size_t retained_capacity = v.capacity();
            bool keep_resident = true;
            RETURN_IF_ERROR(reserve_resident_capacity(retained_capacity, &keep_resident));
            if (!keep_resident) {
                const ::doris::Slice s(v.data(), v.size());
                RETURN_IF_ERROR(to_snii(temp_writer_->appendv(&s, 1)));
                spilled_bytes_ += v.size();
                return Status::OK();
            }
            chunks_.push_back(std::move(v));
            ram_bytes_ += logical_bytes;
            ram_capacity_bytes_ += chunks_.back().capacity();
            DCHECK_EQ(chunks_.back().capacity(), retained_capacity);
        }
        if (over_cap()) return spill_to_disk();
        return Status::OK();
    }

    // Must be called once after the last append, before stream_into(): flushes the temp
    // (if spilled) so it can be read back. A no-op for a RAM-resident buffer.
    Status seal() {
        if (consumed_) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spillable buffer: seal after stream");
        }
        if (spilled_ && !sealed_) {
            RETURN_IF_ERROR(to_snii(temp_writer_->close()));
            sealed_ = true;
        }
        return Status::OK();
    }

    // Streams the whole section (RAM chunks or sealed temp) into `out`, in append order.
    Status stream_into(io::FileWriter* out) const {
        if (consumed_) {
            return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                    "spillable buffer: stream called twice");
        }
        if (!spilled_) {
            for (const auto& c : chunks_) {
                if (!c.empty()) RETURN_IF_ERROR(out->append(Slice(c)));
            }
            return Status::OK();
        }
        ::doris::io::FileReaderSPtr reader;
        RETURN_IF_ERROR(
                to_snii(::doris::io::global_local_filesystem()->open_file(temp_path_, &reader)));
        constexpr uint64_t kMaxChunk = 1u << 20; // bounded copy window (no whole-section reload)
        size_t read_capacity = static_cast<size_t>(std::min(kMaxChunk, spilled_bytes_));
        MemoryReporter::Reservation read_reservation = reporter_ == nullptr
                                                               ? MemoryReporter::Reservation()
                                                               : reporter_->make_reservation();
        if (reporter_ != nullptr) {
            Status reserve_status = read_reservation.set_bytes(read_capacity);
            while (!reserve_status.ok() && reserve_status.is<ErrorCode::MEM_LIMIT_EXCEEDED>() &&
                   read_capacity > 1) {
                read_capacity = std::max<size_t>(1, read_capacity / 2);
                reserve_status = read_reservation.set_bytes(read_capacity);
            }
            RETURN_IF_ERROR(reserve_status);
        }
        std::vector<uint8_t> buf;
        buf.reserve(read_capacity);
        DCHECK_EQ(buf.capacity(), read_capacity);
        for (uint64_t off = 0; off < spilled_bytes_; off += read_capacity) {
            const uint64_t n = std::min<uint64_t>(read_capacity, spilled_bytes_ - off);
            buf.resize(static_cast<size_t>(n));
            size_t bytes_read = 0;
            RETURN_IF_ERROR(to_snii(reader->read_at(
                    off, ::doris::Slice(buf.data(), static_cast<size_t>(n)), &bytes_read)));
            if (bytes_read != n) {
                return Status::Error<ErrorCode::IO_ERROR, false>(
                        "short read from spill scratch file");
            }
            RETURN_IF_ERROR(out->append(Slice(buf.data(), static_cast<size_t>(n))));
        }
        return Status::OK();
    }

    // The staged bytes have no readers after they are copied into the compound
    // output. Releasing them here, rather than in the logical-writer destructor,
    // keeps multi-destination compaction from retaining one DICT image per
    // completed index until rowset close.
    Status stream_into_and_release(io::FileWriter* out) {
        RETURN_IF_ERROR(stream_into(out));
        release_storage();
        return Status::OK();
    }

    bool spilled() const { return spilled_; }

private:
    Status reserve_resident_capacity(size_t additional_capacity, bool* keep_resident) {
        DCHECK(keep_resident != nullptr);
        *keep_resident = true;
        if (reporter_ == nullptr) return Status::OK();
        if (additional_capacity > std::numeric_limits<uint64_t>::max() - ram_capacity_bytes_) {
            return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                    "spillable buffer: resident capacity overflows uint64");
        }
        const Status reserved = reservation_.set_bytes(ram_capacity_bytes_ + additional_capacity);
        if (reserved.ok()) return Status::OK();
        if (!reserved.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) return reserved;
        RETURN_IF_ERROR(spill_to_disk());
        *keep_resident = false;
        return Status::OK();
    }

    // Gate-2 spill condition (UNIFIED): spill when the writer's TOTAL build RAM crosses
    // the one shared cap (reporter_->over_cap()), with the local cap_bytes_ kept only as
    // a defensive per-buffer hard ceiling (e.g. when no reporter is attached).
    bool over_cap() const {
        return (reporter_ != nullptr && reporter_->over_cap()) || ram_capacity_bytes_ >= cap_bytes_;
    }
    // Bridge a Doris IO Status into SNII's Status. R01 (status migration) is not done yet,
    // so this buffer still returns Status; this mirrors snii_doris_adapter's
    // to_snii_status (ok -> OK, otherwise IoError carrying the Doris message).
    static Status to_snii(const Status& s) {
        if (s.ok()) return Status::OK();
        return Status::Error<ErrorCode::IO_ERROR, false>(s.to_string_no_stack());
    }
    Status spill_to_disk() {
        temp_path_ = resolve_temp_dir() + "/snii_" + tag_ + "_" + std::to_string(::getpid()) + "_" +
                     std::to_string(reinterpret_cast<uintptr_t>(this)) + ".tmp";
        RETURN_IF_ERROR(to_snii(
                ::doris::io::global_local_filesystem()->create_file(temp_path_, &temp_writer_)));
        for (const auto& c : chunks_) {
            if (!c.empty()) {
                const ::doris::Slice s(c.data(), c.size());
                RETURN_IF_ERROR(to_snii(temp_writer_->appendv(&s, 1)));
            }
        }
        spilled_bytes_ = ram_bytes_;
        std::vector<std::vector<uint8_t>>().swap(chunks_); // reclaim the RAM immediately
        ram_bytes_ = 0;
        ram_capacity_bytes_ = 0;
        reservation_.reset();
        spilled_ = true;
        return Status::OK();
    }

    void release_storage() {
        if (consumed_) {
            return;
        }
        consumed_size_ = spilled_ ? spilled_bytes_ : ram_bytes_;
        std::vector<std::vector<uint8_t>>().swap(chunks_);
        ram_bytes_ = 0;
        ram_capacity_bytes_ = 0;
        reservation_.reset();

        // A sealed temp writer is already closed; an unsealed error-path writer
        // aborts on reset. In both cases remove the scratch path best-effort.
        temp_writer_.reset();
        if (!temp_path_.empty()) {
            std::remove(temp_path_.c_str());
            temp_path_.clear();
        }
        spilled_bytes_ = 0;
        consumed_ = true;
    }

    uint64_t cap_bytes_;
    std::string tag_;
    MemoryReporter* reporter_ = nullptr;       // optional build-RAM reporter (null off-Doris)
    MemoryReporter::Reservation reservation_;  // resident inner-vector capacities
    std::vector<std::vector<uint8_t>> chunks_; // resident tier: one chunk per append
    uint64_t ram_bytes_ = 0;                   // logical section bytes retained in RAM
    uint64_t ram_capacity_bytes_ = 0;          // resident capacities of the retained chunks
    bool spilled_ = false;
    bool sealed_ = false;
    ::doris::io::FileWriterPtr temp_writer_; // Doris local writer for the spill scratch file
    std::string temp_path_;
    uint64_t spilled_bytes_ = 0;
    uint64_t consumed_size_ = 0;
    bool consumed_ = false;
};

} // namespace doris::snii::writer
