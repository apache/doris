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

#include "storage/index/snii/compaction/region_reader.h"

#include <algorithm>
#include <limits>

namespace doris::snii::compaction {

namespace {

Status reserve_read_buffer(size_t target, std::vector<uint8_t>* buffer,
                           writer::MemoryReporter::Reservation* reservation) {
    if (reservation == nullptr) return Status::OK();
    if (buffer->capacity() >= target) {
        DCHECK_EQ(reservation->bytes(), buffer->capacity());
        return Status::OK();
    }
    writer::MemoryReporter::Reservation replacement;
    RETURN_IF_ERROR(reservation->prepare_replacement(target, &replacement));
    buffer->reserve(target);
    DCHECK_EQ(buffer->capacity(), target);
    *reservation = std::move(replacement);
    return Status::OK();
}

} // namespace

size_t SharedAlignedRegionCache::stream_index(PostingStream stream) {
    const size_t index = static_cast<size_t>(stream);
    DCHECK_LT(index, kStreamCount);
    return index;
}

Status SharedAlignedRegionCache::init() {
    if (initialized_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "shared_region_cache: init called twice");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "shared_region_cache: null reader");
    }
    if (total_budget_bytes_ < kSlotCount) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "shared_region_cache: budget must hold two chunks");
    }
    if (region_len_ > std::numeric_limits<uint64_t>::max() - region_off_ ||
        region_off_ > reader_->size() || region_len_ > reader_->size() - region_off_) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "shared_region_cache: region outside source file");
    }

    block_bytes_ = total_budget_bytes_ / kSlotCount;
    for (size_t slot_index = 0; slot_index < slots_.size(); ++slot_index) {
        Slot& slot = slots_[slot_index];
        if (memory_reporter_ != nullptr) {
            slot_reservations_[slot_index] = memory_reporter_->make_reservation();
            RETURN_IF_ERROR(slot_reservations_[slot_index].set_bytes(block_bytes_));
            slot.bytes.reserve(block_bytes_);
            DCHECK_EQ(slot.bytes.capacity(), block_bytes_);
        }
        slot.bytes.resize(block_bytes_);
    }
    if (resident_capacity_bytes() > total_budget_bytes_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "shared_region_cache: allocator exceeded cache budget");
    }
    initialized_ = true;
    return Status::OK();
}

size_t SharedAlignedRegionCache::resident_capacity_bytes() const {
    size_t capacity = 0;
    for (const Slot& slot : slots_) {
        capacity += slot.bytes.capacity();
    }
    return capacity;
}

uint64_t SharedAlignedRegionCache::read_calls(PostingStream stream) const {
    return read_calls_[stream_index(stream)];
}

uint64_t SharedAlignedRegionCache::buffer_hits(PostingStream stream) const {
    return buffer_hits_[stream_index(stream)];
}

void SharedAlignedRegionCache::unpin(PostingStream stream) {
    const size_t index = stream_index(stream);
    const int8_t slot_index = stream_slots_[index];
    if (slot_index < 0) {
        return;
    }
    Slot& slot = slots_[static_cast<size_t>(slot_index)];
    DCHECK_GT(slot.pins, 0);
    --slot.pins;
    stream_slots_[index] = -1;
}

Status SharedAlignedRegionCache::read_physical(PostingStream stream, uint64_t abs_off, size_t len,
                                               std::vector<uint8_t>* out) {
    RETURN_IF_ERROR(reader_->read_at(abs_off, len, out));
    ++physical_read_ranges_;
    physical_read_bytes_ += len;
    ++read_calls_[stream_index(stream)];
    return Status::OK();
}

Status SharedAlignedRegionCache::resolve(PostingStream stream, uint64_t abs_off, uint64_t len,
                                         std::vector<uint8_t>* scratch, Slice* out,
                                         writer::MemoryReporter::Reservation* scratch_reservation) {
    if (scratch == nullptr || out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("shared_region_cache: null out");
    }
    if (!initialized_) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "shared_region_cache: resolve before init");
    }
    *out = Slice();
    if (abs_off < region_off_) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "shared_region_cache: window outside region");
    }
    const uint64_t relative_off = abs_off - region_off_;
    if (relative_off > region_len_ || len > region_len_ - relative_off) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "shared_region_cache: window outside region");
    }
    if (len > std::numeric_limits<size_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "shared_region_cache: window length out of range");
    }

    unpin(stream);
    const size_t want = static_cast<size_t>(len);
    if (want == 0) {
        return Status::OK();
    }

    const uint64_t aligned_relative_off = (relative_off / block_bytes_) * block_bytes_;
    const uint64_t block_off = region_off_ + aligned_relative_off;
    const size_t block_len = static_cast<size_t>(
            std::min<uint64_t>(block_bytes_, region_len_ - aligned_relative_off));
    const size_t offset_in_block = static_cast<size_t>(relative_off - aligned_relative_off);
    const bool fits_one_block = want <= block_len - offset_in_block;
    if (!fits_one_block) {
        RETURN_IF_ERROR(reserve_read_buffer(want, scratch, scratch_reservation));
        RETURN_IF_ERROR(read_physical(stream, abs_off, want, scratch));
        if (scratch->size() != want) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "shared_region_cache: short exact read");
        }
        *out = Slice(scratch->data(), want);
        return Status::OK();
    }

    const size_t stream_id = stream_index(stream);
    for (size_t slot_index = 0; slot_index < slots_.size(); ++slot_index) {
        Slot& slot = slots_[slot_index];
        if (slot.valid && slot.offset == block_off && offset_in_block + want <= slot.bytes.size()) {
            ++slot.pins;
            stream_slots_[stream_id] = static_cast<int8_t>(slot_index);
            ++buffer_hits_[stream_id];
            *out = Slice(slot.bytes.data() + offset_in_block, want);
            return Status::OK();
        }
    }

    size_t slot_index = 0;
    while (slot_index < slots_.size() && slots_[slot_index].pins != 0) {
        ++slot_index;
    }
    DCHECK_LT(slot_index, slots_.size());
    Slot& slot = slots_[slot_index];
    slot.valid = false;
    RETURN_IF_ERROR(read_physical(stream, block_off, block_len, &slot.bytes));
    if (slot.bytes.size() != block_len) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "shared_region_cache: short chunk read");
    }
    slot.offset = block_off;
    slot.pins = 1;
    slot.valid = true;
    stream_slots_[stream_id] = static_cast<int8_t>(slot_index);
    *out = Slice(slot.bytes.data() + offset_in_block, want);
    return Status::OK();
}

Status SequentialRegionReader::resolve(uint64_t abs_off, uint64_t len,
                                       std::vector<uint8_t>* scratch, Slice* out) {
    if (scratch == nullptr || out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("region_reader: null out");
    }
    if (reader_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("region_reader: null reader");
    }
    *out = Slice();
    // Subtraction-based bounds avoid wrapping region_offset+region_length or
    // abs_off+len at UINT64_MAX.
    if (region_len_ > std::numeric_limits<uint64_t>::max() - region_off_ || abs_off < region_off_) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "region_reader: window outside region");
    }
    const uint64_t relative_off = abs_off - region_off_;
    if (relative_off > region_len_ || len > region_len_ - relative_off) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "region_reader: window outside region");
    }
    if (len > std::numeric_limits<size_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "region_reader: window length out of range");
    }
    const size_t want = static_cast<size_t>(len);
    if (want == 0) {
        return Status::OK();
    }

    // 1. Buffered hit: zero-copy slice, no file read.
    if (!buf_.empty() && abs_off >= buf_off_ && abs_off - buf_off_ + want <= buf_.size()) {
        ++buffer_hits_;
        *out = Slice(buf_.data() + (abs_off - buf_off_), want);
        return Status::OK();
    }

    // 3. Oversized or backward miss: one exact range read into the caller's
    // scratch, keeping the buffered chunk (and the forward stream position)
    // intact.
    const bool backward = !buf_.empty() && abs_off < buf_off_;
    if (want > chunk_bytes_ || backward) {
        RETURN_IF_ERROR(reader_->read_at(abs_off, want, scratch));
        ++read_calls_;
        if (scratch->size() != want) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "region_reader: short read");
        }
        *out = Slice(scratch->data(), want);
        return Status::OK();
    }

    // 2. Forward miss: refill the chunk starting at the window, clamped to the
    // region end so read-ahead never reads past the region (whose tail may abut
    // other file sections or EOF). want <= chunk and the range check above
    // guarantee the window fits the refilled chunk.
    const uint64_t remaining = region_len_ - relative_off;
    const size_t fill = static_cast<size_t>(std::min<uint64_t>(chunk_bytes_, remaining));
    RETURN_IF_ERROR(reader_->read_at(abs_off, fill, &buf_));
    ++read_calls_;
    if (buf_.size() != fill) {
        buf_.clear();
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "region_reader: short chunk read");
    }
    buf_off_ = abs_off;
    *out = Slice(buf_.data(), want);
    return Status::OK();
}

} // namespace doris::snii::compaction
