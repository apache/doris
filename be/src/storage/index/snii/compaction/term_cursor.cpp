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

#include "storage/index/snii/compaction/term_cursor.h"

#include <limits>

#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/format/sampled_term_index.h"

namespace doris::snii::compaction {

namespace {

Status add_entry_memory(uint64_t bytes, uint64_t* total) {
    if (bytes > std::numeric_limits<uint64_t>::max() - *total) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "term_cursor: decoded dictionary entries exceed uint64 memory accounting");
    }
    *total += bytes;
    return Status::OK();
}

Status decoded_entries_memory_bytes(const std::vector<format::DictEntry>& entries, uint64_t* out) {
    if (entries.capacity() > std::numeric_limits<uint64_t>::max() / sizeof(format::DictEntry)) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "term_cursor: decoded dictionary entry slots exceed uint64 memory accounting");
    }
    uint64_t bytes = entries.capacity() * sizeof(format::DictEntry);
    for (const format::DictEntry& entry : entries) {
        RETURN_IF_ERROR(add_entry_memory(format::std_string_heap_bytes(entry.term), &bytes));
        RETURN_IF_ERROR(add_entry_memory(entry.frq_bytes.capacity(), &bytes));
        RETURN_IF_ERROR(add_entry_memory(entry.prx_bytes.capacity(), &bytes));
    }
    *out = bytes;
    return Status::OK();
}

} // namespace

uint64_t big_endian_term_prefix(std::string_view term) {
    uint64_t prefix = 0;
    for (size_t i = 0; i < term.size() && i < sizeof(prefix); ++i) {
        prefix |= static_cast<uint64_t>(static_cast<uint8_t>(term[i])) << (56 - i * 8);
    }
    return prefix;
}

Status SniiSegmentTermCursor::next(bool* has_term) {
    if (has_term == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("term_cursor: null has_term");
    }
    *has_term = false;
    if (!failed_.ok()) {
        return failed_;
    }
    if (index_ == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("term_cursor: null index");
    }
    if (exhausted_) {
        return Status::OK();
    }

    if (started_) {
        ++pos_;
    } else {
        started_ = true;
    }
    // Cross a block boundary (or start): materialize the next DICT block. The
    // loop form also tolerates a (format-legal but writer-never-produced)
    // empty block.
    while (pos_ >= entries_.size()) {
        if (next_block_ >= index_->n_dict_blocks()) {
            exhausted_ = true;
            entries_.clear();
            return Status::OK();
        }

        writer::MemoryReporter::Reservation decode_reservation =
                memory_reporter_ == nullptr ? writer::MemoryReporter::Reservation()
                                            : memory_reporter_->make_reservation();
        if (memory_reporter_ != nullptr) {
            reader::DictBlockScanMemory memory;
            Status st = index_->dict_block_scan_memory(next_block_, &memory);
            if (!st.ok()) {
                failed_ = st;
                return failed_;
            }
            st = decode_reservation.set_bytes(memory.decode_bytes);
            if (!st.ok()) {
                failed_ = st;
                return failed_;
            }

            previous_entries_reservation_.reset();
            previous_entries_reservation_ = std::move(entries_reservation_);
            entries_reservation_ = memory_reporter_->make_reservation();
            st = entries_reservation_.set_bytes(memory.entries_bytes);
            if (!st.ok()) {
                failed_ = st;
                return failed_;
            }
        }
        const Status st = index_->decode_dict_block(next_block_, &entries_, &frq_base_, &prx_base_);
        if (!st.ok()) {
            failed_ = st;
            return failed_;
        }
        if (memory_reporter_ != nullptr) {
            uint64_t actual_entries_bytes = 0;
            Status memory_status = decoded_entries_memory_bytes(entries_, &actual_entries_bytes);
            if (memory_status.ok()) {
                memory_status = entries_reservation_.set_bytes(actual_entries_bytes);
            }
            if (!memory_status.ok()) {
                std::vector<format::DictEntry>().swap(entries_);
                entries_reservation_.reset();
                failed_ = memory_status;
                return failed_;
            }
        }
        ++next_block_;
        pos_ = 0;
    }

    const format::DictEntry& e = entries_[pos_];
    // Hidden bigram / sentinel gate: classify by the FULL marker so a user term
    // that merely begins with a raw 0x1F byte passes through (design ruling on
    // the 0x1F-prefix corner). Any full-marker term aborts this column's merge
    // -- the caller must fall back to rebuild (v1 excludes legacy bigrams).
    if (format::is_phrase_bigram_term(e.term)) {
        failed_ = Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "term_cursor: source dictionary contains a legacy phrase-bigram/sentinel term; "
                "column must fall back to index rebuild (src_ord={})",
                source_ordinal_);
        return failed_;
    }
    if (has_prev_ && e.term <= prev_term_) {
        failed_ = Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "term_cursor: dictionary term order violated (src_ord={})", source_ordinal_);
        return failed_;
    }
    prev_term_ = e.term;
    term_prefix_ = big_endian_term_prefix(e.term);
    has_prev_ = true;
    *has_term = true;
    return Status::OK();
}

} // namespace doris::snii::compaction
