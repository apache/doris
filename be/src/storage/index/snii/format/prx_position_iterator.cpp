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

#include "storage/index/snii/format/prx_position_iterator.h"

#include <algorithm>
#include <limits>

#include "storage/index/snii/encoding/pfor.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/prx_frame.h"
#include "storage/index/snii/format/prx_pod.h"

namespace doris::snii::format {
namespace {

Status invalid_iterator_state(const char* message) {
    return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(message);
}

Status corrupted_iterator_payload(const char* message) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(message);
}

Status validate_selected_ordinals(std::span<const uint32_t> selected_doc_ordinals,
                                  uint32_t doc_count) {
    uint32_t previous_ordinal = 0;
    bool first_ordinal = true;
    for (uint32_t ordinal : selected_doc_ordinals) {
        if (ordinal >= doc_count || (!first_ordinal && ordinal <= previous_ordinal)) {
            return invalid_iterator_state(
                    "prx iterator: selected doc ordinals must be strictly increasing and valid");
        }
        previous_ordinal = ordinal;
        first_ordinal = false;
    }
    return Status::OK();
}

} // namespace

Status PrxPositionIterator::fail(Status status) {
    failed_ = true;
    return status;
}

void PrxPositionIterator::reset_state(PrxDecodeContext* context) {
    context_ = context;
    frame_stats_ = {};
    payload_source_.reset();
    decompressed_.clear();
    pfor_counts_.clear();
    pfor_offsets_.clear();
    pfor_run_begin_ = 0;
    pfor_run_length_ = 0;
    pfor_run_index_ = 0;
    pfor_stream_index_ = 0;
    codec_ = PrxCodec::kRaw;
    doc_count_ = 0;
    next_doc_ordinal_ = 0;
    frequency_ = 0;
    decoded_from_doc_ = 0;
    scratch_position_ = 0;
    scratch_size_ = 0;
    previous_position_ = 0;
    first_position_ = true;
    active_doc_ = false;
    failed_ = false;
    finished_ = false;
}

Status PrxPositionIterator::initialize_frame(Slice framed_window, uint32_t expected_doc_count,
                                             std::span<const uint32_t> selected_doc_ordinals) {
    ByteSource frame_source(framed_window);
    PrxFrameView frame;
    Status status = read_prx_frame(&frame_source, &frame);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    if (!frame_source.eof()) {
        return fail(corrupted_iterator_payload("prx iterator: trailing bytes after frame"));
    }
    codec_ = frame.codec;

    Slice plaintext = frame.payload;
    if (frame.codec == PrxCodec::kZstd) {
        status = zstd_decompress(frame.payload, frame.uncompressed_length, &decompressed_);
        if (!status.ok()) {
            return fail(std::move(status));
        }
        plaintext = Slice(decompressed_);
        frame_stats_.zstd_frames = 1;
    } else if (frame.codec == PrxCodec::kPfor) {
        frame_stats_.pfor_frames = 1;
    } else {
        frame_stats_.raw_frames = 1;
    }
    frame_stats_.plaintext_bytes = frame.uncompressed_length;
    payload_source_.emplace(plaintext);
    status = payload_source_->get_varint32(&doc_count_);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    if (doc_count_ > kReaderPrxWindowLimits.max_docs) {
        return fail(corrupted_iterator_payload("prx iterator: doc count exceeds sane cap"));
    }
    if (doc_count_ != expected_doc_count) {
        return fail(corrupted_iterator_payload(
                "prx iterator: doc count differs from posting metadata"));
    }
    frame_stats_.total_docs = doc_count_;

    status = validate_selected_ordinals(selected_doc_ordinals, doc_count_);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    if (codec_ == PrxCodec::kPfor) {
        uint32_t declared_total_positions = 0;
        status = payload_source_->get_varint32(&declared_total_positions);
        if (!status.ok()) {
            return fail(std::move(status));
        }
        if (declared_total_positions > kReaderPrxWindowLimits.max_positions) {
            return fail(
                    corrupted_iterator_payload("prx iterator: position count exceeds sane cap"));
        }
        RETURN_IF_ERROR(decode_pfor_counts(declared_total_positions));
    }
    return Status::OK();
}

Status PrxPositionIterator::reset(Slice framed_window, uint32_t expected_doc_count,
                                  std::span<const uint32_t> selected_doc_ordinals,
                                  PrxDecodeContext* context) {
    reset_state(context);
    return initialize_frame(framed_window, expected_doc_count, selected_doc_ordinals);
}

Status PrxPositionIterator::decode_pfor_counts(uint32_t declared_total_positions) {
    pfor_counts_.resize(doc_count_);
    pfor_offsets_.resize(static_cast<size_t>(doc_count_) + 1);

    for (uint32_t offset = 0; offset < doc_count_; offset += kFrqBaseUnit) {
        const uint32_t run_length = std::min<uint32_t>(kFrqBaseUnit, doc_count_ - offset);
        Status status = pfor_decode(&*payload_source_, run_length, pfor_counts_.data() + offset);
        if (!status.ok()) {
            return fail(std::move(status));
        }
    }
    uint64_t count_sum = 0;
    pfor_offsets_[0] = 0;
    for (uint32_t doc = 0; doc < doc_count_; ++doc) {
        count_sum += pfor_counts_[doc];
        if (count_sum > kReaderPrxWindowLimits.max_positions) {
            return fail(
                    corrupted_iterator_payload("prx iterator: position count exceeds sane cap"));
        }
        pfor_offsets_[doc + 1] = static_cast<uint32_t>(count_sum);
    }
    if (count_sum != declared_total_positions) {
        return fail(corrupted_iterator_payload(
                "prx iterator: position count sum differs from declared total"));
    }
    frame_stats_.total_positions = declared_total_positions;
    return Status::OK();
}

Status PrxPositionIterator::decode_pfor_run(uint32_t run_begin, uint32_t run_length) {
    DCHECK_EQ(run_begin, pfor_stream_index_);
    Status status = pfor_decode(&*payload_source_, run_length, pfor_run_.data());
    if (!status.ok()) {
        return fail(std::move(status));
    }
    pfor_run_begin_ = run_begin;
    pfor_run_length_ = run_length;
    pfor_run_index_ = 0;
    pfor_stream_index_ += run_length;
    return Status::OK();
}

Status PrxPositionIterator::skip_pfor_run(uint32_t run_length) {
    Status status = pfor_skip(&*payload_source_, run_length);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    pfor_stream_index_ += run_length;
    return Status::OK();
}

Status PrxPositionIterator::advance_pfor_cursor(uint32_t target, bool decode_partial_run,
                                                bool require_position) {
    const uint32_t total_positions = pfor_offsets_.back();
    DCHECK_LE(target, total_positions);

    if (pfor_run_length_ != 0) {
        const uint32_t run_end = pfor_run_begin_ + pfor_run_length_;
        if (target >= pfor_run_begin_ &&
            (target < run_end || (!require_position && target == run_end))) {
            pfor_run_index_ = target - pfor_run_begin_;
            return Status::OK();
        }
        DCHECK_GE(target, run_end);
        pfor_run_length_ = 0;
        pfor_run_index_ = 0;
    }

    DCHECK_LE(pfor_stream_index_, target);
    while (pfor_stream_index_ < target) {
        const uint32_t run_begin = pfor_stream_index_;
        const uint32_t run_length = std::min<uint32_t>(kFrqBaseUnit, total_positions - run_begin);
        if (run_begin + run_length <= target) {
            RETURN_IF_ERROR(skip_pfor_run(run_length));
            continue;
        }
        if (!decode_partial_run) {
            return Status::OK();
        }
        RETURN_IF_ERROR(decode_pfor_run(run_begin, run_length));
        pfor_run_index_ = target - run_begin;
        return Status::OK();
    }

    if (require_position) {
        DCHECK_LT(target, total_positions);
        const uint32_t run_length =
                std::min<uint32_t>(kFrqBaseUnit, total_positions - pfor_stream_index_);
        RETURN_IF_ERROR(decode_pfor_run(pfor_stream_index_, run_length));
    }
    return Status::OK();
}

// NOLINTNEXTLINE(readability-non-const-parameter): frequency is populated from the payload cursor.
Status PrxPositionIterator::read_frequency(uint32_t* frequency) {
    DCHECK(codec_ != PrxCodec::kPfor);
    Status status = payload_source_->get_varint32_fast(frequency);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    frame_stats_.total_positions += *frequency;
    if (frame_stats_.total_positions > kReaderPrxWindowLimits.max_positions) {
        return fail(corrupted_iterator_payload("prx iterator: position count exceeds sane cap"));
    }
    return Status::OK();
}

Status PrxPositionIterator::skip_positions(uint32_t count) {
    DCHECK(codec_ != PrxCodec::kPfor);
    Status status = payload_source_->skip_varints(count);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    return Status::OK();
}

Status PrxPositionIterator::seek(uint32_t doc_ordinal) {
    if (failed_ || finished_ || active_doc_) {
        return fail(invalid_iterator_state("prx iterator: seek in invalid state"));
    }
    if (doc_ordinal < next_doc_ordinal_ || doc_ordinal >= doc_count_) {
        return fail(invalid_iterator_state("prx iterator: seek ordinal is not increasing"));
    }
    if (codec_ == PrxCodec::kPfor) {
        next_doc_ordinal_ = doc_ordinal;
        frequency_ = pfor_counts_[doc_ordinal];
        RETURN_IF_ERROR(advance_pfor_cursor(pfor_offsets_[doc_ordinal], false, false));
    } else {
        while (next_doc_ordinal_ < doc_ordinal) {
            uint32_t skipped_frequency = 0;
            RETURN_IF_ERROR(read_frequency(&skipped_frequency));
            RETURN_IF_ERROR(skip_positions(skipped_frequency));
            ++next_doc_ordinal_;
        }
        RETURN_IF_ERROR(read_frequency(&frequency_));
    }
    ++frame_stats_.selected_docs;
    frame_stats_.selected_positions += frequency_;
    decoded_from_doc_ = 0;
    scratch_position_ = 0;
    scratch_size_ = 0;
    previous_position_ = 0;
    first_position_ = true;
    active_doc_ = true;
    return Status::OK();
}

Status PrxPositionIterator::next_position(uint32_t* position, bool* available) {
    if (failed_ || finished_ || !active_doc_) {
        return fail(invalid_iterator_state("prx iterator: next_position in invalid state"));
    }
    if (codec_ == PrxCodec::kPfor) {
        if (decoded_from_doc_ == frequency_) {
            *available = false;
            return Status::OK();
        }
        const uint32_t stream_position = pfor_offsets_[next_doc_ordinal_] + decoded_from_doc_;
        RETURN_IF_ERROR(advance_pfor_cursor(stream_position, true, true));
        const uint32_t delta = pfor_run_[pfor_run_index_++];
        if (!first_position_ && delta > std::numeric_limits<uint32_t>::max() - previous_position_) {
            return fail(corrupted_iterator_payload("prx iterator: position accumulation overflow"));
        }
        previous_position_ = first_position_ ? delta : previous_position_ + delta;
        first_position_ = false;
        ++decoded_from_doc_;
        *position = previous_position_;
        *available = true;
        return Status::OK();
    }
    if (scratch_position_ == scratch_size_) {
        if (decoded_from_doc_ == frequency_) {
            *available = false;
            return Status::OK();
        }
        const uint32_t batch_size = std::min<uint32_t>(static_cast<uint32_t>(scratch_.size()),
                                                       frequency_ - decoded_from_doc_);
        Status status =
                payload_source_->decode_delta_batch(std::span<uint32_t>(scratch_).first(batch_size),
                                                    &previous_position_, &first_position_);
        if (!status.ok()) {
            return fail(std::move(status));
        }
        decoded_from_doc_ += batch_size;
        scratch_position_ = 0;
        scratch_size_ = batch_size;
    }
    *position = scratch_[scratch_position_++];
    *available = true;
    return Status::OK();
}

Status PrxPositionIterator::finish_doc() {
    if (failed_ || finished_ || !active_doc_) {
        return fail(invalid_iterator_state("prx iterator: finish_doc in invalid state"));
    }
    if (codec_ == PrxCodec::kPfor) {
        RETURN_IF_ERROR(
                advance_pfor_cursor(pfor_offsets_[next_doc_ordinal_ + 1], frequency_ != 0, false));
        decoded_from_doc_ = frequency_;
    } else {
        RETURN_IF_ERROR(skip_positions(frequency_ - decoded_from_doc_));
    }
    scratch_position_ = 0;
    scratch_size_ = 0;
    active_doc_ = false;
    ++next_doc_ordinal_;
    return Status::OK();
}

Status PrxPositionIterator::finish_frame() {
    if (failed_ || finished_) {
        return fail(invalid_iterator_state("prx iterator: finish_frame in invalid state"));
    }
    if (active_doc_) {
        RETURN_IF_ERROR(finish_doc());
    }
    if (codec_ == PrxCodec::kPfor) {
        RETURN_IF_ERROR(advance_pfor_cursor(pfor_offsets_.back(), false, false));
        next_doc_ordinal_ = doc_count_;
    } else {
        while (next_doc_ordinal_ < doc_count_) {
            uint32_t skipped_frequency = 0;
            RETURN_IF_ERROR(read_frequency(&skipped_frequency));
            RETURN_IF_ERROR(skip_positions(skipped_frequency));
            ++next_doc_ordinal_;
        }
    }
    if (!payload_source_->eof()) {
        return fail(corrupted_iterator_payload("prx iterator: trailing bytes after payload"));
    }
    if (context_ != nullptr && context_->stats != nullptr) {
        context_->stats->merge(frame_stats_);
    }
    if (context_ != nullptr && context_->query_stats != nullptr) {
        ++context_->query_stats->prx_streaming_frames;
    }
    finished_ = true;
    return Status::OK();
}

} // namespace doris::snii::format
