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

#include "storage/index/snii/writer/posting_prx_encoder.h"

#define ZSTD_STATIC_LINKING_ONLY
#include <zstd.h>

#include <algorithm>
#include <array>
#include <limits>
#include <memory>

#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/pfor.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/io/file_writer.h"
#include "storage/index/snii/writer/term_posting_source.h"

namespace doris::snii::writer {
namespace {

Status posting_zstd_status(size_t result) {
    if (ZSTD_isError(result)) {
        return Status::Error<ErrorCode::INTERNAL_ERROR, false>("posting zstd: {}",
                                                               ZSTD_getErrorName(result));
    }
    return Status::OK();
}

Status invalid_position_input(const char* reason) {
    return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("prx: {}", reason);
}

Status append_sink(PostingByteBuffer* buffer, const ByteSink& sink) {
    return buffer->append({sink.view().data(), sink.size()});
}

Status admit_resident_encoder(uint64_t positions, size_t documents, int level,
                              MemoryReporter* reporter, MemoryReporter::Reservation* reservation,
                              bool* admitted) {
    // Cover overlapping candidate vectors, geometric growth, and the one-shot
    // compressor before entering the existing fast path.
    const size_t plain_bound = 5 * (positions + documents + 1);
    const int compression_level = level < 0 ? -level : level;
    size_t context_bytes = 0;
    if (level != 0 && (level > 0 || plain_bound >= format::kPrxAutoZstdMinBytes)) {
        context_bytes = ZSTD_estimateCCtxSize_usingCParams(
                ZSTD_getCParams(compression_level, plain_bound, 0));
        RETURN_IF_ERROR(posting_zstd_status(context_bytes));
    }
    const uint64_t scratch = 40 * (positions + documents) + 4096 + context_bytes;
    const uint64_t available =
            reporter == nullptr ? 32ULL * 1024 * 1024 : reporter->postings_available_bytes();
    *admitted = scratch <= available;
    if (!*admitted || reporter == nullptr) {
        return Status::OK();
    }
    const Status status = reservation->set_bytes(scratch);
    if (status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) {
        *admitted = false;
        return Status::OK();
    }
    return status;
}

class PositionDeltaEncoder {
public:
    PositionDeltaEncoder(PostingByteBuffer* plain, PostingByteBuffer* pfor)
            : plain_(plain), pfor_(pfor) {}

    Status append(uint32_t delta) {
        RETURN_IF_ERROR(plain_->append_varint(delta));
        if (pfor_ == nullptr) {
            return Status::OK();
        }
        deltas_[count_++] = delta;
        if (count_ == deltas_.size()) {
            return flush();
        }
        return Status::OK();
    }

    Status flush() {
        if (count_ == 0) {
            return Status::OK();
        }
        encoded_.clear();
        pfor_encode(deltas_.data(), count_, &encoded_);
        RETURN_IF_ERROR(append_sink(pfor_, encoded_));
        count_ = 0;
        return Status::OK();
    }

private:
    PostingByteBuffer* plain_;
    PostingByteBuffer* pfor_;
    std::array<uint32_t, format::kFrqBaseUnit> deltas_ {};
    size_t count_ = 0;
    ByteSink encoded_;
};

Status encode_document_positions(const PostingPositionView& positions, uint64_t offset,
                                 uint32_t frequency, std::span<uint32_t> input,
                                 PositionDeltaEncoder* deltas) {
    uint32_t previous = 0;
    for (uint64_t ordinal = 0; ordinal < frequency;) {
        const size_t count = std::min<uint64_t>(input.size(), frequency - ordinal);
        RETURN_IF_ERROR(positions.read(offset + ordinal, input.first(count)));
        for (size_t i = 0; i < count; ++i) {
            const uint32_t value = input[i];
            if (ordinal + i != 0 && value < previous) {
                return invalid_position_input("positions within a doc must be ascending");
            }
            RETURN_IF_ERROR(deltas->append(value - previous));
            previous = value;
        }
        ordinal += count;
    }
    return Status::OK();
}

Status encode_pfor_frequencies(std::span<const uint32_t> freqs, uint64_t positions,
                               PostingByteBuffer* pfor) {
    RETURN_IF_ERROR(pfor->append_varint(freqs.size()));
    RETURN_IF_ERROR(pfor->append_varint(positions));
    ByteSink encoded;
    for (size_t begin = 0; begin < freqs.size(); begin += format::kFrqBaseUnit) {
        encoded.clear();
        pfor_encode(freqs.data() + begin,
                    std::min<size_t>(format::kFrqBaseUnit, freqs.size() - begin), &encoded);
        RETURN_IF_ERROR(append_sink(pfor, encoded));
    }
    return Status::OK();
}

Status encode_position_payloads(const PostingPositionView& positions,
                                std::span<const uint32_t> freqs, PostingByteBuffer* plain,
                                PostingByteBuffer* pfor) {
    constexpr size_t kReadPositions = 16 * 1024;
    auto scratch = plain->reporter() == nullptr ? MemoryReporter::Reservation()
                                                : plain->reporter()->make_postings_reservation();
    if (plain->reporter() != nullptr) {
        RETURN_IF_ERROR(scratch.set_bytes(kReadPositions * sizeof(uint32_t) + 16 * 1024));
    }
    RETURN_IF_ERROR(plain->append_varint(freqs.size()));
    if (pfor != nullptr) {
        RETURN_IF_ERROR(encode_pfor_frequencies(freqs, positions.count, pfor));
    }
    auto input = std::make_unique<uint32_t[]>(kReadPositions);
    PositionDeltaEncoder deltas(plain, pfor);
    uint64_t offset = 0;
    for (uint32_t frequency : freqs) {
        RETURN_IF_ERROR(plain->append_varint(frequency));
        RETURN_IF_ERROR(encode_document_positions(positions, offset, frequency,
                                                  {input.get(), kReadPositions}, &deltas));
        offset += frequency;
    }
    return deltas.flush();
}

Status choose_position_codec(PostingByteBuffer* plain, PostingByteBuffer* pfor,
                             PostingByteBuffer* compressed, int level, bool singleton,
                             uint32_t byte_limit, format::PrxCodec* codec) {
    if (level >= 0) {
        *codec = format::PrxCodec::kRaw;
        if (level > 0) {
            RETURN_IF_ERROR(compress_posting_bytes(plain, level, compressed));
            *codec = format::PrxCodec::kZstd;
        }
        return Status::OK();
    }
    *codec = singleton ? format::PrxCodec::kRaw : format::PrxCodec::kPfor;
    const bool use_zstd = plain->size() >= format::kPrxAutoZstdMinBytes;
    if (singleton || plain->size() > byte_limit || (!use_zstd && pfor->size() <= byte_limit)) {
        return Status::OK();
    }
    if (use_zstd) {
        RETURN_IF_ERROR(compress_posting_bytes(plain, -level, compressed));
    }
    *codec = format::select_auto_prx_codec(pfor->size(), plain->size(), compressed->size(),
                                           use_zstd, byte_limit)
                     .codec;
    return Status::OK();
}

// A split is valid only if each resulting document can be represented. This
// counting pass preserves the legacy failure contract without a per-doc vector.
Status validate_split_documents(const PostingPositionView& positions,
                                std::span<const uint32_t> freqs, bool auto_codec,
                                const format::PrxWindowLimits& limits, MemoryReporter* reporter) {
    auto reservation = reporter == nullptr ? MemoryReporter::Reservation()
                                           : reporter->make_postings_reservation();
    if (reporter != nullptr) {
        RETURN_IF_ERROR(reservation.set_bytes(4096));
    }
    std::array<uint32_t, format::kFrqBaseUnit> block {};
    ByteSink encoded;
    uint64_t offset = 0;
    for (uint32_t frequency : freqs) {
        if (frequency > limits.max_positions) {
            return invalid_position_input("one document exceeds the writer window position limit");
        }
        uint64_t plain_size = 1 + varint_len(frequency);
        uint64_t pfor_size = 1 + varint_len(frequency);
        if (auto_codec) {
            encoded.clear();
            pfor_encode(&frequency, 1, &encoded);
            pfor_size += encoded.size();
        }
        uint32_t previous = 0;
        for (uint64_t ordinal = 0; ordinal < frequency;) {
            const size_t count = std::min<uint64_t>(block.size(), frequency - ordinal);
            RETURN_IF_ERROR(positions.read(offset, std::span(block).first(count)));
            for (size_t i = 0; i < count; ++i) {
                const uint32_t position = block[i];
                if (ordinal + i != 0 && position < previous) {
                    return invalid_position_input("positions within a doc must be ascending");
                }
                block[i] = position - previous;
                plain_size += varint_len(block[i]);
                previous = position;
            }
            if (auto_codec) {
                encoded.clear();
                pfor_encode(block.data(), count, &encoded);
                pfor_size += encoded.size();
            }
            ordinal += count;
            offset += count;
        }
        if (plain_size > limits.max_uncomp_bytes &&
            (!auto_codec || pfor_size > limits.max_uncomp_bytes)) {
            return invalid_position_input("one document exceeds the writer window byte limit");
        }
    }
    return Status::OK();
}

} // namespace

Status PostingPositionView::read(uint64_t begin, std::span<uint32_t> destination) const {
    if (begin > count || destination.size() > count - begin) {
        return invalid_position_input("position slice out of range");
    }
    if (buffer != nullptr) {
        if (offset > buffer->position_count() || count > buffer->position_count() - offset) {
            return invalid_position_input("position view exceeds its replayable input");
        }
        return buffer->read_positions(offset + begin, destination);
    }
    if (offset > flat.size() || count > flat.size() - offset) {
        return invalid_position_input("position view exceeds its flat input");
    }
    std::ranges::copy(flat.subspan(offset + begin, destination.size()), destination.begin());
    return Status::OK();
}

Status compress_posting_bytes(PostingByteBuffer* input, int level, PostingByteBuffer* output) {
    const ZSTD_compressionParameters params = ZSTD_getCParams(level, input->size(), 0);
    const size_t estimate = ZSTD_estimateCStreamSize_usingCParams(params);
    RETURN_IF_ERROR(posting_zstd_status(estimate));
    const size_t workspace_words = (estimate + sizeof(uint64_t) - 1) / sizeof(uint64_t);
    const size_t output_capacity = ZSTD_CStreamOutSize();
    auto reservation = input->reporter() == nullptr
                               ? MemoryReporter::Reservation()
                               : input->reporter()->make_postings_reservation();
    if (input->reporter() != nullptr) {
        RETURN_IF_ERROR(
                reservation.set_bytes(workspace_words * sizeof(uint64_t) + output_capacity));
    }
    auto workspace = std::make_unique<uint64_t[]>(workspace_words);
    auto encoded = std::make_unique<uint8_t[]>(output_capacity);
    ZSTD_CCtx* context = ZSTD_initStaticCCtx(workspace.get(), workspace_words * sizeof(uint64_t));
    if (context == nullptr) {
        return invalid_position_input("cannot initialize static ZSTD workspace");
    }
    RETURN_IF_ERROR(
            posting_zstd_status(ZSTD_CCtx_setParameter(context, ZSTD_c_compressionLevel, level)));
    RETURN_IF_ERROR(posting_zstd_status(ZSTD_CCtx_setPledgedSrcSize(context, input->size())));
    PostingByteCursor cursor(input);
    RETURN_IF_ERROR(cursor.reset());
    do {
        std::span<const uint8_t> bytes;
        RETURN_IF_ERROR(cursor.next_span(&bytes));
        ZSTD_inBuffer source {bytes.data(), bytes.size(), 0};
        const bool last = cursor.remaining() == 0;
        size_t pending = 1;
        do {
            ZSTD_outBuffer destination {encoded.get(), output_capacity, 0};
            pending = ZSTD_compressStream2(context, &destination, &source,
                                           last ? ZSTD_e_end : ZSTD_e_continue);
            RETURN_IF_ERROR(posting_zstd_status(pending));
            RETURN_IF_ERROR(output->append({encoded.get(), destination.pos}));
        } while (source.pos != source.size || (last && pending != 0));
    } while (cursor.remaining() != 0);
    return Status::OK();
}

PostingPrxEncoder::PostingPrxEncoder(MemoryReporter* reporter)
        : reporter_(reporter),
          resident_reservation_(reporter == nullptr ? MemoryReporter::Reservation()
                                                    : reporter->make_postings_reservation()),
          replayable_(reporter) {}

void PostingPrxEncoder::clear() {
    std::vector<uint8_t>().swap(resident_);
    resident_reservation_.reset();
    replayable_.release();
    replayable_result_ = false;
}

uint64_t PostingPrxEncoder::size() const {
    return replayable_result_ ? replayable_.size() : resident_.size();
}

bool PostingPrxEncoder::resident() const {
    return !replayable_result_ || !replayable_.spilled();
}

Slice PostingPrxEncoder::resident_bytes() const {
    DORIS_CHECK(resident());
    if (!replayable_result_) {
        return Slice(resident_);
    }
    const auto bytes = replayable_.resident_bytes();
    return {bytes.data(), bytes.size()};
}

Status PostingPrxEncoder::stream_into(io::FileWriter* output) {
    if (replayable_result_) {
        return replayable_.stream_into(output);
    }
    return output->append(Slice(resident_));
}

Status PostingPrxEncoder::copy_to(PostingByteBuffer* output) {
    if (replayable_result_) {
        return replayable_.copy_to(output);
    }
    return output->append(resident_);
}

Status PostingPrxEncoder::visit_bytes(const std::function<Status(Slice)>& append) {
    if (!replayable_result_) {
        return append(Slice(resident_));
    }
    PostingByteCursor cursor(&replayable_);
    RETURN_IF_ERROR(cursor.reset());
    while (cursor.remaining() != 0) {
        std::span<const uint8_t> bytes;
        RETURN_IF_ERROR(cursor.next_span(&bytes));
        RETURN_IF_ERROR(append(Slice(bytes.data(), bytes.size())));
    }
    return Status::OK();
}

Status PostingPrxEncoder::freeze_inline() {
    DORIS_CHECK(replayable_result_);
    return replayable_.spill_and_release_buffer();
}

Status PostingPrxEncoder::build(const PostingPositionView& positions,
                                std::span<const uint32_t> freqs, int level,
                                const format::PrxWindowLimits& limits,
                                format::PrxWindowBuildOutcome* outcome) {
    clear();
    if (outcome == nullptr) {
        return invalid_position_input("null build outcome");
    }
    if (level == -1) {
        level = -3; // Existing auto-mode default.
    }
    RETURN_IF_ERROR(format::validate_prx_window_limits(limits));
    uint64_t total = 0;
    for (uint32_t frequency : freqs) {
        if (frequency > positions.count - total) {
            return invalid_position_input("position partition mismatch");
        }
        total += frequency;
    }
    if (total != positions.count) {
        return invalid_position_input("position partition mismatch");
    }
    if (freqs.size() > limits.max_docs || total > limits.max_positions) {
        RETURN_IF_ERROR(validate_split_documents(positions, freqs, level < 0, limits, reporter_));
        if (freqs.size() <= 1) {
            return invalid_position_input("one document exceeds the writer window shape limit");
        }
        *outcome = format::PrxWindowBuildOutcome::kNeedsSplit;
        return Status::OK();
    }
    if (positions.buffer == nullptr) {
        bool admitted = false;
        RETURN_IF_ERROR(admit_resident_encoder(positions.count, freqs.size(), level, reporter_,
                                               &resident_reservation_, &admitted));
        if (admitted) {
            if (positions.offset > positions.flat.size() ||
                positions.count > positions.flat.size() - positions.offset) {
                return invalid_position_input("position view exceeds its flat input");
            }
            ByteSink sink;
            RETURN_IF_ERROR(format::try_build_prx_window_flat(
                    positions.flat.subspan(positions.offset, positions.count), freqs, level, limits,
                    &sink, outcome));
            resident_ = sink.take();
            if (reporter_ != nullptr) {
                RETURN_IF_ERROR(resident_reservation_.set_bytes(resident_.capacity()));
            }
            return Status::OK();
        }
    }
    replayable_result_ = true;
    return build_replayable(positions, freqs, level, limits, outcome);
}

Status PostingPrxEncoder::build_replayable(const PostingPositionView& positions,
                                           std::span<const uint32_t> freqs, int level,
                                           const format::PrxWindowLimits& limits,
                                           format::PrxWindowBuildOutcome* outcome) {
    PostingByteBuffer plain(reporter_);
    PostingByteBuffer pfor(reporter_);
    PostingByteBuffer compressed(reporter_);
    // All read/delta scratch is released by this call before compression.
    RETURN_IF_ERROR(
            encode_position_payloads(positions, freqs, &plain, level < 0 ? &pfor : nullptr));
    const bool plain_readable = plain.size() <= limits.max_uncomp_bytes;
    const bool pfor_readable = level < 0 && pfor.size() <= limits.max_uncomp_bytes;
    if (!plain_readable && !pfor_readable) {
        if (freqs.size() <= 1) {
            return invalid_position_input("one document exceeds the writer window byte limit");
        }
        RETURN_IF_ERROR(validate_split_documents(positions, freqs, level < 0, limits, reporter_));
        *outcome = format::PrxWindowBuildOutcome::kNeedsSplit;
        return Status::OK();
    }
    format::PrxCodec codec = format::PrxCodec::kRaw;
    RETURN_IF_ERROR(choose_position_codec(&plain, &pfor, &compressed, level,
                                          freqs.size() == 1 && freqs.front() == 1,
                                          limits.max_uncomp_bytes, &codec));
    ByteSink header;
    header.put_u8(static_cast<uint8_t>(codec));
    header.put_varint32(
            static_cast<uint32_t>(codec == format::PrxCodec::kPfor ? pfor.size() : plain.size()));
    if (codec == format::PrxCodec::kZstd) {
        header.put_varint32(static_cast<uint32_t>(compressed.size()));
    }
    RETURN_IF_ERROR(append_sink(&replayable_, header));
    PostingByteBuffer* selected = &plain;
    if (codec == format::PrxCodec::kPfor) {
        selected = &pfor;
    }
    if (codec == format::PrxCodec::kZstd) {
        selected = &compressed;
    }
    RETURN_IF_ERROR(selected->copy_to(&replayable_));
    uint32_t crc = 0;
    RETURN_IF_ERROR(replayable_.checksum(&crc));
    RETURN_IF_ERROR(replayable_.append_u32(std::span(&crc, 1)));
    *outcome = format::PrxWindowBuildOutcome::kBuilt;
    return Status::OK();
}

} // namespace doris::snii::writer
