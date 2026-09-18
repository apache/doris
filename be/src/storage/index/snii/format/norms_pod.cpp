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

#include "storage/index/snii/format/norms_pod.h"

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <string_view>

#include "common/check.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::format {

namespace {

constexpr uint64_t kBlockSpan = uint64_t {1} << 16;
constexpr size_t kBlockHeaderBytes = 12;
constexpr size_t kBitsetWords = 1024;
constexpr size_t kBitsetRankEntries = 128;
constexpr size_t kWordsPerRankEntry = kBitsetWords / kBitsetRankEntries;
constexpr size_t kBitsetBlockBytes =
        kBitsetWords * sizeof(uint64_t) + kBitsetRankEntries * sizeof(uint16_t);
constexpr size_t kRunBytes = 3 * sizeof(uint16_t);

enum class BlockKind : uint8_t { kAll = 0, kArray = 1, kBitset = 2, kRuns = 3 };

Status corrupted(std::string_view reason) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("norms: {}", reason);
}

Status invalid_input(std::string_view reason) {
    return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("norms: {}", reason);
}

uint16_t load16(const uint8_t* p) {
    return static_cast<uint16_t>(p[0] | (static_cast<uint16_t>(p[1]) << 8));
}

uint32_t load32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) | (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) | (static_cast<uint32_t>(p[3]) << 24);
}

uint64_t load64(const uint8_t* p) {
    return static_cast<uint64_t>(load32(p)) | (static_cast<uint64_t>(load32(p + 4)) << 32);
}

size_t framed_section_bytes(size_t payload_bytes) {
    return 1 + varint_len(payload_bytes) + payload_bytes + sizeof(uint32_t);
}

// Ascending docids of the documents without a norm: null_docids minus null_docids_with_norms.
// The input is validated by plan_norms_section, so null_docids_with_norms is a subset.
class NormlessDocids {
public:
    NormlessDocids(std::span<const uint32_t> null_docids,
                   std::span<const uint32_t> null_docids_with_norms)
            : null_docids_(null_docids), null_docids_with_norms_(null_docids_with_norms) {
        skip_null_docids_with_norms();
    }

    bool at_end() const { return null_index_ == null_docids_.size(); }
    uint32_t value() const { return null_docids_[null_index_]; }
    void advance() {
        ++null_index_;
        skip_null_docids_with_norms();
    }

private:
    void skip_null_docids_with_norms() {
        while (null_index_ < null_docids_.size() &&
               with_norms_index_ < null_docids_with_norms_.size() &&
               null_docids_[null_index_] == null_docids_with_norms_[with_norms_index_]) {
            ++null_index_;
            ++with_norms_index_;
        }
    }

    std::span<const uint32_t> null_docids_;
    std::span<const uint32_t> null_docids_with_norms_;
    size_t null_index_ = 0;
    size_t with_norms_index_ = 0;
};

// One SPARSE block holding at least one present docid.
struct PresentBlock {
    uint32_t key;
    uint32_t span;
    uint32_t cardinality;
    uint32_t runs;
    BlockKind kind;
    size_t payload_bytes;
    // Positioned at the first docid without a norm at or after the block start.
    NormlessDocids normless;
};

// Visits the non-empty blocks in key order. Work is O(doc_count / 65536 + normless docids).
template <typename Fn>
void for_each_present_block(uint32_t doc_count, NormlessDocids normless, Fn&& fn) {
    for (uint64_t base = 0; base < doc_count; base += kBlockSpan) {
        const auto span = static_cast<uint32_t>(std::min<uint64_t>(kBlockSpan, doc_count - base));
        const NormlessDocids block_normless = normless;
        uint32_t absent = 0;
        uint32_t runs = 0;
        uint32_t next_low = 0;
        while (!normless.at_end() && normless.value() < base + span) {
            const auto low = static_cast<uint32_t>(normless.value() - base);
            if (low > next_low) {
                ++runs;
            }
            next_low = low + 1;
            ++absent;
            normless.advance();
        }
        if (next_low < span) {
            ++runs;
        }
        const uint32_t cardinality = span - absent;
        if (cardinality == 0) {
            continue;
        }
        BlockKind kind = BlockKind::kAll;
        size_t payload_bytes = 0;
        if (absent != 0) {
            const size_t array_bytes = size_t {cardinality} * sizeof(uint16_t);
            const size_t run_bytes = sizeof(uint16_t) + size_t {runs} * kRunBytes;
            if (array_bytes <= std::min(run_bytes, kBitsetBlockBytes)) {
                kind = BlockKind::kArray;
                payload_bytes = array_bytes;
            } else if (run_bytes <= kBitsetBlockBytes) {
                kind = BlockKind::kRuns;
                payload_bytes = run_bytes;
            } else {
                kind = BlockKind::kBitset;
                payload_bytes = kBitsetBlockBytes;
            }
        }
        fn(PresentBlock {.key = static_cast<uint32_t>(base >> 16),
                         .span = span,
                         .cardinality = cardinality,
                         .runs = runs,
                         .kind = kind,
                         .payload_bytes = payload_bytes,
                         .normless = block_normless});
    }
}

// Calls fn(first_low, last_low) for every maximal run of present lows of the block.
template <typename Fn>
void for_each_present_run(const PresentBlock& block, Fn&& fn) {
    NormlessDocids normless = block.normless;
    const uint64_t base = uint64_t {block.key} << 16;
    uint32_t next_low = 0;
    while (!normless.at_end() && normless.value() < base + block.span) {
        const auto low = static_cast<uint32_t>(normless.value() - base);
        if (low > next_low) {
            fn(next_low, low - 1);
        }
        next_low = low + 1;
        normless.advance();
    }
    if (next_low < block.span) {
        fn(next_low, block.span - 1);
    }
}

void write_block_payload(const PresentBlock& block, ByteSink* out) {
    switch (block.kind) {
    case BlockKind::kAll:
        return;
    case BlockKind::kArray:
        for_each_present_run(block, [out](uint32_t first, uint32_t last) {
            for (uint32_t low = first; low <= last; ++low) {
                out->put_fixed16(static_cast<uint16_t>(low));
            }
        });
        return;
    case BlockKind::kRuns: {
        out->put_fixed16(static_cast<uint16_t>(block.runs));
        uint32_t rank_before = 0;
        for_each_present_run(block, [out, &rank_before](uint32_t first, uint32_t last) {
            out->put_fixed16(static_cast<uint16_t>(first));
            out->put_fixed16(static_cast<uint16_t>(last));
            out->put_fixed16(static_cast<uint16_t>(rank_before));
            rank_before += last - first + 1;
        });
        DORIS_CHECK_EQ(rank_before, block.cardinality);
        return;
    }
    case BlockKind::kBitset: {
        std::array<uint64_t, kBitsetWords> words {};
        for_each_present_run(block, [&words](uint32_t first, uint32_t last) {
            for (uint32_t low = first; low <= last;) {
                if ((low & 63) == 0 && last - low >= 63) {
                    words[low >> 6] = ~uint64_t {0};
                    low += 64;
                } else {
                    words[low >> 6] |= uint64_t {1} << (low & 63);
                    ++low;
                }
            }
        });
        for (uint64_t word : words) {
            out->put_fixed64(word);
        }
        uint32_t rank = 0;
        for (size_t word = 0; word < kBitsetWords; ++word) {
            if (word % kWordsPerRankEntry == 0) {
                out->put_fixed16(static_cast<uint16_t>(rank));
            }
            rank += std::popcount(words[word]);
        }
        DORIS_CHECK_EQ(rank, block.cardinality);
        return;
    }
    }
    DORIS_CHECK(false);
}

Status validate_array_block(const uint8_t* payload, uint64_t available, uint32_t span,
                            uint32_t cardinality, size_t* payload_bytes) {
    *payload_bytes = size_t {cardinality} * sizeof(uint16_t);
    if (*payload_bytes > available) {
        return corrupted("ARRAY block past block data");
    }
    uint32_t previous = 0;
    for (uint32_t i = 0; i < cardinality; ++i) {
        const uint32_t low = load16(payload + i * sizeof(uint16_t));
        if ((i != 0 && low <= previous) || low >= span) {
            return corrupted("ARRAY block values not ascending inside the span");
        }
        previous = low;
    }
    return Status::OK();
}

Status validate_bitset_block(const uint8_t* payload, uint64_t available, uint32_t span,
                             uint32_t cardinality, size_t* payload_bytes) {
    *payload_bytes = kBitsetBlockBytes;
    if (*payload_bytes > available) {
        return corrupted("BITSET block past block data");
    }
    const uint8_t* rank_table = payload + kBitsetWords * sizeof(uint64_t);
    uint32_t rank = 0;
    for (size_t word_index = 0; word_index < kBitsetWords; ++word_index) {
        if (word_index % kWordsPerRankEntry == 0 &&
            load16(rank_table + word_index / kWordsPerRankEntry * sizeof(uint16_t)) != rank) {
            return corrupted("BITSET block rank table mismatch");
        }
        const uint64_t word = load64(payload + word_index * sizeof(uint64_t));
        const uint64_t word_base = word_index * 64;
        const bool bits_past_span =
                word_base >= span ? word != 0
                                  : span - word_base < 64 && (word >> (span - word_base)) != 0;
        if (bits_past_span) {
            return corrupted("BITSET block has bits past its span");
        }
        rank += std::popcount(word);
    }
    if (rank != cardinality) {
        return corrupted("BITSET block cardinality mismatch");
    }
    return Status::OK();
}

Status validate_runs_block(const uint8_t* payload, uint64_t available, uint32_t span,
                           uint32_t cardinality, size_t* payload_bytes) {
    if (available < sizeof(uint16_t)) {
        return corrupted("RUNS block past block data");
    }
    const uint32_t run_count = load16(payload);
    *payload_bytes = sizeof(uint16_t) + size_t {run_count} * kRunBytes;
    if (run_count == 0 || *payload_bytes > available) {
        return corrupted("RUNS block run count out of range");
    }
    uint32_t rank_before = 0;
    uint32_t previous_last = 0;
    for (uint32_t run = 0; run < run_count; ++run) {
        const uint8_t* entry = payload + sizeof(uint16_t) + run * kRunBytes;
        const uint32_t first = load16(entry);
        const uint32_t last = load16(entry + sizeof(uint16_t));
        if (first > last || last >= span || (run != 0 && first <= previous_last + 1)) {
            return corrupted("RUNS block runs not ascending and separated inside the span");
        }
        if (load16(entry + 2 * sizeof(uint16_t)) != rank_before) {
            return corrupted("RUNS block rank mismatch");
        }
        rank_before += last - first + 1;
        previous_last = last;
    }
    if (rank_before != cardinality) {
        return corrupted("RUNS block cardinality mismatch");
    }
    return Status::OK();
}

// Validates one block payload and returns its length.
Status validate_block(BlockKind kind, Slice block_data, uint64_t offset, uint32_t span,
                      uint32_t cardinality, size_t* payload_bytes) {
    if (offset > block_data.size()) {
        return corrupted("block payload offset past block data");
    }
    const uint64_t available = block_data.size() - offset;
    const uint8_t* payload = block_data.data() + offset;
    switch (kind) {
    case BlockKind::kAll:
        if (cardinality != span) {
            return corrupted("ALL block cardinality differs from its span");
        }
        *payload_bytes = 0;
        return Status::OK();
    case BlockKind::kArray:
        return validate_array_block(payload, available, span, cardinality, payload_bytes);
    case BlockKind::kBitset:
        return validate_bitset_block(payload, available, span, cardinality, payload_bytes);
    case BlockKind::kRuns:
        return validate_runs_block(payload, available, span, cardinality, payload_bytes);
    }
    return corrupted("unknown block kind");
}

// Validates the block headers and payloads of a sparse section against its present count and
// block data length.
Status validate_sparse_blocks(uint64_t doc_count, uint64_t present_count, Slice headers,
                              uint64_t block_count, Slice block_data) {
    uint64_t expected_rank = 0;
    uint64_t expected_offset = 0;
    for (uint64_t block = 0; block < block_count; ++block) {
        const uint8_t* header = headers.data() + block * kBlockHeaderBytes;
        const uint32_t key = load16(header);
        const uint8_t kind = header[2];
        if (kind > static_cast<uint8_t>(BlockKind::kRuns) || header[3] != 0) {
            return corrupted("invalid block header");
        }
        if (block != 0 && key <= load16(header - kBlockHeaderBytes)) {
            return corrupted("block keys not ascending");
        }
        const uint64_t base = uint64_t {key} << 16;
        if (base >= doc_count) {
            return corrupted("block outside the document domain");
        }
        if (load32(header + 4) != expected_rank || load32(header + 8) != expected_offset) {
            return corrupted("block rank or payload offset mismatch");
        }
        const uint64_t next_rank =
                block + 1 < block_count ? load32(header + kBlockHeaderBytes + 4) : present_count;
        const auto span = static_cast<uint32_t>(std::min(kBlockSpan, doc_count - base));
        if (next_rank <= expected_rank || next_rank - expected_rank > span) {
            return corrupted("block cardinality out of range");
        }
        size_t block_payload_bytes = 0;
        RETURN_IF_ERROR(validate_block(static_cast<BlockKind>(kind), block_data, expected_offset,
                                       span, static_cast<uint32_t>(next_rank - expected_rank),
                                       &block_payload_bytes));
        expected_rank = next_rank;
        expected_offset += block_payload_bytes;
    }
    if (expected_rank != present_count) {
        return corrupted("block cardinalities differ from present count");
    }
    if (expected_offset != block_data.size()) {
        return corrupted("block payloads differ from block data length");
    }
    return Status::OK();
}

// Index of low among the present lows of a block, or false when low is absent.
bool array_block_rank(const uint8_t* payload, uint32_t cardinality, uint32_t low, uint32_t* rank) {
    uint32_t first = 0;
    uint32_t count = cardinality;
    while (count > 0) {
        const uint32_t half = count / 2;
        if (load16(payload + (first + half) * sizeof(uint16_t)) < low) {
            first += half + 1;
            count -= half + 1;
        } else {
            count = half;
        }
    }
    if (first == cardinality || load16(payload + first * sizeof(uint16_t)) != low) {
        return false;
    }
    *rank = first;
    return true;
}

bool bitset_block_rank(const uint8_t* payload, uint32_t low, uint32_t* rank) {
    const uint32_t word_index = low >> 6;
    const uint64_t word = load64(payload + word_index * sizeof(uint64_t));
    if (((word >> (low & 63)) & 1) == 0) {
        return false;
    }
    const uint32_t group = low >> 9;
    uint32_t in_block =
            load16(payload + kBitsetWords * sizeof(uint64_t) + group * sizeof(uint16_t));
    for (uint32_t w = group * kWordsPerRankEntry; w < word_index; ++w) {
        in_block += std::popcount(load64(payload + w * sizeof(uint64_t)));
    }
    in_block += std::popcount(word & ((uint64_t {1} << (low & 63)) - 1));
    *rank = in_block;
    return true;
}

bool runs_block_rank(const uint8_t* payload, uint32_t low, uint32_t* rank) {
    // The last run whose first low is <= low.
    uint32_t after = 0;
    uint32_t count = load16(payload);
    while (count > 0) {
        const uint32_t half = count / 2;
        if (load16(payload + sizeof(uint16_t) + (after + half) * kRunBytes) <= low) {
            after += half + 1;
            count -= half + 1;
        } else {
            count = half;
        }
    }
    if (after == 0) {
        return false;
    }
    const uint8_t* entry = payload + sizeof(uint16_t) + (after - 1) * kRunBytes;
    const uint32_t run_first = load16(entry);
    if (low > load16(entry + sizeof(uint16_t))) {
        return false;
    }
    *rank = load16(entry + 2 * sizeof(uint16_t)) + (low - run_first);
    return true;
}

Status get_canonical_doc_count(ByteSource* payload, uint64_t* doc_count) {
    RETURN_IF_ERROR(payload->get_varint64(doc_count));
    if (payload->position() != varint_len(*doc_count)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD non-canonical doc_count");
    }
    if (*doc_count > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD doc_count overflows uint32");
    }
    return Status::OK();
}

} // namespace

void NormsPodWriter::finish(ByteSink* sink) const {
    finish(norms_, sink);
}

void NormsPodWriter::finish(std::span<const uint8_t> norms, ByteSink* sink) {
    // Build inner payload: [varint64 doc_count][raw norm bytes].
    ByteSink payload;
    const size_t payload_size = varint_len(norms.size()) + norms.size();
    payload.reserve(payload_size);
    payload.put_varint64(norms.size());
    payload.put_bytes(Slice(norms.data(), norms.size()));
    // Delegate outer framing to SectionFramer to append type+len+crc32c, avoiding manual checksum assembly.
    sink->reserve(framed_section_bytes(payload_size));
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kNormsPod), payload.view());
}

uint64_t dense_norms_section_bytes(uint64_t doc_count) {
    return framed_section_bytes(varint_len(doc_count) + doc_count);
}

Status plan_norms_section(const NormsSectionInput& in, bool force_dense, NormsSectionPlan* out) {
    for (size_t i = 0; i < in.null_docids.size(); ++i) {
        if (in.null_docids[i] >= in.doc_count ||
            (i != 0 && in.null_docids[i] <= in.null_docids[i - 1])) {
            return invalid_input("null docids must be ascending and inside the document domain");
        }
    }
    size_t null_index = 0;
    for (size_t i = 0; i < in.null_docids_with_norms.size(); ++i) {
        const uint32_t docid = in.null_docids_with_norms[i];
        while (null_index < in.null_docids.size() && in.null_docids[null_index] < docid) {
            ++null_index;
        }
        if (null_index == in.null_docids.size() || in.null_docids[null_index] != docid) {
            return invalid_input(
                    "NULL docids with norms must be an ascending subset of null docids");
        }
        ++null_index;
    }
    const uint64_t present_count =
            uint64_t {in.doc_count} - in.null_docids.size() + in.null_docids_with_norms.size();
    if (in.norms.size() != present_count) {
        return invalid_input("norm count differs from the documents that carry a norm");
    }

    NormsSectionPlan plan;
    plan.payload_bytes = varint_len(in.doc_count) + in.doc_count;
    plan.framed_bytes = dense_norms_section_bytes(in.doc_count);
    if (force_dense || present_count == in.doc_count) {
        *out = plan;
        return Status::OK();
    }

    const uint8_t constant_norm = in.norms.empty() ? 0 : in.norms.front();
    const bool all_equal =
            std::all_of(in.norms.begin(), in.norms.end(),
                        [constant_norm](uint8_t norm) { return norm == constant_norm; });
    size_t block_count = 0;
    size_t block_data_bytes = 0;
    for_each_present_block(in.doc_count, NormlessDocids(in.null_docids, in.null_docids_with_norms),
                           [&](const PresentBlock& block) {
                               ++block_count;
                               block_data_bytes += block.payload_bytes;
                           });
    const size_t norm_bytes = all_equal ? 1 : present_count;
    const size_t payload_bytes = varint_len(in.doc_count) + varint_len(present_count) + 1 +
                                 varint_len(block_count) + block_count * kBlockHeaderBytes +
                                 varint_len(block_data_bytes) + block_data_bytes + norm_bytes;
    const size_t framed_bytes = framed_section_bytes(payload_bytes);
    if (framed_bytes < plan.framed_bytes) {
        plan = NormsSectionPlan {.layout = NormsLayout::kSparse,
                                 .bytes_per_norm = static_cast<uint8_t>(all_equal ? 0 : 1),
                                 .constant_norm = all_equal ? constant_norm : uint8_t {0},
                                 .payload_bytes = payload_bytes,
                                 .framed_bytes = framed_bytes};
    }
    *out = plan;
    return Status::OK();
}

void write_norms_section(const NormsSectionInput& in, const NormsSectionPlan& plan,
                         ByteSink* sink) {
    ByteSink payload;
    payload.reserve(plan.payload_bytes);
    payload.put_varint64(in.doc_count);
    const NormlessDocids normless_begin(in.null_docids, in.null_docids_with_norms);
    auto section_type = static_cast<uint8_t>(SectionType::kNormsPod);
    if (plan.layout == NormsLayout::kDense) {
        // Documents without a norm keep kEmptyDocumentNorm, the byte every earlier writer
        // stored for them, so the section is identical to theirs.
        size_t norm_index = 0;
        uint32_t next_docid = 0;
        for (NormlessDocids normless = normless_begin; !normless.at_end(); normless.advance()) {
            const uint32_t docid = normless.value();
            payload.put_bytes(Slice(in.norms.data() + norm_index, docid - next_docid));
            norm_index += docid - next_docid;
            payload.put_u8(kEmptyDocumentNorm);
            next_docid = docid + 1;
        }
        DORIS_CHECK_EQ(in.norms.size() - norm_index, in.doc_count - next_docid);
        payload.put_bytes(Slice(in.norms.data() + norm_index, in.norms.size() - norm_index));
    } else {
        section_type = static_cast<uint8_t>(SectionType::kNormsSparse);
        payload.put_varint64(in.norms.size());
        payload.put_u8(plan.bytes_per_norm);
        size_t block_count = 0;
        size_t block_data_bytes = 0;
        for_each_present_block(in.doc_count, normless_begin, [&](const PresentBlock& block) {
            ++block_count;
            block_data_bytes += block.payload_bytes;
        });
        payload.put_varint64(block_count);
        uint32_t rank_base = 0;
        size_t payload_offset = 0;
        for_each_present_block(in.doc_count, normless_begin, [&](const PresentBlock& block) {
            payload.put_fixed16(static_cast<uint16_t>(block.key));
            payload.put_u8(static_cast<uint8_t>(block.kind));
            payload.put_u8(0);
            payload.put_fixed32(rank_base);
            payload.put_fixed32(static_cast<uint32_t>(payload_offset));
            rank_base += block.cardinality;
            payload_offset += block.payload_bytes;
        });
        DORIS_CHECK_EQ(rank_base, in.norms.size());
        payload.put_varint64(block_data_bytes);
        const size_t block_data_begin = payload.size();
        for_each_present_block(in.doc_count, normless_begin, [&](const PresentBlock& block) {
            write_block_payload(block, &payload);
        });
        DORIS_CHECK_EQ(payload.size() - block_data_begin, block_data_bytes);
        if (plan.bytes_per_norm == 0) {
            payload.put_u8(plan.constant_norm);
        } else {
            payload.put_bytes(Slice(in.norms.data(), in.norms.size()));
        }
    }
    DORIS_CHECK_EQ(payload.size(), plan.payload_bytes);

    const size_t start = sink->size();
    sink->reserve(plan.framed_bytes);
    SectionFramer::write(*sink, section_type, payload.view());
    DORIS_CHECK_EQ(sink->size() - start, plan.framed_bytes);
}

Status NormsPodReader::open(Slice framed, NormsPodReader* out) {
    // framer handles CRC verify, truncation detection, and payload slicing.
    ByteSource src(framed);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kNormsPod) &&
        sec.type != static_cast<uint8_t>(SectionType::kNormsSparse)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD section type mismatch");
    }
    if (!src.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD trailing framed bytes");
    }
    if (sec.type == static_cast<uint8_t>(SectionType::kNormsSparse)) {
        return open_sparse(sec.payload, out);
    }

    // Parse inner payload: [varint64 doc_count][bytes].
    ByteSource payload(sec.payload);
    uint64_t doc_count = 0;
    RETURN_IF_ERROR(get_canonical_doc_count(&payload, &doc_count));
    // doc_count must exactly equal the remaining byte count (1 byte per doc).
    if (payload.remaining() != doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD length mismatch");
    }

    Slice bytes;
    RETURN_IF_ERROR(payload.get_bytes(static_cast<size_t>(doc_count), &bytes));
    NormsPodReader reader;
    reader.layout_ = NormsLayout::kDense;
    reader.doc_count_ = static_cast<uint32_t>(doc_count);
    reader.present_count_ = reader.doc_count_;
    reader.norms_ = bytes.data();
    *out = reader;
    return Status::OK();
}

Status NormsPodReader::open_sparse(Slice payload_bytes, NormsPodReader* out) {
    ByteSource payload(payload_bytes);
    uint64_t doc_count = 0;
    RETURN_IF_ERROR(get_canonical_doc_count(&payload, &doc_count));
    uint64_t present_count = 0;
    RETURN_IF_ERROR(payload.get_varint64(&present_count));
    if (present_count > doc_count) {
        return corrupted("present count exceeds doc count");
    }
    uint8_t bytes_per_norm = 0;
    RETURN_IF_ERROR(payload.get_u8(&bytes_per_norm));
    if (bytes_per_norm > 1) {
        return corrupted("bytes_per_norm out of range");
    }
    uint64_t block_count = 0;
    RETURN_IF_ERROR(payload.get_varint64(&block_count));
    if (block_count > payload.remaining() / kBlockHeaderBytes) {
        return corrupted("block headers past section end");
    }
    Slice headers;
    RETURN_IF_ERROR(
            payload.get_bytes(static_cast<size_t>(block_count * kBlockHeaderBytes), &headers));
    uint64_t block_data_bytes = 0;
    RETURN_IF_ERROR(payload.get_varint64(&block_data_bytes));
    Slice block_data;
    RETURN_IF_ERROR(payload.get_bytes(static_cast<size_t>(block_data_bytes), &block_data));
    uint8_t constant_norm = 0;
    Slice norms;
    if (bytes_per_norm == 0) {
        RETURN_IF_ERROR(payload.get_u8(&constant_norm));
    } else {
        RETURN_IF_ERROR(payload.get_bytes(static_cast<size_t>(present_count), &norms));
    }
    if (!payload.eof()) {
        return corrupted("trailing sparse payload bytes");
    }

    RETURN_IF_ERROR(
            validate_sparse_blocks(doc_count, present_count, headers, block_count, block_data));

    NormsPodReader reader;
    reader.layout_ = NormsLayout::kSparse;
    reader.doc_count_ = static_cast<uint32_t>(doc_count);
    reader.present_count_ = static_cast<uint32_t>(present_count);
    reader.norms_ = norms.data();
    reader.bytes_per_norm_ = bytes_per_norm;
    reader.constant_norm_ = constant_norm;
    reader.block_count_ = static_cast<uint32_t>(block_count);
    reader.block_headers_ = headers.data();
    reader.block_data_ = block_data.data();
    *out = reader;
    return Status::OK();
}

bool NormsPodReader::sparse_rank(uint32_t docid, uint32_t* rank) const {
    const uint32_t key = docid >> 16;
    const uint32_t low = docid & 0xFFFFU;
    uint32_t block = 0;
    uint32_t count = block_count_;
    while (count > 0) {
        const uint32_t half = count / 2;
        if (load16(block_headers_ + (block + half) * kBlockHeaderBytes) < key) {
            block += half + 1;
            count -= half + 1;
        } else {
            count = half;
        }
    }
    if (block == block_count_) {
        return false;
    }
    const uint8_t* header = block_headers_ + block * kBlockHeaderBytes;
    if (load16(header) != key) {
        return false;
    }
    const uint32_t rank_base = load32(header + 4);
    const uint8_t* payload = block_data_ + load32(header + 8);
    uint32_t in_block = 0;
    bool present = false;
    switch (static_cast<BlockKind>(header[2])) {
    case BlockKind::kAll:
        in_block = low;
        present = true;
        break;
    case BlockKind::kArray: {
        const uint32_t next_rank =
                block + 1 < block_count_ ? load32(header + kBlockHeaderBytes + 4) : present_count_;
        present = array_block_rank(payload, next_rank - rank_base, low, &in_block);
        break;
    }
    case BlockKind::kBitset:
        present = bitset_block_rank(payload, low, &in_block);
        break;
    case BlockKind::kRuns:
        present = runs_block_rank(payload, low, &in_block);
        break;
    }
    *rank = rank_base + in_block;
    return present;
}

uint8_t NormsPodReader::sparse_encoded_norm(uint32_t docid) const {
    uint32_t rank = 0;
    const bool has_norm = sparse_rank(docid, &rank);
    // Per-posting hot path: the docid comes from a decoded posting, so it carries a norm. For a
    // docid without one, rank stays inside the section (0 or the rank base of a stored block).
    DCHECK(has_norm);
    return bytes_per_norm_ == 0 ? constant_norm_ : norms_[rank];
}

Status NormsPodReader::try_sparse_encoded_norm(uint32_t docid, uint8_t* out) const {
    uint32_t rank = 0;
    if (!sparse_rank(docid, &rank)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms: docid {} carries no norm", docid);
    }
    *out = bytes_per_norm_ == 0 ? constant_norm_ : norms_[rank];
    return Status::OK();
}

} // namespace doris::snii::format
