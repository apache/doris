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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <numeric>
#include <span>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/pfor.h"
#include "storage/index/snii/encoding/zstd_codec.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/prx_decode_stats.h"
#include "storage/index/snii/format/prx_frame.h"
#include "storage/index/snii/format/prx_pod.h"

namespace doris::snii::format {
namespace {

using PerDoc = std::vector<std::vector<uint32_t>>;

PerDoc make_repeated_positions(uint32_t doc_count, uint32_t positions_per_doc) {
    PerDoc docs(doc_count);
    for (auto& positions : docs) {
        positions.resize(positions_per_doc);
        std::iota(positions.begin(), positions.end(), 0U);
    }
    return docs;
}

std::vector<uint8_t> make_raw_frame(Slice payload) {
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kRaw));
    framed.put_varint32(static_cast<uint32_t>(payload.size()));
    framed.put_bytes(payload);
    framed.put_fixed32(crc32c(framed.view()));
    return framed.take();
}

void append_pfor_runs(std::span<const uint32_t> values, ByteSink* sink) {
    for (size_t offset = 0; offset < values.size(); offset += kFrqBaseUnit) {
        const size_t run_length = std::min<size_t>(kFrqBaseUnit, values.size() - offset);
        pfor_encode(values.data() + offset, run_length, sink);
    }
}

std::vector<uint32_t> make_document_deltas(std::span<const uint32_t> counts) {
    std::vector<uint32_t> deltas;
    deltas.reserve(std::accumulate(counts.begin(), counts.end(), 0U));
    for (size_t doc = 0; doc < counts.size(); ++doc) {
        for (uint32_t position = 0; position < counts[doc]; ++position) {
            deltas.push_back(position == 0 ? static_cast<uint32_t>(doc + 1) : 1U);
        }
    }
    return deltas;
}

ByteSink make_pfor_frame_from_position_runs(std::span<const uint32_t> counts,
                                            uint32_t declared_total_positions, Slice position_runs,
                                            std::span<const uint8_t> trailing = {}) {
    ByteSink payload;
    payload.put_varint32(static_cast<uint32_t>(counts.size()));
    payload.put_varint32(declared_total_positions);
    append_pfor_runs(counts, &payload);
    payload.put_bytes(position_runs);
    payload.put_bytes(Slice(trailing.data(), trailing.size()));

    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(PrxCodec::kPfor));
    framed.put_varint32(static_cast<uint32_t>(payload.size()));
    framed.put_bytes(payload.view());
    framed.put_fixed32(crc32c(framed.view()));
    return framed;
}

ByteSink make_pfor_frame(std::span<const uint32_t> counts, std::span<const uint32_t> deltas,
                         std::span<const uint8_t> trailing = {}) {
    ByteSink position_runs;
    append_pfor_runs(deltas, &position_runs);
    return make_pfor_frame_from_position_runs(counts, static_cast<uint32_t>(deltas.size()),
                                              position_runs.view(), trailing);
}

void expect_next_position(PrxPositionIterator* iterator, uint32_t expected) {
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(iterator->next_position(&position, &available).ok());
    ASSERT_TRUE(available);
    EXPECT_EQ(position, expected);
}

void expect_document_positions(PrxPositionIterator* iterator,
                               std::span<const uint32_t> expected_positions) {
    for (uint32_t expected : expected_positions) {
        expect_next_position(iterator, expected);
    }
    uint32_t position = 0;
    bool available = true;
    ASSERT_TRUE(iterator->next_position(&position, &available).ok());
    EXPECT_FALSE(available);
}

void expect_first_position_and_finish(PrxPositionIterator* iterator, uint32_t ordinal,
                                      uint32_t expected_frequency, uint32_t expected_position) {
    ASSERT_TRUE(iterator->seek(ordinal).ok());
    EXPECT_EQ(iterator->freq(), expected_frequency);
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(iterator->next_position(&position, &available).ok());
    EXPECT_TRUE(available);
    EXPECT_EQ(position, expected_position);
    ASSERT_TRUE(iterator->finish_doc().ok());
}

void expect_all_positions_and_finish(PrxPositionIterator* iterator, uint32_t ordinal,
                                     std::span<const uint32_t> expected_positions) {
    ASSERT_TRUE(iterator->seek(ordinal).ok());
    EXPECT_EQ(iterator->freq(), expected_positions.size());
    expect_document_positions(iterator, expected_positions);
    ASSERT_TRUE(iterator->finish_doc().ok());
}

void expect_first_hit_stats(const PrxDecodeStats& stats) {
    EXPECT_EQ(stats.selected_positions, 96U);
}

void expect_sparse_decode_stats(const PrxDecodeStats& stats) {
    EXPECT_EQ(stats.total_docs, 5U);
    EXPECT_EQ(stats.total_positions, 10U);
    EXPECT_EQ(stats.selected_docs, 2U);
    EXPECT_EQ(stats.selected_positions, 5U);
    EXPECT_EQ(stats.decode_ns, 0U);
}

void rewrite_trailing_crc(std::vector<uint8_t>* frame) {
    const size_t crc_offset = frame->size() - sizeof(uint32_t);
    const uint32_t crc = crc32c(Slice(frame->data(), crc_offset));
    (*frame)[crc_offset] = static_cast<uint8_t>(crc);
    (*frame)[crc_offset + 1] = static_cast<uint8_t>(crc >> 8);
    (*frame)[crc_offset + 2] = static_cast<uint8_t>(crc >> 16);
    (*frame)[crc_offset + 3] = static_cast<uint8_t>(crc >> 24);
}

void expect_frame_codec(Slice frame, PrxCodec expected) {
    ByteSource source(frame);
    PrxFrameView view;
    ASSERT_TRUE(read_prx_frame(&source, &view).ok());
    EXPECT_EQ(view.codec, expected);
    EXPECT_TRUE(source.eof());
}

std::vector<uint8_t> make_plain_payload(std::span<const uint32_t> counts,
                                        std::span<const uint32_t> deltas) {
    ByteSink payload;
    payload.put_varint32(static_cast<uint32_t>(counts.size()));
    size_t offset = 0;
    for (uint32_t count : counts) {
        payload.put_varint32(count);
        for (uint32_t position = 0; position < count; ++position) {
            payload.put_varint32(deltas[offset++]);
        }
    }
    EXPECT_EQ(offset, deltas.size());
    return payload.take();
}

std::vector<uint8_t> make_plain_frame(Slice payload, PrxCodec codec) {
    EXPECT_TRUE(codec == PrxCodec::kRaw || codec == PrxCodec::kZstd);
    ByteSink framed;
    framed.put_u8(static_cast<uint8_t>(codec));
    framed.put_varint32(static_cast<uint32_t>(payload.size()));
    if (codec == PrxCodec::kZstd) {
        std::vector<uint8_t> compressed;
        EXPECT_TRUE(zstd_compress(payload, /*level=*/3, &compressed).ok());
        framed.put_varint32(static_cast<uint32_t>(compressed.size()));
        framed.put_bytes(Slice(compressed));
    } else {
        framed.put_bytes(payload);
    }
    framed.put_fixed32(crc32c(framed.view()));
    return framed.take();
}

void expect_failed_frame_preserves_profile(
        Slice frame, uint32_t expected_doc_count, std::span<const uint32_t> selected_ordinals,
        const std::function<Status(PrxPositionIterator*)>& consume) {
    PrxDecodeStats decode_stats;
    decode_stats.raw_frames = 7;
    const PrxDecodeStats decode_before = decode_stats;
    PhraseQueryExecutionStats query_stats;
    query_stats.prx_streaming_frames = 11;
    PrxDecodeContext context {.stats = &decode_stats, .query_stats = &query_stats};
    PrxPositionIterator iterator;

    Status status = iterator.reset(frame, expected_doc_count, selected_ordinals, &context);
    if (status.ok()) {
        status = consume(&iterator);
    }
    EXPECT_FALSE(status.ok()) << status;
    EXPECT_EQ(decode_stats, decode_before);
    EXPECT_EQ(query_stats.prx_streaming_frames, 11U);
}

class PrxPositionIteratorRawTest : public ::testing::TestWithParam<int> {};

TEST_P(PrxPositionIteratorRawTest, FirstHitDecodesOneScratchBatchPerDocument) {
    const auto docs = make_repeated_positions(/*doc_count=*/2, /*positions_per_doc=*/48);
    ByteSink frame;
    ASSERT_TRUE(build_prx_window(docs, GetParam(), &frame).ok());
    expect_frame_codec(frame.view(), GetParam() == 0 ? PrxCodec::kRaw : PrxCodec::kZstd);

    PrxDecodeStats decode_stats;
    PrxDecodeContext context {.stats = &decode_stats};
    PrxPositionIterator iterator;
    ASSERT_TRUE(iterator.reset(frame.view(), docs.size(), {}, &context).ok());
    for (uint32_t ordinal = 0; ordinal < 2; ++ordinal) {
        expect_first_position_and_finish(&iterator, ordinal, 48U, 0U);
    }
    ASSERT_TRUE(iterator.finish_frame().ok());
    expect_first_hit_stats(decode_stats);
}

TEST_P(PrxPositionIteratorRawTest, SparseSelectedOrdinalsDecodeOnlySelectedDocs) {
    const PerDoc docs {{1, 3}, {5, 8, 13}, {2}, {7, 11}, {17, 19}};
    const std::vector<uint32_t> selected {1, 4};
    ByteSink frame;
    ASSERT_TRUE(build_prx_window(docs, GetParam(), &frame).ok());
    expect_frame_codec(frame.view(), GetParam() == 0 ? PrxCodec::kRaw : PrxCodec::kZstd);

    PrxDecodeStats decode_stats;
    PrxDecodeContext context {.stats = &decode_stats};
    PrxPositionIterator iterator;
    testing::reset_prx_clock_read_count();
    ASSERT_TRUE(iterator.reset(frame.view(), docs.size(), selected, &context).ok());
    for (uint32_t ordinal : selected) {
        expect_all_positions_and_finish(&iterator, ordinal, docs[ordinal]);
    }
    ASSERT_TRUE(iterator.finish_frame().ok());
    expect_sparse_decode_stats(decode_stats);
    EXPECT_EQ(testing::prx_clock_read_count(), 0U);
}

TEST_P(PrxPositionIteratorRawTest, RejectsTruncatedPositionValueWithoutCommittingProfile) {
    const std::vector<uint32_t> counts = {1};
    const std::vector<uint32_t> deltas = {300};
    std::vector<uint8_t> payload = make_plain_payload(counts, deltas);
    ASSERT_GE(payload.size(), 2U);
    payload.pop_back();
    const PrxCodec codec = GetParam() == 0 ? PrxCodec::kRaw : PrxCodec::kZstd;
    const std::vector<uint8_t> frame = make_plain_frame(Slice(payload), codec);
    const std::vector<uint32_t> selected = {0};

    expect_failed_frame_preserves_profile(Slice(frame), counts.size(), selected,
                                          [](PrxPositionIterator* iterator) {
                                              RETURN_IF_ERROR(iterator->seek(0));
                                              uint32_t position = 0;
                                              bool available = false;
                                              return iterator->next_position(&position, &available);
                                          });
}

TEST_P(PrxPositionIteratorRawTest, RejectsPositionPrefixOverflowWithoutCommittingProfile) {
    const std::vector<uint32_t> counts = {2};
    const std::vector<uint32_t> deltas = {std::numeric_limits<uint32_t>::max(), 1};
    const std::vector<uint8_t> payload = make_plain_payload(counts, deltas);
    const PrxCodec codec = GetParam() == 0 ? PrxCodec::kRaw : PrxCodec::kZstd;
    const std::vector<uint8_t> frame = make_plain_frame(Slice(payload), codec);
    const std::vector<uint32_t> selected = {0};

    expect_failed_frame_preserves_profile(Slice(frame), counts.size(), selected,
                                          [](PrxPositionIterator* iterator) {
                                              RETURN_IF_ERROR(iterator->seek(0));
                                              uint32_t position = 0;
                                              bool available = false;
                                              return iterator->next_position(&position, &available);
                                          });
}

TEST_P(PrxPositionIteratorRawTest, RejectsTrailingPayloadByteWithoutCommittingProfile) {
    const std::vector<uint32_t> counts = {1};
    const std::vector<uint32_t> deltas = {7};
    std::vector<uint8_t> payload = make_plain_payload(counts, deltas);
    payload.push_back(0x55);
    const PrxCodec codec = GetParam() == 0 ? PrxCodec::kRaw : PrxCodec::kZstd;
    const std::vector<uint8_t> frame = make_plain_frame(Slice(payload), codec);
    const std::vector<uint32_t> selected = {0};

    expect_failed_frame_preserves_profile(Slice(frame), counts.size(), selected,
                                          [](PrxPositionIterator* iterator) {
                                              RETURN_IF_ERROR(iterator->seek(0));
                                              return iterator->finish_frame();
                                          });
}

TEST_P(PrxPositionIteratorRawTest, RejectsCandidateOrdinalOutsideDocCountWithoutCommittingProfile) {
    const std::vector<uint32_t> counts = {1};
    const std::vector<uint32_t> deltas = {7};
    const std::vector<uint8_t> payload = make_plain_payload(counts, deltas);
    const PrxCodec codec = GetParam() == 0 ? PrxCodec::kRaw : PrxCodec::kZstd;
    const std::vector<uint8_t> frame = make_plain_frame(Slice(payload), codec);
    const std::vector<uint32_t> selected = {1};

    expect_failed_frame_preserves_profile(
            Slice(frame), counts.size(), selected,
            [](PrxPositionIterator* iterator) { return iterator->finish_frame(); });
}

TEST_P(PrxPositionIteratorRawTest, NullStatsDoesNotReadClock) {
    const PerDoc docs {{1, 3}, {5, 8, 13}};
    const std::vector<uint32_t> selected {1};
    ByteSink frame;
    ASSERT_TRUE(build_prx_window(docs, GetParam(), &frame).ok());
    PrxPositionIterator iterator;

    testing::reset_prx_clock_read_count();
    ASSERT_TRUE(iterator.reset(frame.view(), docs.size(), selected, nullptr).ok());
    ASSERT_TRUE(iterator.seek(1).ok());
    expect_document_positions(&iterator, docs[1]);
    ASSERT_TRUE(iterator.finish_doc().ok());
    ASSERT_TRUE(iterator.finish_frame().ok());
    EXPECT_EQ(testing::prx_clock_read_count(), 0U);
}

INSTANTIATE_TEST_SUITE_P(RawAndZstd, PrxPositionIteratorRawTest, ::testing::Values(0, 3));

TEST(PrxPositionIteratorTest, RejectsFrameDocCountMismatchWithoutCommittingProfile) {
    ByteSink frame;
    ASSERT_TRUE(build_prx_window(PerDoc {{1}, {2}}, 0, &frame).ok());
    for (uint32_t expected_doc_count : {1U, 3U}) {
        PrxDecodeStats decode_stats;
        decode_stats.raw_frames = 7;
        const PrxDecodeStats decode_before = decode_stats;
        PrxDecodeContext context {.stats = &decode_stats};
        PrxPositionIterator iterator;

        const Status status = iterator.reset(frame.view(), expected_doc_count, {}, &context);

        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
        EXPECT_EQ(decode_stats, decode_before);
    }
}

TEST(PrxPositionIteratorPforTest, CarriesDecodedRunAcrossDocumentBoundary) {
    const std::vector<uint32_t> counts = {255, 2, 1};
    const std::vector<uint32_t> deltas = make_document_deltas(counts);
    const ByteSink frame = make_pfor_frame(counts, deltas);
    const std::vector<uint32_t> expected_positions = {1, 2, 3};

    PrxPositionIterator iterator;
    ASSERT_TRUE(iterator.reset(frame.view(), expected_positions.size(), {}, nullptr).ok());
    for (uint32_t ordinal = 0; ordinal < expected_positions.size(); ++ordinal) {
        ASSERT_TRUE(iterator.seek(ordinal).ok());
        uint32_t position = 0;
        bool available = false;
        ASSERT_TRUE(iterator.next_position(&position, &available).ok());
        ASSERT_TRUE(available);
        EXPECT_EQ(position, expected_positions[ordinal]);
        ASSERT_TRUE(iterator.finish_doc().ok());
    }
    ASSERT_TRUE(iterator.finish_frame().ok());
}

TEST(PrxPositionIteratorPforTest, DecodesRunWhenSelectedRangeStartsInItsMiddle) {
    const std::vector<uint32_t> counts = {128, 2, 126};
    const std::vector<uint32_t> selected = {1};
    const ByteSink frame = make_pfor_frame(counts, make_document_deltas(counts));

    PrxPositionIterator iterator;
    ASSERT_TRUE(iterator.reset(frame.view(), counts.size(), selected, nullptr).ok());
    ASSERT_TRUE(iterator.seek(1).ok());
    EXPECT_EQ(iterator.freq(), 2U);
    expect_next_position(&iterator, 2U);
    expect_next_position(&iterator, 3U);
    ASSERT_TRUE(iterator.finish_doc().ok());
    ASSERT_TRUE(iterator.finish_frame().ok());
}

TEST(PrxPositionIteratorPforTest, StructurallySkipsCompleteUnselectedRun) {
    const std::vector<uint32_t> counts = {256, 1};
    const std::vector<uint32_t> selected = {1};
    const ByteSink frame = make_pfor_frame(counts, make_document_deltas(counts));

    PrxPositionIterator iterator;
    ASSERT_TRUE(iterator.reset(frame.view(), counts.size(), selected, nullptr).ok());
    ASSERT_TRUE(iterator.seek(1).ok());
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(iterator.next_position(&position, &available).ok());
    ASSERT_TRUE(available);
    EXPECT_EQ(position, 2U);
    ASSERT_TRUE(iterator.finish_doc().ok());
    ASSERT_TRUE(iterator.finish_frame().ok());
}

TEST(PrxPositionIteratorPforTest, StructurallySkipsLastPartialRunAtFrameEnd) {
    const std::vector<uint32_t> counts = {257};
    const ByteSink frame = make_pfor_frame(counts, make_document_deltas(counts));

    PrxPositionIterator iterator;
    ASSERT_TRUE(iterator.reset(frame.view(), counts.size(), {}, nullptr).ok());
    ASSERT_TRUE(iterator.finish_frame().ok());
}

TEST(PrxPositionIteratorPforTest, ReusesCurrentRunForNextSelectedDocument) {
    const std::vector<uint32_t> counts = {1, 254, 1};
    const std::vector<uint32_t> selected = {0, 2};
    const ByteSink frame = make_pfor_frame(counts, make_document_deltas(counts));

    PrxPositionIterator iterator;
    ASSERT_TRUE(iterator.reset(frame.view(), counts.size(), selected, nullptr).ok());
    ASSERT_TRUE(iterator.seek(0).ok());
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(iterator.next_position(&position, &available).ok());
    ASSERT_TRUE(available);
    EXPECT_EQ(position, 1U);
    ASSERT_TRUE(iterator.finish_doc().ok());

    ASSERT_TRUE(iterator.seek(2).ok());
    ASSERT_TRUE(iterator.next_position(&position, &available).ok());
    ASSERT_TRUE(available);
    EXPECT_EQ(position, 3U);
    ASSERT_TRUE(iterator.finish_doc().ok());
    ASSERT_TRUE(iterator.finish_frame().ok());
}

// Only PFOR has an independently declared total position count. RAW and ZSTD
// interleave each document's count with its values, so their equivalent damaged
// payload is the truncated-position case covered by the parameterized tests.
TEST(PrxPositionIteratorPforTest, RejectsCountSumDifferentFromDeclaredTotal) {
    const std::vector<uint32_t> counts = {1, 2};
    ByteSink position_runs;
    append_pfor_runs(std::vector<uint32_t> {1, 2, 1}, &position_runs);
    const ByteSink frame = make_pfor_frame_from_position_runs(counts, 4, position_runs.view());
    expect_failed_frame_preserves_profile(
            frame.view(), counts.size(), {},
            [](PrxPositionIterator* iterator) { return iterator->finish_frame(); });
}

TEST(PrxPositionIteratorPforTest, RejectsTruncatedPackedPositionPayload) {
    const std::vector<uint32_t> counts = {1};
    const std::vector<uint32_t> selected = {0};
    ByteSink position_runs;
    append_pfor_runs(std::vector<uint32_t> {1}, &position_runs);
    std::vector<uint8_t> truncated = position_runs.buffer();
    ASSERT_FALSE(truncated.empty());
    truncated.pop_back();
    const ByteSink frame = make_pfor_frame_from_position_runs(counts, 1, Slice(truncated));
    expect_failed_frame_preserves_profile(frame.view(), counts.size(), selected,
                                          [](PrxPositionIterator* iterator) {
                                              RETURN_IF_ERROR(iterator->seek(0));
                                              uint32_t position = 0;
                                              bool available = false;
                                              return iterator->next_position(&position, &available);
                                          });
}

TEST(PrxPositionIteratorPforTest, RejectsInvalidPositionExceptionIndex) {
    const std::vector<uint32_t> counts = {4};
    const std::vector<uint32_t> selected = {0};
    ByteSink invalid_position_run;
    invalid_position_run.put_u8(0);
    invalid_position_run.put_varint32(1);
    invalid_position_run.put_varint32(10);
    invalid_position_run.put_varint32(7);
    const ByteSink frame =
            make_pfor_frame_from_position_runs(counts, 4, invalid_position_run.view());
    expect_failed_frame_preserves_profile(frame.view(), counts.size(), selected,
                                          [](PrxPositionIterator* iterator) {
                                              RETURN_IF_ERROR(iterator->seek(0));
                                              uint32_t position = 0;
                                              bool available = false;
                                              return iterator->next_position(&position, &available);
                                          });
}

TEST(PrxPositionIteratorPforTest, RejectsTrailingBytesAfterPositionRuns) {
    const std::vector<uint32_t> counts = {1};
    const std::vector<uint8_t> trailing = {0x55};
    const ByteSink frame = make_pfor_frame(counts, make_document_deltas(counts), trailing);
    expect_failed_frame_preserves_profile(
            frame.view(), counts.size(), {},
            [](PrxPositionIterator* iterator) { return iterator->finish_frame(); });
}

TEST(PrxPositionIteratorPforTest, RejectsPositionPrefixOverflowWithoutCommittingProfile) {
    const std::vector<uint32_t> counts = {2};
    const std::vector<uint32_t> deltas = {std::numeric_limits<uint32_t>::max(), 1};
    const ByteSink frame = make_pfor_frame(counts, deltas);
    const std::vector<uint32_t> selected = {0};

    expect_failed_frame_preserves_profile(
            frame.view(), counts.size(), selected, [](PrxPositionIterator* iterator) {
                RETURN_IF_ERROR(iterator->seek(0));
                uint32_t position = 0;
                bool available = false;
                RETURN_IF_ERROR(iterator->next_position(&position, &available));
                return iterator->next_position(&position, &available);
            });
}

TEST(PrxPositionIteratorPforTest, RejectsCandidateOrdinalOutsideDocCountWithoutCommittingProfile) {
    const std::vector<uint32_t> counts = {1};
    const ByteSink frame = make_pfor_frame(counts, make_document_deltas(counts));
    const std::vector<uint32_t> selected = {1};

    expect_failed_frame_preserves_profile(
            frame.view(), counts.size(), selected,
            [](PrxPositionIterator* iterator) { return iterator->finish_frame(); });
}

TEST(PrxPositionIteratorPforTest, NullStatsDoesNotReadClock) {
    const std::vector<uint32_t> counts = {256, 1};
    const std::vector<uint32_t> selected = {1};
    const ByteSink frame = make_pfor_frame(counts, make_document_deltas(counts));
    PrxPositionIterator iterator;

    testing::reset_prx_clock_read_count();
    ASSERT_TRUE(iterator.reset(frame.view(), counts.size(), selected, nullptr).ok());
    ASSERT_TRUE(iterator.seek(1).ok());
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(iterator.next_position(&position, &available).ok());
    ASSERT_TRUE(iterator.finish_doc().ok());
    ASSERT_TRUE(iterator.finish_frame().ok());
    EXPECT_EQ(testing::prx_clock_read_count(), 0U);
}

TEST(PrxPositionIteratorPforTest, ProfiledStreamingDoesNotReadClock) {
    const std::vector<uint32_t> counts = {1, 2, 1};
    const std::vector<uint32_t> selected = {1};
    const ByteSink frame = make_pfor_frame(counts, make_document_deltas(counts));
    PrxDecodeStats context_stats;
    PrxDecodeContext context {.stats = &context_stats};
    PrxPositionIterator iterator;

    testing::reset_prx_clock_read_count();
    ASSERT_TRUE(iterator.reset(frame.view(), counts.size(), selected, &context).ok());
    ASSERT_TRUE(iterator.seek(1).ok());
    ASSERT_TRUE(iterator.finish_doc().ok());
    ASSERT_TRUE(iterator.finish_frame().ok());

    EXPECT_EQ(context_stats.pfor_frames, 1U);
    EXPECT_EQ(context_stats.total_docs, 3U);
    EXPECT_EQ(context_stats.total_positions, 4U);
    EXPECT_EQ(context_stats.selected_docs, 1U);
    EXPECT_EQ(context_stats.selected_positions, 2U);
    EXPECT_EQ(context_stats.decode_ns, 0U);
    EXPECT_EQ(testing::prx_clock_read_count(), 0U);
}

TEST(PrxPositionIteratorTest, RejectsNonMonotonicSeekWithoutMergingStats) {
    const PerDoc docs {{1}, {2}, {3}};
    ByteSink frame;
    ASSERT_TRUE(build_prx_window(docs, 0, &frame).ok());
    PrxDecodeStats decode_stats;
    PrxDecodeContext context {.stats = &decode_stats};
    PrxPositionIterator iterator;

    ASSERT_TRUE(iterator.reset(frame.view(), docs.size(), {}, &context).ok());
    ASSERT_TRUE(iterator.seek(1).ok());
    ASSERT_TRUE(iterator.finish_doc().ok());
    const Status status = iterator.seek(0);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    EXPECT_EQ(decode_stats, PrxDecodeStats {});
}

TEST(PrxPositionIteratorTest, TruncatedVarintMergesNoStats) {
    ByteSink payload;
    payload.put_varint32(1);
    payload.put_varint32(1);
    payload.put_u8(0x80);
    const std::vector<uint8_t> frame = make_raw_frame(payload.view());
    PrxDecodeStats decode_stats;
    PrxDecodeContext context {.stats = &decode_stats};
    PrxPositionIterator iterator;

    ASSERT_TRUE(iterator.reset(Slice(frame), /*expected_doc_count=*/1, {}, &context).ok());
    ASSERT_TRUE(iterator.seek(0).ok());
    uint32_t position = 0;
    bool available = false;
    EXPECT_FALSE(iterator.next_position(&position, &available).ok());
    EXPECT_EQ(decode_stats, PrxDecodeStats {});
}

TEST(PrxPositionIteratorTest, CrcFailureMergesNoStats) {
    ByteSink frame;
    ASSERT_TRUE(build_prx_window(PerDoc {{1, 2, 3}}, 0, &frame).ok());
    std::vector<uint8_t> corrupted = frame.buffer();
    corrupted.back() ^= 0x80;
    PrxDecodeStats decode_stats;
    PrxDecodeContext context {.stats = &decode_stats};
    PrxPositionIterator iterator;

    EXPECT_FALSE(iterator.reset(Slice(corrupted), /*expected_doc_count=*/1, {}, &context).ok());
    EXPECT_EQ(decode_stats, PrxDecodeStats {});
}

TEST(PrxPositionIteratorTest, TrailingPlaintextMergesNoStats) {
    ByteSink payload;
    payload.put_varint32(1);
    payload.put_varint32(1);
    payload.put_varint32(0);
    payload.put_u8(0x55);
    const std::vector<uint8_t> frame = make_raw_frame(payload.view());
    PrxDecodeStats decode_stats;
    PrxDecodeContext context {.stats = &decode_stats};
    PrxPositionIterator iterator;

    ASSERT_TRUE(iterator.reset(Slice(frame), /*expected_doc_count=*/1, {}, &context).ok());
    ASSERT_TRUE(iterator.seek(0).ok());
    uint32_t position = 0;
    bool available = false;
    ASSERT_TRUE(iterator.next_position(&position, &available).ok());
    ASSERT_TRUE(available);
    ASSERT_TRUE(iterator.finish_doc().ok());
    EXPECT_FALSE(iterator.finish_frame().ok());
    EXPECT_EQ(decode_stats, PrxDecodeStats {});
}

TEST(PrxPositionIteratorTest, ZstdDeclaredLengthMismatchMergesNoStats) {
    ByteSink frame;
    ASSERT_TRUE(build_prx_window(make_repeated_positions(2, 48), 3, &frame).ok());
    std::vector<uint8_t> corrupted = frame.buffer();
    ASSERT_EQ(corrupted[0], static_cast<uint8_t>(PrxCodec::kZstd));
    ASSERT_EQ(corrupted[1], 99U);
    corrupted[1] = 100;
    rewrite_trailing_crc(&corrupted);
    PrxDecodeStats decode_stats;
    PrxDecodeContext context {.stats = &decode_stats};
    PrxPositionIterator iterator;

    EXPECT_FALSE(iterator.reset(Slice(corrupted), /*expected_doc_count=*/2, {}, &context).ok());
    EXPECT_EQ(decode_stats, PrxDecodeStats {});
}

} // namespace
} // namespace doris::snii::format
