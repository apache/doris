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

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/snii/compaction/posting_cursor.h"
#include "storage/index/snii/format/core_metadata.h"
#include "storage/index/snii/format/dict_entry.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace {

using namespace doris::snii;            // NOLINT
using namespace doris::snii::snii_test; // NOLINT
namespace ErrorCode = doris::ErrorCode;
using doris::Status;
using compaction::PostingStream;
using compaction::RemappedPostingChunk;
using compaction::SharedAlignedRegionCache;
using compaction::SniiPostingCursor;
using compaction::SniiPostingReadContext;
using compaction::ValidatedRowIdConversion;
using writer::MemoryReporter;

static_assert(!std::is_constructible_v<
              SniiPostingCursor, SniiPostingReadContext*, format::DictEntry, uint64_t, uint64_t,
              uint32_t, std::span<const std::pair<uint32_t, uint32_t>>, std::span<const uint32_t>>);

constexpr uint64_t kIndexId = 9;
constexpr std::string_view kIndexSuffix = "body";
constexpr uint32_t kDocCount = 640;
constexpr uint32_t kFreqDroppedDocCount = 65536;
constexpr auto kDeleted = std::pair<uint32_t, uint32_t> {std::numeric_limits<uint32_t>::max(),
                                                         std::numeric_limits<uint32_t>::max()};
constexpr std::array<uint32_t, 0> kNoDestinationRows {};
constexpr std::array<uint32_t, 1> kDestinationRows1 {1};
constexpr std::array<uint32_t, 1> kDestinationRows8 {8};
constexpr std::array<uint32_t, 1> kDestinationRows10 {10};
constexpr std::array<uint32_t, 2> kTwoDestinationRows10 {10, 10};

struct SourceFixture {
    MemoryFile file;
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;
};

struct TermRef {
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
};

Status build_source(std::vector<writer::TermPostings> terms, uint32_t doc_count,
                    SourceFixture* fixture, bool write_freq = true) {
    writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = kIndexSuffix;
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = doc_count;
    input.write_freq = write_freq;
    input.terms = std::move(terms);

    writer::SniiCompoundWriter compound(&fixture->file);
    RETURN_IF_ERROR(compound.add_logical_index(input));
    RETURN_IF_ERROR(compound.finish());
    RETURN_IF_ERROR(reader::SniiSegmentReader::open(&fixture->file, &fixture->segment));
    return fixture->segment.open_index(kIndexId, kIndexSuffix, &fixture->index);
}

Status build_hybrid_source(std::vector<writer::TermPostings> terms, uint32_t doc_count,
                           SourceFixture* fixture) {
    namespace inverted_index = doris::segment_v2::inverted_index;
    inverted_index::CommonGramsQueryIdentity identity {.common_grams_dictionary_identity = "dict-a",
                                                       .base_analyzer_fingerprint = "base-a",
                                                       .common_grams_fingerprint = "grams-a"};
    auto metadata = inverted_index::make_common_grams_segment_metadata(identity);
    metadata.common_grams_coverage = inverted_index::CommonGramsCoverage::kMixed;
    metadata.scoring_doc_count = doc_count;
    metadata.scoring_token_count = 1;

    terms.push_back(make_term("plain", {{.docid = 0, .positions = {0}}}));
    std::ranges::sort(terms, [](const auto& lhs, const auto& rhs) { return lhs.term < rhs.term; });
    writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = kIndexSuffix;
    input.config = format::IndexConfig::kDocsPositionsScoring;
    input.doc_count = doc_count;
    input.write_freq = true;
    input.encoded_norms.assign(doc_count, 1);
    input.common_grams_metadata = std::move(metadata);
    input.common_grams_posting_policy = format::CommonGramsPostingPolicy::kHybridV1;
    input.terms = std::move(terms);

    writer::SniiCompoundWriter compound(&fixture->file);
    RETURN_IF_ERROR(compound.add_logical_index(input));
    RETURN_IF_ERROR(compound.finish());
    RETURN_IF_ERROR(reader::SniiSegmentReader::open(&fixture->file, &fixture->segment));
    return fixture->segment.open_index(kIndexId, kIndexSuffix, &fixture->index);
}

writer::TermPostings make_docs_only_gram(std::string left, std::string right,
                                         std::vector<uint32_t> docids) {
    writer::TermPostings term;
    term.term = doris::segment_v2::inverted_index::encode_common_gram(left, right).value();
    term.docids = std::move(docids);
    term.retain_positions = false;
    return term;
}

std::vector<writer::TermPostings> posting_shapes() {
    std::vector<writer::TermPostings> terms;
    terms.push_back(make_term("inline",
                              {{.docid = 1, .positions = {2, 7}}, {.docid = 5, .positions = {3}}}));

    std::vector<PostingDoc> slim;
    slim.reserve(500);
    for (uint32_t docid = 0; docid < 500; ++docid) {
        uint32_t mixed = docid * 2654435761U;
        mixed ^= mixed >> 16;
        const uint32_t frequency = mixed % 97 + 1;
        std::vector<uint32_t> positions(frequency);
        for (uint32_t i = 0; i < frequency; ++i) {
            positions[i] = i * 3;
        }
        slim.push_back({.docid = docid, .positions = std::move(positions)});
    }
    terms.push_back(make_term("slim", std::move(slim)));

    std::vector<PostingDoc> windowed;
    windowed.reserve(600);
    for (uint32_t docid = 0; docid < 600; ++docid) {
        windowed.push_back({.docid = docid, .positions = {1, 4, 9}});
    }
    terms.push_back(make_term("windowed", std::move(windowed)));
    return terms;
}

std::vector<writer::TermPostings> freq_dropped_posting_shapes() {
    std::vector<writer::TermPostings> terms;
    terms.push_back(make_term("inline",
                              {{.docid = 1, .positions = {2, 7}}, {.docid = 5, .positions = {3}}}));

    std::vector<PostingDoc> slim;
    slim.reserve(500);
    uint32_t docid = 1;
    slim.push_back({.docid = docid, .positions = {2, 7}});
    for (uint32_t i = 1; i < 500; ++i) {
        docid += 1 + (i * 73 + i * i * 19) % 230;
        slim.push_back({.docid = docid, .positions = {2, 7}});
    }
    terms.push_back(make_term("slim", std::move(slim)));

    std::vector<PostingDoc> windowed;
    windowed.reserve(600);
    for (uint32_t docid = 0; docid < 600; ++docid) {
        windowed.push_back({.docid = docid, .positions = {1, 4, 9}});
    }
    terms.push_back(make_term("windowed", std::move(windowed)));
    return terms;
}

std::vector<writer::TermPostings> repeated_windowed_terms() {
    std::vector<writer::TermPostings> terms;
    for (std::string term : {"first", "second"}) {
        std::vector<PostingDoc> docs;
        docs.reserve(600);
        for (uint32_t docid = 0; docid < 600; ++docid) {
            docs.push_back({.docid = docid, .positions = {1, 4, 9}});
        }
        terms.push_back(make_term(std::move(term), std::move(docs)));
    }
    return terms;
}

TermRef lookup_term(const reader::LogicalIndexReader& index, std::string_view term) {
    bool found = false;
    TermRef ref;
    assert_ok(index.lookup(term, &found, &ref.entry, &ref.frq_base, &ref.prx_base));
    EXPECT_TRUE(found) << term;
    return ref;
}

std::vector<std::pair<uint32_t, uint32_t>> deleted_map(uint32_t doc_count = kDocCount) {
    return std::vector<std::pair<uint32_t, uint32_t>>(doc_count, kDeleted);
}

struct PostingCopy {
    uint32_t segment = 0;
    uint32_t docid = 0;
    uint32_t freq = 0;
    std::vector<uint32_t> positions;

    bool operator==(const PostingCopy&) const = default;
};

Status drain(SniiPostingCursor* cursor, std::vector<PostingCopy>* output) {
    RemappedPostingChunk chunk;
    bool has_chunk = false;
    RETURN_IF_ERROR(cursor->next_chunk(&chunk, &has_chunk));
    while (has_chunk) {
        for (size_t ordinal = 0; ordinal < chunk.destination_docids.size(); ++ordinal) {
            const uint32_t segment = chunk.destination_segment;
            const uint32_t docid = chunk.destination_docids[ordinal];
            uint32_t frequency = 0;
            std::vector<uint32_t> positions;
            if (!chunk.freqs.empty()) {
                frequency = chunk.freqs[ordinal];
                const uint32_t position_base = chunk.position_offsets.front();
                const uint32_t begin = chunk.position_offsets[ordinal] - position_base;
                const uint32_t end = chunk.position_offsets[ordinal + 1] - position_base;
                positions.assign(chunk.positions_flat.begin() + begin,
                                 chunk.positions_flat.begin() + end);
            }
            output->push_back({segment, docid, frequency, std::move(positions)});
        }
        RETURN_IF_ERROR(cursor->next_chunk(&chunk, &has_chunk));
    }
    return Status::OK();
}

std::unique_ptr<SniiPostingReadContext> make_read_context(
        const reader::LogicalIndexReader* index, size_t total_read_ahead_budget_bytes = 128) {
    auto context = std::make_unique<SniiPostingReadContext>(index, total_read_ahead_budget_bytes);
    assert_ok(context->init());
    return context;
}

struct CursorMapping {
    compaction::RowIdConversionMap conversion;
    std::unique_ptr<ValidatedRowIdConversion> validated;
};

const ValidatedRowIdConversion* make_validated_cursor_mapping(
        const std::vector<std::pair<uint32_t, uint32_t>>& trans) {
    static std::vector<std::unique_ptr<CursorMapping>> arena;
    auto fixture = std::make_unique<CursorMapping>();
    fixture->conversion.resize(4);
    fixture->conversion[3] = trans;

    uint32_t max_segment = 0;
    bool has_live_row = false;
    for (const auto& [segment, docid] : trans) {
        if (segment != std::numeric_limits<uint32_t>::max()) {
            max_segment = std::max(max_segment, segment);
            has_live_row = true;
        }
    }
    std::vector<uint32_t> destination_rows(has_live_row ? max_segment + 1 : 0, 0);
    for (auto& [segment, docid] : fixture->conversion[3]) {
        if (segment != std::numeric_limits<uint32_t>::max()) {
            docid = destination_rows[segment]++;
        }
    }
    const std::array<uint32_t, 4> source_rows {0, 0, 0, static_cast<uint32_t>(trans.size())};
    assert_ok(ValidatedRowIdConversion::create(&fixture->conversion, source_rows, destination_rows,
                                               &fixture->validated));
    const ValidatedRowIdConversion* validated = fixture->validated.get();
    arena.push_back(std::move(fixture));
    return validated;
}

SniiPostingCursor make_cursor(SniiPostingReadContext* read_context, TermRef ref,
                              const std::vector<std::pair<uint32_t, uint32_t>>& trans,
                              std::span<const uint32_t>) {
    return SniiPostingCursor(read_context, std::move(ref.entry), ref.frq_base, ref.prx_base,
                             /*source_ordinal=*/3, make_validated_cursor_mapping(trans));
}

TEST(SniiPostingCursorTest, ScratchGrowthChargesOldAndNewAllocationsBeforeReserve) {
    MemoryReporter reporter(nullptr, /*cap_bytes=*/35);
    {
        MemoryFile file;
        std::vector<uint8_t> bytes(32, 7);
        assert_ok(file.append(Slice(bytes)));

        SharedAlignedRegionCache cache(&file, /*region_offset=*/0, bytes.size(),
                                       /*total_budget_bytes=*/16, &reporter);
        assert_ok(cache.init());
        ASSERT_EQ(reporter.current_bytes(), 16);

        std::vector<uint8_t> scratch;
        scratch.reserve(8);
        auto scratch_reservation = reporter.make_reservation();
        assert_ok(scratch_reservation.set_bytes(scratch.capacity()));
        ASSERT_EQ(reporter.current_bytes(), 24);

        Slice resolved;
        const Status status = cache.resolve(PostingStream::kDocs, /*abs_off=*/4, /*len=*/12,
                                            &scratch, &resolved, &scratch_reservation);
        EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status;
        EXPECT_EQ(scratch.capacity(), 8);
        EXPECT_EQ(scratch_reservation.bytes(), 8);
        EXPECT_EQ(reporter.current_bytes(), 24);
        EXPECT_TRUE(resolved.empty());
        EXPECT_TRUE(file.reads().empty());
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiPostingCursorTest, ChargesEveryRetainedDecoderWorkspaceBuffer) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef ref = lookup_term(source.index, "slim");
    auto trans = deleted_map();
    trans[0] = {0, 0};

    MemoryReporter reporter(nullptr, /*cap_bytes=*/4U << 20);
    {
        SniiPostingReadContext read_context(&source.index,
                                            /*total_read_ahead_budget_bytes=*/1U << 20, &reporter);
        assert_ok(read_context.init());
        SniiPostingCursor cursor = make_cursor(&read_context, ref, trans, kDestinationRows1);
        assert_ok(cursor.init());

        RemappedPostingChunk chunk;
        bool has_chunk = false;
        assert_ok(cursor.next_chunk(&chunk, &has_chunk));
        ASSERT_TRUE(has_chunk);
        EXPECT_EQ(static_cast<size_t>(reporter.current_bytes()),
                  read_context.resident_read_ahead_capacity_bytes() +
                          read_context.decoder_workspace_capacity_bytes());
    }
    EXPECT_EQ(reporter.current_bytes(), 0);
}

TEST(SniiPostingCursorTest, PositionWorkspaceLimitPoisonsReadContext) {
    std::vector<uint32_t> positions(100000);
    std::iota(positions.begin(), positions.end(), 0);
    SourceFixture source;
    assert_ok(build_source({make_term("large", {{.docid = 0, .positions = std::move(positions)}})},
                           /*doc_count=*/1, &source));
    const TermRef ref = lookup_term(source.index, "large");
    std::vector<std::pair<uint32_t, uint32_t>> trans = {{0, 0}};

    MemoryReporter reporter(nullptr, /*cap_bytes=*/192U << 10);
    SniiPostingReadContext read_context(&source.index, /*total_read_ahead_budget_bytes=*/128,
                                        &reporter);
    assert_ok(read_context.init());
    SniiPostingCursor cursor = make_cursor(&read_context, ref, trans, kDestinationRows1);
    assert_ok(cursor.init());

    RemappedPostingChunk chunk;
    bool has_chunk = false;
    const Status first = cursor.next_chunk(&chunk, &has_chunk);
    EXPECT_TRUE(first.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << first;
    EXPECT_FALSE(has_chunk);
    EXPECT_LE(reporter.current_bytes(), static_cast<int64_t>(reporter.cap_bytes()));

    SniiPostingCursor retry = make_cursor(&read_context, ref, trans, kDestinationRows1);
    EXPECT_EQ(retry.init(), first);
}

TEST(SniiPostingCursorTest, DecodesInlineAndSkipsDeletedDocs) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef ref = lookup_term(source.index, "inline");
    ASSERT_EQ(ref.entry.kind, format::DictEntryKind::kInline);

    auto trans = deleted_map();
    trans[1] = {0, 2};
    auto read_context = make_read_context(&source.index);
    SniiPostingCursor cursor = make_cursor(read_context.get(), ref, trans, kDestinationRows10);
    assert_ok(cursor.init());

    std::vector<PostingCopy> got;
    assert_ok(drain(&cursor, &got));
    EXPECT_EQ(got, (std::vector<PostingCopy> {{0, 0, 2, {2, 7}}}));
}

TEST(SniiPostingCursorTest, DecodesZstdDdWithoutMemoryReporter) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    TermRef ref = lookup_term(source.index, "inline");
    ASSERT_EQ(ref.entry.kind, format::DictEntryKind::kInline);

    const auto freq_begin = ref.entry.frq_bytes.begin() + ref.entry.inline_dd_disk_len;
    std::vector<uint8_t> freq_bytes(freq_begin, ref.entry.frq_bytes.end());
    ByteSink dd_sink;
    format::FrqRegionMeta dd_meta;
    const std::array<uint32_t, 2> docids {1, 5};
    assert_ok(
            format::build_dd_region(docids, /*win_base=*/0, /*zstd_level=*/3, &dd_sink, &dd_meta));
    ASSERT_TRUE(dd_meta.zstd);

    ref.entry.frq_bytes = dd_sink.take();
    ref.entry.frq_bytes.insert(ref.entry.frq_bytes.end(), freq_bytes.begin(), freq_bytes.end());
    ref.entry.dd_meta = dd_meta;
    ref.entry.inline_dd_disk_len = dd_meta.disk_len;
    ref.entry.frq_len = ref.entry.frq_bytes.size();

    auto trans = deleted_map();
    trans[1] = {0, 2};
    auto read_context = make_read_context(&source.index);
    SniiPostingCursor cursor =
            make_cursor(read_context.get(), std::move(ref), trans, kDestinationRows10);
    assert_ok(cursor.init());

    std::vector<PostingCopy> got;
    assert_ok(drain(&cursor, &got));
    EXPECT_EQ(got, (std::vector<PostingCopy> {{0, 0, 2, {2, 7}}}));
}

TEST(SniiPostingCursorTest, DecodesSlimPodRefAcrossDestinations) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef ref = lookup_term(source.index, "slim");
    ASSERT_EQ(ref.entry.kind, format::DictEntryKind::kPodRef);
    ASSERT_EQ(ref.entry.enc, format::DictEntryEnc::kSlim);

    auto trans = deleted_map();
    trans[0] = {0, 3};
    trans[150] = {1, 0};
    trans[499] = {1, 4};
    auto read_context = make_read_context(&source.index);
    SniiPostingCursor cursor = make_cursor(read_context.get(), ref, trans, kTwoDestinationRows10);
    assert_ok(cursor.init());

    std::vector<PostingCopy> got;
    assert_ok(drain(&cursor, &got));
    auto expected_positions = [](uint32_t docid) {
        uint32_t mixed = docid * 2654435761U;
        mixed ^= mixed >> 16;
        std::vector<uint32_t> positions(mixed % 97 + 1);
        for (uint32_t i = 0; i < positions.size(); ++i) {
            positions[i] = i * 3;
        }
        return positions;
    };
    const std::vector<uint32_t> positions_0 = expected_positions(0);
    const std::vector<uint32_t> positions_150 = expected_positions(150);
    const std::vector<uint32_t> positions_499 = expected_positions(499);
    EXPECT_EQ(got, (std::vector<PostingCopy> {
                           {0, 0, static_cast<uint32_t>(positions_0.size()), positions_0},
                           {1, 0, static_cast<uint32_t>(positions_150.size()), positions_150},
                           {1, 1, static_cast<uint32_t>(positions_499.size()), positions_499}}));
}

TEST(SniiPostingCursorTest, DecodesWindowedPodRefSequentially) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef ref = lookup_term(source.index, "windowed");
    ASSERT_EQ(ref.entry.kind, format::DictEntryKind::kPodRef);
    ASSERT_EQ(ref.entry.enc, format::DictEntryEnc::kWindowed);

    auto trans = deleted_map();
    trans[0] = {0, 1};
    trans[511] = {0, 8};
    trans[512] = {1, 1};
    trans[599] = {1, 9};
    auto read_context = make_read_context(&source.index);
    SniiPostingCursor cursor = make_cursor(read_context.get(), ref, trans, kTwoDestinationRows10);
    assert_ok(cursor.init());

    std::vector<PostingCopy> got;
    assert_ok(drain(&cursor, &got));
    EXPECT_EQ(got, (std::vector<PostingCopy> {{0, 0, 3, {1, 4, 9}},
                                              {0, 1, 3, {1, 4, 9}},
                                              {1, 0, 3, {1, 4, 9}},
                                              {1, 1, 3, {1, 4, 9}}}));
}

TEST(SniiPostingCursorTest, EmitsMappedChunksAtDecoderWindowBoundaries) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef ref = lookup_term(source.index, "windowed");
    auto trans = deleted_map();
    trans[0] = {0, 0};
    trans[511] = {0, 1};
    trans[512] = {1, 0};
    trans[599] = {1, 1};
    auto read_context = make_read_context(&source.index);
    SniiPostingCursor cursor = make_cursor(read_context.get(), ref, trans, kTwoDestinationRows10);
    assert_ok(cursor.init());

    std::vector<size_t> chunk_sizes;
    std::vector<std::pair<uint32_t, uint32_t>> keys;
    RemappedPostingChunk chunk;
    bool has_chunk = false;
    assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    while (has_chunk) {
        chunk_sizes.push_back(chunk.destination_docids.size());
        for (uint32_t docid : chunk.destination_docids) {
            keys.emplace_back(chunk.destination_segment, docid);
        }
        EXPECT_EQ(chunk.freqs.size(), chunk.destination_docids.size());
        EXPECT_EQ(chunk.position_offsets.size(), chunk.destination_docids.size() + 1);
        EXPECT_EQ(chunk.position_offsets.back() - chunk.position_offsets.front(),
                  chunk.positions_flat.size());
        assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    }

    EXPECT_EQ(chunk_sizes, std::vector<size_t>({1, 1, 2}));
    EXPECT_EQ(keys, (std::vector<std::pair<uint32_t, uint32_t>> {{0, 0}, {0, 1}, {1, 0}, {1, 1}}));
}

TEST(SniiPostingCursorTest, SplitsOneDecodedChunkIntoDestinationHomogeneousRuns) {
    SourceFixture source;
    assert_ok(build_source({make_term("split", {{.docid = 0, .positions = {2, 7}},
                                                {.docid = 1, .positions = {3}}})},
                           /*doc_count=*/2, &source));
    const TermRef ref = lookup_term(source.index, "split");
    ASSERT_EQ(ref.entry.kind, format::DictEntryKind::kInline);
    std::vector<std::pair<uint32_t, uint32_t>> trans {{0, 0}, {1, 0}};
    std::array<uint32_t, 2> destination_rows {1, 1};
    auto read_context = make_read_context(&source.index);
    SniiPostingCursor cursor = make_cursor(read_context.get(), ref, trans, destination_rows);
    assert_ok(cursor.init());

    RemappedPostingChunk chunk;
    bool has_chunk = false;
    assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    ASSERT_TRUE(has_chunk);
    EXPECT_EQ(chunk.destination_segment, 0);
    EXPECT_EQ(
            std::vector<uint32_t>(chunk.destination_docids.begin(), chunk.destination_docids.end()),
            (std::vector<uint32_t> {0}));
    EXPECT_EQ(std::vector<uint32_t>(chunk.position_offsets.begin(), chunk.position_offsets.end()),
              (std::vector<uint32_t> {0, 2}));
    EXPECT_EQ(std::vector<uint32_t>(chunk.positions_flat.begin(), chunk.positions_flat.end()),
              (std::vector<uint32_t> {2, 7}));

    assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    ASSERT_TRUE(has_chunk);
    EXPECT_EQ(chunk.destination_segment, 1);
    EXPECT_EQ(
            std::vector<uint32_t>(chunk.destination_docids.begin(), chunk.destination_docids.end()),
            (std::vector<uint32_t> {0}));
    EXPECT_EQ(std::vector<uint32_t>(chunk.position_offsets.begin(), chunk.position_offsets.end()),
              (std::vector<uint32_t> {2, 3}));
    EXPECT_EQ(std::vector<uint32_t>(chunk.positions_flat.begin(), chunk.positions_flat.end()),
              (std::vector<uint32_t> {3}));

    assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    EXPECT_FALSE(has_chunk);
}

TEST(SniiPostingCursorTest, ExposesBaseRelativePositionSliceForDirectRun) {
    SourceFixture source;
    assert_ok(build_source({make_term("split", {{.docid = 0, .positions = {2, 7}},
                                                {.docid = 1, .positions = {3}}})},
                           /*doc_count=*/2, &source));
    const TermRef ref = lookup_term(source.index, "split");
    std::vector<std::pair<uint32_t, uint32_t>> trans {{0, 0}, {1, 0}};
    auto read_context = make_read_context(&source.index);
    SniiPostingCursor cursor =
            make_cursor(read_context.get(), ref, trans, std::array<uint32_t, 2> {1, 1});
    assert_ok(cursor.init());

    RemappedPostingChunk chunk;
    bool has_chunk = false;
    assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    ASSERT_TRUE(has_chunk);
    assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    ASSERT_TRUE(has_chunk);
    ASSERT_EQ(chunk.position_offsets.size(), 2U);
    EXPECT_GT(chunk.position_offsets.front(), 0U);
    EXPECT_EQ(chunk.position_offsets.back() - chunk.position_offsets.front(),
              chunk.positions_flat.size());
    EXPECT_EQ(std::vector<uint32_t>(chunk.positions_flat.begin(), chunk.positions_flat.end()),
              (std::vector<uint32_t> {3}));
}

TEST(SniiPostingCursorTest, UsesChunkLevelNoDeletionFastPath) {
    SourceFixture source;
    assert_ok(build_source({make_term("all-live", {{.docid = 1, .positions = {2, 7}},
                                                   {.docid = 5, .positions = {3}}})},
                           /*doc_count=*/6, &source));
    const TermRef ref = lookup_term(source.index, "all-live");
    std::vector<std::pair<uint32_t, uint32_t>> trans(6);
    for (uint32_t docid = 0; docid < trans.size(); ++docid) {
        trans[docid] = {0, docid};
    }
    auto read_context = make_read_context(&source.index);
    SniiPostingCursor cursor = make_cursor(read_context.get(), ref, trans, kDestinationRows10);
    assert_ok(cursor.init());

    RemappedPostingChunk chunk;
    bool has_chunk = false;
    assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    ASSERT_TRUE(has_chunk);
    EXPECT_EQ(chunk.destination_segment, 0);
    EXPECT_EQ(
            std::vector<uint32_t>(chunk.destination_docids.begin(), chunk.destination_docids.end()),
            (std::vector<uint32_t> {1, 5}));
    EXPECT_EQ(chunk.freqs.size(), 2U);
    EXPECT_EQ(chunk.position_offsets.size(), 3U);
    EXPECT_EQ(chunk.positions_flat.size(), 3U);
    assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    EXPECT_FALSE(has_chunk);
}

TEST(SniiPostingCursorTest, DecodesFlatAndWindowedDocsOnlyWithoutInventingPayloads) {
    constexpr uint32_t kWideDocs = format::kSlimDfThreshold;
    std::vector<uint32_t> wide_docids(kWideDocs);
    std::iota(wide_docids.begin(), wide_docids.end(), 0);
    SourceFixture source;
    assert_ok(build_hybrid_source({make_docs_only_gram("a", "of", {0, 4}),
                                   make_docs_only_gram("z", "of", std::move(wide_docids))},
                                  kWideDocs, &source));

    const TermRef flat_ref = lookup_term(
            source.index, doris::segment_v2::inverted_index::encode_common_gram("a", "of").value());
    const TermRef windowed_ref = lookup_term(
            source.index, doris::segment_v2::inverted_index::encode_common_gram("z", "of").value());
    ASSERT_EQ(flat_ref.entry.enc, format::DictEntryEnc::kSlim);
    ASSERT_EQ(windowed_ref.entry.enc, format::DictEntryEnc::kWindowed);

    auto trans = deleted_map(kWideDocs);
    trans[0] = {0, 1};
    trans[4] = {0, 5};
    {
        auto read_context = make_read_context(&source.index);
        SniiPostingCursor cursor =
                make_cursor(read_context.get(), flat_ref, trans, kDestinationRows10);
        assert_ok(cursor.init());
        EXPECT_FALSE(cursor.has_positions());
        std::vector<PostingCopy> got;
        assert_ok(drain(&cursor, &got));
        EXPECT_EQ(got, (std::vector<PostingCopy> {{0, 0, 0, {}}, {0, 1, 0, {}}}));
    }

    trans = deleted_map(kWideDocs);
    trans[0] = {0, 0};
    trans[kWideDocs - 1] = {0, 9};
    {
        auto read_context = make_read_context(&source.index);
        SniiPostingCursor cursor =
                make_cursor(read_context.get(), windowed_ref, trans, kDestinationRows10);
        assert_ok(cursor.init());
        EXPECT_FALSE(cursor.has_positions());
        std::vector<PostingCopy> got;
        assert_ok(drain(&cursor, &got));
        EXPECT_EQ(got, (std::vector<PostingCopy> {{0, 0, 0, {}}, {0, 1, 0, {}}}));
    }
}

TEST(SniiPostingCursorTest, RejectsOverflowAndOutOfFilePostingRegions) {
    format::RegionRef region;
    region.offset = std::numeric_limits<uint64_t>::max() - 3;
    region.length = 8;
    Status status = compaction::validate_posting_region(region, UINT64_MAX);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;

    region.offset = 90;
    region.length = 11;
    status = compaction::validate_posting_region(region, 100);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;

    region.length = 10;
    EXPECT_TRUE(compaction::validate_posting_region(region, 100).ok());
}

TEST(SniiPostingCursorTest, RejectsSourceDocOutsideIndexDocCount) {
    SourceFixture posting_source;
    assert_ok(build_source({make_term("bad", {{.docid = 8, .positions = {1}}})},
                           /*doc_count=*/9, &posting_source));
    const TermRef ref = lookup_term(posting_source.index, "bad");
    ASSERT_EQ(ref.entry.kind, format::DictEntryKind::kInline);

    SourceFixture bounded_source;
    assert_ok(build_source({}, /*doc_count=*/8, &bounded_source));
    auto trans = deleted_map(8);
    auto read_context = make_read_context(&bounded_source.index);
    SniiPostingCursor cursor = make_cursor(read_context.get(), ref, trans, kDestinationRows8);
    assert_ok(cursor.init());

    RemappedPostingChunk chunk;
    bool has_chunk = false;
    const Status status = cursor.next_chunk(&chunk, &has_chunk);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_FALSE(has_chunk);
}

TEST(SniiPostingCursorTest, RejectsInconsistentTermStatisticsAndPositions) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));

    TermRef stats_ref = lookup_term(source.index, "inline");
    ++stats_ref.entry.ttf_delta;
    auto trans = deleted_map();
    trans[1] = {0, 1};
    trans[5] = {0, 2};
    auto stats_read_context = make_read_context(&source.index);
    SniiPostingCursor stats_cursor =
            make_cursor(stats_read_context.get(), stats_ref, trans, kDestinationRows10);
    assert_ok(stats_cursor.init());
    std::vector<PostingCopy> ignored;
    Status status = drain(&stats_cursor, &ignored);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    const TermRef valid_stats_ref = lookup_term(source.index, "inline");
    SniiPostingCursor stats_retry =
            make_cursor(stats_read_context.get(), valid_stats_ref, trans, kDestinationRows10);
    EXPECT_EQ(stats_retry.init(), status);

    TermRef positions_ref = lookup_term(source.index, "inline");
    ASSERT_FALSE(positions_ref.entry.prx_bytes.empty());
    positions_ref.entry.prx_bytes.pop_back();
    positions_ref.entry.prx_len = positions_ref.entry.prx_bytes.size();
    auto positions_read_context = make_read_context(&source.index);
    SniiPostingCursor positions_cursor =
            make_cursor(positions_read_context.get(), positions_ref, trans, kDestinationRows10);
    assert_ok(positions_cursor.init());
    status = positions_cursor.next_chunk(nullptr, nullptr);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    RemappedPostingChunk chunk;
    bool has_chunk = false;
    status = positions_cursor.next_chunk(&chunk, &has_chunk);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    const TermRef retry_ref = lookup_term(source.index, "inline");
    SniiPostingCursor retry =
            make_cursor(positions_read_context.get(), retry_ref, trans, kDestinationRows10);
    const Status retry_status = retry.init();
    EXPECT_EQ(retry_status, status);
}

TEST(SniiPostingCursorTest, RejectsWindowMaxFrequencyMismatchWithoutRescanningPositions) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef valid_ref = lookup_term(source.index, "windowed");
    uint64_t dd_block_offset = 0;
    uint64_t frq_payload_length = 0;
    assert_ok(source.index.resolve_frq_window(valid_ref.entry, valid_ref.frq_base, &dd_block_offset,
                                              &frq_payload_length));
    ASSERT_GE(dd_block_offset, valid_ref.entry.prelude_len);
    const uint64_t prelude_offset = dd_block_offset - valid_ref.entry.prelude_len;
    const Slice original_prelude(source.file.data().data() + prelude_offset,
                                 valid_ref.entry.prelude_len);
    format::FrqPreludeReader prelude;
    assert_ok(format::FrqPreludeReader::open(original_prelude, &prelude));

    format::FrqPreludeColumns columns;
    columns.has_freq = prelude.has_freq();
    columns.has_prx = prelude.has_prx();
    columns.group_size = 64;
    for (uint32_t window = 0; window < prelude.n_windows(); ++window) {
        format::WindowMeta meta;
        assert_ok(prelude.window(window, &meta));
        columns.windows.push_back(meta);
    }
    ASSERT_FALSE(columns.windows.empty());
    ++columns.windows.front().max_freq;
    ByteSink corrupt_prelude;
    assert_ok(format::build_frq_prelude(columns, &corrupt_prelude));
    ASSERT_EQ(corrupt_prelude.size(), valid_ref.entry.prelude_len);

    std::vector<uint8_t> corrupt_file_bytes = source.file.data();
    std::copy(corrupt_prelude.buffer().begin(), corrupt_prelude.buffer().end(),
              corrupt_file_bytes.begin() + prelude_offset);
    SourceFixture corrupt_source;
    assert_ok(corrupt_source.file.append(Slice(corrupt_file_bytes)));
    assert_ok(corrupt_source.file.finalize());
    assert_ok(reader::SniiSegmentReader::open(&corrupt_source.file, &corrupt_source.segment));
    assert_ok(corrupt_source.segment.open_index(kIndexId, kIndexSuffix, &corrupt_source.index));

    const TermRef corrupt_ref = lookup_term(corrupt_source.index, "windowed");
    auto trans = deleted_map();
    trans[0] = {0, 0};
    auto read_context = make_read_context(&corrupt_source.index);
    SniiPostingCursor cursor =
            make_cursor(read_context.get(), corrupt_ref, trans, kDestinationRows1);
    assert_ok(cursor.init());
    RemappedPostingChunk chunk;
    bool has_chunk = false;
    const Status status = cursor.next_chunk(&chunk, &has_chunk);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    EXPECT_FALSE(has_chunk);
}

TEST(SniiPostingCursorTest, ReusesReadAheadAcrossConsecutivePodRefTerms) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef slim_ref = lookup_term(source.index, "slim");
    const TermRef windowed_ref = lookup_term(source.index, "windowed");
    ASSERT_EQ(slim_ref.entry.kind, format::DictEntryKind::kPodRef);
    ASSERT_EQ(windowed_ref.entry.kind, format::DictEntryKind::kPodRef);

    auto trans = deleted_map();
    source.file.clear_reads();
    auto read_context =
            make_read_context(&source.index, /*total_read_ahead_budget_bytes=*/1U << 20);
    SniiPostingCursor slim_cursor =
            make_cursor(read_context.get(), slim_ref, trans, kNoDestinationRows);
    assert_ok(slim_cursor.init());
    std::vector<PostingCopy> ignored;
    assert_ok(drain(&slim_cursor, &ignored));
    const uint64_t physical_reads_after_slim = read_context->physical_read_ranges();
    const uint64_t docs_hits_after_slim = read_context->docs_buffer_hits();
    const uint64_t prx_hits_after_slim = read_context->prx_buffer_hits();
    ASSERT_GT(physical_reads_after_slim, 0);

    SniiPostingCursor windowed_cursor =
            make_cursor(read_context.get(), windowed_ref, trans, kNoDestinationRows);
    assert_ok(windowed_cursor.init());
    assert_ok(drain(&windowed_cursor, &ignored));

    EXPECT_EQ(read_context->physical_read_ranges(), physical_reads_after_slim);
    EXPECT_GT(read_context->docs_buffer_hits(), docs_hits_after_slim);
    EXPECT_GT(read_context->prx_buffer_hits(), prx_hits_after_slim);

    const format::RegionRef& posting_region = source.index.section_refs().posting_region;
    EXPECT_EQ(read_context->physical_read_bytes(), source.file.read_bytes());
    EXPECT_LE(read_context->physical_read_bytes(), posting_region.length);
    EXPECT_EQ(read_context->physical_read_ranges(), source.file.reads().size());
    EXPECT_LE(read_context->resident_read_ahead_capacity_bytes(),
              read_context->total_read_ahead_budget_bytes());
    for (const MemoryFile::Read& range : source.file.reads()) {
        EXPECT_GE(range.offset, posting_region.offset);
        EXPECT_LE(range.offset - posting_region.offset, posting_region.length);
        EXPECT_LE(range.len, posting_region.length - (range.offset - posting_region.offset));
    }
}

TEST(SniiPostingCursorTest, ReusesDecoderWorkspaceAcrossTerms) {
    SourceFixture source;
    assert_ok(build_source(repeated_windowed_terms(), kDocCount, &source));
    const TermRef first_ref = lookup_term(source.index, "first");
    const TermRef second_ref = lookup_term(source.index, "second");
    auto trans = deleted_map();
    auto read_context = make_read_context(&source.index, 1U << 20);
    std::vector<PostingCopy> ignored;

    SniiPostingCursor first = make_cursor(read_context.get(), first_ref, trans, kNoDestinationRows);
    assert_ok(first.init());
    assert_ok(drain(&first, &ignored));
    const size_t capacity_after_first = read_context->decoder_workspace_capacity_bytes();
    ASSERT_GT(capacity_after_first, 0);

    SniiPostingCursor second =
            make_cursor(read_context.get(), second_ref, trans, kNoDestinationRows);
    assert_ok(second.init());
    assert_ok(drain(&second, &ignored));
    EXPECT_EQ(read_context->decoder_workspace_capacity_bytes(), capacity_after_first);
}

TEST(SniiPostingCursorTest, ReleasesLargeDecoderWorkspaceAtTermBoundary) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef slim_ref = lookup_term(source.index, "slim");
    auto trans = deleted_map();
    trans[0] = {0, 0};
    auto read_context = make_read_context(&source.index, 1U << 20);
    SniiPostingCursor cursor = make_cursor(read_context.get(), slim_ref, trans, kDestinationRows1);
    assert_ok(cursor.init());

    RemappedPostingChunk chunk;
    bool has_chunk = false;
    assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    ASSERT_TRUE(has_chunk);
    ASSERT_GT(read_context->decoder_workspace_capacity_bytes(),
              read_context->retained_decoder_workspace_limit_bytes());

    std::vector<PostingCopy> ignored;
    assert_ok(drain(&cursor, &ignored));
    EXPECT_LE(read_context->decoder_workspace_capacity_bytes(),
              read_context->retained_decoder_workspace_limit_bytes());
}

TEST(SniiPostingCursorTest, UsesValidatedCapabilityMapping) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef ref = lookup_term(source.index, "inline");
    auto trans = deleted_map();
    trans[1] = {0, 1};
    std::array<uint32_t, 1> destination_rows {1};
    auto read_context = make_read_context(&source.index);
    SniiPostingCursor cursor = make_cursor(read_context.get(), ref, trans, destination_rows);
    assert_ok(cursor.init());

    RemappedPostingChunk chunk;
    bool has_chunk = false;
    assert_ok(cursor.next_chunk(&chunk, &has_chunk));
    ASSERT_TRUE(has_chunk);
    ASSERT_EQ(chunk.destination_docids.size(), 1U);
    EXPECT_EQ(chunk.destination_segment, 0);
    EXPECT_EQ(chunk.destination_docids.front(), 0);
}

TEST(SniiPostingCursorTest, EnforcesReadContextBudgetAndSingleActiveCursor) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef inline_ref = lookup_term(source.index, "inline");
    const TermRef slim_ref = lookup_term(source.index, "slim");
    auto trans = deleted_map();

    SniiPostingReadContext zero_budget(&source.index, 0);
    Status status = zero_budget.init();
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    SniiPostingReadContext oversized_budget(&source.index,
                                            SniiPostingReadContext::kMaxReadAheadBudgetBytes + 1);
    status = oversized_budget.init();
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;

    SniiPostingReadContext read_context(&source.index, 128);
    EXPECT_EQ(read_context.total_read_ahead_budget_bytes(), 128);
    SniiPostingCursor before_context =
            make_cursor(&read_context, inline_ref, trans, kNoDestinationRows);
    status = before_context.init();
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    assert_ok(read_context.init());

    {
        SniiPostingCursor active =
                make_cursor(&read_context, inline_ref, trans, kNoDestinationRows);
        assert_ok(active.init());
        SniiPostingCursor concurrent =
                make_cursor(&read_context, slim_ref, trans, kNoDestinationRows);
        status = concurrent.init();
        EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    }

    SniiPostingCursor after_release =
            make_cursor(&read_context, slim_ref, trans, kNoDestinationRows);
    assert_ok(after_release.init());
    std::vector<PostingCopy> ignored;
    assert_ok(drain(&after_release, &ignored));
}

TEST(SniiPostingCursorTest, RejectsBackwardTermRangesAndPoisonsReadContext) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef slim_ref = lookup_term(source.index, "slim");
    const TermRef windowed_ref = lookup_term(source.index, "windowed");
    auto trans = deleted_map();
    auto read_context = make_read_context(&source.index, 128);

    SniiPostingCursor later =
            make_cursor(read_context.get(), windowed_ref, trans, kNoDestinationRows);
    assert_ok(later.init());
    std::vector<PostingCopy> ignored;
    assert_ok(drain(&later, &ignored));

    SniiPostingCursor backward =
            make_cursor(read_context.get(), slim_ref, trans, kNoDestinationRows);
    Status status = backward.init();
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status;
    SniiPostingCursor retry = make_cursor(read_context.get(), slim_ref, trans, kNoDestinationRows);
    const Status retry_status = retry.init();
    EXPECT_EQ(retry_status, status);
}

TEST(SniiPostingCursorTest, CorruptPreludePoisonsContextAndReleasesLease) {
    SourceFixture source;
    assert_ok(build_source(posting_shapes(), kDocCount, &source));
    const TermRef valid_ref = lookup_term(source.index, "windowed");
    TermRef corrupt_ref = valid_ref;
    ASSERT_GT(corrupt_ref.entry.frq_off_delta, 0);
    --corrupt_ref.entry.frq_off_delta;
    auto trans = deleted_map();
    auto read_context = make_read_context(&source.index, 128);

    SniiPostingCursor corrupt =
            make_cursor(read_context.get(), corrupt_ref, trans, kNoDestinationRows);
    const Status first = corrupt.init();
    EXPECT_TRUE(first.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << first;
    const uint64_t physical_bytes_after_failure = read_context->physical_read_bytes();

    SniiPostingCursor retry = make_cursor(read_context.get(), valid_ref, trans, kNoDestinationRows);
    const Status second = retry.init();
    EXPECT_EQ(second, first);
    EXPECT_EQ(read_context->physical_read_bytes(), physical_bytes_after_failure);
}

TEST(SniiPostingCursorTest, DecodesAllShapesWithoutStoredFrequencies) {
    SourceFixture source;
    assert_ok(build_source(freq_dropped_posting_shapes(), kFreqDroppedDocCount, &source,
                           /*write_freq=*/false));
    const TermRef inline_ref = lookup_term(source.index, "inline");
    const TermRef slim_ref = lookup_term(source.index, "slim");
    const TermRef windowed_ref = lookup_term(source.index, "windowed");
    ASSERT_EQ(inline_ref.entry.kind, format::DictEntryKind::kInline);
    ASSERT_EQ(slim_ref.entry.kind, format::DictEntryKind::kPodRef);
    ASSERT_EQ(slim_ref.entry.enc, format::DictEntryEnc::kSlim);
    ASSERT_EQ(windowed_ref.entry.kind, format::DictEntryKind::kPodRef);
    ASSERT_EQ(windowed_ref.entry.enc, format::DictEntryEnc::kWindowed);
    EXPECT_FALSE(inline_ref.entry.term_stats_present);
    EXPECT_FALSE(slim_ref.entry.term_stats_present);
    EXPECT_FALSE(windowed_ref.entry.term_stats_present);

    auto trans = deleted_map(kFreqDroppedDocCount);
    trans[1] = {0, 0};
    auto read_context = make_read_context(&source.index, 1U << 20);

    std::vector<PostingCopy> got;
    SniiPostingCursor inline_cursor =
            make_cursor(read_context.get(), inline_ref, trans, kDestinationRows1);
    assert_ok(inline_cursor.init());
    assert_ok(drain(&inline_cursor, &got));
    EXPECT_EQ(got, (std::vector<PostingCopy> {{0, 0, 2, {2, 7}}}));

    got.clear();
    SniiPostingCursor slim_cursor =
            make_cursor(read_context.get(), slim_ref, trans, kDestinationRows1);
    assert_ok(slim_cursor.init());
    assert_ok(drain(&slim_cursor, &got));
    EXPECT_EQ(got, (std::vector<PostingCopy> {{0, 0, 2, {2, 7}}}));

    got.clear();
    SniiPostingCursor windowed_cursor =
            make_cursor(read_context.get(), windowed_ref, trans, kDestinationRows1);
    assert_ok(windowed_cursor.init());
    assert_ok(drain(&windowed_cursor, &got));
    EXPECT_EQ(got, (std::vector<PostingCopy> {{0, 0, 3, {1, 4, 9}}}));
}

} // namespace
