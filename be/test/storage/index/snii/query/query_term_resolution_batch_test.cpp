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

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <random>
#include <roaring/roaring.hh>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/reader/dict_block_cache.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_index_source.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_query_oracle.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"
#include "testutil/benchmark_control.h"

namespace doris::snii::query {
namespace {

using snii_test::MemoryFile;
using snii_test::ScopedEnv;
using snii_test::assert_ok;
using snii_test::make_term;

constexpr uint64_t kIndexId = 1;
constexpr const char* kIndexSuffix = "body";

class CountingReader final : public io::FileReader {
public:
    explicit CountingReader(io::FileReader* inner) : inner_(inner) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        ++read_at_calls_;
        bytes_ += len;
        ++ranges_;
        return inner_->read_at(offset, len, out);
    }

    Status read_batch(const std::vector<io::Range>& ranges,
                      index_query::IoReadResult* outs) override {
        ++read_batch_calls_;
        batch_range_counts_.push_back(ranges.size());
        ranges_ += ranges.size();
        uint64_t batch_bytes = 0;
        for (const auto& range : ranges) {
            batch_bytes += range.len;
        }
        bytes_ += batch_bytes;
        batch_bytes_.push_back(batch_bytes);
        if (read_batch_calls_ == fail_batch_) {
            return Status::IOError("Injected dictionary batch failure");
        }
        return inner_->read_batch(ranges, outs);
    }

    uint64_t size() const override { return inner_->size(); }
    const io::IoMetrics* io_metrics() const override { return inner_->io_metrics(); }

    void reset_counts() {
        read_at_calls_ = 0;
        read_batch_calls_ = 0;
        batch_range_counts_.clear();
        batch_bytes_.clear();
        bytes_ = 0;
        ranges_ = 0;
    }

    void fail_batch(uint64_t call) { fail_batch_ = call; }

    uint64_t bytes() const { return bytes_; }
    uint64_t ranges() const { return ranges_; }
    uint64_t rounds() const { return read_at_calls_ + read_batch_calls_; }

    uint64_t read_at_calls() const { return read_at_calls_; }
    uint64_t read_batch_calls() const { return read_batch_calls_; }
    const std::vector<size_t>& batch_range_counts() const { return batch_range_counts_; }
    const std::vector<uint64_t>& batch_bytes() const { return batch_bytes_; }

private:
    io::FileReader* inner_;
    uint64_t bytes_ = 0;
    uint64_t ranges_ = 0;
    uint64_t fail_batch_ = 0;
    uint64_t read_at_calls_ = 0;
    uint64_t read_batch_calls_ = 0;
    std::vector<size_t> batch_range_counts_;
    std::vector<uint64_t> batch_bytes_;
};

Status write_index(MemoryFile* file, const std::vector<std::string>& terms,
                   uint32_t target_dict_block_bytes) {
    writer::SniiIndexInput input;
    input.index_id = kIndexId;
    input.index_suffix = kIndexSuffix;
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = static_cast<uint32_t>(terms.size());
    input.target_dict_block_bytes = target_dict_block_bytes;
    input.terms.reserve(terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        input.terms.push_back(
                make_term(terms[i], {{.docid = static_cast<uint32_t>(i), .positions = {0}}}));
    }

    writer::SniiCompoundWriter compound_writer(file);
    RETURN_IF_ERROR(compound_writer.add_logical_index(input));
    return compound_writer.finish();
}

std::vector<std::string> numbered_terms(size_t count) {
    std::vector<std::string> terms;
    terms.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        std::string term = "term_";
        if (i < 10) {
            term.push_back('0');
        }
        term += std::to_string(i);
        terms.push_back(std::move(term));
    }
    return terms;
}

// The found flags of a batch lookup, aligned with its terms.
std::vector<uint8_t> found_flags(
        const std::vector<reader::LogicalIndexReader::BatchLookupResult>& results) {
    std::vector<uint8_t> flags;
    for (const auto& result : results) {
        flags.push_back(result.found ? 1 : 0);
    }
    return flags;
}

TEST(SniiQueryTermResolutionBatch, ResolvesColdDictBlocksInOnePhysicalBatch) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");

    MemoryFile file;
    writer::SniiIndexInput input;
    input.index_id = 1;
    input.index_suffix = "body";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 6;
    input.target_dict_block_bytes = 1;
    input.terms = {
            make_term("alpha", {{.docid = 1, .positions = {0}}}),
            make_term("bravo", {{.docid = 2, .positions = {1}}}),
            make_term("kappa", {{.docid = 3, .positions = {2}}}),
            make_term("lambda", {{.docid = 4, .positions = {3}}}),
            make_term("omega", {{.docid = 5, .positions = {4}}}),
    };

    writer::SniiCompoundWriter compound_writer(&file);
    assert_ok(compound_writer.add_logical_index(input));
    assert_ok(compound_writer.finish());

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    CountingReader counting(&metered);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(input.index_id, input.index_suffix, &index_reader));
    ASSERT_EQ(index_reader.n_dict_blocks(), input.terms.size());
    metered.reset_metrics();
    counting.reset_counts();
    const std::vector<std::string> terms {"alpha", "kappa", "omega"};
    std::vector<reader::LogicalIndexReader::BatchLookupResult> resolved;
    assert_ok(index_reader.lookup_batch(terms, &resolved));
    const std::vector<uint8_t> found = found_flags(resolved);

    ASSERT_EQ(resolved.size(), terms.size());
    ASSERT_EQ(found, (std::vector<uint8_t> {1, 1, 1}));
    for (size_t i = 0; i < terms.size(); ++i) {
        EXPECT_EQ(resolved[i].entry.term, terms[i]);
        EXPECT_EQ(resolved[i].entry.df, 1U);
    }
    EXPECT_EQ(counting.read_at_calls(), 0U);
    EXPECT_EQ(counting.read_batch_calls(), 1U);
    EXPECT_EQ(counting.batch_range_counts(), (std::vector<size_t> {3}));
    EXPECT_EQ(metered.metrics().serial_rounds, 1U)
            << "independent cold DICT blocks must be fetched in one physical batch";
}

TEST(SniiQueryTermResolutionBatch, AlignsAbsentTermsAndReadsOneColdBlockSynchronously) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    ScopedEnv bsbf_resident_max("SNII_BSBF_RESIDENT_MAX", "0");

    MemoryFile file;
    assert_ok(write_index(&file, {"alpha", "kappa", "omega"},
                          /*target_dict_block_bytes=*/4096));

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    CountingReader counting(&metered);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    ASSERT_EQ(index_reader.n_dict_blocks(), 1U);
    metered.reset_metrics();
    counting.reset_counts();
    const std::vector<std::string> terms {"aardvark", "alpha", "beta", "kappa",
                                          "lambda",   "omega", "zulu"};
    std::vector<reader::LogicalIndexReader::BatchLookupResult> resolved;
    assert_ok(index_reader.lookup_batch(terms, &resolved));
    const std::vector<uint8_t> found = found_flags(resolved);

    ASSERT_EQ(resolved.size(), terms.size());
    ASSERT_EQ(found, (std::vector<uint8_t> {0, 1, 0, 1, 0, 1, 0}));
    for (size_t i : {1U, 3U, 5U}) {
        EXPECT_EQ(resolved[i].entry.term, terms[i]);
        EXPECT_EQ(resolved[i].entry.df, 1U);
    }
    EXPECT_EQ(counting.read_at_calls(), 1U);
    EXPECT_EQ(counting.read_batch_calls(), 0U);
    EXPECT_EQ(metered.metrics().serial_rounds, 1U);
}

TEST(SniiQueryTermResolutionBatch, ResolvesResidentBlocksWithoutQueryIo) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "1048576");

    MemoryFile file;
    const std::vector<std::string> terms {"alpha", "kappa", "omega"};
    assert_ok(write_index(&file, terms, /*target_dict_block_bytes=*/1));

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    CountingReader counting(&metered);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    ASSERT_EQ(index_reader.n_dict_blocks(), terms.size());

    metered.reset_metrics();
    counting.reset_counts();
    std::vector<reader::LogicalIndexReader::BatchLookupResult> resolved;
    assert_ok(index_reader.lookup_batch(terms, &resolved));
    const std::vector<uint8_t> found = found_flags(resolved);

    ASSERT_EQ(found, (std::vector<uint8_t> {1, 1, 1}));
    ASSERT_EQ(resolved.size(), terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        EXPECT_EQ(resolved[i].entry.term, terms[i]);
    }
    EXPECT_EQ(counting.read_at_calls(), 0U);
    EXPECT_EQ(counting.read_batch_calls(), 0U);
    EXPECT_EQ(metered.metrics().serial_rounds, 0U);
}

TEST(SniiQueryTermResolutionBatch, ResolvesCompressedDictBlocksFromBatchBuffers) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");

    MemoryFile file;
    const std::vector<std::string> terms {
            "a" + std::string(4096, 'x'),
            "b" + std::string(4096, 'y'),
            "c" + std::string(4096, 'z'),
    };
    assert_ok(write_index(&file, terms, /*target_dict_block_bytes=*/1));

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&metered, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    ASSERT_EQ(index_reader.n_dict_blocks(), terms.size());
    ASSERT_LT(index_reader.section_refs().dict_region.length, terms.size() * terms.front().size());

    metered.reset_metrics();
    std::vector<reader::LogicalIndexReader::BatchLookupResult> resolved;
    assert_ok(index_reader.lookup_batch(terms, &resolved));
    const std::vector<uint8_t> found = found_flags(resolved);

    ASSERT_EQ(found, (std::vector<uint8_t> {1, 1, 1}));
    for (size_t i = 0; i < terms.size(); ++i) {
        EXPECT_EQ(resolved[i].entry.term, terms[i]);
    }
    EXPECT_EQ(metered.metrics().serial_rounds, 1U);
}

// GTest assertion macros inflate clang-tidy's branch count for this table-style I/O check.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiQueryTermResolutionBatch, ResolvesSeventeenDisjointBlocksInTwoBoundedWaves) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");

    MemoryFile file;
    const std::vector<std::string> indexed_terms = numbered_terms(33);
    assert_ok(write_index(&file, indexed_terms, /*target_dict_block_bytes=*/1));

    io::MeteredFileReader metered(&file, /*block_size=*/1);
    CountingReader counting(&metered);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    ASSERT_EQ(index_reader.n_dict_blocks(), indexed_terms.size());

    std::vector<std::string> query_terms;
    query_terms.reserve(17);
    for (size_t i = 0; i < indexed_terms.size(); i += 2) {
        query_terms.push_back(indexed_terms[i]);
    }
    ASSERT_EQ(query_terms.size(), 17U);

    metered.reset_metrics();
    counting.reset_counts();
    std::vector<reader::LogicalIndexReader::BatchLookupResult> resolved;
    assert_ok(index_reader.lookup_batch(query_terms, &resolved));
    const std::vector<uint8_t> found = found_flags(resolved);

    ASSERT_EQ(found, std::vector<uint8_t>(query_terms.size(), 1));
    ASSERT_EQ(resolved.size(), query_terms.size());
    for (size_t i = 0; i < query_terms.size(); ++i) {
        EXPECT_EQ(resolved[i].entry.term, query_terms[i]);
        EXPECT_EQ(resolved[i].entry.df, 1U);
    }
    EXPECT_EQ(resolved.back().entry.term, "term_32");
    EXPECT_EQ(counting.read_at_calls(), 0U);
    EXPECT_EQ(counting.read_batch_calls(), 2U);
    EXPECT_EQ(counting.batch_range_counts(), (std::vector<size_t> {16, 1}));
    EXPECT_EQ(metered.metrics().serial_rounds, 2U);
    EXPECT_EQ(metered.metrics().range_gets, query_terms.size());
}

// Terms of about 4 KiB of random bytes after a number that keeps them in order; zstd cannot
// shrink their dictionary blocks.
std::vector<std::string> wide_terms(size_t count) {
    std::mt19937 rng(20261002);
    std::vector<std::string> terms;
    terms.reserve(count);
    for (size_t i = 0; i < count; ++i) {
        std::string term = fmt::format("t{:05d}_", i);
        for (size_t c = 0; c < 4000; ++c) {
            term.push_back(static_cast<char>(1 + rng() % 255));
        }
        terms.push_back(std::move(term));
    }
    return terms;
}

// Consecutive dictionary blocks read as one run, so only the 4 MiB byte bound splits their
// waves: every wave stays under it although together the blocks exceed it.
TEST(SniiQueryTermResolutionBatch, ResolvesConsecutiveBlocksInWavesOfAtMostFourMiB) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    constexpr uint64_t kWaveBytes = 4ULL * 1024 * 1024;

    MemoryFile file;
    const std::vector<std::string> indexed_terms = wide_terms(1300);
    assert_ok(write_index(&file, indexed_terms, /*target_dict_block_bytes=*/1024 * 1024));

    CountingReader counting(&file);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    ASSERT_GE(index_reader.n_dict_blocks(), 6U);

    std::vector<std::string> query_terms;
    for (size_t i = 0; i < 1280; i += 80) {
        query_terms.push_back(indexed_terms[i]);
    }
    counting.reset_counts();
    std::vector<reader::LogicalIndexReader::BatchLookupResult> resolved;
    assert_ok(index_reader.lookup_batch(query_terms, &resolved));

    ASSERT_EQ(found_flags(resolved), std::vector<uint8_t>(query_terms.size(), 1));
    for (size_t i = 0; i < query_terms.size(); ++i) {
        EXPECT_EQ(resolved[i].entry.term, query_terms[i]);
    }
    EXPECT_EQ(counting.read_at_calls(), 0U);
    EXPECT_GE(counting.read_batch_calls(), 2U);
    EXPECT_GT(counting.bytes(), kWaveBytes);
    for (const uint64_t bytes : counting.batch_bytes()) {
        EXPECT_LE(bytes, kWaveBytes);
    }
}

TEST(SniiQueryTermResolutionBatch, StopsAfterFailedDictionaryWave) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    const auto indexed_terms = numbered_terms(65);
    assert_ok(write_index(&file, indexed_terms, /*target_dict_block_bytes=*/1));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    std::vector<std::string> terms;
    for (size_t i = 0; i < indexed_terms.size(); i += 2) {
        terms.push_back(indexed_terms[i]);
    }
    counting.reset_counts();
    counting.fail_batch(2);
    std::vector<reader::LogicalIndexReader::BatchLookupResult> results;
    const Status status = index_reader.lookup_batch(terms, &results);
    EXPECT_TRUE(status.is<ErrorCode::IO_ERROR>()) << status.to_string();
    EXPECT_EQ(counting.read_batch_calls(), 2U);
    EXPECT_EQ(counting.batch_range_counts(), (std::vector<size_t> {16, 16}));
    EXPECT_EQ(counting.read_at_calls(), 0U);
}

void expect_lookup_terms(const std::vector<reader::LogicalIndexReader::BatchLookupResult>& results,
                         const std::vector<std::string>& terms) {
    ASSERT_EQ(results.size(), terms.size());
    for (size_t i = 0; i < results.size(); ++i) {
        EXPECT_TRUE(results[i].found);
        EXPECT_EQ(results[i].entry.term, terms[i]);
        EXPECT_EQ(results[i].entry.df, 1U);
    }
}

TEST(SniiQueryTermResolutionBatch, PreparesAndConsumesColdWavesWithoutReading) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    auto indexed_terms = numbered_terms(65);
    for (auto& term : indexed_terms) {
        term.append(4096, 'x');
    }
    assert_ok(write_index(&file, indexed_terms, /*target_dict_block_bytes=*/1));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    std::vector<std::string> terms;
    for (size_t i = 0; i < indexed_terms.size(); i += 2) {
        terms.push_back(indexed_terms[i]);
    }
    counting.reset_counts();
    std::vector<reader::LogicalIndexReader::BatchLookupResult> results;
    reader::LogicalIndexReader::BatchLookupState state;
    assert_ok(index_reader.prepare_lookup_batch(terms, &results, &state));
    EXPECT_EQ(counting.rounds(), 0U);
    ASSERT_FALSE(state.done());
    io::BatchRangeFetcher fetcher(&counting);
    while (!state.done()) {
        const uint64_t previous_rounds = counting.rounds();
        assert_ok(index_reader.prepare_lookup_wave(&state, &fetcher));
        EXPECT_EQ(counting.rounds(), previous_rounds);
        assert_ok(fetcher.fetch());
        assert_ok(index_reader.consume_lookup_wave(&state, fetcher));
        EXPECT_EQ(counting.rounds(), previous_rounds + 1);
        fetcher.clear();
    }
    EXPECT_EQ(counting.batch_range_counts(), (std::vector<size_t> {16, 16, 1}));
    expect_lookup_terms(results, terms);
}

TEST(SniiQueryTermResolutionBatch, ResidentPreparationCompletesWithoutAReadWave) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "1048576");
    MemoryFile file;
    const std::vector<std::string> terms {"alpha", "kappa", "omega"};
    assert_ok(write_index(&file, terms, /*target_dict_block_bytes=*/1));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    counting.reset_counts();
    std::vector<reader::LogicalIndexReader::BatchLookupResult> results;
    reader::LogicalIndexReader::BatchLookupState state;
    assert_ok(index_reader.prepare_lookup_batch(terms, &results, &state));
    EXPECT_TRUE(state.done());
    EXPECT_EQ(counting.rounds(), 0U);
    expect_lookup_terms(results, terms);
}

TEST(SniiQueryTermResolutionBatch, ResidentSingleTermPreservesHitMissAndStateReuse) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "1048576");
    MemoryFile file;
    assert_ok(write_index(&file, {"alpha"}, /*target_dict_block_bytes=*/4096));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    counting.reset_counts();
    const std::vector<std::vector<std::string>> cases {
            {"alpha"}, {"aardvark"}, {"zulu"}, {"alpha"}};
    std::vector<reader::LogicalIndexReader::BatchLookupResult> results;
    reader::LogicalIndexReader::BatchLookupState state;
    for (const auto& terms : cases) {
        assert_ok(index_reader.prepare_lookup_batch(terms, &results, &state));
        EXPECT_TRUE(state.done());
        ASSERT_EQ(results.size(), 1U);
        EXPECT_EQ(results.front().found, terms.front() == "alpha");
        EXPECT_EQ(counting.rounds(), 0U);
    }
    expect_lookup_terms(results, {"alpha"});
}

TEST(SniiQueryTermResolutionBatch, FieldsInTheSameFileShareOneDictionaryRead) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    writer::SniiCompoundWriter compound_writer(&file);
    for (uint64_t index_id : {1, 2}) {
        writer::SniiIndexInput input;
        input.index_id = index_id;
        input.index_suffix = kIndexSuffix;
        input.config = format::IndexConfig::kDocsPositions;
        input.doc_count = 1;
        input.terms = {
                make_term(index_id == 1 ? "alpha" : "omega", {{.docid = 0, .positions = {0}}})};
        assert_ok(compound_writer.add_logical_index(input));
    }
    assert_ok(compound_writer.finish());
    CountingReader counting(&file);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader first;
    reader::LogicalIndexReader second;
    assert_ok(segment_reader.open_index(1, kIndexSuffix, &first));
    assert_ok(segment_reader.open_index(2, kIndexSuffix, &second));
    counting.reset_counts();
    const std::vector<std::string> first_terms {"alpha"};
    const std::vector<std::string> second_terms {"omega"};
    std::vector<reader::LogicalIndexReader::BatchLookupResult> first_results;
    std::vector<reader::LogicalIndexReader::BatchLookupResult> second_results;
    reader::LogicalIndexReader::BatchLookupState first_state;
    reader::LogicalIndexReader::BatchLookupState second_state;
    assert_ok(first.prepare_lookup_batch(first_terms, &first_results, &first_state));
    assert_ok(second.prepare_lookup_batch(second_terms, &second_results, &second_state));
    io::BatchRangeFetcher fetcher(&counting);
    assert_ok(first.prepare_lookup_wave(&first_state, &fetcher));
    assert_ok(second.prepare_lookup_wave(&second_state, &fetcher));
    EXPECT_EQ(counting.rounds(), 0U);
    assert_ok(fetcher.fetch());
    assert_ok(first.consume_lookup_wave(&first_state, fetcher));
    assert_ok(second.consume_lookup_wave(&second_state, fetcher));
    fetcher.clear();
    EXPECT_TRUE(first_state.done());
    EXPECT_TRUE(second_state.done());
    EXPECT_EQ(counting.read_batch_calls(), 1U);
    EXPECT_EQ(counting.read_at_calls(), 0U);
    expect_lookup_terms(first_results, first_terms);
    expect_lookup_terms(second_results, second_terms);
}

void check_shared_dictionary_waves(size_t indexed_count,
                                   const std::vector<size_t>& expected_ranges) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    const auto indexed_terms = numbered_terms(indexed_count);
    writer::SniiCompoundWriter compound_writer(&file);
    for (uint64_t index_id : {1, 2}) {
        writer::SniiIndexInput input;
        input.index_id = index_id;
        input.index_suffix = kIndexSuffix;
        input.config = format::IndexConfig::kDocsPositions;
        input.doc_count = static_cast<uint32_t>(indexed_terms.size());
        input.target_dict_block_bytes = 1;
        for (size_t i = 0; i < indexed_terms.size(); ++i) {
            input.terms.push_back(make_term(
                    indexed_terms[i], {{.docid = static_cast<uint32_t>(i), .positions = {0}}}));
        }
        assert_ok(compound_writer.add_logical_index(input));
    }
    assert_ok(compound_writer.finish());
    CountingReader counting(&file);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    std::array<reader::LogicalIndexReader, 2> fields;
    std::array<reader::LogicalIndexReader::BatchLookupState, 2> states;
    std::array<std::vector<reader::LogicalIndexReader::BatchLookupResult>, 2> results;
    std::vector<std::string> terms;
    for (size_t i = 0; i < indexed_terms.size(); i += 2) {
        terms.push_back(indexed_terms[i]);
    }
    for (size_t i = 0; i < fields.size(); ++i) {
        assert_ok(segment_reader.open_index(i + 1, kIndexSuffix, &fields[i]));
        assert_ok(fields[i].prepare_lookup_batch(terms, &results[i], &states[i]));
    }
    counting.reset_counts();
    io::BatchRangeFetcher fetcher(&counting);
    while (!states[0].done() || !states[1].done()) {
        ASSERT_LT(counting.rounds(), expected_ranges.size());
        for (size_t i = 0; i < fields.size(); ++i) {
            if (!states[i].done()) {
                assert_ok(fields[i].prepare_lookup_wave(&states[i], &fetcher));
            }
        }
        assert_ok(fetcher.fetch());
        for (size_t i = 0; i < fields.size(); ++i) {
            if (!states[i].done()) {
                assert_ok(fields[i].consume_lookup_wave(&states[i], fetcher));
            }
        }
        fetcher.clear();
    }
    EXPECT_EQ(counting.batch_range_counts(), expected_ranges);
    EXPECT_EQ(counting.ranges(), terms.size() * fields.size());
    for (const auto& field_results : results) {
        expect_lookup_terms(field_results, terms);
    }
}

TEST(SniiQueryTermResolutionBatch, FieldsShareTheDictionaryWaveRangeLimit) {
    check_shared_dictionary_waves(17, {16, 2});
}

TEST(SniiQueryTermResolutionBatch, FullWaveDefersAnotherFieldWithoutLosingResults) {
    check_shared_dictionary_waves(33, {16, 16, 2});
}

uint32_t lookup_bench_parameter(const char* name, uint32_t fallback) {
    const char* value = std::getenv(name);
    return value == nullptr ? fallback : static_cast<uint32_t>(std::stoul(value));
}

uint64_t lookup_cpu_ns() {
    timespec value {};
    DORIS_CHECK_EQ(clock_gettime(CLOCK_THREAD_CPUTIME_ID, &value), 0);
    return static_cast<uint64_t>(value.tv_sec) * 1000000000ULL + value.tv_nsec;
}

struct LookupBenchSpec {
    const char* name;
    const char* resident_limit;
    uint32_t terms;
    uint32_t block_bytes;
    uint32_t stride;
    bool compressed;
};

void benchmark_lookup(const LookupBenchSpec& spec) {
    ScopedEnv dict_resident_max("SNII_DICT_RESIDENT_MAX", spec.resident_limit);
    MemoryFile file;
    auto indexed_terms = numbered_terms(spec.terms);
    if (spec.compressed) {
        for (auto& term : indexed_terms) {
            term.append(4096, 'x');
        }
    }
    assert_ok(write_index(&file, indexed_terms, spec.block_bytes));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment_reader;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment_reader));
    reader::LogicalIndexReader index_reader;
    assert_ok(segment_reader.open_index(kIndexId, kIndexSuffix, &index_reader));
    std::vector<std::string> terms;
    for (size_t i = 0; i < indexed_terms.size(); i += spec.stride) {
        terms.push_back(indexed_terms[i]);
    }
    auto execute = [&]() {
        std::vector<reader::LogicalIndexReader::BatchLookupResult> results;
        const Status status = index_reader.lookup_batch(terms, &results);
        DORIS_CHECK(status.ok()) << status.to_string();
        DORIS_CHECK_EQ(results.size(), terms.size());
        uint64_t checksum = 0;
        for (size_t i = 0; i < results.size(); ++i) {
            const auto& result = results[i];
            DORIS_CHECK(result.found);
            DORIS_CHECK_EQ(result.entry.term, terms[i]);
            checksum += (i + 1) * (result.entry.df + result.frq_base + result.prx_base);
            for (unsigned char byte : result.entry.frq_bytes) {
                checksum = checksum * 31 + byte;
            }
        }
        return checksum;
    };
    counting.reset_counts();
    const uint64_t expected = execute();
    const uint64_t expected_ranges = counting.ranges();
    const uint64_t expected_rounds = counting.rounds();
    const uint64_t expected_bytes = counting.bytes();
    const std::string label = std::string("dictionary_lookup/") + spec.name;
    const uint32_t samples = lookup_bench_parameter("QUERY_ENGINE_BENCH_SAMPLES", 32);
    const uint32_t iterations = lookup_bench_parameter("DICT_LOOKUP_BENCH_ITERATIONS", 64);
    std::cout << "DICT_LOOKUP_IO," << label << ',' << expected_ranges << ',' << expected_rounds
              << ',' << expected_bytes << '\n';
    for (uint32_t sample = 0; sample < samples; ++sample) {
        benchmark::wait_for_turn(label, sample);
        counting.reset_counts();
        uint64_t checksum = 0;
        const uint64_t start = lookup_cpu_ns();
        for (uint32_t iteration = 0; iteration < iterations; ++iteration) {
            checksum += execute();
        }
        const uint64_t elapsed = lookup_cpu_ns() - start;
        ASSERT_EQ(checksum, expected * iterations) << label;
        EXPECT_EQ(counting.ranges(), expected_ranges * iterations) << label;
        EXPECT_EQ(counting.rounds(), expected_rounds * iterations) << label;
        EXPECT_EQ(counting.bytes(), expected_bytes * iterations) << label;
        benchmark::report_sample(label, sample, iterations, elapsed, checksum);
    }
}

// Four terms in documents 0-3, each at its own position so "alpha bravo charlie delta" is a
// phrase of every document, written with one dictionary block per term and opened with the
// dictionary on demand. The postings are inline, so every read a query makes is a dictionary
// read.
class SniiTermResolutionIoTest : public ::testing::Test {
protected:
    void open_index() {
        writer::SniiIndexInput input;
        input.index_id = kIndexId;
        input.index_suffix = kIndexSuffix;
        input.config = format::IndexConfig::kDocsPositions;
        input.doc_count = 4;
        input.target_dict_block_bytes = 1;
        const std::vector<std::string> terms = {"alpha", "bravo", "charlie", "delta"};
        for (uint32_t position = 0; position < terms.size(); ++position) {
            input.terms.push_back(
                    make_term(terms[position], {{.docid = 0, .positions = {position}},
                                                {.docid = 1, .positions = {position}},
                                                {.docid = 2, .positions = {position}},
                                                {.docid = 3, .positions = {position}}}));
        }
        input.encoded_norms.assign(input.doc_count, encode_norm(terms.size()));
        writer::SniiCompoundWriter compound_writer(&_file);
        assert_ok(compound_writer.add_logical_index(input));
        assert_ok(compound_writer.finish());

        assert_ok(reader::SniiSegmentReader::open(&_counter, &_segment_reader));
        assert_ok(_segment_reader.open_index(kIndexId, kIndexSuffix, &_index));
        ASSERT_EQ(_index.n_dict_blocks(), terms.size());
        _counter.reset_counts();
    }

    ScopedEnv _dictionary_on_demand {"SNII_DICT_RESIDENT_MAX", "0"};
    MemoryFile _file;
    CountingReader _counter {&_file};
    reader::SniiSegmentReader _segment_reader;
    reader::LogicalIndexReader _index;
};

const std::vector<uint32_t> kAllDocs = {0, 1, 2, 3};

// Blocks next to each other in the file are read as one range, so "charlie" and "delta" share one.
TEST_F(SniiTermResolutionIoTest, OrReadsItsColdDictionaryBlocksInOneRound) {
    open_index();
    std::vector<uint32_t> docids;
    assert_ok(boolean_or(_index, {"alpha", "charlie", "delta"}, &docids));

    EXPECT_EQ(docids, kAllDocs);
    EXPECT_EQ(_counter.rounds(), 1U);
    EXPECT_EQ(_counter.ranges(), 2U);
}

TEST_F(SniiTermResolutionIoTest, AndReadsItsColdDictionaryBlocksInOneRound) {
    open_index();
    std::vector<uint32_t> docids;
    assert_ok(boolean_and(_index, {"alpha", "charlie", "delta"}, &docids));

    EXPECT_EQ(docids, kAllDocs);
    EXPECT_EQ(_counter.rounds(), 1U);
    EXPECT_EQ(_counter.ranges(), 2U);
}

// A term the dictionary rules out without reading ("aaa" sorts before every term) ends the
// conjunction before any dictionary block is read.
TEST_F(SniiTermResolutionIoTest, AndWithATermRuledOutReadsNoDictionaryBlock) {
    open_index();
    std::vector<uint32_t> docids;
    assert_ok(boolean_and(_index, {"alpha", "aaa", "charlie"}, &docids));

    EXPECT_TRUE(docids.empty());
    EXPECT_EQ(_counter.rounds(), 0U);
    EXPECT_EQ(_counter.ranges(), 0U);
}

TEST_F(SniiTermResolutionIoTest, PhraseReadsItsColdDictionaryBlocksInOneRound) {
    open_index();
    std::vector<uint32_t> docids;
    assert_ok(phrase_query(_index, {"alpha", "bravo", "charlie"}, &docids));

    EXPECT_EQ(docids, kAllDocs);
    EXPECT_EQ(_counter.rounds(), 1U);
    EXPECT_EQ(_counter.ranges(), 1U);
}

// The exact terms of a phrase prefix cost one round on top of what expanding the tail costs.
TEST_F(SniiTermResolutionIoTest, PhrasePrefixResolvesItsExactTermsInOneRound) {
    open_index();
    std::vector<uint32_t> tail_docids;
    assert_ok(prefix_query(_index, "cha", &tail_docids));
    const uint64_t tail_rounds = _counter.rounds();
    ASSERT_GT(tail_rounds, 0U);
    _counter.reset_counts();

    std::vector<uint32_t> docids;
    assert_ok(phrase_prefix_query(_index, {"alpha", "bravo", "cha"}, &docids));

    EXPECT_EQ(docids, kAllDocs);
    EXPECT_EQ(_counter.rounds(), tail_rounds + 1);
}

// Without the resident filter a term missing from its block is only found by reading it, so the
// other blocks of the conjunction come in the same round instead of not at all.
TEST_F(SniiTermResolutionIoTest, AndWithoutTheFilterReadsEveryCandidateBlockInOneRound) {
    ScopedEnv filter_off("SNII_BSBF_RESIDENT_MAX", "0");
    open_index();
    std::vector<uint32_t> docids;
    assert_ok(boolean_and(_index, {"alphz", "charlie", "delta"}, &docids));

    EXPECT_TRUE(docids.empty());
    EXPECT_EQ(_counter.rounds(), 1U);
    EXPECT_EQ(_counter.ranges(), 2U);
}

// A scored group resolves its distinct terms together in one round, like the unscored ones.
TEST_F(SniiTermResolutionIoTest, ScoringResolvesItsColdDictionaryBlocksInOneRound) {
    open_index();
    reader::SniiIndexSource source(_index);
    const std::vector<std::string> terms = {"alpha", "charlie", "alpha", "delta"};
    _counter.reset_counts();
    assert_ok(source.prepare_terms(terms));
    EXPECT_EQ(_counter.rounds(), 1U);
    EXPECT_EQ(_counter.ranges(), 2U);
}

TEST(SniiQueryTermResolutionBatch, DISABLED_DictionaryLookupBenchmark) {
    for (const LookupBenchSpec spec : {
                 LookupBenchSpec {.name = "resident_single",
                                 .resident_limit = "1048576",
                                 .terms = 1,
                                 .block_bytes = 4096,
                                 .stride = 1,
                                 .compressed = false},
                 LookupBenchSpec {.name = "resident_many",
                                 .resident_limit = "1048576",
                                 .terms = 65,
                                 .block_bytes = 1,
                                 .stride = 2,
                                 .compressed = false},
                 LookupBenchSpec {.name = "cold_single",
                                 .resident_limit = "0",
                                 .terms = 9,
                                 .block_bytes = 4096,
                                 .stride = 2,
                                 .compressed = false},
                 LookupBenchSpec {.name = "cold_three_runs",
                                 .resident_limit = "0",
                                 .terms = 5,
                                 .block_bytes = 1,
                                 .stride = 2,
                                 .compressed = false},
                 LookupBenchSpec {.name = "cold_three_waves",
                                 .resident_limit = "0",
                                 .terms = 65,
                                 .block_bytes = 1,
                                 .stride = 2,
                                 .compressed = false},
                 LookupBenchSpec {.name = "compressed",
                                 .resident_limit = "0",
                                 .terms = 33,
                                 .block_bytes = 1,
                                 .stride = 2,
                                 .compressed = true},
         }) {
        benchmark_lookup(spec);
    }
}

} // namespace
} // namespace doris::snii::query
