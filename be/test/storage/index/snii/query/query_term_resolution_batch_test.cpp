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

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <roaring/roaring.hh>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/query/spi/io_batch.h"
#include "storage/index/snii/io/batch_range_fetcher.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/internal/docid_conjunction.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/scoring_query.h"
#include "storage/index/snii/reader/batch_lookup_results.h"
#include "storage/index/snii/reader/dict_block_cache.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
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

class CorruptingReader final : public io::FileReader {
public:
    explicit CorruptingReader(io::FileReader* inner) : inner_(inner) {}
    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        RETURN_IF_ERROR(inner_->read_at(offset, len, out));
        if (corrupt && !out->empty()) {
            out->back() ^= 1;
        }
        return Status::OK();
    }
    uint64_t size() const override { return inner_->size(); }
    bool corrupt = false;

private:
    io::FileReader* inner_;
};

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
                      std::vector<std::vector<uint8_t>>* outs) override {
        ++read_batch_calls_;
        batch_range_counts_.push_back(ranges.size());
        ranges_ += ranges.size();
        for (const auto& range : ranges) {
            bytes_ += range.len;
        }
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

private:
    io::FileReader* inner_;
    uint64_t bytes_ = 0;
    uint64_t ranges_ = 0;
    uint64_t fail_batch_ = 0;
    uint64_t read_at_calls_ = 0;
    uint64_t read_batch_calls_ = 0;
    std::vector<size_t> batch_range_counts_;
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

TEST(SniiQueryTermResolutionBatch, CacheBudgetRejectsBeforeReadingTheDictionary) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    assert_ok(write_index(&file, {"alpha", "omega"}, 4096));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    counting.reset_counts();
    index_query::MemoryBudget budget(0);
    reader::DictBlockCache cache(8, &budget);
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    const Status status = index.lookup("alpha", &found, &entry, &frq_base, &prx_base, &cache);
    EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status.to_string();
    EXPECT_EQ(counting.rounds(), 0U);
    EXPECT_EQ(cache.size(), 0U);
    EXPECT_EQ(budget.used_bytes(), 0U);
}

// GTest assertions inflate the branch count for the cache lifetime checks.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiQueryTermResolutionBatch, CacheBudgetEvictsBeforeLoadingAndPreservesHits) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    assert_ok(write_index(&file, {"alpha", "omega"}, 1));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    ASSERT_EQ(index.n_dict_blocks(), 2U);
    reader::DictBlockScanMemory first;
    reader::DictBlockScanMemory second;
    assert_ok(index.dict_block_scan_memory(0, &first));
    assert_ok(index.dict_block_scan_memory(1, &second));
    const uint64_t limit =
            std::max(first.decode_bytes, second.decode_bytes) + sizeof(reader::DecodedDictBlock);
    index_query::MemoryBudget budget(limit);
    counting.reset_counts();
    {
        reader::DictBlockCache cache(8, &budget);
        for (const std::string term : {"alpha", "alpha", "omega", "omega", "alpha"}) {
            bool found = false;
            format::DictEntry entry;
            uint64_t frq_base = 0;
            uint64_t prx_base = 0;
            assert_ok(index.lookup(term, &found, &entry, &frq_base, &prx_base, &cache));
            ASSERT_TRUE(found);
            EXPECT_EQ(entry.term, term);
            EXPECT_EQ(entry.df, 1U);
            EXPECT_EQ(cache.size(), 1U);
            EXPECT_GT(budget.used_bytes(), 0U);
            EXPECT_LE(budget.used_bytes(), limit);
        }
        EXPECT_EQ(counting.read_at_calls(), 3U);
        EXPECT_LE(budget.peak_bytes(), limit);
    }
    EXPECT_EQ(budget.used_bytes(), 0U);
}

// GTest assertions inflate the branch count for the cache lifetime checks.
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(SniiQueryTermResolutionBatch, CachePinsRetainOneChargeAfterEvictionAndCacheDestruction) {
    index_query::MemoryBudget budget(8);
    std::shared_ptr<const reader::DecodedDictBlock> pin;
    int loads = 0;
    {
        reader::DictBlockCache cache(1, &budget);
        const auto loader = [&](std::shared_ptr<const reader::DecodedDictBlock>* out) -> Status {
            auto block = std::make_shared<reader::DecodedDictBlock>();
            RETURN_IF_ERROR(cache.reserve_memory(8, &block->memory));
            ++loads;
            block->bytes.assign(8, 42);
            *out = std::move(block);
            return Status::OK();
        };
        assert_ok(cache.get_or_load(0, loader, &pin));
        std::shared_ptr<const reader::DecodedDictBlock> alias;
        assert_ok(cache.get_or_load(0, loader, &alias));
        EXPECT_EQ(loads, 1);
        EXPECT_EQ(budget.used_bytes(), 8U);
        std::shared_ptr<const reader::DecodedDictBlock> next;
        EXPECT_TRUE(cache.get_or_load(1, loader, &next).is<ErrorCode::MEM_LIMIT_EXCEEDED>());
        EXPECT_EQ(cache.size(), 0U);
        EXPECT_EQ(budget.used_bytes(), 8U);
        EXPECT_EQ(pin->bytes.back(), 42);
        pin.reset();
        EXPECT_EQ(budget.used_bytes(), 8U);
        EXPECT_EQ(alias->bytes.front(), 42);
        alias.reset();
        EXPECT_EQ(budget.used_bytes(), 0U);
        assert_ok(cache.get_or_load(1, loader, &pin));
        EXPECT_EQ(loads, 2);
    }
    EXPECT_EQ(budget.used_bytes(), 8U);
    EXPECT_EQ(pin->bytes.back(), 42);
    pin.reset();
    EXPECT_EQ(budget.used_bytes(), 0U);
    EXPECT_EQ(budget.peak_bytes(), 8U);
}

TEST(SniiQueryTermResolutionBatch, CacheBudgetReleasesFailedReadsAndCorruptDecodes) {
    class FaultReader final : public io::FileReader {
    public:
        explicit FaultReader(io::FileReader* inner) : inner_(inner) {}
        Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
            RETURN_IF_ERROR(inner_->read_at(offset, len, out));
            if (fail) {
                return Status::IOError("Injected dictionary read failure");
            }
            if (corrupt && !out->empty()) {
                out->back() ^= 1;
            }
            return Status::OK();
        }
        uint64_t size() const override { return inner_->size(); }
        bool fail = false;
        bool corrupt = false;

    private:
        io::FileReader* inner_;
    };
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    assert_ok(write_index(&file, {"alpha"}, 4096));
    FaultReader fault(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&fault, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    reader::DictBlockScanMemory memory;
    assert_ok(index.dict_block_scan_memory(0, &memory));
    const uint64_t limit = memory.decode_bytes + sizeof(reader::DecodedDictBlock);
    index_query::MemoryBudget budget(limit);
    reader::DictBlockCache cache(8, &budget);
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    fault.fail = true;
    EXPECT_FALSE(index.lookup("alpha", &found, &entry, &frq_base, &prx_base, &cache).ok());
    EXPECT_EQ(budget.used_bytes(), 0U);
    EXPECT_EQ(cache.size(), 0U);
    fault.fail = false;
    fault.corrupt = true;
    EXPECT_TRUE(index.lookup("alpha", &found, &entry, &frq_base, &prx_base, &cache)
                        .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
    EXPECT_EQ(budget.used_bytes(), 0U);
    EXPECT_EQ(cache.size(), 0U);
    fault.corrupt = false;
    assert_ok(index.lookup("alpha", &found, &entry, &frq_base, &prx_base, &cache));
    EXPECT_TRUE(found);
    EXPECT_EQ(entry.term, "alpha");
    EXPECT_EQ(budget.used_bytes(), limit);
    EXPECT_EQ(budget.peak_bytes(), limit);
}

TEST(SniiQueryTermResolutionBatch, ReadPayloadAndDecodedBlocksShareOneBudget) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    assert_ok(write_index(&file, {"alpha"}, 4096));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    reader::DictBlockScanMemory memory;
    assert_ok(index.dict_block_scan_memory(0, &memory));
    const uint64_t limit = memory.decode_bytes + sizeof(reader::DecodedDictBlock) + 8;
    index_query::MemoryBudget budget(limit);
    io::BatchRangeFetcher read(&counting, 0, &budget);
    read.add(0, 8);
    assert_ok(read.fetch());
    EXPECT_EQ(budget.used_bytes(), 8U);
    {
        reader::DictBlockCache cache(8, &budget);
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        assert_ok(index.lookup("alpha", &found, &entry, &frq_base, &prx_base, &cache));
        ASSERT_TRUE(found);
        EXPECT_EQ(budget.used_bytes(), limit);
        io::BatchRangeFetcher blocked(&counting, 0, &budget);
        blocked.add(16, 1);
        const uint64_t rounds = counting.rounds();
        EXPECT_TRUE(blocked.fetch().is<ErrorCode::MEM_LIMIT_EXCEEDED>());
        EXPECT_EQ(counting.rounds(), rounds);
    }
    EXPECT_EQ(budget.used_bytes(), 8U);
    EXPECT_EQ(read.get(0).size(), 8U);
    read.clear();
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(SniiQueryTermResolutionBatch, CompressedCacheAdmissionIncludesDecodedMemory) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    const std::string term(4096, 'a');
    assert_ok(write_index(&file, {term}, 8192));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    ASSERT_LT(index.section_refs().dict_region.length, term.size());
    counting.reset_counts();
    index_query::MemoryBudget compressed_only(index.section_refs().dict_region.length);
    reader::DictBlockCache rejected(8, &compressed_only);
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    EXPECT_TRUE(index.lookup(term, &found, &entry, &frq_base, &prx_base, &rejected)
                        .is<ErrorCode::MEM_LIMIT_EXCEEDED>());
    EXPECT_EQ(counting.rounds(), 0U);
    reader::DictBlockScanMemory memory;
    assert_ok(index.dict_block_scan_memory(0, &memory));
    const uint64_t limit = memory.decode_bytes + sizeof(reader::DecodedDictBlock);
    index_query::MemoryBudget full(limit);
    reader::DictBlockCache accepted(8, &full);
    assert_ok(index.lookup(term, &found, &entry, &frq_base, &prx_base, &accepted));
    EXPECT_TRUE(found);
    EXPECT_EQ(entry.term, term);
    EXPECT_EQ(full.used_bytes(), limit);
    EXPECT_EQ(full.peak_bytes(), limit);
    EXPECT_EQ(counting.read_at_calls(), 1U);
}

TEST(SniiQueryTermResolutionBatch, ResidentDictionaryDoesNotChargeTheRequestCache) {
    ScopedEnv resident("SNII_DICT_RESIDENT_MAX", "1048576");
    MemoryFile file;
    assert_ok(write_index(&file, {"alpha"}, 4096));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    counting.reset_counts();
    index_query::MemoryBudget budget(0);
    reader::DictBlockCache cache(8, &budget);
    bool found = false;
    format::DictEntry entry;
    uint64_t frq_base = 0;
    uint64_t prx_base = 0;
    assert_ok(index.lookup("alpha", &found, &entry, &frq_base, &prx_base, &cache));
    EXPECT_TRUE(found);
    EXPECT_EQ(entry.term, "alpha");
    EXPECT_EQ(counting.rounds(), 0U);
    EXPECT_EQ(cache.size(), 0U);
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(SniiQueryTermResolutionBatch, DictionaryWaveRequiresBudgetForDecodeWorkspace) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    for (const std::string& term : {std::string("alpha"), std::string(4096, 'a')}) {
        SCOPED_TRACE(term.size());
        MemoryFile file;
        assert_ok(write_index(&file, {term}, 8192));
        CountingReader counting(&file);
        reader::SniiSegmentReader segment;
        assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
        reader::LogicalIndexReader index;
        assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
        const std::vector<std::string> terms {term};
        std::vector<reader::LogicalIndexReader::BatchLookupResult> results;
        reader::LogicalIndexReader::BatchLookupState state;
        assert_ok(index.prepare_lookup_batch(terms, &results, &state));
        const uint64_t read_bytes = index.section_refs().dict_region.length;
        index_query::MemoryBudget budget(read_bytes);
        io::BatchRangeFetcher fetcher(&counting, 0, &budget);
        assert_ok(index.prepare_lookup_wave(&state, &fetcher));
        counting.reset_counts();
        assert_ok(fetcher.fetch());
        ASSERT_EQ(budget.used_bytes(), read_bytes);
        const uint64_t rounds = counting.rounds();
        const Status status = index.consume_lookup_wave(&state, fetcher);
        EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status.to_string();
        EXPECT_EQ(counting.rounds(), rounds);
        EXPECT_EQ(budget.used_bytes(), read_bytes);
        EXPECT_FALSE(results.front().found);
        fetcher.clear();
        EXPECT_EQ(budget.used_bytes(), 0U);
    }
}

TEST(SniiQueryTermResolutionBatch, DictionaryWaveReleasesDecodeWorkspaceAfterConsumption) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    for (const std::string& term : {std::string("alpha"), std::string(4096, 'a')}) {
        SCOPED_TRACE(term.size());
        MemoryFile file;
        assert_ok(write_index(&file, {term}, 8192));
        CountingReader counting(&file);
        reader::SniiSegmentReader segment;
        assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
        reader::LogicalIndexReader index;
        assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
        reader::DictBlockScanMemory memory;
        assert_ok(index.dict_block_scan_memory(0, &memory));
        index_query::MemoryBudget budget(memory.decode_bytes);
        io::BatchRangeFetcher fetcher(&counting, 0, &budget);
        const std::vector<std::string> terms {term};
        std::vector<reader::LogicalIndexReader::BatchLookupResult> results;
        reader::LogicalIndexReader::BatchLookupState state;
        assert_ok(index.prepare_lookup_batch(terms, &results, &state));
        assert_ok(index.prepare_lookup_wave(&state, &fetcher));
        counting.reset_counts();
        assert_ok(fetcher.fetch());
        const uint64_t read_bytes = budget.used_bytes();
        const uint64_t rounds = counting.rounds();
        assert_ok(index.consume_lookup_wave(&state, fetcher));
        EXPECT_TRUE(state.done());
        EXPECT_TRUE(results.front().found);
        EXPECT_EQ(results.front().entry.term, term);
        EXPECT_EQ(counting.rounds(), rounds);
        EXPECT_GT(budget.peak_bytes(), read_bytes);
        EXPECT_LE(budget.peak_bytes(), budget.limit_bytes());
        EXPECT_EQ(budget.used_bytes(), read_bytes);
        fetcher.clear();
        EXPECT_EQ(budget.used_bytes(), 0U);
    }
}

TEST(SniiQueryTermResolutionBatch, DictionaryWaveReleasesDecodeWorkspaceOnCorruption) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    assert_ok(write_index(&file, {"alpha"}, 8192));
    CorruptingReader corrupting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&corrupting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    reader::DictBlockScanMemory memory;
    assert_ok(index.dict_block_scan_memory(0, &memory));
    index_query::MemoryBudget budget(memory.decode_bytes);
    io::BatchRangeFetcher fetcher(&corrupting, 0, &budget);
    const std::vector<std::string> terms {"alpha"};
    std::vector<reader::LogicalIndexReader::BatchLookupResult> results;
    reader::LogicalIndexReader::BatchLookupState state;
    assert_ok(index.prepare_lookup_batch(terms, &results, &state));
    assert_ok(index.prepare_lookup_wave(&state, &fetcher));
    corrupting.corrupt = true;
    assert_ok(fetcher.fetch());
    const uint64_t read_bytes = budget.used_bytes();
    const Status status = index.consume_lookup_wave(&state, fetcher);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>()) << status.to_string();
    EXPECT_GT(budget.peak_bytes(), read_bytes);
    EXPECT_EQ(budget.used_bytes(), read_bytes);
    fetcher.clear();
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(SniiQueryTermResolutionBatch, DictionaryWaveReusesWorkspaceAcrossBlocks) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    std::vector<std::string> terms {"alpha", "bravo", "omega"};
    for (std::string& term : terms) {
        term.append(4096, 'a');
    }
    assert_ok(write_index(&file, terms, 1));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    ASSERT_EQ(index.n_dict_blocks(), terms.size());
    uint64_t max_decode_bytes = 0;
    for (uint32_t ordinal = 0; ordinal < index.n_dict_blocks(); ++ordinal) {
        reader::DictBlockScanMemory memory;
        assert_ok(index.dict_block_scan_memory(ordinal, &memory));
        max_decode_bytes = std::max(max_decode_bytes, memory.decode_bytes);
    }
    const uint64_t read_bytes = index.section_refs().dict_region.length;
    ASSERT_LT(read_bytes, max_decode_bytes);
    index_query::MemoryBudget budget(read_bytes + max_decode_bytes);
    io::BatchRangeFetcher fetcher(&counting, 0, &budget);
    std::vector<reader::LogicalIndexReader::BatchLookupResult> results;
    reader::LogicalIndexReader::BatchLookupState state;
    assert_ok(index.prepare_lookup_batch(terms, &results, &state));
    assert_ok(index.prepare_lookup_wave(&state, &fetcher));
    counting.reset_counts();
    assert_ok(fetcher.fetch());
    assert_ok(index.consume_lookup_wave(&state, fetcher));
    EXPECT_TRUE(state.done());
    ASSERT_EQ(results.size(), terms.size());
    for (size_t i = 0; i < terms.size(); ++i) {
        EXPECT_TRUE(results[i].found);
        EXPECT_EQ(results[i].entry.term, terms[i]);
    }
    EXPECT_EQ(counting.rounds(), 1U);
    EXPECT_EQ(budget.used_bytes(), read_bytes);
    EXPECT_LE(budget.peak_bytes(), budget.limit_bytes());
    fetcher.clear();
    EXPECT_EQ(budget.used_bytes(), 0U);
}

namespace {
struct OpenedDictionary {
    MemoryFile file;
    CountingReader counting {&file};
    reader::SniiSegmentReader segment;
    reader::LogicalIndexReader index;

    Status open(const std::vector<std::string>& terms, uint32_t target_block_bytes) {
        RETURN_IF_ERROR(write_index(&file, terms, target_block_bytes));
        RETURN_IF_ERROR(reader::SniiSegmentReader::open(&counting, &segment));
        RETURN_IF_ERROR(segment.open_index(kIndexId, kIndexSuffix, &index));
        counting.reset_counts();
        return Status::OK();
    }
};
} // namespace

TEST(SniiQueryTermResolutionBatch, SharedIoBatchPreparesMultipleReadersWithoutIO) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    const std::vector<std::string> first_terms {"alpha"};
    const std::vector<std::string> second_terms {"bravo"};
    OpenedDictionary first;
    OpenedDictionary second;
    assert_ok(first.open(first_terms, 1));
    assert_ok(second.open(second_terms, 1));
    index_query::MemoryBudget budget(1048576);
    reader::BatchLookupResults first_results(budget);
    reader::BatchLookupResults second_results(budget);
    reader::LogicalIndexReader::BatchLookupState first_state;
    reader::LogicalIndexReader::BatchLookupState second_state;
    assert_ok(first.index.prepare_lookup_batch(first_terms, &first_results, &first_state));
    assert_ok(second.index.prepare_lookup_batch(second_terms, &second_results, &second_state));
    index_query::IoBatch wave(budget, {.bytes = 1048576, .ranges = 16});
    assert_ok(first.index.prepare_lookup_wave(&first_state, &wave));
    assert_ok(second.index.prepare_lookup_wave(&second_state, &wave));
    EXPECT_EQ(first.counting.rounds(), 0U);
    EXPECT_EQ(second.counting.rounds(), 0U);
    EXPECT_EQ(wave.pending(), 2U);
    assert_ok(wave.fetch());
    assert_ok(first.index.consume_lookup_wave(&first_state, wave));
    assert_ok(second.index.consume_lookup_wave(&second_state, wave));
    EXPECT_TRUE(first_state.done());
    EXPECT_TRUE(second_state.done());
    EXPECT_EQ(first_results.results().front().entry.term, "alpha");
    EXPECT_EQ(second_results.results().front().entry.term, "bravo");
}

TEST(SniiQueryTermResolutionBatch, BudgetedResultsRejectPreparationWithoutSlotMemory) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    assert_ok(write_index(&file, {"alpha"}, 8192));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    index_query::MemoryBudget budget(0);
    reader::BatchLookupResults results(budget);
    reader::LogicalIndexReader::BatchLookupState state;
    const std::vector<std::string> terms {"alpha"};
    counting.reset_counts();
    const Status status = index.prepare_lookup_batch(terms, &results, &state);
    EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status.to_string();
    EXPECT_TRUE(results.results().empty());
    EXPECT_EQ(budget.used_bytes(), 0U);
    EXPECT_EQ(counting.rounds(), 0U);
}

TEST(SniiQueryTermResolutionBatch, BudgetedResultsRejectResidentKeysWithoutHeapMemory) {
    ScopedEnv resident("SNII_DICT_RESIDENT_MAX", "1048576");
    MemoryFile file;
    const std::vector<std::string> terms {std::string(4096, 'a')};
    assert_ok(write_index(&file, terms, 8192));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    index_query::MemoryBudget budget(sizeof(reader::LogicalIndexReader::BatchLookupResult));
    reader::BatchLookupResults results(budget);
    reader::LogicalIndexReader::BatchLookupState state;
    counting.reset_counts();
    const Status status = index.prepare_lookup_batch(terms, &results, &state);
    EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status.to_string();
    ASSERT_EQ(results.results().size(), 1U);
    EXPECT_FALSE(results.results().front().found);
    EXPECT_EQ(counting.rounds(), 0U);
}

uint64_t retained_result_bytes(const reader::BatchLookupResults& owner) {
    const auto& results = owner.results();
    uint64_t bytes = results.capacity() * sizeof(reader::LogicalIndexReader::BatchLookupResult);
    for (const auto& result : results) {
        const auto& entry = result.entry;
        if (entry.term.capacity() > std::string().capacity()) {
            bytes += entry.term.capacity() + 1;
        }
        bytes += entry.frq_bytes.capacity() + entry.prx_bytes.capacity();
    }
    return bytes;
}

TEST(SniiQueryTermResolutionBatch, BudgetedResultsKeepFrontCodedKeysAfterStateDestruction) {
    ScopedEnv resident("SNII_DICT_RESIDENT_MAX", "1048576");
    MemoryFile file;
    auto terms = numbered_terms(64);
    for (std::string& term : terms) {
        term.insert(0, 4096, 'p');
    }
    assert_ok(write_index(&file, terms, 1048576));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    ASSERT_EQ(index.n_dict_blocks(), 1U);
    reader::DictBlockScanMemory memory;
    assert_ok(index.dict_block_scan_memory(0, &memory));
    index_query::MemoryBudget budget(1048576);
    counting.reset_counts();
    {
        reader::BatchLookupResults results(budget);
        {
            reader::LogicalIndexReader::BatchLookupState state;
            assert_ok(index.prepare_lookup_batch(terms, &results, &state));
            EXPECT_TRUE(state.done());
        }
        ASSERT_EQ(results.results().size(), terms.size());
        EXPECT_EQ(results.results().back().entry.term, terms.back());
        EXPECT_GT(retained_result_bytes(results), memory.entries_bytes);
        EXPECT_EQ(budget.used_bytes(), retained_result_bytes(results));
        EXPECT_LE(budget.peak_bytes(), budget.limit_bytes());
        EXPECT_EQ(counting.rounds(), 0U);
    }
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(SniiQueryTermResolutionBatch, BudgetedResultsKeepInlinePayloadsAfterWaveDestruction) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    const auto indexed_terms = numbered_terms(65);
    std::vector<std::string> terms;
    for (size_t i = 0; i < indexed_terms.size(); i += 2) {
        terms.push_back(indexed_terms[i]);
    }
    assert_ok(write_index(&file, indexed_terms, 1));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    index_query::MemoryBudget budget(1048576);
    counting.reset_counts();
    {
        reader::BatchLookupResults results(budget);
        {
            reader::LogicalIndexReader::BatchLookupState state;
            assert_ok(index.prepare_lookup_batch(terms, &results, &state));
            io::BatchRangeFetcher fetcher(&counting, 0, &budget);
            while (!state.done()) {
                assert_ok(index.prepare_lookup_wave(&state, &fetcher));
                assert_ok(fetcher.fetch());
                assert_ok(index.consume_lookup_wave(&state, fetcher));
                fetcher.clear();
            }
        }
        EXPECT_EQ(counting.read_batch_calls(), 3U);
        uint64_t payload_bytes = 0;
        for (const auto& result : results.results()) {
            EXPECT_TRUE(result.found);
            payload_bytes += result.entry.frq_bytes.size() + result.entry.prx_bytes.size();
        }
        EXPECT_GT(payload_bytes, 0U);
        EXPECT_EQ(results.results().back().entry.term, terms.back());
        EXPECT_EQ(budget.used_bytes(), retained_result_bytes(results));
        EXPECT_GT(budget.used_bytes(),
                  results.results().size() * sizeof(reader::LogicalIndexReader::BatchLookupResult));
    }
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(SniiQueryTermResolutionBatch, BudgetedResultsRetainPartialResultsOnDecodeFailure) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    MemoryFile file;
    const std::vector<std::string> terms {"alpha", "bravo", "omega"};
    assert_ok(write_index(&file, terms, 1));
    CorruptingReader corrupting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&corrupting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    ASSERT_EQ(index.n_dict_blocks(), terms.size());
    index_query::MemoryBudget budget(1048576);
    {
        reader::BatchLookupResults results(budget);
        {
            reader::LogicalIndexReader::BatchLookupState state;
            assert_ok(index.prepare_lookup_batch(terms, &results, &state));
            io::BatchRangeFetcher fetcher(&corrupting, 0, &budget);
            assert_ok(index.prepare_lookup_wave(&state, &fetcher));
            corrupting.corrupt = true;
            assert_ok(fetcher.fetch());
            EXPECT_TRUE(index.consume_lookup_wave(&state, fetcher)
                                .is<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED>());
            fetcher.clear();
        }
        EXPECT_TRUE(results.results().front().found);
        EXPECT_EQ(results.results().front().entry.term, terms.front());
        EXPECT_FALSE(results.results().back().found);
        EXPECT_EQ(budget.used_bytes(), retained_result_bytes(results));
    }
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(SniiQueryTermResolutionBatch, BudgetedResultsReleasePreviousStorageWhenReused) {
    ScopedEnv resident("SNII_DICT_RESIDENT_MAX", "1048576");
    MemoryFile file;
    const std::vector<std::string> terms {std::string(4096, 'a')};
    assert_ok(write_index(&file, terms, 8192));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    index_query::MemoryBudget budget(1048576);
    reader::BatchLookupResults results(budget);
    reader::LogicalIndexReader::BatchLookupState state;
    assert_ok(index.prepare_lookup_batch(terms, &results, &state));
    EXPECT_EQ(budget.used_bytes(), retained_result_bytes(results));
    const std::vector<std::string> empty;
    assert_ok(index.prepare_lookup_batch(empty, &results, &state));
    EXPECT_TRUE(state.done());
    EXPECT_TRUE(results.results().empty());
    EXPECT_EQ(budget.used_bytes(), 0U);
    assert_ok(index.prepare_lookup_batch(terms, &results, &state));
    EXPECT_TRUE(results.results().front().found);
    EXPECT_EQ(budget.used_bytes(), retained_result_bytes(results));
}

TEST(SniiQueryTermResolutionBatch, BudgetedResultsSkipDefinitelyAbsentKeysWithoutHeapAllowance) {
    ScopedEnv resident("SNII_DICT_RESIDENT_MAX", "1048576");
    MemoryFile file;
    assert_ok(write_index(&file, {"bravo"}, 8192));
    CountingReader counting(&file);
    reader::SniiSegmentReader segment;
    assert_ok(reader::SniiSegmentReader::open(&counting, &segment));
    reader::LogicalIndexReader index;
    assert_ok(segment.open_index(kIndexId, kIndexSuffix, &index));
    index_query::MemoryBudget budget(sizeof(reader::LogicalIndexReader::BatchLookupResult));
    reader::BatchLookupResults results(budget);
    reader::LogicalIndexReader::BatchLookupState state;
    const std::vector<std::string> terms {"alpha"};
    counting.reset_counts();
    assert_ok(index.prepare_lookup_batch(terms, &results, &state));
    EXPECT_TRUE(state.done());
    EXPECT_FALSE(results.results().front().found);
    EXPECT_EQ(budget.used_bytes(), retained_result_bytes(results));
    EXPECT_EQ(counting.rounds(), 0U);
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
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    assert_ok(internal::resolve_query_terms_batch(index_reader, terms, &resolved, &found));

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
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    assert_ok(internal::resolve_query_terms_batch(index_reader, terms, &resolved, &found));

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
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    assert_ok(internal::resolve_query_terms_batch(index_reader, terms, &resolved, &found));

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
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    assert_ok(internal::resolve_query_terms_batch(index_reader, terms, &resolved, &found));

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
    std::vector<internal::ResolvedQueryTerm> resolved;
    std::vector<uint8_t> found;
    assert_ok(internal::resolve_query_terms_batch(index_reader, query_terms, &resolved, &found));

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

TEST(SniiQueryTermResolutionBatch, SharedIoBatchAppliesRangeLimitsAcrossFiles) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    const auto indexed_terms = numbered_terms(33);
    std::vector<std::string> terms;
    for (size_t i = 0; i < indexed_terms.size(); i += 2) {
        terms.push_back(indexed_terms[i]);
    }
    std::array<OpenedDictionary, 2> fields;
    index_query::MemoryBudget budget(1048576);
    std::array<reader::BatchLookupResults, 2> results {reader::BatchLookupResults(budget),
                                                       reader::BatchLookupResults(budget)};
    std::array<reader::LogicalIndexReader::BatchLookupState, 2> states;
    for (size_t i = 0; i < fields.size(); ++i) {
        assert_ok(fields[i].open(indexed_terms, 1));
        assert_ok(fields[i].index.prepare_lookup_batch(terms, &results[i], &states[i]));
    }
    std::vector<size_t> wave_ranges;
    for (size_t wave_number = 0; wave_number < 3; ++wave_number) {
        index_query::IoBatch wave(budget, {.bytes = 1048576, .ranges = 16});
        const uint64_t before = fields[0].counting.ranges() + fields[1].counting.ranges();
        for (size_t i = 0; i < fields.size(); ++i) {
            if (!states[i].done()) {
                assert_ok(fields[i].index.prepare_lookup_wave(&states[i], &wave));
            }
        }
        EXPECT_EQ(fields[0].counting.ranges() + fields[1].counting.ranges(), before);
        assert_ok(wave.fetch());
        wave_ranges.push_back(fields[0].counting.ranges() + fields[1].counting.ranges() - before);
        for (size_t i = 0; i < fields.size(); ++i) {
            if (!states[i].done()) {
                assert_ok(fields[i].index.consume_lookup_wave(&states[i], wave));
            }
        }
    }
    EXPECT_EQ(wave_ranges, (std::vector<size_t> {16, 16, 2}));
    for (size_t i = 0; i < fields.size(); ++i) {
        EXPECT_TRUE(states[i].done());
        EXPECT_EQ(fields[i].counting.read_batch_calls(), 2U);
        expect_lookup_terms(results[i].results(), terms);
    }
    EXPECT_LE(budget.peak_bytes(), budget.limit_bytes());
}

TEST(SniiQueryTermResolutionBatch, SharedIoBatchDeduplicatesOneBlockAcrossStates) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    OpenedDictionary field;
    const std::vector<std::string> terms {std::string(4096, 'a')};
    assert_ok(field.open(terms, 8192));
    const uint64_t read_bytes = field.index.section_refs().dict_region.length;
    index_query::MemoryBudget budget(1048576);
    reader::BatchLookupResults first_results(budget);
    reader::BatchLookupResults second_results(budget);
    {
        reader::LogicalIndexReader::BatchLookupState first_state;
        reader::LogicalIndexReader::BatchLookupState second_state;
        assert_ok(field.index.prepare_lookup_batch(terms, &first_results, &first_state));
        assert_ok(field.index.prepare_lookup_batch(terms, &second_results, &second_state));
        const uint64_t result_slots = budget.used_bytes();
        index_query::IoBatch wave(budget, {.bytes = read_bytes, .ranges = 1});
        assert_ok(field.index.prepare_lookup_wave(&first_state, &wave));
        assert_ok(field.index.prepare_lookup_wave(&second_state, &wave));
        assert_ok(wave.fetch());
        EXPECT_EQ(budget.used_bytes(), result_slots + read_bytes);
        assert_ok(field.index.consume_lookup_wave(&first_state, wave));
        assert_ok(field.index.consume_lookup_wave(&second_state, wave));
        EXPECT_TRUE(first_state.done());
        EXPECT_TRUE(second_state.done());
        EXPECT_EQ(field.counting.ranges(), 1U);
        EXPECT_EQ(field.counting.bytes(), read_bytes);
        EXPECT_EQ(field.counting.rounds(), 1U);
    }
    expect_lookup_terms(first_results.results(), terms);
    expect_lookup_terms(second_results.results(), terms);
    EXPECT_GE(budget.used_bytes(), terms.front().size() * 2);
    EXPECT_LE(budget.peak_bytes(), budget.limit_bytes());
}

void check_shared_dictionary_byte_limit(bool oversized) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    OpenedDictionary first;
    OpenedDictionary second;
    const std::vector<std::string> terms {"alpha"};
    assert_ok(first.open(terms, 1));
    assert_ok(second.open(terms, 1));
    const uint64_t read_bytes = first.index.section_refs().dict_region.length;
    index_query::MemoryBudget budget(1048576);
    std::vector<reader::LogicalIndexReader::BatchLookupResult> first_results;
    std::vector<reader::LogicalIndexReader::BatchLookupResult> second_results;
    reader::LogicalIndexReader::BatchLookupState first_state;
    reader::LogicalIndexReader::BatchLookupState second_state;
    assert_ok(first.index.prepare_lookup_batch(terms, &first_results, &first_state));
    assert_ok(second.index.prepare_lookup_batch(terms, &second_results, &second_state));
    index_query::IoBatch wave(budget, {.bytes = oversized ? 1 : read_bytes, .ranges = 16});
    assert_ok(first.index.prepare_lookup_wave(&first_state, &wave));
    assert_ok(second.index.prepare_lookup_wave(&second_state, &wave));
    EXPECT_EQ(wave.pending(), 1U);
    assert_ok(wave.fetch());
    assert_ok(first.index.consume_lookup_wave(&first_state, wave));
    assert_ok(second.index.consume_lookup_wave(&second_state, wave));
    EXPECT_TRUE(first_state.done());
    EXPECT_FALSE(second_state.done());
    EXPECT_EQ(second.counting.rounds(), 0U);
    wave.clear();
    assert_ok(second.index.prepare_lookup_wave(&second_state, &wave));
    assert_ok(wave.fetch());
    assert_ok(second.index.consume_lookup_wave(&second_state, wave));
    EXPECT_TRUE(second_state.done());
    EXPECT_EQ(second.counting.rounds(), 1U);
    expect_lookup_terms(first_results, terms);
    expect_lookup_terms(second_results, terms);
    wave.clear();
    EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(SniiQueryTermResolutionBatch, SharedIoBatchAppliesByteLimitsAcrossFiles) {
    check_shared_dictionary_byte_limit(false);
}

TEST(SniiQueryTermResolutionBatch, SharedIoBatchLetsAnOversizedBlockMakeProgress) {
    check_shared_dictionary_byte_limit(true);
}

TEST(SniiQueryTermResolutionBatch, SharedIoBatchRejectsAnEmptyWaveWithNoRangeSlots) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    OpenedDictionary field;
    const std::vector<std::string> terms {"alpha"};
    assert_ok(field.open(terms, 1));
    index_query::MemoryBudget budget(1048576);
    reader::BatchLookupResults results(budget);
    reader::LogicalIndexReader::BatchLookupState state;
    assert_ok(field.index.prepare_lookup_batch(terms, &results, &state));
    index_query::IoBatch empty(budget, {.bytes = 1048576, .ranges = 0});
    const Status status = field.index.prepare_lookup_wave(&state, &empty);
    EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status.to_string();
    EXPECT_EQ(empty.pending(), 0U);
    EXPECT_EQ(field.counting.rounds(), 0U);
    index_query::IoBatch wave(budget, {.bytes = 1048576, .ranges = 16});
    assert_ok(field.index.prepare_lookup_wave(&state, &wave));
    assert_ok(wave.fetch());
    assert_ok(field.index.consume_lookup_wave(&state, wave));
    EXPECT_TRUE(state.done());
    expect_lookup_terms(results.results(), terms);
}

TEST(SniiQueryTermResolutionBatch, SharedIoBatchPreservesStatesAfterAReaderFailure) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    OpenedDictionary first;
    OpenedDictionary second;
    const std::vector<std::string> terms {"alpha"};
    assert_ok(first.open(terms, 1));
    assert_ok(second.open(terms, 1));
    index_query::MemoryBudget budget(1048576);
    reader::BatchLookupResults first_results(budget);
    reader::BatchLookupResults second_results(budget);
    reader::LogicalIndexReader::BatchLookupState first_state;
    reader::LogicalIndexReader::BatchLookupState second_state;
    assert_ok(first.index.prepare_lookup_batch(terms, &first_results, &first_state));
    assert_ok(second.index.prepare_lookup_batch(terms, &second_results, &second_state));
    const uint64_t slots = budget.used_bytes();
    index_query::IoBatch wave(budget, {.bytes = 1048576, .ranges = 16});
    assert_ok(first.index.prepare_lookup_wave(&first_state, &wave));
    assert_ok(second.index.prepare_lookup_wave(&second_state, &wave));
    second.counting.fail_batch(1);
    EXPECT_TRUE(wave.fetch().is<ErrorCode::IO_ERROR>());
    EXPECT_EQ(budget.used_bytes(), slots);
    EXPECT_EQ(wave.pending(), 2U);
    EXPECT_FALSE(first_results.results().front().found);
    second.counting.fail_batch(0);
    assert_ok(wave.fetch());
    assert_ok(first.index.consume_lookup_wave(&first_state, wave));
    assert_ok(second.index.consume_lookup_wave(&second_state, wave));
    EXPECT_TRUE(first_state.done());
    EXPECT_TRUE(second_state.done());
    expect_lookup_terms(first_results.results(), terms);
    expect_lookup_terms(second_results.results(), terms);
}

TEST(SniiQueryTermResolutionBatch, SharedIoBatchChargesDecodeAlongsideAllReadBuffers) {
    ScopedEnv on_demand("SNII_DICT_RESIDENT_MAX", "0");
    OpenedDictionary first;
    OpenedDictionary second;
    const std::vector<std::string> terms {"alpha"};
    assert_ok(first.open(terms, 1));
    assert_ok(second.open(terms, 1));
    const uint64_t read_bytes = first.index.section_refs().dict_region.length +
                                second.index.section_refs().dict_region.length;
    index_query::MemoryBudget budget(read_bytes);
    std::vector<reader::LogicalIndexReader::BatchLookupResult> first_results;
    std::vector<reader::LogicalIndexReader::BatchLookupResult> second_results;
    reader::LogicalIndexReader::BatchLookupState first_state;
    reader::LogicalIndexReader::BatchLookupState second_state;
    assert_ok(first.index.prepare_lookup_batch(terms, &first_results, &first_state));
    assert_ok(second.index.prepare_lookup_batch(terms, &second_results, &second_state));
    index_query::IoBatch wave(budget, {.bytes = read_bytes, .ranges = 16});
    assert_ok(first.index.prepare_lookup_wave(&first_state, &wave));
    assert_ok(second.index.prepare_lookup_wave(&second_state, &wave));
    assert_ok(wave.fetch());
    EXPECT_EQ(budget.used_bytes(), read_bytes);
    const Status status = first.index.consume_lookup_wave(&first_state, wave);
    EXPECT_TRUE(status.is<ErrorCode::MEM_LIMIT_EXCEEDED>()) << status.to_string();
    EXPECT_FALSE(first_results.front().found);
    EXPECT_EQ(first.counting.rounds(), 1U);
    EXPECT_EQ(second.counting.rounds(), 1U);
    EXPECT_EQ(budget.used_bytes(), read_bytes);
    wave.clear();
    EXPECT_EQ(budget.used_bytes(), 0U);
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

// Scoring resolves its distinct terms together, and a repeated term still scores once per clause.
TEST_F(SniiTermResolutionIoTest, ScoringReadsItsColdDictionaryBlocksInOneRound) {
    open_index();
    stats::SniiStatsProvider segment_stats;
    assert_ok(stats::SniiStatsProvider::open(&_index, &segment_stats));
    const std::vector<CollectionScoringTerm> clauses = {{.physical_term = "alpha", .idf = 0.5},
                                                        {.physical_term = "charlie", .idf = 1.5},
                                                        {.physical_term = "alpha", .idf = 0.5},
                                                        {.physical_term = "delta", .idf = 2.5}};
    roaring::Roaring candidates;
    candidates.addRange(0, kAllDocs.size());
    constexpr double kCollectionAvgdl = 4.0;
    _counter.reset_counts();
    std::vector<ScoredDoc> scored;
    assert_ok(scoring_query_candidates(_index, segment_stats, clauses, candidates, kCollectionAvgdl,
                                       Bm25Params {}, &scored));

    double expected = 0.0;
    for (const CollectionScoringTerm& clause : clauses) {
        expected += ScorerContext::from_idf(clause.idf)
                            .score(1, encode_norm(4), kCollectionAvgdl, Bm25Params {});
    }
    ASSERT_EQ(scored.size(), kAllDocs.size());
    for (size_t i = 0; i < scored.size(); ++i) {
        EXPECT_EQ(scored[i].docid, kAllDocs[i]);
        EXPECT_DOUBLE_EQ(scored[i].score, expected);
    }
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
