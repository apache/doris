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
#include <cstdint>
#include <roaring/roaring.hh>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/io/file_reader.h"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/query/boolean_query.h"
#include "storage/index/snii/query/phrase_query.h"
#include "storage/index/snii/query/prefix_query.h"
#include "storage/index/snii/query/scoring_query.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::query {
namespace {

using snii_test::MemoryFile;
using snii_test::ScopedEnv;
using snii_test::assert_ok;
using snii_test::make_term;

constexpr uint64_t kIndexId = 1;
constexpr const char* kIndexSuffix = "body";

// Counts the reads that touch the dictionary: one read_at or read_batch call is one serial round,
// and each range it asks inside the dictionary is one request (adjacent blocks read together).
class DictionaryReadCounter final : public io::FileReader {
public:
    explicit DictionaryReadCounter(io::FileReader* inner) : inner_(inner) {}

    Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) override {
        count({{.offset = offset, .len = len}});
        return inner_->read_at(offset, len, out);
    }

    Status read_batch(const std::vector<io::Range>& ranges,
                      std::vector<std::vector<uint8_t>>* outs) override {
        count(ranges);
        return inner_->read_batch(ranges, outs);
    }

    uint64_t size() const override { return inner_->size(); }

    void watch(uint64_t offset, uint64_t length) {
        _begin = offset;
        _end = offset + length;
        reset();
    }

    void reset() {
        _rounds = 0;
        _ranges = 0;
    }

    uint64_t rounds() const { return _rounds; }
    uint64_t ranges() const { return _ranges; }

private:
    void count(const std::vector<io::Range>& ranges) {
        const auto requests = std::ranges::count_if(ranges, [this](const io::Range& range) {
            return range.offset >= _begin && range.offset < _end;
        });
        if (requests > 0) {
            ++_rounds;
            _ranges += static_cast<uint64_t>(requests);
        }
    }

    io::FileReader* inner_;
    uint64_t _begin = 0;
    uint64_t _end = 0;
    uint64_t _rounds = 0;
    uint64_t _ranges = 0;
};

// Four terms in documents 0-3, each at its own position so "alpha bravo charlie delta" is a
// phrase of every document, written with one dictionary block per term and opened with the
// dictionary on demand.
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
        const auto& dictionary = _index.section_refs().dict_region;
        _counter.watch(dictionary.offset, dictionary.length);
    }

    ScopedEnv _dictionary_on_demand {"SNII_DICT_RESIDENT_MAX", "0"};
    MemoryFile _file;
    DictionaryReadCounter _counter {&_file};
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
    _counter.reset();

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
    _counter.reset();
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

} // namespace
} // namespace doris::snii::query
