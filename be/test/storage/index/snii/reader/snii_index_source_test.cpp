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

#include "storage/index/snii/reader/snii_index_source.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "storage/index/query/exec/block_doc_set.h"
#include "storage/index/query/term_pattern.h"
#include "storage/index/snii/format/phrase_bigram.h"
#include "storage/index/snii/io/metered_file_reader.h"
#include "storage/index/snii/query/internal/docid_posting_reader.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"

namespace doris::snii::reader {
namespace {

using snii_test::assert_ok;
using snii_test::make_term;
using snii_test::MemoryFile;

class SniiIndexSourceTest : public ::testing::Test {
protected:
    void SetUp() override {
        assert_ok(snii_test::build_reader(&_file, &_segment, &_index));
        _source = std::make_unique<SniiIndexSource>(_index);
    }

    std::vector<uint32_t> oracle(const std::string& term) const {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        EXPECT_TRUE(_index.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
        EXPECT_TRUE(found) << term;
        std::vector<uint32_t> docids;
        EXPECT_TRUE(query::internal::read_docid_posting(_index, entry, frq_base, prx_base, &docids)
                            .ok());
        return docids;
    }

    std::unique_ptr<index_query::PostingsCursor> open(const std::string& term, bool positions,
                                                      bool scoring) const {
        std::unique_ptr<index_query::PostingsCursor> cursor;
        EXPECT_TRUE(_source->open_term(term, positions, scoring, &cursor).ok());
        return cursor;
    }

    std::vector<uint32_t> list(const std::string& term) const {
        auto cursor = open(term, false, false);
        EXPECT_NE(cursor, nullptr) << term;
        std::vector<uint32_t> docs;
        index_query::BlockDocSet set(*cursor);
        while (!set.exhausted()) {
            docs.push_back(set.doc());
            set.advance();
        }
        return docs;
    }

    std::vector<std::string> expand(index_query::TermPatternKind kind, const std::string& text,
                                    int32_t max_expansions) const {
        index_query::TermPattern pattern;
        EXPECT_TRUE(index_query::TermPattern::create(kind, text, &pattern).ok());
        std::vector<std::string> terms;
        EXPECT_TRUE(_source->expand_terms(pattern, max_expansions, &terms).ok());
        return terms;
    }

    MemoryFile _file;
    SniiSegmentReader _segment;
    LogicalIndexReader _index;
    std::unique_ptr<SniiIndexSource> _source;
};

TEST_F(SniiIndexSourceTest, CountsTheDocuments) {
    EXPECT_EQ(_source->doc_count(), 9000U);
    EXPECT_TRUE(_source->segments().empty());
    EXPECT_TRUE(_source->is_live(0));
}

TEST_F(SniiIndexSourceTest, OpensPresentTermsAndNotAbsentOnes) {
    for (const char* name : {"needle", "123", "failed", "sparse_left"}) {
        EXPECT_EQ(list(name), oracle(name)) << name;
    }
    EXPECT_EQ(open("absent", false, false), nullptr);
}

TEST_F(SniiIndexSourceTest, PreparedTermsOpenLikeUnpreparedOnes) {
    const std::vector<std::string> terms = {"needle", "absent", "failed", "needle"};
    assert_ok(_source->prepare_terms(terms));
    EXPECT_EQ(list("needle"), oracle("needle"));
    EXPECT_EQ(list("failed"), oracle("failed"));
    EXPECT_EQ(open("absent", false, false), nullptr);
    EXPECT_EQ(list("sparse_right"), oracle("sparse_right"));
}

TEST_F(SniiIndexSourceTest, ExpandsTermsInDictionaryOrder) {
    using index_query::TermPatternKind;
    EXPECT_EQ(expand(TermPatternKind::kPrefix, "ord", 0),
              (std::vector<std::string> {"order", "ordinal"}));
    EXPECT_EQ(expand(TermPatternKind::kPrefix, "ord", 1), (std::vector<std::string> {"order"}));
    EXPECT_EQ(expand(TermPatternKind::kSuffix, "ed", 0), (std::vector<std::string> {"failed"}));
    EXPECT_EQ(expand(TermPatternKind::kContains, "ar", 0),
              (std::vector<std::string> {"sparse_left", "sparse_right"}));
    EXPECT_TRUE(expand(TermPatternKind::kPrefix, "zzz", 0).empty());
}

TEST_F(SniiIndexSourceTest, ScoredPostingsCarryFrequenciesAndPositions) {
    auto cursor = open("repeat", true, true);
    ASSERT_NE(cursor, nullptr);
    index_query::PostingsBlock block;
    bool eof = false;
    assert_ok(cursor->next_block(&block, &eof));
    ASSERT_FALSE(eof);
    ASSERT_EQ(block.freqs.size(), block.size());
    // The corpus has no norms section, so every norm reads as 1.
    EXPECT_TRUE(block.norms.empty());
    EXPECT_EQ(block.freq_at(0), 3U);
    EXPECT_EQ(block.norm_at(0), 1U);
    index_query::PositionCursor* positions = nullptr;
    assert_ok(cursor->open_positions(0, &positions));
    std::vector<uint32_t> out;
    assert_ok(positions->append_remaining_positions(0, out));
    EXPECT_EQ(out, (std::vector<uint32_t> {0, 1, 2}));
}

// A term or an expansion that reaches the dictionary's internal phrase-bigram namespace bypasses
// the index, as the SNII format requires.
TEST_F(SniiIndexSourceTest, TermsReachingTheInternalNamespaceBypass) {
    const std::string internal = std::string(format::kPhraseBigramTermMarker) + "retry attempt";
    std::unique_ptr<index_query::PostingsCursor> cursor;
    EXPECT_TRUE(_source->open_term(internal, false, false, &cursor)
                        .is<ErrorCode::INVERTED_INDEX_BYPASS>());
    const std::vector<std::string> terms = {"needle", internal};
    EXPECT_TRUE(_source->prepare_terms(terms).is<ErrorCode::INVERTED_INDEX_BYPASS>());
    std::vector<std::unique_ptr<index_query::PostingsCursor>> cursors;
    EXPECT_TRUE(_source->open_terms(terms, false, false, &cursors)
                        .is<ErrorCode::INVERTED_INDEX_BYPASS>());
    index_query::TermPattern pattern;
    assert_ok(index_query::TermPattern::create(index_query::TermPatternKind::kPrefix,
                                               std::string(format::kPhraseBigramTermMarker, 0, 4),
                                               &pattern));
    std::vector<std::string> expanded;
    EXPECT_TRUE(
            _source->expand_terms(pattern, 0, &expanded).is<ErrorCode::INVERTED_INDEX_BYPASS>());
    // This corpus holds no internal term, so an expansion from the start runs.
    using index_query::TermPatternKind;
    EXPECT_EQ(expand(TermPatternKind::kContains, "ar", 0),
              (std::vector<std::string> {"sparse_left", "sparse_right"}));
}

// An older image may hold internal terms: an expansion enumerating from the dictionary's start
// then bypasses, and one with a user prefix still runs.
TEST_F(SniiIndexSourceTest, AnExpansionFromTheStartBypassesAnIndexWithInternalTerms) {
    MemoryFile file;
    writer::SniiIndexInput input;
    input.index_id = 5;
    input.index_suffix = "legacy";
    input.config = format::IndexConfig::kDocsPositions;
    input.doc_count = 10;
    input.terms = {
            make_term(std::string(format::kPhraseBigramTermMarker) + "alpha beta",
                      {{.docid = 1, .positions = {0}}}),
            make_term("alpha", {{.docid = 1, .positions = {0}}, {.docid = 4, .positions = {0}}}),
            make_term("beta", {{.docid = 1, .positions = {1}}})};
    writer::SniiCompoundWriter compound_writer(&file);
    assert_ok(compound_writer.add_logical_index(input));
    assert_ok(compound_writer.finish());
    SniiSegmentReader segment;
    LogicalIndexReader index;
    assert_ok(SniiSegmentReader::open(&file, &segment));
    assert_ok(segment.open_index(5, "legacy", &index));
    SniiIndexSource source(index);

    std::vector<std::string> terms;
    index_query::TermPattern contains;
    assert_ok(index_query::TermPattern::create(index_query::TermPatternKind::kContains, "a",
                                               &contains));
    EXPECT_TRUE(source.expand_terms(contains, 0, &terms).is<ErrorCode::INVERTED_INDEX_BYPASS>());
    index_query::TermPattern prefix;
    assert_ok(
            index_query::TermPattern::create(index_query::TermPatternKind::kPrefix, "al", &prefix));
    assert_ok(source.expand_terms(prefix, 0, &terms));
    EXPECT_EQ(terms, (std::vector<std::string> {"alpha"}));
    std::unique_ptr<index_query::PostingsCursor> cursor;
    assert_ok(source.open_term("beta", false, false, &cursor));
    EXPECT_NE(cursor, nullptr);
}

TEST_F(SniiIndexSourceTest, PositionsNeedAPositionedIndex) {
    MemoryFile file;
    writer::SniiIndexInput input;
    input.index_id = 3;
    input.index_suffix = "tag";
    input.config = format::IndexConfig::kDocsOnly;
    input.doc_count = 10;
    input.terms = {
            make_term("only", {{.docid = 2, .positions = {0}}, {.docid = 7, .positions = {0}}})};
    writer::SniiCompoundWriter compound_writer(&file);
    assert_ok(compound_writer.add_logical_index(input));
    assert_ok(compound_writer.finish());
    SniiSegmentReader segment;
    LogicalIndexReader index;
    assert_ok(SniiSegmentReader::open(&file, &segment));
    assert_ok(segment.open_index(3, "tag", &index));
    SniiIndexSource source(index);

    std::unique_ptr<index_query::PostingsCursor> cursor;
    EXPECT_TRUE(source.open_term("only", /*positions=*/true, false, &cursor)
                        .is<ErrorCode::NOT_IMPLEMENTED_ERROR>());
    assert_ok(source.open_term("only", false, false, &cursor));
    ASSERT_NE(cursor, nullptr);
    index_query::BlockDocSet set(*cursor);
    EXPECT_EQ(set.doc(), 2U);
    EXPECT_TRUE(set.advance());
    EXPECT_EQ(set.doc(), 7U);
    EXPECT_FALSE(set.advance());
}

// The corpus read through a metered reader, so the rounds of every open count.
class SniiIndexSourceRoundsTest : public ::testing::Test {
protected:
    void SetUp() override {
        SniiSegmentReader written;
        LogicalIndexReader written_index;
        assert_ok(snii_test::build_reader(&_file, &written, &written_index));
        assert_ok(SniiSegmentReader::open(&_metered, &_segment));
        assert_ok(_segment.open_index(7, "Body", &_index));
        _source = std::make_unique<SniiIndexSource>(_index);
    }

    uint64_t rounds() const { return _metered.metrics().serial_rounds; }

    std::vector<uint32_t> list(index_query::PostingsCursor& cursor) const {
        std::vector<uint32_t> docs;
        index_query::BlockDocSet set(cursor);
        while (!set.exhausted()) {
            docs.push_back(set.doc());
            set.advance();
        }
        return docs;
    }

    std::vector<uint32_t> oracle(const std::string& term) const {
        bool found = false;
        format::DictEntry entry;
        uint64_t frq_base = 0;
        uint64_t prx_base = 0;
        EXPECT_TRUE(_index.lookup(term, &found, &entry, &frq_base, &prx_base).ok());
        EXPECT_TRUE(found) << term;
        std::vector<uint32_t> docids;
        EXPECT_TRUE(query::internal::read_docid_posting(_index, entry, frq_base, prx_base, &docids)
                            .ok());
        return docids;
    }

    MemoryFile _file;
    io::MeteredFileReader _metered {&_file, /*block_size=*/256};
    SniiSegmentReader _segment;
    LogicalIndexReader _index;
    std::unique_ptr<SniiIndexSource> _source;
};

TEST_F(SniiIndexSourceRoundsTest, TermsOpenedTogetherReadInSharedRounds) {
    const std::vector<std::string> terms = {"sparse_left", "absent", "sparse_right", "failed"};
    std::vector<std::unique_ptr<index_query::PostingsCursor>> cursors;
    assert_ok(_source->open_terms(terms, /*positions=*/false, /*scoring=*/false, &cursors));
    ASSERT_EQ(cursors.size(), 4U);
    EXPECT_EQ(cursors[1], nullptr);
    const uint64_t opened = rounds();
    // The first cursor needing its prelude reads every opened term's in one round; then every
    // span registers on the wave and one round reads them all.
    for (const auto& cursor : cursors) {
        if (cursor != nullptr) {
            assert_ok(cursor->prefetch(nullptr, /*positions=*/false));
        }
    }
    EXPECT_EQ(rounds(), opened + 1);
    assert_ok(_source->fetch_pending());
    EXPECT_EQ(rounds(), opened + 2);
    EXPECT_EQ(list(*cursors[0]), oracle("sparse_left"));
    EXPECT_EQ(list(*cursors[2]), oracle("sparse_right"));
    EXPECT_EQ(list(*cursors[3]), oracle("failed"));
    EXPECT_EQ(rounds(), opened + 2);
    EXPECT_EQ(_source->wave_rounds(), 2U);
}

// Opening terms reads no prelude, so a caller that finds one of them missing and gives up reads
// nothing past the dictionary, even when a later fetch serves another caller.
TEST_F(SniiIndexSourceRoundsTest, OpeningTermsReadsNoPrelude) {
    const std::vector<std::string> terms = {"failed", "absent"};
    std::vector<std::unique_ptr<index_query::PostingsCursor>> cursors;
    assert_ok(_source->open_terms(terms, /*positions=*/false, /*scoring=*/false, &cursors));
    ASSERT_NE(cursors[0], nullptr);
    EXPECT_EQ(cursors[1], nullptr);
    const uint64_t resolved = rounds();
    cursors.clear();
    assert_ok(_source->fetch_pending());
    EXPECT_EQ(rounds(), resolved);
    EXPECT_EQ(_source->wave_rounds(), 0U);
}

// Whether the index may hold a term is answered without reading the dictionary.
TEST_F(SniiIndexSourceRoundsTest, MayHoldReadsNoDictionary) {
    const uint64_t before = rounds();
    bool held = false;
    assert_ok(_source->may_hold("failed", &held));
    EXPECT_TRUE(held);
    assert_ok(_source->may_hold("absent", &held));
    EXPECT_FALSE(held);
    EXPECT_EQ(rounds(), before);
    const std::string internal = std::string(format::kPhraseBigramTermMarker) + "a b";
    EXPECT_TRUE(_source->may_hold(internal, &held).is<ErrorCode::INVERTED_INDEX_BYPASS>());
}

TEST_F(SniiIndexSourceRoundsTest, ALaterOpenOfATermStartsFromItsPrelude) {
    const std::vector<std::string> terms = {"sparse_left", "failed"};
    std::vector<std::unique_ptr<index_query::PostingsCursor>> cursors;
    assert_ok(_source->open_terms(terms, false, false, &cursors));
    // Listing one cursor reads the preludes of every term opened with it.
    EXPECT_EQ(list(*cursors[1]), oracle("failed"));
    const uint64_t opened = rounds();
    // Neither the dictionary nor the prelude is read again: only the span, in one round.
    std::unique_ptr<index_query::PostingsCursor> again;
    assert_ok(_source->open_term("sparse_left", false, false, &again));
    ASSERT_NE(again, nullptr);
    EXPECT_EQ(rounds(), opened);
    EXPECT_EQ(list(*again), oracle("sparse_left"));
    EXPECT_EQ(rounds(), opened + 1);
    std::vector<std::unique_ptr<index_query::PostingsCursor>> together;
    assert_ok(_source->open_terms(terms, false, false, &together));
    EXPECT_EQ(rounds(), opened + 1);
}

TEST_F(SniiIndexSourceRoundsTest, ExpandedTermsOpenWithoutALookup) {
    index_query::TermPattern pattern;
    assert_ok(index_query::TermPattern::create(index_query::TermPatternKind::kPrefix, "sparse",
                                               &pattern));
    std::vector<std::string> terms;
    assert_ok(_source->expand_terms(pattern, 0, &terms));
    ASSERT_EQ(terms, (std::vector<std::string> {"sparse_left", "sparse_right"}));
    const uint64_t expanded = rounds();
    std::vector<std::unique_ptr<index_query::PostingsCursor>> cursors;
    assert_ok(_source->open_terms(terms, false, false, &cursors));
    // The dictionary answered the terms while enumerating, and their preludes wait for a cursor
    // to need one.
    EXPECT_EQ(rounds(), expanded);
    EXPECT_EQ(list(*cursors[0]), oracle("sparse_left"));
    EXPECT_EQ(list(*cursors[1]), oracle("sparse_right"));
}

} // namespace
} // namespace doris::snii::reader
