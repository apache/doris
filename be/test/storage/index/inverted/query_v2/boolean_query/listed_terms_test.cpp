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

#include "storage/index/inverted/query_v2/boolean_query/listed_terms.h"

#include <gtest/gtest.h>

#include <functional>
#include <map>
#include <memory>
#include <roaring/roaring.hh>
#include <string>
#include <utility>
#include <vector>

#include "storage/index/index_iterator.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query_v2/boolean_query/operator.h"
#include "storage/index/inverted/query_v2/null_bitmap_fetcher.h"
#include "storage/index/inverted/query_v2/scorer.h"
#include "storage/index/inverted/query_v2/term_query/term_query.h"
#include "storage/index/inverted/similarity/collection_statistics.h"
#include "storage/index/query/boolean/truth_set.h"
#include "storage/index/query/fake_index_source.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {

using index_query::testing::FakeIndexSource;

const std::wstring kField = L"body";

class FieldNullIterator final : public segment_v2::IndexIterator {
public:
    explicit FieldNullIterator(roaring::Roaring nulls)
            : _nulls(std::move(nulls)), _cache(1024 * 1024, 1) {}

    segment_v2::IndexReaderPtr get_reader(segment_v2::IndexReaderType /*type*/) const override {
        return nullptr;
    }
    Status read_from_index(const segment_v2::IndexParam& /*param*/) override {
        return Status::OK();
    }
    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* handle) override {
        _cache.insert(_key, std::make_shared<roaring::Roaring>(_nulls), handle);
        return Status::OK();
    }
    Result<bool> has_null() override { return !_nulls.isEmpty(); }

private:
    roaring::Roaring _nulls;
    segment_v2::InvertedIndexQueryCache _cache;
    segment_v2::InvertedIndexQueryCache::CacheKey _key {
            .index_path = "listed_terms",
            .column_name = "body",
            .query_type = segment_v2::InvertedIndexQueryType::UNKNOWN_QUERY,
            .value = "nulls"};
};

class FieldNullResolver final : public NullBitmapResolver {
public:
    explicit FieldNullResolver(segment_v2::IndexIterator* iterator) : _iterator(iterator) {}
    segment_v2::IndexIterator* iterator_for(const Scorer& /*scorer*/,
                                            const std::string& logical_field) const override {
        return logical_field == "body" ? _iterator : nullptr;
    }

private:
    segment_v2::IndexIterator* _iterator;
};

// Statistics for scoring: idf 1, 2 and 3 for "a", "b" and any other term, an average length of 4.
class FixedCollectionStatistics final : public CollectionStatistics {
public:
    float get_or_calculate_idf(const std::wstring& /*field_name*/,
                               const std::wstring& term) override {
        static const std::map<std::wstring, float> idfs {{L"a", 1.0F}, {L"b", 2.0F}};
        const auto it = idfs.find(term);
        return it == idfs.end() ? 3.0F : it->second;
    }
    float get_or_calculate_avg_dl(const std::wstring& /*field_name*/) override { return 4.0F; }
};

FakeIndexSource::Posting posting(uint32_t doc, std::vector<uint32_t> positions) {
    return {.doc = doc, .positions = std::move(positions)};
}

// The corpus: "a" holds 1 2 3 5 8, "b" 2 3 8 9 and "c" 3 8 20, with one to three positions
// each; the norm of a row is its number modulo 5, plus one; rows 40 and 41 are NULL.
class ListedTermsTest : public ::testing::Test {
protected:
    using MakeQuery = std::function<QueryPtr()>;

    void SetUp() override {
        _context->collection_statistics = std::make_shared<FixedCollectionStatistics>();
    }

    std::shared_ptr<FakeIndexSource> source(bool batches) const {
        auto source = std::make_shared<FakeIndexSource>();
        source->batches = batches;
        source->set_doc_count(64);
        source->add("a", {posting(1, {0}), posting(2, {0, 3}), posting(3, {1}),
                          posting(5, {2, 4, 6}), posting(8, {0})});
        source->add("b",
                    {posting(2, {1}), posting(3, {0, 2}), posting(8, {1, 5}), posting(9, {0})});
        source->add("c", {posting(3, {3}), posting(8, {2}), posting(20, {0, 1})});
        for (uint32_t doc = 0; doc < 64; ++doc) {
            source->norms[doc] = doc % 5 + 1;
        }
        return source;
    }

    // The rows of the scored query on one source, each with its score.
    std::map<uint32_t, float> scored(const MakeQuery& make_query,
                                     const std::shared_ptr<FakeIndexSource>& source) const {
        QueryExecutionContext exec_ctx;
        exec_ctx.segment_num_rows = source->doc_count();
        exec_ctx.sources = {source};
        exec_ctx.field_sources.emplace(kField, source);
        exec_ctx.null_resolver = &_resolver;
        auto scorer = make_query()->weight(true)->scorer(exec_ctx);
        std::map<uint32_t, float> rows;
        for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
            rows[doc] = scorer->score();
        }
        return rows;
    }

    static std::vector<uint32_t> keys(const std::map<uint32_t, float>& rows) {
        std::vector<uint32_t> keys;
        for (const auto& [doc, score] : rows) {
            keys.push_back(doc);
        }
        return keys;
    }

    static void expect_scores_eq(const std::map<uint32_t, float>& actual,
                                 const std::map<uint32_t, float>& expected) {
        ASSERT_EQ(keys(actual), keys(expected));
        for (const auto& [doc, score] : expected) {
            EXPECT_FLOAT_EQ(actual.at(doc), score) << doc;
        }
    }

    QueryPtr term(const std::string& text) const {
        return std::make_shared<TermQuery>(_context, kField, text);
    }

    QueryPtr boolean(OperatorType op, const std::vector<QueryPtr>& clauses) const {
        auto builder = create_operator_boolean_query_builder(op);
        for (const auto& clause : clauses) {
            builder->add(clause);
        }
        return builder->build();
    }

    // The TRUE and UNKNOWN rows of the unscored query on one source. A query yields one
    // weight, so every evaluation builds its own.
    index_query::TruthSet evaluate(const MakeQuery& make_query,
                                   const std::shared_ptr<FakeIndexSource>& source) const {
        QueryExecutionContext exec_ctx;
        exec_ctx.segment_num_rows = source->doc_count();
        exec_ctx.sources = {source};
        exec_ctx.field_sources.emplace(kField, source);
        exec_ctx.null_resolver = &_resolver;
        auto scorer = make_query()->weight(false)->scorer(exec_ctx);
        index_query::TruthSet rows;
        for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
            rows.true_rows.add(doc);
        }
        if (const auto* nulls = scorer->get_null_bitmap(&_resolver); nulls != nullptr) {
            rows.null_rows = *nulls;
        }
        return rows;
    }

    // Both sources answer the query alike; the batching source is returned. A streaming source
    // `lists` its terms together only for an unscored conjunction it may answer.
    std::shared_ptr<FakeIndexSource> expect_alike(const MakeQuery& make_query,
                                                  const roaring::Roaring& true_rows,
                                                  const roaring::Roaring& null_rows,
                                                  bool lists = false) const {
        auto streamed = source(false);
        const auto streamed_rows = evaluate(make_query, streamed);
        EXPECT_EQ(streamed_rows.true_rows, true_rows);
        EXPECT_EQ(streamed_rows.null_rows, null_rows);
        EXPECT_EQ(streamed->opened_together.empty(), !lists);
        auto listed = source(true);
        const auto listed_rows = evaluate(make_query, listed);
        EXPECT_EQ(listed_rows.true_rows, true_rows);
        EXPECT_EQ(listed_rows.null_rows, null_rows);
        EXPECT_TRUE(listed->opened.empty());
        return listed;
    }

    IndexQueryContextPtr _context = std::make_shared<IndexQueryContext>();
    FieldNullIterator _iterator {roaring::Roaring::bitmapOf(2, 40U, 41U)};
    FieldNullResolver _resolver {&_iterator};
};

TEST_F(ListedTermsTest, AGroupListsWithTheFieldsNullRows) {
    auto nulls = FieldNullBitmapFetcher::fetch(&_resolver, "body");
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(*nulls, roaring::Roaring::bitmapOf(2, 40U, 41U));
    auto listed = source(true);
    ListedTerms group(listed, nulls);
    group.add(0, "a");
    group.add(2, "b");
    EXPECT_TRUE(group.holds(2));
    EXPECT_FALSE(group.holds(1));
    group.open(/*conjunctive=*/true);
    EXPECT_FALSE(group.has_absent_term());
    EXPECT_EQ(group.cheapest_doc_freq(), 4U);
    const auto rows = group.conjunction(nullptr);
    EXPECT_EQ(rows.true_rows, roaring::Roaring::bitmapOf(3, 2U, 3U, 8U));
    EXPECT_EQ(rows.null_rows, roaring::Roaring::bitmapOf(2, 40U, 41U));
    ListedTerms any(listed, nulls);
    any.add(0, "a");
    any.add(1, "c");
    any.open(/*conjunctive=*/false);
    const auto union_rows = any.disjunction();
    EXPECT_EQ(union_rows.true_rows, roaring::Roaring::bitmapOf(6, 1U, 2U, 3U, 5U, 8U, 20U));
    EXPECT_EQ(union_rows.null_rows, roaring::Roaring::bitmapOf(2, 40U, 41U));
}

// A conjunction holding a term the source surely lacks opens none of its terms.
TEST_F(ListedTermsTest, AConjunctionWithAnAbsentTermOpensNothing) {
    auto listed = source(true);
    ListedTerms group(listed, nullptr);
    group.add(0, "a");
    group.add(1, "absent");
    group.open(/*conjunctive=*/true);
    EXPECT_TRUE(group.has_absent_term());
    EXPECT_TRUE(group.conjunction(nullptr).true_rows.isEmpty());
    EXPECT_TRUE(listed->opened_together.empty());
}

TEST_F(ListedTermsTest, AConjunctionChainsItsTermsFromTheCheapest) {
    auto listed = expect_alike(
            [&] {
                return boolean(OperatorType::OP_AND, {term("a"), term("b"), term("c")});
            },
            roaring::Roaring::bitmapOf(2, 3U, 8U), roaring::Roaring::bitmapOf(2, 40U, 41U),
            /*lists=*/true);
    EXPECT_EQ(listed->opened_together, (std::vector<std::vector<std::string>> {{"a", "b", "c"}}));
    ASSERT_EQ(listed->prefetches["c"].size(), 1U);
    EXPECT_TRUE(listed->prefetches["c"][0].whole);
    ASSERT_EQ(listed->prefetches["b"].size(), 1U);
    EXPECT_EQ(listed->prefetches["b"][0].candidates, (std::vector<uint32_t> {3, 8, 20}));
    ASSERT_EQ(listed->prefetches["a"].size(), 1U);
    EXPECT_EQ(listed->prefetches["a"][0].candidates, (std::vector<uint32_t> {3, 8}));
    EXPECT_EQ(listed->fetches, 0U);
}

TEST_F(ListedTermsTest, ADisjunctionReadsItsTermsInOneRound) {
    auto listed = expect_alike(
            [&] {
                return boolean(OperatorType::OP_OR, {term("a"), term("c")});
            },
            roaring::Roaring::bitmapOf(6, 1U, 2U, 3U, 5U, 8U, 20U),
            roaring::Roaring::bitmapOf(2, 40U, 41U));
    EXPECT_EQ(listed->opened_together, (std::vector<std::vector<std::string>> {{"a", "c"}}));
    EXPECT_TRUE(listed->prefetches["a"][0].whole);
    EXPECT_TRUE(listed->prefetches["c"][0].whole);
    EXPECT_EQ(listed->fetches, 1U);
}

TEST_F(ListedTermsTest, ANegationListsItsTermsBeforeNegating) {
    roaring::Roaring expected;
    expected.addRange(0, 64);
    expected -= roaring::Roaring::bitmapOf(7, 2U, 3U, 8U, 9U, 20U, 40U, 41U);
    expect_alike(
            [&] {
                return boolean(OperatorType::OP_NOT, {term("b"), term("c")});
            },
            expected, roaring::Roaring::bitmapOf(2, 40U, 41U));
}

TEST_F(ListedTermsTest, AnAbsentTermMakesTheConjunctionFalseEverywhere) {
    auto listed = expect_alike(
            [&] {
                return boolean(OperatorType::OP_AND, {term("a"), term("absent")});
            },
            roaring::Roaring(), roaring::Roaring());
    // A term surely absent opens nothing on either source.
    EXPECT_TRUE(listed->opened_together.empty());
    EXPECT_TRUE(listed->prefetches["a"].empty());
    expect_alike(
            [&] {
                return boolean(OperatorType::OP_OR, {term("absent"), term("c")});
            },
            roaring::Roaring::bitmapOf(3, 3U, 8U, 20U), roaring::Roaring::bitmapOf(2, 40U, 41U));
    expect_alike(
            [&] {
                return boolean(OperatorType::OP_OR, {term("absent"), term("missing")});
            },
            roaring::Roaring(), roaring::Roaring());
}

TEST_F(ListedTermsTest, OtherClausesRunInCostOrderAroundTheChain) {
    // The bit set holds four rows and leads; the chain starts from them. A bit set is FALSE
    // on the NULL rows, so the conjunction leaves none UNKNOWN.
    auto listed = expect_alike(
            [&] {
                return boolean(OperatorType::OP_AND,
                               {term("a"),
                                std::make_shared<BitSetQuery>(
                                        roaring::Roaring::bitmapOf(4, 2U, 3U, 8U, 30U)),
                                term("b")});
            },
            roaring::Roaring::bitmapOf(3, 2U, 3U, 8U), roaring::Roaring(), /*lists=*/true);
    EXPECT_EQ(listed->opened_together, (std::vector<std::vector<std::string>> {{"a", "b"}}));
    ASSERT_EQ(listed->prefetches["b"].size(), 1U);
    EXPECT_EQ(listed->prefetches["b"][0].candidates, (std::vector<uint32_t> {2, 3, 8, 30}));
    // A wide bit set runs after the chain, on the rows it kept.
    listed = expect_alike(
            [&] {
                return boolean(OperatorType::OP_AND,
                               {std::make_shared<BitSetQuery>(
                                        roaring::Roaring::bitmapOf(6, 1U, 3U, 8U, 30U, 31U, 32U)),
                                term("c")});
            },
            roaring::Roaring::bitmapOf(2, 3U, 8U), roaring::Roaring(), /*lists=*/true);
    EXPECT_TRUE(listed->prefetches["c"][0].whole);
}

// An unscored conjunction chains its terms on a source reading them one at a time too: the
// chain seeks the blocks the cheaper terms' rows fall in, and the source has no round to fetch.
TEST_F(ListedTermsTest, AConjunctionOnAStreamingSourceChainsItsTermsToo) {
    auto streamed = source(false);
    const auto rows = evaluate(
            [&] {
                return boolean(OperatorType::OP_AND, {term("a"), term("b"), term("c")});
            },
            streamed);
    EXPECT_EQ(rows.true_rows, roaring::Roaring::bitmapOf(2, 3U, 8U));
    EXPECT_EQ(rows.null_rows, roaring::Roaring::bitmapOf(2, 40U, 41U));
    EXPECT_EQ(streamed->opened_together, (std::vector<std::vector<std::string>> {{"a", "b", "c"}}));
    ASSERT_EQ(streamed->prefetches["c"].size(), 1U);
    EXPECT_TRUE(streamed->prefetches["c"][0].whole);
    ASSERT_EQ(streamed->prefetches["b"].size(), 1U);
    EXPECT_EQ(streamed->prefetches["b"][0].candidates, (std::vector<uint32_t> {3, 8, 20}));
    ASSERT_EQ(streamed->prefetches["a"].size(), 1U);
    EXPECT_EQ(streamed->prefetches["a"][0].candidates, (std::vector<uint32_t> {3, 8}));
    EXPECT_EQ(streamed->fetches, 0U);
    // A disjunction on it still streams.
    auto any = source(false);
    evaluate([&] { return boolean(OperatorType::OP_OR, {term("a"), term("c")}); }, any);
    EXPECT_TRUE(any->opened_together.empty());
}

// A scored conjunction on a batching source lists its rows as a chain and scores them on the
// positions it reads; its scores are the streamed strategy's.
TEST_F(ListedTermsTest, AScoredConjunctionScoresTheRowsItLists) {
    const auto make = [&] { return boolean(OperatorType::OP_AND, {term("a"), term("b")}); };
    auto streamed = source(false);
    const auto expected = scored(make, streamed);
    EXPECT_EQ(keys(expected), (std::vector<uint32_t> {2, 3, 8}));
    EXPECT_TRUE(streamed->opened_together.empty());
    auto listed = source(true);
    expect_scores_eq(scored(make, listed), expected);
    EXPECT_EQ(listed->opened_together, (std::vector<std::vector<std::string>> {{"a", "b"}}));
    EXPECT_TRUE(listed->opened.empty());
}

// The chain lists the rarer term whole and the other on its rows; then both read the positions
// of the rows holding every term, in one round.
TEST_F(ListedTermsTest, AScoredConjunctionReadsTheListedRowsPositionsInOneRound) {
    const auto make = [&] { return boolean(OperatorType::OP_AND, {term("a"), term("b")}); };
    auto listed = source(true);
    EXPECT_EQ(keys(scored(make, listed)), (std::vector<uint32_t> {2, 3, 8}));
    ASSERT_EQ(listed->prefetches["b"].size(), 2U);
    ASSERT_EQ(listed->prefetches["a"].size(), 2U);
    EXPECT_TRUE(listed->prefetches["b"][0].whole);
    EXPECT_FALSE(listed->prefetches["b"][0].positions);
    EXPECT_EQ(listed->prefetches["a"][0].candidates, (std::vector<uint32_t> {2, 3, 8, 9}));
    EXPECT_FALSE(listed->prefetches["a"][0].positions);
    EXPECT_EQ(listed->prefetches["a"][1].candidates, (std::vector<uint32_t> {2, 3, 8}));
    EXPECT_TRUE(listed->prefetches["a"][1].positions);
    EXPECT_EQ(listed->prefetches["b"][1].candidates, (std::vector<uint32_t> {2, 3, 8}));
    EXPECT_TRUE(listed->prefetches["b"][1].positions);
    EXPECT_EQ(listed->fetches, 1U);
}

// A scored disjunction reads every term's rows, frequencies and norms in one round and sums,
// per row, the scores of the terms holding it.
TEST_F(ListedTermsTest, AScoredDisjunctionSumsTheScoresOfEachTerm) {
    const auto make = [&] { return boolean(OperatorType::OP_OR, {term("a"), term("c")}); };
    auto streamed = source(false);
    const auto expected = scored(make, streamed);
    EXPECT_EQ(keys(expected), (std::vector<uint32_t> {1, 2, 3, 5, 8, 20}));
    auto listed = source(true);
    expect_scores_eq(scored(make, listed), expected);
    EXPECT_EQ(listed->opened_together, (std::vector<std::vector<std::string>> {{"a", "c"}}));
    ASSERT_EQ(listed->prefetches["a"].size(), 1U);
    EXPECT_TRUE(listed->prefetches["a"][0].whole);
    EXPECT_FALSE(listed->prefetches["a"][0].positions);
    ASSERT_EQ(listed->prefetches["c"].size(), 1U);
    EXPECT_TRUE(listed->prefetches["c"][0].whole);
    EXPECT_FALSE(listed->prefetches["c"][0].positions);
    EXPECT_EQ(listed->fetches, 1U);
}

// A scored conjunction with a term the source surely lacks opens nothing and matches nothing.
TEST_F(ListedTermsTest, AScoredConjunctionWithAnAbsentTermMatchesNothing) {
    const auto make = [&] { return boolean(OperatorType::OP_AND, {term("a"), term("absent")}); };
    auto listed = source(true);
    EXPECT_TRUE(scored(make, listed).empty());
    EXPECT_TRUE(listed->opened_together.empty());
    EXPECT_TRUE(listed->opened.empty());
}

} // namespace
} // namespace doris::segment_v2::inverted_index::query_v2
