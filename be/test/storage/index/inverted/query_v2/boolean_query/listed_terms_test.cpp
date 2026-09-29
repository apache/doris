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

// The corpus: "a" holds 1 2 3 5 8, "b" 2 3 8 9 and "c" 3 8 20; rows 40 and 41 are NULL.
class ListedTermsTest : public ::testing::Test {
protected:
    using MakeQuery = std::function<QueryPtr()>;

    std::shared_ptr<FakeIndexSource> source(bool batches) const {
        auto source = std::make_shared<FakeIndexSource>();
        source->batches = batches;
        source->set_doc_count(64);
        source->add("a", {1, 2, 3, 5, 8});
        source->add("b", {2, 3, 8, 9});
        source->add("c", {3, 8, 20});
        return source;
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

    // Both strategies answer the query alike; the batching source is returned.
    std::shared_ptr<FakeIndexSource> expect_alike(const MakeQuery& make_query,
                                                  const roaring::Roaring& true_rows,
                                                  const roaring::Roaring& null_rows) const {
        auto streamed = source(false);
        const auto streamed_rows = evaluate(make_query, streamed);
        EXPECT_EQ(streamed_rows.true_rows, true_rows);
        EXPECT_EQ(streamed_rows.null_rows, null_rows);
        EXPECT_TRUE(streamed->opened_together.empty());
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
    group.open();
    EXPECT_FALSE(group.has_absent_term());
    EXPECT_EQ(group.cheapest_doc_freq(), 4U);
    const auto rows = group.conjunction(nullptr);
    EXPECT_EQ(rows.true_rows, roaring::Roaring::bitmapOf(3, 2U, 3U, 8U));
    EXPECT_EQ(rows.null_rows, roaring::Roaring::bitmapOf(2, 40U, 41U));
    ListedTerms any(listed, nulls);
    any.add(0, "a");
    any.add(1, "c");
    any.open();
    const auto union_rows = any.disjunction();
    EXPECT_EQ(union_rows.true_rows, roaring::Roaring::bitmapOf(6, 1U, 2U, 3U, 5U, 8U, 20U));
    EXPECT_EQ(union_rows.null_rows, roaring::Roaring::bitmapOf(2, 40U, 41U));
}

TEST_F(ListedTermsTest, AConjunctionChainsItsTermsFromTheCheapest) {
    auto listed = expect_alike(
            [&] {
                return boolean(OperatorType::OP_AND, {term("a"), term("b"), term("c")});
            },
            roaring::Roaring::bitmapOf(2, 3U, 8U), roaring::Roaring::bitmapOf(2, 40U, 41U));
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
            roaring::Roaring::bitmapOf(3, 2U, 3U, 8U), roaring::Roaring());
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
            roaring::Roaring::bitmapOf(2, 3U, 8U), roaring::Roaring());
    EXPECT_TRUE(listed->prefetches["c"][0].whole);
}

} // namespace
} // namespace doris::segment_v2::inverted_index::query_v2
