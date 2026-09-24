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
#include <memory>
#include <roaring/roaring.hh>
#include <vector>

#include "storage/index/inverted/query_v2/all_query/all_query.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_scorer.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"
#include "storage/index/inverted/query_v2/boolean_query/operator.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {

enum class ScalarTruth { False, True, Unknown };

ScalarTruth evaluate_binary(OperatorType op, ScalarTruth left, ScalarTruth right) {
    if (op == OperatorType::OP_AND) {
        if (left == ScalarTruth::False || right == ScalarTruth::False) {
            return ScalarTruth::False;
        }
        if (left == ScalarTruth::Unknown || right == ScalarTruth::Unknown) {
            return ScalarTruth::Unknown;
        }
        return ScalarTruth::True;
    }
    if (left == ScalarTruth::True || right == ScalarTruth::True) {
        return ScalarTruth::True;
    }
    if (left == ScalarTruth::Unknown || right == ScalarTruth::Unknown) {
        return ScalarTruth::Unknown;
    }
    return ScalarTruth::False;
}

struct TruthRows {
    std::shared_ptr<roaring::Roaring> true_rows = std::make_shared<roaring::Roaring>();
    std::shared_ptr<roaring::Roaring> null_rows = std::make_shared<roaring::Roaring>();

    void add(uint32_t row, ScalarTruth value) const {
        if (value == ScalarTruth::True) {
            true_rows->add(row);
        } else if (value == ScalarTruth::Unknown) {
            null_rows->add(row);
        }
    }
};

struct ScorerWork {
    uint32_t advances = 0;
    uint32_t seeks = 0;
    uint32_t scores = 0;
};

class ForwardOnlyScorer final : public Scorer {
public:
    ForwardOnlyScorer(const TruthRows& rows, float base_score,
                      std::shared_ptr<ScorerWork> work = nullptr)
            : _rows(rows.true_rows, rows.null_rows),
              _base_score(base_score),
              _work(std::move(work)) {}
    uint32_t advance() override {
        if (_work) {
            ++_work->advances;
        }
        return _rows.advance();
    }
    uint32_t seek(uint32_t target) override {
        if (_work) {
            ++_work->seeks;
        }
        return _rows.seek(target);
    }
    uint32_t doc() const override { return _rows.doc(); }
    uint32_t size_hint() const override { return _rows.size_hint(); }
    float score() override {
        if (_work) {
            ++_work->scores;
        }
        return _base_score + static_cast<float>(doc()) * 0.5F;
    }
    bool has_null_bitmap(const NullBitmapResolver* resolver = nullptr) override {
        return _rows.has_null_bitmap(resolver);
    }
    const roaring::Roaring* get_null_bitmap(const NullBitmapResolver* resolver = nullptr) override {
        return _rows.get_null_bitmap(resolver);
    }

private:
    BitSetScorer _rows;
    float _base_score;
    std::shared_ptr<ScorerWork> _work;
};

class ForwardOnlyWeight final : public Weight {
public:
    ForwardOnlyWeight(TruthRows rows, float base_score, std::shared_ptr<ScorerWork> work)
            : _rows(std::move(rows)), _base_score(base_score), _work(std::move(work)) {}
    ScorerPtr scorer(const QueryExecutionContext& /*context*/) override {
        return std::make_shared<ForwardOnlyScorer>(_rows, _base_score, _work);
    }

private:
    TruthRows _rows;
    float _base_score;
    std::shared_ptr<ScorerWork> _work;
};

class ForwardOnlyQuery final : public Query {
public:
    ForwardOnlyQuery(TruthRows rows, float base_score, std::shared_ptr<ScorerWork> work = nullptr)
            : _rows(std::move(rows)), _base_score(base_score), _work(std::move(work)) {}
    WeightPtr weight(bool /*enable_scoring*/) override {
        return std::make_shared<ForwardOnlyWeight>(_rows, _base_score, _work);
    }

private:
    TruthRows _rows;
    float _base_score;
    std::shared_ptr<ScorerWork> _work;
};

QueryPtr shared_null_query(const std::array<QueryPtr, 3>& leaves, OperatorType op) {
    OperatorBooleanQueryBuilder builder(op);
    for (const auto& leaf : leaves) {
        builder.add(leaf);
    }
    return builder.build();
}

TruthRows shared_null_rows(size_t leaf, uint32_t row_count) {
    TruthRows rows;
    for (uint32_t doc = 0; doc < row_count; ++doc) {
        if (doc % 11 == 0) {
            rows.null_rows->add(doc);
        } else if (doc % 4 != leaf) {
            rows.true_rows->add(doc);
        }
    }
    return rows;
}

void verify_shared_null_streaming(OperatorType op) {
    constexpr uint32_t row_count = 16384;
    std::array<TruthRows, 3> rows;
    std::array<QueryPtr, 3> leaves;
    std::array<std::shared_ptr<ScorerWork>, 3> work;
    for (size_t leaf = 0; leaf < leaves.size(); ++leaf) {
        rows[leaf] = shared_null_rows(leaf, row_count);
        work[leaf] = std::make_shared<ScorerWork>();
        leaves[leaf] = std::make_shared<ForwardOnlyQuery>(rows[leaf], 1.0F, work[leaf]);
    }
    QueryExecutionContext context;
    context.segment_num_rows = row_count;
    auto scorer = shared_null_query(leaves, op)->weight(true)->scorer(context);
    const auto* nulls = scorer->get_null_bitmap();
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(*nulls, *rows.front().null_rows);
    for (const auto& child : work) {
        EXPECT_LT(child->advances, row_count / 2);
        EXPECT_LT(child->scores, row_count / 2);
    }
    roaring::Roaring expected;
    roaring::Roaring actual;
    const uint32_t minimum = op == OperatorType::OP_AND ? 3 : 1;
    for (uint32_t doc = 0; doc < row_count; ++doc) {
        uint32_t count = 0;
        for (const auto& leaf : rows) {
            count += leaf.true_rows->contains(doc);
        }
        if (count >= minimum) {
            expected.add(doc);
        }
    }
    for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
        actual.add(doc);
        float expected_score = 0.0F;
        for (const auto& leaf : rows) {
            if (leaf.true_rows->contains(doc)) {
                expected_score += 1.0F + static_cast<float>(doc) * 0.5F;
            }
        }
        EXPECT_FLOAT_EQ(scorer->score(), expected_score);
    }
    EXPECT_EQ(actual, expected);
    EXPECT_EQ(*scorer->get_null_bitmap(), *rows.front().null_rows);
}

TEST(BooleanTruthContractTest, SharedUnknownRowsDoNotRequireEagerScoring) {
    for (OperatorType op : {OperatorType::OP_AND, OperatorType::OP_OR}) {
        SCOPED_TRACE(static_cast<int>(op));
        verify_shared_null_streaming(op);
    }
}

// An occur Boolean is two-valued: a required clause's UNKNOWN rows do not match, and the broad
// clause is scored only on the rows the selective one matches.
TEST(BooleanTruthContractTest, RequiredOccurClausesScoreOnlyMatchingRows) {
    constexpr uint32_t row_count = 4096;
    for (bool empty : {false, true}) {
        SCOPED_TRACE(empty);
        TruthRows selective;
        if (!empty) {
            selective.true_rows->add(10);
            selective.true_rows->add(20);
            selective.null_rows->add(30);
        }
        TruthRows broad;
        broad.true_rows->addRange(0, row_count);
        broad.true_rows->remove(40);
        broad.null_rows->add(40);
        auto work = std::make_shared<ScorerWork>();
        OccurBooleanQueryBuilder builder;
        builder.add(std::make_shared<ForwardOnlyQuery>(selective, 1.0F), Occur::MUST);
        builder.add(std::make_shared<ForwardOnlyQuery>(broad, 1.0F, work), Occur::MUST);
        QueryExecutionContext context;
        context.segment_num_rows = row_count;
        auto scorer = builder.build()->weight(true)->scorer(context);
        EXPECT_FALSE(scorer->has_null_bitmap());
        roaring::Roaring actual;
        for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
            actual.add(doc);
            EXPECT_FLOAT_EQ(scorer->score(), 2.0F + static_cast<float>(doc));
        }
        EXPECT_EQ(actual, *selective.true_rows);
        EXPECT_LE(work->scores, empty ? 0 : 3);
    }
}

// An unscored AND reads its children cheapest first, each only within the rows the ones before
// it leave TRUE or UNKNOWN.
TEST(BooleanTruthContractTest, OperatorAndReadsABroadChildWithinSelectiveRows) {
    constexpr uint32_t row_count = 4096;
    TruthRows broad;
    broad.true_rows->addRange(0, row_count);
    broad.true_rows->remove(40);
    broad.null_rows->add(40);
    TruthRows selective;
    selective.true_rows->add(10);
    selective.true_rows->add(40);
    selective.null_rows->add(30);
    auto work = std::make_shared<ScorerWork>();
    OperatorBooleanQueryBuilder builder(OperatorType::OP_AND);
    builder.add(std::make_shared<ForwardOnlyQuery>(broad, 1.0F, work));
    builder.add(std::make_shared<ForwardOnlyQuery>(selective, 1.0F));
    QueryExecutionContext context;
    context.segment_num_rows = row_count;
    auto scorer = builder.build()->weight(false)->scorer(context);

    roaring::Roaring actual;
    for (uint32_t doc = scorer->doc(); doc != TERMINATED; doc = scorer->advance()) {
        actual.add(doc);
    }
    EXPECT_EQ(actual, roaring::Roaring::bitmapOf(1, 10));
    const auto* nulls = scorer->get_null_bitmap();
    ASSERT_NE(nulls, nullptr);
    EXPECT_EQ(*nulls, roaring::Roaring::bitmapOf(2, 30, 40));
    EXPECT_LE(work->advances + work->seeks, 8U);
}

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
            .index_path = "boolean_truth_contract",
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

struct NullableAllCase {
    QueryPtr query;
    TruthRows expected;
    std::array<float, 9> scores {};
};

NullableAllCase nullable_all_case(int mode, const roaring::Roaring& field_nulls) {
    const auto right_true =
            std::make_shared<roaring::Roaring>(roaring::Roaring::bitmapOf(3, 3, 4, 8));
    const auto right_null = std::make_shared<roaring::Roaring>(roaring::Roaring::bitmapOf(2, 1, 6));
    NullableAllCase result;
    result.query = std::make_shared<AllQuery>(L"body", true);
    constexpr std::array ops {OperatorType::OP_OR, OperatorType::OP_NOT, OperatorType::OP_AND,
                              OperatorType::OP_OR};
    if (mode != 0) {
        OperatorBooleanQueryBuilder builder(ops[mode]);
        builder.add(result.query);
        if (mode != 1) {
            builder.add(std::make_shared<BitSetQuery>(right_true, right_null));
        }
        result.query = builder.build();
    }
    for (uint32_t row = 0; row < result.scores.size(); ++row) {
        ScalarTruth value = field_nulls.contains(row) ? ScalarTruth::Unknown : ScalarTruth::True;
        if (mode == 1 && value == ScalarTruth::True) {
            value = ScalarTruth::False;
        } else if (mode >= 2) {
            ScalarTruth right = ScalarTruth::False;
            if (right_true->contains(row)) {
                right = ScalarTruth::True;
            } else if (right_null->contains(row)) {
                right = ScalarTruth::Unknown;
            }
            value = evaluate_binary(ops[mode], value, right);
        }
        result.expected.add(row, value);
        result.scores[row] = static_cast<float>(!field_nulls.contains(row)) +
                             static_cast<float>(mode >= 2 && right_true->contains(row));
    }
    return result;
}

void verify_nullable_all_case(const NullableAllCase& query, const QueryExecutionContext& context,
                              bool scoring, uint32_t target) {
    auto scorer = query.query->weight(scoring)->scorer(context);
    const auto observed_nulls = [&]() {
        const auto* nulls = scorer->get_null_bitmap();
        return nulls == nullptr ? roaring::Roaring() : *nulls;
    };
    EXPECT_EQ(observed_nulls(), *query.expected.null_rows);
    roaring::Roaring actual;
    uint32_t doc = scorer->seek(target);
    while (doc != TERMINATED) {
        ASSERT_LT(doc, query.scores.size());
        actual.add(doc);
        if (scoring) {
            EXPECT_FLOAT_EQ(scorer->score(), query.scores[doc]);
        }
        doc = scorer->advance();
    }
    auto expected_true = *query.expected.true_rows;
    expected_true.removeRange(0, target);
    EXPECT_EQ(actual, expected_true);
    EXPECT_EQ(observed_nulls(), *query.expected.null_rows);
}

TEST(BooleanTruthContractTest, NullableMatchAllPreservesUnknownThroughNestedOperators) {
    const auto field_nulls = roaring::Roaring::bitmapOf(3, 1, 4, 7);
    FieldNullIterator iterator(field_nulls);
    FieldNullResolver resolver(&iterator);
    QueryExecutionContext context;
    context.segment_num_rows = 9;
    context.null_resolver = &resolver;
    for (int mode = 0; mode < 4; ++mode) {
        const auto query = nullable_all_case(mode, field_nulls);
        for (bool scoring : {false, true}) {
            for (uint32_t target : {0U, 3U, 8U}) {
                SCOPED_TRACE(testing::Message()
                             << "scoring=" << scoring << " mode=" << mode << " seek=" << target);
                verify_nullable_all_case(query, context, scoring, target);
            }
        }
    }
}

struct BinaryTruthFixture {
    std::array<TruthRows, 2> leaves;
    TruthRows expected;

    explicit BinaryTruthFixture(OperatorType op) {
        constexpr std::array kValues {ScalarTruth::False, ScalarTruth::True, ScalarTruth::Unknown};
        uint32_t row = 0;
        for (ScalarTruth left : kValues) {
            for (ScalarTruth right : kValues) {
                leaves[0].add(row, left);
                leaves[1].add(row, right);
                expected.add(row, evaluate_binary(op, left, right));
                ++row;
            }
        }
    }

    QueryPtr query(OperatorType op, bool forward_only = false) const {
        OperatorBooleanQueryBuilder builder(op);
        for (size_t index = 0; index < leaves.size(); ++index) {
            const auto& leaf = leaves[index];
            if (forward_only) {
                builder.add(std::make_shared<ForwardOnlyQuery>(
                        leaf, 1.25F + static_cast<float>(index) * 0.25F));
            } else {
                builder.add(std::make_shared<BitSetQuery>(leaf.true_rows, leaf.null_rows));
            }
        }
        return builder.build();
    }
};

void expect_nulls(const ScorerPtr& scorer, const roaring::Roaring& expected) {
    const auto* actual = scorer->get_null_bitmap();
    ASSERT_NE(actual, nullptr);
    EXPECT_EQ(*actual, expected);
}

void verify_binary_traversal(OperatorType op, bool scoring, uint32_t seek_target,
                             bool forward_only = false) {
    SCOPED_TRACE(testing::Message() << "op=" << static_cast<int>(op) << " scoring=" << scoring
                                    << " seek=" << seek_target);
    const BinaryTruthFixture fixture(op);
    auto weight = fixture.query(op, forward_only)->weight(scoring);
    QueryExecutionContext context;
    context.segment_num_rows = 9;
    auto scorer = weight->scorer(context);
    ASSERT_NE(scorer, nullptr);
    expect_nulls(scorer, *fixture.expected.null_rows);
    uint32_t doc = scorer->doc();
    if (doc < seek_target) {
        doc = scorer->seek(seek_target);
    }
    expect_nulls(scorer, *fixture.expected.null_rows);
    roaring::Roaring actual_true;
    while (doc != TERMINATED) {
        actual_true.add(doc);
        if (scoring) {
            float expected_score = 0.0F;
            for (size_t leaf = 0; leaf < fixture.leaves.size(); ++leaf) {
                if (fixture.leaves[leaf].true_rows->contains(doc)) {
                    expected_score += forward_only ? 1.25F + static_cast<float>(leaf) * 0.25F +
                                                             static_cast<float>(doc) * 0.5F
                                                   : 1.0F;
                }
            }
            EXPECT_FLOAT_EQ(scorer->score(), expected_score);
        }
        doc = scorer->advance();
    }
    auto expected_true = *fixture.expected.true_rows;
    expected_true.removeRange(0, seek_target);
    EXPECT_EQ(actual_true, expected_true);
    expect_nulls(scorer, *fixture.expected.null_rows);
}

TEST(BooleanTruthContractTest, NullRowsAreCompleteBeforeAndAfterTrueTraversal) {
    for (OperatorType op : {OperatorType::OP_AND, OperatorType::OP_OR}) {
        for (bool scoring : {false, true}) {
            for (uint32_t seek_target : {0U, 3U, 8U}) {
                verify_binary_traversal(op, scoring, seek_target);
            }
        }
    }
}

TEST(BooleanTruthContractTest, ForwardOnlyChildrenPreserveScoresAndSeekWithCompleteNullRows) {
    for (OperatorType op : {OperatorType::OP_AND, OperatorType::OP_OR}) {
        for (bool scoring : {false, true}) {
            for (uint32_t seek_target : {0U, 3U, 8U}) {
                verify_binary_traversal(op, scoring, seek_target, true);
            }
        }
    }
}

TEST(BooleanTruthContractTest, NegationPreservesUnknownBeforeAndAfterTraversal) {
    const BinaryTruthFixture fixture(OperatorType::OP_OR);
    roaring::Roaring expected_true;
    expected_true.addRange(0, 9);
    expected_true -= *fixture.expected.true_rows;
    expected_true -= *fixture.expected.null_rows;
    for (bool scoring : {false, true}) {
        QueryExecutionContext context;
        context.segment_num_rows = 9;
        auto scorer = fixture.query(OperatorType::OP_NOT)->weight(scoring)->scorer(context);
        expect_nulls(scorer, *fixture.expected.null_rows);
        roaring::Roaring actual_true;
        uint32_t doc = scorer->doc();
        while (doc != TERMINATED) {
            actual_true.add(doc);
            if (scoring) {
                EXPECT_FLOAT_EQ(scorer->score(), 1.0F);
            }
            doc = scorer->advance();
        }
        EXPECT_EQ(actual_true, expected_true);
        expect_nulls(scorer, *fixture.expected.null_rows);
    }
}

// The required clause alone decides the rows, and its UNKNOWN rows are FALSE for the Boolean.
TEST(BooleanTruthContractTest, OptionalShouldBesideARequiredClauseIsTwoValued) {
    const BinaryTruthFixture fixture(OperatorType::OP_AND);
    for (bool scoring : {false, true}) {
        for (uint32_t target : {0U, 3U, 8U}) {
            OccurBooleanQueryBuilder builder;
            builder.add(std::make_shared<BitSetQuery>(fixture.leaves[0].true_rows,
                                                      fixture.leaves[0].null_rows),
                        Occur::MUST);
            builder.add(std::make_shared<BitSetQuery>(fixture.leaves[1].true_rows,
                                                      fixture.leaves[1].null_rows),
                        Occur::SHOULD);
            QueryExecutionContext context;
            context.segment_num_rows = 9;
            auto scorer = builder.build()->weight(scoring)->scorer(context);
            roaring::Roaring actual;
            uint32_t doc = scorer->seek(target);
            while (doc != TERMINATED) {
                actual.add(doc);
                if (scoring) {
                    const float expected_score =
                            fixture.leaves[1].true_rows->contains(doc) ? 2.0F : 1.0F;
                    EXPECT_FLOAT_EQ(scorer->score(), expected_score);
                }
                doc = scorer->advance();
            }
            auto expected = *fixture.leaves[0].true_rows;
            expected.removeRange(0, target);
            EXPECT_EQ(actual, expected);
            EXPECT_FALSE(scorer->has_null_bitmap());
        }
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index::query_v2
