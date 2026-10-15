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

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/data_type/primitive_type.h"
#include "exprs/score_runtime.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr_context.h"
#include "storage/index/collection_similarity.h"
#include "storage/index/index_iterator.h"
#include "storage/index/index_query_context.h"
#include "storage/schema.h"

namespace doris {
namespace {

class ScoredBitmapExpr final : public VExpr {
public:
    ScoredBitmapExpr(std::initializer_list<uint32_t> rows, float score) : _score(score) {
        set_node_type(TExprNodeType::MATCH_PRED);
        for (uint32_t row : rows) {
            _rows.add(row);
        }
    }

    const std::string& expr_name() const override {
        static const std::string name = "ScoredBitmapExpr";
        return name;
    }

    Status execute(VExprContext*, Block*, int*) const override { return Status::OK(); }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::OK();
    }

    Status evaluate_inverted_index(VExprContext* context, uint32_t) override {
        auto index_context = context->get_index_context();
        auto query_context = index_context->get_index_query_context();
        for (uint32_t row : _rows) {
            query_context->collection_similarity->collect(row, _score);
        }
        index_context->set_index_result_for_expr(
                this,
                segment_v2::InvertedIndexResultBitmap(std::make_shared<roaring::Roaring>(_rows),
                                                      std::make_shared<roaring::Roaring>()));
        return Status::OK();
    }

private:
    roaring::Roaring _rows;
    float _score;
};

class UnindexedExpr final : public VExpr {
public:
    const std::string& expr_name() const override {
        static const std::string name = "UnindexedExpr";
        return name;
    }

    Status execute(VExprContext*, Block*, int*) const override { return Status::OK(); }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::OK();
    }

    Status evaluate_inverted_index(VExprContext*, uint32_t) override { return Status::OK(); }
};

std::shared_ptr<VCompoundPred> make_compound(TExprOpcode::type op, VExprSPtr left,
                                             VExprSPtr right) {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::COMPOUND_PRED);
    node.__set_opcode(op);
    node.__set_num_children(2);
    node.__set_is_nullable(false);
    auto expr = VCompoundPred::create_shared(node);
    expr->add_child(left);
    expr->add_child(right);
    return expr;
}

std::shared_ptr<VCompoundPred> make_not(VExprSPtr child) {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(TExprNodeType::COMPOUND_PRED);
    node.__set_opcode(TExprOpcode::COMPOUND_NOT);
    node.__set_num_children(1);
    node.__set_is_nullable(false);
    auto expr = VCompoundPred::create_shared(node);
    expr->add_child(child);
    return expr;
}

TEST(VCompoundPredScoreTest, FailedAndBranchDoesNotContributeToOrScore) {
    auto first = make_compound(
            TExprOpcode::COMPOUND_AND,
            std::make_shared<ScoredBitmapExpr>(std::initializer_list<uint32_t> {1, 2, 4}, 1.0F),
            std::make_shared<ScoredBitmapExpr>(std::initializer_list<uint32_t> {1, 2, 5}, 2.0F));
    auto second = make_compound(
            TExprOpcode::COMPOUND_AND,
            std::make_shared<ScoredBitmapExpr>(std::initializer_list<uint32_t> {1, 2, 3, 4, 5},
                                               3.0F),
            std::make_shared<ScoredBitmapExpr>(std::initializer_list<uint32_t> {1, 3, 4, 5}, 4.0F));
    auto root = make_compound(TExprOpcode::COMPOUND_OR, first, second);

    std::vector<std::unique_ptr<segment_v2::IndexIterator>> iterators;
    std::vector<IndexFieldNameAndTypePair> storage_types;
    std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> status_map;
    segment_v2::ColumnIteratorOptions iterator_options;
    auto score_runtime = std::make_shared<ScoreRuntime>(nullptr, false, 0);
    auto index_context = std::make_shared<IndexExecContext>(
            iterators, storage_types, status_map, score_runtime, nullptr, iterator_options,
            std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {}));
    auto query_context = std::make_shared<segment_v2::IndexQueryContext>();
    query_context->collection_similarity = std::make_shared<CollectionSimilarity>();
    auto root_similarity = query_context->collection_similarity;
    index_context->set_index_query_context(query_context);
    auto expr_context = VExprContext::create_shared(root);
    expr_context->set_index_context(index_context);

    ASSERT_TRUE(root->evaluate_inverted_index(expr_context.get(), 6).ok());
    const auto* result = index_context->get_index_result_for_expr(root.get());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->get_data_bitmap()->cardinality(), 5);
    EXPECT_EQ(query_context->collection_similarity, root_similarity);

    auto scores = root_similarity->release_scores();
    ASSERT_EQ(scores.size(), 5);
    EXPECT_FLOAT_EQ(scores.at(1), 10.0F);
    EXPECT_FLOAT_EQ(scores.at(2), 3.0F);
    EXPECT_FLOAT_EQ(scores.at(3), 7.0F);
    EXPECT_FLOAT_EQ(scores.at(4), 7.0F);
    EXPECT_FLOAT_EQ(scores.at(5), 7.0F);
}

TEST(VCompoundPredScoreTest, NotBranchDoesNotContributeToOrScore) {
    auto negative = make_not(
            std::make_shared<ScoredBitmapExpr>(std::initializer_list<uint32_t> {1, 2}, 1.0F));
    auto positive =
            std::make_shared<ScoredBitmapExpr>(std::initializer_list<uint32_t> {1, 3}, 5.0F);
    auto root = make_compound(TExprOpcode::COMPOUND_OR, negative, positive);

    std::vector<std::unique_ptr<segment_v2::IndexIterator>> iterators;
    std::vector<IndexFieldNameAndTypePair> storage_types;
    std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> status_map;
    segment_v2::ColumnIteratorOptions iterator_options;
    auto score_runtime = std::make_shared<ScoreRuntime>(nullptr, false, 0);
    auto index_context = std::make_shared<IndexExecContext>(
            iterators, storage_types, status_map, score_runtime, nullptr, iterator_options,
            std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {}));
    auto query_context = std::make_shared<segment_v2::IndexQueryContext>();
    query_context->collection_similarity = std::make_shared<CollectionSimilarity>();
    index_context->set_index_query_context(query_context);
    auto expr_context = VExprContext::create_shared(root);
    expr_context->set_index_context(index_context);

    ASSERT_TRUE(root->evaluate_inverted_index(expr_context.get(), 4).ok());
    const auto* result = index_context->get_index_result_for_expr(root.get());
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->get_data_bitmap()->cardinality(), 3);
    EXPECT_TRUE(result->get_data_bitmap()->contains(0));
    EXPECT_TRUE(result->get_data_bitmap()->contains(1));
    EXPECT_TRUE(result->get_data_bitmap()->contains(3));

    auto scores = query_context->collection_similarity->release_scores();
    ASSERT_EQ(scores.size(), 2);
    EXPECT_FLOAT_EQ(scores.at(1), 5.0F);
    EXPECT_FLOAT_EQ(scores.at(3), 5.0F);
}

TEST(VCompoundPredScoreTest, IncompleteScoredBranchReturnsErrorWithoutPublishingScores) {
    auto root = make_compound(
            TExprOpcode::COMPOUND_AND,
            std::make_shared<ScoredBitmapExpr>(std::initializer_list<uint32_t> {1}, 1.0F),
            std::make_shared<UnindexedExpr>());

    std::vector<std::unique_ptr<segment_v2::IndexIterator>> iterators;
    std::vector<IndexFieldNameAndTypePair> storage_types;
    std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> status_map;
    segment_v2::ColumnIteratorOptions iterator_options;
    auto score_runtime = std::make_shared<ScoreRuntime>(nullptr, false, 0);
    auto index_context = std::make_shared<IndexExecContext>(
            iterators, storage_types, status_map, score_runtime, nullptr, iterator_options,
            std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {}));
    auto query_context = std::make_shared<segment_v2::IndexQueryContext>();
    query_context->collection_similarity = std::make_shared<CollectionSimilarity>();
    auto parent_similarity = query_context->collection_similarity;
    index_context->set_index_query_context(query_context);
    auto expr_context = VExprContext::create_shared(root);
    expr_context->set_index_context(index_context);

    auto status = root->evaluate_inverted_index(expr_context.get(), 2);
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    EXPECT_EQ(query_context->collection_similarity, parent_similarity);
    EXPECT_TRUE(parent_similarity->release_scores().empty());
}

} // namespace
} // namespace doris
