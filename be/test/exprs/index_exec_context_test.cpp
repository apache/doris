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

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/olap_common.h"

namespace doris::index_exec_context_test {

// Only used to provide distinct "expression identity" keys: IndexExecContext treats a VExpr* as a
// map key and never dereferences it, so there is no need to build a real expression tree here.
class StubVExpr final : public VExpr {
public:
    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::NotSupported("StubVExpr is identity-only");
    }

private:
    const std::string _name = "stub_vexpr";
};

// Build a candidate bitmap holding the single row row_id.
segment_v2::InvertedIndexResultBitmap make_bitmap(uint32_t row_id, bool approximate) {
    auto data_bitmap = std::make_shared<roaring::Roaring>();
    data_bitmap->add(row_id);
    segment_v2::InvertedIndexResultBitmap result(std::move(data_bitmap), nullptr);
    result.set_approximate(approximate);
    return result;
}

// An approximate result must be fully isolated from the exact tables: it may not enter
// _index_result_bitmap / _index_result_column and may not touch a column's index execution
// status, or fast_execute would take the candidates for the result, or the column would be judged
// not to need its data read.
TEST(IndexExecContextTest, ApproxResultIsIsolatedFromExactTables) {
    const std::vector<std::unique_ptr<segment_v2::IndexIterator>> iterators;
    const std::vector<IndexFieldNameAndTypePair> storage_name_and_type;
    std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> index_status;
    const segment_v2::ColumnIteratorOptions column_iter_opts;
    IndexExecContext ctx(iterators, storage_name_and_type, index_status, nullptr, nullptr,
                         column_iter_opts);

    StubVExpr expr;
    ctx.set_approx_index_result_for_expr(&expr, make_bitmap(7, true));

    const auto* approx = ctx.get_approx_index_result_for_expr(&expr);
    ASSERT_NE(approx, nullptr);
    EXPECT_TRUE(approx->approximate());
    EXPECT_TRUE(approx->get_data_bitmap()->contains(7));

    // All three tables on the exact side must be empty.
    EXPECT_FALSE(ctx.has_index_result_for_expr(&expr));
    EXPECT_EQ(ctx.get_index_result_for_expr(&expr), nullptr);
    EXPECT_TRUE(ctx.get_index_result_bitmap().empty());
    EXPECT_TRUE(ctx.get_index_result_column().empty());
    EXPECT_TRUE(index_status.empty());

    // An expression that was never written cannot be found in the approximate table.
    StubVExpr other;
    EXPECT_EQ(ctx.get_approx_index_result_for_expr(&other), nullptr);
}

// The other half of the split: an exact result still enters the exact table and sets the column
// status to true; an approximate result flips no column status.
TEST(IndexExecContextTest, ExactResultKeepsExactTableAndColumnStatus) {
    const std::vector<std::unique_ptr<segment_v2::IndexIterator>> iterators;
    const std::vector<IndexFieldNameAndTypePair> storage_name_and_type;
    StubVExpr exact_expr;
    StubVExpr approx_expr;
    constexpr ColumnId kColumnId = 3;
    std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> index_status;
    index_status[kColumnId][&exact_expr] = false;
    index_status[kColumnId][&approx_expr] = false;
    const segment_v2::ColumnIteratorOptions column_iter_opts;
    IndexExecContext ctx(iterators, storage_name_and_type, index_status, nullptr, nullptr,
                         column_iter_opts);

    // The exact path: the same shape as in VExpr::evaluate_inverted_index.
    ctx.set_index_result_for_expr(&exact_expr, make_bitmap(11, false));
    ctx.set_true_for_index_status(&exact_expr, static_cast<int32_t>(kColumnId));
    EXPECT_TRUE(ctx.has_index_result_for_expr(&exact_expr));
    ASSERT_NE(ctx.get_index_result_for_expr(&exact_expr), nullptr);
    EXPECT_FALSE(ctx.get_index_result_for_expr(&exact_expr)->approximate());
    EXPECT_TRUE(index_status[kColumnId][&exact_expr]);
    EXPECT_EQ(ctx.get_approx_index_result_for_expr(&exact_expr), nullptr);

    // The approximate path: only the approximate table is written, the column status stays false.
    ctx.set_approx_index_result_for_expr(&approx_expr, make_bitmap(12, true));
    EXPECT_FALSE(ctx.has_index_result_for_expr(&approx_expr));
    EXPECT_FALSE(index_status[kColumnId][&approx_expr]);
    EXPECT_EQ(ctx.get_index_result_bitmap().size(), 1U);
    EXPECT_TRUE(ctx.get_index_result_column().empty());
}

} // namespace doris::index_exec_context_test
