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

// 仅用于提供互不相同的「表达式身份」键：IndexExecContext 只把 VExpr* 当作 map 的 key，
// 从不解引用，所以这里不需要构造真实表达式树。
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

// 造一个只含 row_id 一行的候选位图。
segment_v2::InvertedIndexResultBitmap make_bitmap(uint32_t row_id, bool approximate) {
    auto data_bitmap = std::make_shared<roaring::Roaring>();
    data_bitmap->add(row_id);
    segment_v2::InvertedIndexResultBitmap result(std::move(data_bitmap), nullptr);
    result.set_approximate(approximate);
    return result;
}

// 近似结果必须与精确表完全隔离：不进 _index_result_bitmap / _index_result_column，
// 也不触碰列的索引执行状态，否则 fast_execute 会把候选当结果、或列被判定为无需读数据。
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

    // 精确侧的三张表都必须是空的。
    EXPECT_FALSE(ctx.has_index_result_for_expr(&expr));
    EXPECT_EQ(ctx.get_index_result_for_expr(&expr), nullptr);
    EXPECT_TRUE(ctx.get_index_result_bitmap().empty());
    EXPECT_TRUE(ctx.get_index_result_column().empty());
    EXPECT_TRUE(index_status.empty());

    // 未写入的表达式在近似表里查不到。
    StubVExpr other;
    EXPECT_EQ(ctx.get_approx_index_result_for_expr(&other), nullptr);
}

// 分流的另一半：精确结果仍旧进精确表并把列状态置真；近似结果不会翻转任何列状态。
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

    // 精确路径：与 VExpr::evaluate_inverted_index 中的写法一致。
    ctx.set_index_result_for_expr(&exact_expr, make_bitmap(11, false));
    ctx.set_true_for_index_status(&exact_expr, static_cast<int32_t>(kColumnId));
    EXPECT_TRUE(ctx.has_index_result_for_expr(&exact_expr));
    ASSERT_NE(ctx.get_index_result_for_expr(&exact_expr), nullptr);
    EXPECT_FALSE(ctx.get_index_result_for_expr(&exact_expr)->approximate());
    EXPECT_TRUE(index_status[kColumnId][&exact_expr]);
    EXPECT_EQ(ctx.get_approx_index_result_for_expr(&exact_expr), nullptr);

    // 近似路径：只落近似表，列状态保持 false。
    ctx.set_approx_index_result_for_expr(&approx_expr, make_bitmap(12, true));
    EXPECT_FALSE(ctx.has_index_result_for_expr(&approx_expr));
    EXPECT_FALSE(index_status[kColumnId][&approx_expr]);
    EXPECT_EQ(ctx.get_index_result_bitmap().size(), 1U);
    EXPECT_TRUE(ctx.get_index_result_column().empty());
}

} // namespace doris::index_exec_context_test
