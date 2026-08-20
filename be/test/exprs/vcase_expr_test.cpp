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

#include "exprs/vcase_expr.h"

#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <utility>

#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_number.h"
#include "core/value/vdatetime_value.h"
#include "exprs/vexpr_context.h"

namespace doris {
namespace {

class ColumnVExpr final : public VExpr {
public:
    ColumnVExpr(ColumnPtr column, DataTypePtr data_type)
            : _column(std::move(column)), _name("column expr") {
        _data_type = std::move(data_type);
    }

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* /*context*/, const Block* /*block*/,
                               const Selector* /*selector*/, size_t /*count*/,
                               ColumnPtr& result_column) const override {
        result_column = _column;
        return Status::OK();
    }

private:
    ColumnPtr _column;
    std::string _name;
};

TExprNode create_date_v2_case_node() {
    TCaseExpr case_node;
    case_node.__set_has_case_expr(false);
    case_node.__set_has_else_expr(true);

    TExprNode node;
    node.__set_node_type(TExprNodeType::CASE_EXPR);
    node.__set_type(DataTypeDateV2().to_thrift());
    node.__set_is_nullable(false);
    node.__set_num_children(3);
    node.__set_case_expr(case_node);
    return node;
}

ColumnDateV2::value_type create_date(uint32_t year, uint32_t month, uint32_t day) {
    const uint64_t olap_date = (year << 9) | (month << 5) | day;
    return ColumnDateV2::value_type::create_from_olap_date(olap_date);
}

TEST(VCaseExprTest, UpdateNonNullableDateV2Result) {
    auto condition = ColumnUInt8::create();
    condition->insert_value(1);
    condition->insert_value(0);
    condition->insert_value(1);

    auto then_column = ColumnDateV2::create();
    then_column->insert_value(create_date(2024, 1, 1));
    then_column->insert_value(create_date(2024, 1, 2));
    then_column->insert_value(create_date(2024, 1, 3));

    auto else_column = ColumnDateV2::create();
    else_column->insert_value(create_date(2025, 1, 1));
    else_column->insert_value(create_date(2025, 1, 2));
    else_column->insert_value(create_date(2025, 1, 3));

    auto case_expr = VCaseExpr::create_shared(create_date_v2_case_node());
    auto date_type = std::make_shared<DataTypeDateV2>();
    case_expr->add_child(
            std::make_shared<ColumnVExpr>(std::move(condition), std::make_shared<DataTypeUInt8>()));
    case_expr->add_child(std::make_shared<ColumnVExpr>(std::move(then_column), date_type));
    case_expr->add_child(std::make_shared<ColumnVExpr>(std::move(else_column), date_type));

    VExprContext context(case_expr);
    ColumnPtr result;
    const auto status = case_expr->execute_column(&context, /*block=*/nullptr,
                                                  /*selector=*/nullptr, /*count=*/3, result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& result_data = assert_cast<const ColumnDateV2&>(*result).get_data();
    ASSERT_EQ(result_data.size(), 3);
    EXPECT_EQ(result_data[0], create_date(2024, 1, 1));
    EXPECT_EQ(result_data[1], create_date(2025, 1, 2));
    EXPECT_EQ(result_data[2], create_date(2024, 1, 3));
}

} // namespace
} // namespace doris
