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

#include "exprs/vcolumn_ref.h"

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

static TTypeDesc make_int_type_desc() {
    TTypeDesc type_desc;
    TTypeNode type_node;
    TScalarType scalar_type;
    scalar_type.__set_type(TPrimitiveType::INT);
    type_node.__set_type(TTypeNodeType::SCALAR);
    type_node.__set_scalar_type(scalar_type);
    type_desc.types.push_back(type_node);
    return type_desc;
}

static TExprNode make_column_ref_node(int column_id, const std::string& column_name) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::COLUMN_REF);
    node.__set_num_children(0);
    node.__set_type(make_int_type_desc());
    node.__set_is_nullable(false);

    TColumnRef column_ref;
    column_ref.__set_column_id(column_id);
    column_ref.__set_column_name(column_name);
    node.__set_column_ref(column_ref);
    return node;
}

static ColumnPtr make_int_column(const std::vector<int32_t>& values) {
    auto column = ColumnInt32::create();
    for (auto value : values) {
        column->insert_value(value);
    }
    return column;
}

static Block make_int_block() {
    auto int_type = std::make_shared<DataTypeInt32>();
    Block block;
    block.insert({make_int_column({10, 11}), int_type, "c0"});
    block.insert({make_int_column({20, 21}), int_type, "c1"});
    block.insert({make_int_column({30, 31}), int_type, "c2"});
    return block;
}

static void open_expr(const VExprSPtr& expr, VExprContext* context) {
    RuntimeState state;
    RowDescriptor row_desc;
    ASSERT_TRUE(expr->prepare(&state, row_desc, context).ok());
    ASSERT_TRUE(expr->open(&state, context, FunctionContext::THREAD_LOCAL).ok());
}

static std::vector<int32_t> get_int_values(const ColumnPtr& column) {
    const auto* int_column = assert_cast<const ColumnInt32*>(column.get());
    std::vector<int32_t> values;
    for (auto value : int_column->get_data()) {
        values.push_back(value);
    }
    return values;
}

TEST(VColumnRefTest, SetGapOverridesPreviousGap) {
    auto ref = VColumnRef::create_shared(make_column_ref_node(0, "x"));
    VExprContext context(ref);
    open_expr(ref, &context);

    auto block = make_int_block();
    ColumnPtr result;

    EXPECT_FALSE(ref->has_gap());
    ref->set_gap(0);
    EXPECT_TRUE(ref->has_gap());

    ref->set_gap(1);
    auto status = ref->execute_column(&context, &block, nullptr, block.rows(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(get_int_values(result), std::vector<int32_t>({20, 21}));

    result.reset();
    ref->set_gap(2);
    status = ref->execute_column(&context, &block, nullptr, block.rows(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(get_int_values(result), std::vector<int32_t>({30, 31}));
}

TEST(VColumnRefTest, OutOfRangeColumnPositionReturnsError) {
    auto ref = VColumnRef::create_shared(make_column_ref_node(1, "x"));
    VExprContext context(ref);
    open_expr(ref, &context);

    auto block = make_int_block();
    ref->set_gap(3);

    ColumnPtr result;
    auto status = ref->execute_column(&context, &block, nullptr, block.rows(), result);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("input block not contain column ref x"), std::string::npos);
    EXPECT_NE(status.to_string().find("column_id=1"), std::string::npos);
    EXPECT_NE(status.to_string().find("gap=3"), std::string::npos);
}

TEST(VColumnRefTest, OutOfRangeExecuteTypeThrowsException) {
    auto ref = VColumnRef::create_shared(make_column_ref_node(1, "x"));
    VExprContext context(ref);
    open_expr(ref, &context);

    auto block = make_int_block();
    ref->set_gap(3);

    EXPECT_THROW({ static_cast<void>(ref->execute_type(&block)); }, Exception);
}

} // namespace doris
