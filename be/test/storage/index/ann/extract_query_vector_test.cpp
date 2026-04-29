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

#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "exprs/vexpr.h"
#include "storage/index/ann/ann_topn_runtime.h"

namespace doris::segment_v2 {

// A minimal mock VExpr that returns a pre-set constant column.
class MockConstVExpr : public VExpr {
public:
    static TExprNode make_tnode() {
        TExprNode node;
        node.node_type = TExprNodeType::FLOAT_LITERAL;
        node.type = TTypeDesc();
        TTypeNode type_node;
        type_node.type = TTypeNodeType::SCALAR;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::FLOAT);
        type_node.__set_scalar_type(scalar_type);
        node.type.types.push_back(type_node);
        return node;
    }

    MockConstVExpr() : VExpr(make_tnode()) {}

    Status get_const_col(VExprContext* /*context*/,
                         std::shared_ptr<ColumnPtrWrapper>* output) override {
        *output = std::make_shared<ColumnPtrWrapper>(_col);
        return Status::OK();
    }

    bool is_constant() const override { return _is_constant; }

    Status execute_column(VExprContext* context, const Block* block, const Selector* selector,
                          size_t count, ColumnPtr& result_column) const {
        return Status::OK();
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        return Status::OK();
    }

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        return Status::OK();
    }

    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }

    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override {}

    const std::string& expr_name() const override {
        static std::string name = "MockConstVExpr";
        return name;
    }

    std::string debug_string() const override { return "MockConstVExpr"; }

    void set_column(IColumn::Ptr col) { _col = std::move(col); }
    void set_is_constant(bool v) { _is_constant = v; }

private:
    IColumn::Ptr _col;
    bool _is_constant = true;
};

// Helper: build Nullable(ColumnArray(Nullable(ColumnFloat32))) with 1 row of given floats
static IColumn::Ptr make_nullable_array_column(const std::vector<float>& values) {
    auto float_col = ColumnFloat32::create();
    for (float v : values) {
        float_col->insert_value(v);
    }
    auto null_map_inner = ColumnUInt8::create(values.size(), 0);
    auto nullable_inner = ColumnNullable::create(std::move(float_col), std::move(null_map_inner));

    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(static_cast<IColumn::Offset>(values.size()));
    auto array_col = ColumnArray::create(std::move(nullable_inner), std::move(offsets));

    auto null_map_outer = ColumnUInt8::create(1, 0);
    return ColumnNullable::create(std::move(array_col), std::move(null_map_outer));
}

class ExtractQueryVectorTest : public ::testing::Test {};

// ColumnConst wrapping a Nullable(Array(Nullable(Float32))) — the array_repeat case
TEST_F(ExtractQueryVectorTest, ColumnConstWrappedArray) {
    auto inner = make_nullable_array_column({1.0f, 2.0f, 3.0f});
    auto const_col = ColumnConst::create(std::move(inner), 1);

    auto mock = std::make_shared<MockConstVExpr>();
    mock->set_column(std::move(const_col));

    auto result = extract_query_vector(mock);
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    EXPECT_EQ(result.value()->size(), 3u);
}

// Direct Nullable(Array(Nullable(Float32))) without ColumnConst wrapper
TEST_F(ExtractQueryVectorTest, DirectNullableArray) {
    auto col = make_nullable_array_column({4.0f, 5.0f});

    auto mock = std::make_shared<MockConstVExpr>();
    mock->set_column(std::move(col));

    auto result = extract_query_vector(mock);
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    EXPECT_EQ(result.value()->size(), 2u);
}

// Non-nullable ColumnArray(Nullable(Float32)) directly
TEST_F(ExtractQueryVectorTest, NonNullableArray) {
    auto float_col = ColumnFloat32::create();
    float_col->insert_value(1.0f);
    float_col->insert_value(2.0f);
    auto null_map = ColumnUInt8::create(2, 0);
    auto nullable_inner = ColumnNullable::create(std::move(float_col), std::move(null_map));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(2);
    auto array_col = ColumnArray::create(std::move(nullable_inner), std::move(offsets));

    auto mock = std::make_shared<MockConstVExpr>();
    mock->set_column(std::move(array_col));

    auto result = extract_query_vector(mock);
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    EXPECT_EQ(result.value()->size(), 2u);
}

// ColumnConst wrapping non-nullable array (another possible shape)
TEST_F(ExtractQueryVectorTest, ColumnConstNonNullableArray) {
    auto float_col = ColumnFloat32::create();
    float_col->insert_value(7.0f);
    auto null_map = ColumnUInt8::create(1, 0);
    auto nullable_inner = ColumnNullable::create(std::move(float_col), std::move(null_map));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(1);
    auto array_col = ColumnArray::create(std::move(nullable_inner), std::move(offsets));
    auto const_col = ColumnConst::create(std::move(array_col), 1);

    auto mock = std::make_shared<MockConstVExpr>();
    mock->set_column(std::move(const_col));

    auto result = extract_query_vector(mock);
    ASSERT_TRUE(result.has_value()) << result.error().to_string();
    EXPECT_EQ(result.value()->size(), 1u);
}

// Verify extracted float values match input
TEST_F(ExtractQueryVectorTest, ValuesMatchInput) {
    std::vector<float> input = {1.5f, 2.5f, 3.5f, 4.5f};
    auto col = make_nullable_array_column(input);
    auto const_col = ColumnConst::create(std::move(col), 1);

    auto mock = std::make_shared<MockConstVExpr>();
    mock->set_column(std::move(const_col));

    auto result = extract_query_vector(mock);
    ASSERT_TRUE(result.has_value());
    auto* float_col = assert_cast<const ColumnFloat32*>(result.value().get());
    ASSERT_EQ(float_col->size(), 4u);
    for (size_t i = 0; i < input.size(); ++i) {
        EXPECT_FLOAT_EQ(float_col->get_data()[i], input[i]);
    }
}

// Error: non-constant expression
TEST_F(ExtractQueryVectorTest, NonConstantExprFails) {
    auto mock = std::make_shared<MockConstVExpr>();
    mock->set_is_constant(false);

    auto result = extract_query_vector(mock);
    ASSERT_FALSE(result.has_value());
    EXPECT_TRUE(result.error().to_string().find("must be constant") != std::string::npos);
}

// Error: NULL array
TEST_F(ExtractQueryVectorTest, NullArrayFails) {
    auto float_col = ColumnFloat32::create();
    auto null_map_inner = ColumnUInt8::create(0, 0);
    auto nullable_inner = ColumnNullable::create(std::move(float_col), std::move(null_map_inner));
    auto offsets = ColumnArray::ColumnOffsets::create();
    offsets->insert_value(0);
    auto array_col = ColumnArray::create(std::move(nullable_inner), std::move(offsets));
    // Outer nullable with null flag set to 1
    auto null_map_outer = ColumnUInt8::create(1, 1);
    auto nullable_outer = ColumnNullable::create(std::move(array_col), std::move(null_map_outer));

    auto mock = std::make_shared<MockConstVExpr>();
    mock->set_column(std::move(nullable_outer));

    auto result = extract_query_vector(mock);
    ASSERT_FALSE(result.has_value());
    EXPECT_TRUE(result.error().to_string().find("cannot be NULL") != std::string::npos);
}

// Error: empty array
TEST_F(ExtractQueryVectorTest, EmptyArrayFails) {
    auto col = make_nullable_array_column({});

    auto mock = std::make_shared<MockConstVExpr>();
    mock->set_column(std::move(col));

    auto result = extract_query_vector(mock);
    ASSERT_FALSE(result.has_value());
    EXPECT_TRUE(result.error().to_string().find("cannot be empty") != std::string::npos);
}

// Error: not an array column at all
TEST_F(ExtractQueryVectorTest, NonArrayColumnFails) {
    auto float_col = ColumnFloat32::create();
    float_col->insert_value(1.0f);

    auto mock = std::make_shared<MockConstVExpr>();
    mock->set_column(std::move(float_col));

    auto result = extract_query_vector(mock);
    ASSERT_FALSE(result.has_value());
    EXPECT_TRUE(result.error().to_string().find("Array literal") != std::string::npos);
}

} // namespace doris::segment_v2
