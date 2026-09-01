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

#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nothing.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exprs/vcolumn_ref.h"
#include "exprs/vexpr_context.h"
#include "exprs/vlambda_function_call_expr.h"
#include "exprs/vlambda_function_expr.h"
#include "exprs/vslot_ref.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"

namespace doris {

class MockColumnExpr final : public VExpr {
public:
    MockColumnExpr(ColumnPtr column, DataTypePtr type, std::string name)
            : VExpr(type, false),
              _column(std::move(column)),
              _type(std::move(type)),
              _name(std::move(name)) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* /*context*/, const Block* /*block*/,
                               const Selector* /*selector*/, size_t /*count*/,
                               ColumnPtr& result_column) const override {
        result_column = _column;
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    ColumnPtr _column;
    DataTypePtr _type;
    std::string _name;
};

class MockConstColumnExpr final : public VExpr {
public:
    MockConstColumnExpr(ColumnPtr column, DataTypePtr type, std::string name)
            : VExpr(type, false),
              _column(std::move(column)),
              _type(std::move(type)),
              _name(std::move(name)) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* /*context*/, const Block* /*block*/,
                               const Selector* /*selector*/, size_t count,
                               ColumnPtr& result_column) const override {
        result_column = ColumnConst::create(_column, count);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    ColumnPtr _column;
    DataTypePtr _type;
    std::string _name;
};

class MockBodyExpr final : public VExpr {
public:
    MockBodyExpr(DataTypePtr type, std::string name)
            : VExpr(type, false), _type(std::move(type)), _name(std::move(name)) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* /*context*/, const Block* /*block*/,
                               const Selector* /*selector*/, size_t /*count*/,
                               ColumnPtr& /*result_column*/) const override {
        return Status::InternalError("mock body should not be executed");
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    DataTypePtr _type;
    std::string _name;
};

class MockCapturedInputExpr final : public VExpr {
public:
    MockCapturedInputExpr(DataTypePtr type, const IColumn* expected_input, bool* used_direct_input,
                          bool* kept_capture_const)
            : VExpr(type, false),
              _type(std::move(type)),
              _expected_input(expected_input),
              _used_direct_input(used_direct_input),
              _kept_capture_const(kept_capture_const) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        *_kept_capture_const = is_column_const(*block->get_by_position(0).column);
        *_used_direct_input = block->get_by_position(1).column.get() == _expected_input;

        ColumnPtr captured;
        ColumnPtr input;
        RETURN_IF_ERROR(get_child(0)->execute_column(context, block, selector, count, captured));
        RETURN_IF_ERROR(get_child(1)->execute_column(context, block, selector, count, input));
        captured = captured->convert_to_full_column_if_const();
        input = input->convert_to_full_column_if_const();

        const IColumn* captured_data = captured.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(captured_data)) {
            captured_data = &nullable->get_nested_column();
        }
        const IColumn* input_data = input.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(input_data)) {
            input_data = &nullable->get_nested_column();
        }
        const auto& captured_values = assert_cast<const ColumnInt32&>(*captured_data);
        const auto& input_values = assert_cast<const ColumnInt32&>(*input_data);
        auto result = ColumnInt32::create();
        result->reserve(count);
        for (size_t i = 0; i < count; ++i) {
            result->insert_value(captured_values.get_element(i) + input_values.get_element(i));
        }
        result_column = std::move(result);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    DataTypePtr _type;
    const IColumn* _expected_input;
    bool* _used_direct_input;
    bool* _kept_capture_const;
    std::string _name = "mock_captured_const_direct";
};

class MockIncrementExpr final : public VExpr {
public:
    explicit MockIncrementExpr(DataTypePtr type) : VExpr(type, false), _type(std::move(type)) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        ColumnPtr input;
        RETURN_IF_ERROR(get_child(0)->execute_column(context, block, selector, count, input));
        const IColumn* data = input.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(data)) {
            data = &nullable->get_nested_column();
        }
        const auto& values = assert_cast<const ColumnInt32&>(*data);
        auto result = ColumnInt32::create();
        result->reserve(count);
        for (size_t i = 0; i < count; ++i) {
            result->insert_value(values.get_element(i) + 1);
        }
        result_column = std::move(result);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    DataTypePtr _type;
    std::string _name = "mock_increment";
};

class MockBatchSizeExpr final : public VExpr {
public:
    MockBatchSizeExpr(DataTypePtr type, std::vector<size_t>* observed_batch_sizes)
            : VExpr(type, false),
              _type(std::move(type)),
              _observed_batch_sizes(observed_batch_sizes) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* /*context*/, const Block* /*block*/,
                               const Selector* /*selector*/, size_t count,
                               ColumnPtr& result_column) const override {
        _observed_batch_sizes->push_back(count);
        result_column = ColumnInt32::create(count, 0);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    DataTypePtr _type;
    std::vector<size_t>* _observed_batch_sizes;
    std::string _name = "mock_batch_size";
};

class MockGreatestExpr final : public VExpr {
public:
    MockGreatestExpr(DataTypePtr type, std::vector<size_t>* observed_batch_sizes)
            : VExpr(type, false),
              _type(std::move(type)),
              _observed_batch_sizes(observed_batch_sizes) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        _observed_batch_sizes->push_back(count);
        std::vector<ColumnPtr> inputs;
        inputs.reserve(children().size());
        for (const auto& child : children()) {
            ColumnPtr input;
            RETURN_IF_ERROR(child->execute_column(context, block, selector, count, input));
            inputs.push_back(input->convert_to_full_column_if_const());
        }

        auto result = ColumnInt32::create();
        result->reserve(count);
        for (size_t row = 0; row < count; ++row) {
            int32_t greatest = _get_int_data(inputs[0]).get_element(row);
            for (size_t i = 1; i < inputs.size(); ++i) {
                greatest = std::max(greatest, _get_int_data(inputs[i]).get_element(row));
            }
            result->insert_value(greatest);
        }
        result_column = std::move(result);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    const ColumnInt32& _get_int_data(const ColumnPtr& column) const {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
            return assert_cast<const ColumnInt32&>(nullable->get_nested_column());
        }
        return assert_cast<const ColumnInt32&>(*column);
    }

    DataTypePtr _type;
    std::vector<size_t>* _observed_batch_sizes;
    std::string _name = "mock_greatest";
};

class MockSubtractExpr final : public VExpr {
public:
    explicit MockSubtractExpr(DataTypePtr type) : VExpr(type, false), _type(std::move(type)) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        ColumnPtr left;
        ColumnPtr right;
        RETURN_IF_ERROR(get_child(0)->execute_column(context, block, selector, count, left));
        RETURN_IF_ERROR(get_child(1)->execute_column(context, block, selector, count, right));
        left = left->convert_to_full_column_if_const();
        right = right->convert_to_full_column_if_const();

        const auto& left_data = _get_int_data(left);
        const auto& right_data = _get_int_data(right);
        auto result = ColumnInt32::create();
        for (size_t i = 0; i < count; ++i) {
            result->insert_value(left_data.get_element(i) - right_data.get_element(i));
        }
        result_column = std::move(result);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    const ColumnInt32& _get_int_data(const ColumnPtr& column) const {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
            return assert_cast<const ColumnInt32&>(nullable->get_nested_column());
        }
        return assert_cast<const ColumnInt32&>(*column);
    }

    DataTypePtr _type;
    std::string _name = "mock_subtract";
};

class MockAddExpr final : public VExpr {
public:
    explicit MockAddExpr(DataTypePtr type, std::vector<size_t>* observed_batch_sizes = nullptr)
            : VExpr(type, false),
              _type(std::move(type)),
              _observed_batch_sizes(observed_batch_sizes) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        if (_observed_batch_sizes != nullptr) {
            _observed_batch_sizes->push_back(count);
        }
        ColumnPtr left;
        ColumnPtr right;
        RETURN_IF_ERROR(get_child(0)->execute_column(context, block, selector, count, left));
        RETURN_IF_ERROR(get_child(1)->execute_column(context, block, selector, count, right));
        left = left->convert_to_full_column_if_const();
        right = right->convert_to_full_column_if_const();

        const auto& left_data = _get_int_data(left);
        const auto& right_data = _get_int_data(right);
        auto result = ColumnInt32::create();
        for (size_t i = 0; i < count; ++i) {
            result->insert_value(left_data.get_element(i) + right_data.get_element(i));
        }
        result_column = std::move(result);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    const ColumnInt32& _get_int_data(const ColumnPtr& column) const {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
            return assert_cast<const ColumnInt32&>(nullable->get_nested_column());
        }
        return assert_cast<const ColumnInt32&>(*column);
    }

    DataTypePtr _type;
    std::vector<size_t>* _observed_batch_sizes;
    std::string _name = "mock_add";
};

class MockSparseCapturedInputExpr final : public VExpr {
public:
    MockSparseCapturedInputExpr(DataTypePtr type, size_t captured_column_position,
                                bool* sparse_slots_use_column_nothing)
            : VExpr(type, false),
              _type(std::move(type)),
              _captured_column_position(captured_column_position),
              _sparse_slots_use_column_nothing(sparse_slots_use_column_nothing) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        *_sparse_slots_use_column_nothing = true;
        for (size_t i = 0; i < _captured_column_position; ++i) {
            if (!check_and_get_column<ColumnNothing>(block->get_by_position(i).column.get())) {
                *_sparse_slots_use_column_nothing = false;
                break;
            }
        }

        ColumnPtr captured;
        ColumnPtr input;
        RETURN_IF_ERROR(get_child(0)->execute_column(context, block, selector, count, captured));
        RETURN_IF_ERROR(get_child(1)->execute_column(context, block, selector, count, input));
        const IColumn* captured_data = captured.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(captured_data)) {
            captured_data = &nullable->get_nested_column();
        }
        const IColumn* input_data = input.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(input_data)) {
            input_data = &nullable->get_nested_column();
        }
        const auto& captured_values = assert_cast<const ColumnInt32&>(*captured_data);
        const auto& input_values = assert_cast<const ColumnInt32&>(*input_data);
        auto result = ColumnInt32::create();
        result->reserve(count);
        for (size_t i = 0; i < count; ++i) {
            result->insert_value(captured_values.get_element(i) + input_values.get_element(i));
        }
        result_column = std::move(result);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    DataTypePtr _type;
    size_t _captured_column_position;
    bool* _sparse_slots_use_column_nothing;
    std::string _name = "mock_sparse_captured_input";
};

class MockMultiplyExpr final : public VExpr {
public:
    explicit MockMultiplyExpr(DataTypePtr type) : VExpr(type, false), _type(std::move(type)) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        ColumnPtr left;
        ColumnPtr right;
        RETURN_IF_ERROR(get_child(0)->execute_column(context, block, selector, count, left));
        RETURN_IF_ERROR(get_child(1)->execute_column(context, block, selector, count, right));
        left = left->convert_to_full_column_if_const();
        right = right->convert_to_full_column_if_const();

        const auto& left_data = _get_int_data(left);
        const auto& right_data = _get_int_data(right);
        auto result = ColumnInt32::create();
        for (size_t i = 0; i < count; ++i) {
            result->insert_value(left_data.get_element(i) * right_data.get_element(i));
        }
        result_column = std::move(result);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    const ColumnInt32& _get_int_data(const ColumnPtr& column) const {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
            return assert_cast<const ColumnInt32&>(nullable->get_nested_column());
        }
        return assert_cast<const ColumnInt32&>(*column);
    }

    DataTypePtr _type;
    std::string _name = "mock_multiply";
};

class MockCompareExpr final : public VExpr {
public:
    explicit MockCompareExpr(DataTypePtr type) : VExpr(type, false), _type(std::move(type)) {}

    const std::string& expr_name() const override { return _name; }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        ColumnPtr left;
        ColumnPtr right;
        RETURN_IF_ERROR(get_child(0)->execute_column(context, block, selector, count, left));
        RETURN_IF_ERROR(get_child(1)->execute_column(context, block, selector, count, right));
        left = left->convert_to_full_column_if_const();
        right = right->convert_to_full_column_if_const();

        const auto& left_data = _get_int_data(left);
        const auto& right_data = _get_int_data(right);
        auto result = ColumnInt8::create();
        for (size_t i = 0; i < count; ++i) {
            const auto left_value = left_data.get_element(i);
            const auto right_value = right_data.get_element(i);
            int8_t compare_result = 0;
            if (left_value < right_value) {
                compare_result = -1;
            } else if (left_value > right_value) {
                compare_result = 1;
            }
            result->insert_value(compare_result);
        }
        result_column = std::move(result);
        return Status::OK();
    }

    DataTypePtr execute_type(const Block* /*block*/) const override { return _type; }

private:
    const ColumnInt32& _get_int_data(const ColumnPtr& column) const {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
            return assert_cast<const ColumnInt32&>(nullable->get_nested_column());
        }
        return assert_cast<const ColumnInt32&>(*column);
    }

    DataTypePtr _type;
    std::string _name = "mock_compare";
};

static TExprNode make_lambda_call_node(const DataTypePtr& type, int num_children,
                                       const std::string& function_name = "array_map") {
    TExprNode node;
    node.__set_node_type(TExprNodeType::LAMBDA_FUNCTION_CALL_EXPR);
    node.__set_num_children(num_children);
    node.__set_type(type->to_thrift());
    node.__set_is_nullable(type->is_nullable());

    TFunction fn;
    TFunctionName fn_name;
    fn_name.__set_function_name(function_name);
    fn.__set_name(fn_name);
    node.__set_fn(fn);
    return node;
}

static TExprNode make_lambda_expr_node(const DataTypePtr& type,
                                       const std::vector<std::string>& argument_names,
                                       bool set_argument_names = true) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::LAMBDA_FUNCTION_EXPR);
    node.__set_num_children(1);
    node.__set_type(type->to_thrift());
    node.__set_is_nullable(type->is_nullable());
    if (set_argument_names) {
        node.__set_lambda_argument_names(argument_names);
    }
    return node;
}

static TExprNode make_column_ref_node(int column_id, const std::string& column_name,
                                      const DataTypePtr& type) {
    TExprNode node;
    node.__set_node_type(TExprNodeType::COLUMN_REF);
    node.__set_num_children(0);
    node.__set_type(type->to_thrift());
    node.__set_is_nullable(type->is_nullable());

    TColumnRef column_ref;
    column_ref.__set_column_id(column_id);
    column_ref.__set_column_name(column_name);
    node.__set_column_ref(column_ref);
    return node;
}

static VExprSPtr make_slot_ref(int column_id, const std::string& column_name,
                               const DataTypePtr& type) {
    static std::vector<std::unique_ptr<std::string>> column_names;
    column_names.push_back(std::make_unique<std::string>(column_name));

    auto ref = VSlotRef::create_shared();
    ref->set_node_type(TExprNodeType::SLOT_REF);
    ref->set_slot_id(-1);
    ref->set_column_id(column_id);
    ref->set_column_name(column_names.back().get());
    ref->data_type() = type;
    return ref;
}

static ColumnPtr make_int_column(const std::vector<int32_t>& values) {
    auto column = ColumnInt32::create();
    for (auto value : values) {
        column->insert_value(value);
    }
    return column;
}

static ColumnPtr make_int_array_column(const std::vector<std::vector<int32_t>>& rows) {
    auto int_column = ColumnInt32::create();
    auto offsets = ColumnArray::ColumnOffsets::create();
    int64_t offset = 0;
    for (const auto& row : rows) {
        for (auto value : row) {
            int_column->insert_value(value);
        }
        offset += row.size();
        offsets->insert_value(offset);
    }
    auto int_null_map = ColumnUInt8::create(int_column->size(), 0);
    auto nullable_int_column =
            ColumnNullable::create(std::move(int_column), std::move(int_null_map));
    return ColumnArray::create(std::move(nullable_int_column), std::move(offsets));
}

static ColumnPtr make_nested_int_array_column() {
    // Two input rows:
    //   row 0: [[1, 2], [3]]
    //   row 1: [[4, 5]]
    auto int_column = ColumnInt32::create();
    for (int32_t value : {1, 2, 3, 4, 5}) {
        int_column->insert_value(value);
    }
    auto int_null_map = ColumnUInt8::create(int_column->size(), 0);
    auto nullable_int_column =
            ColumnNullable::create(std::move(int_column), std::move(int_null_map));

    auto inner_offsets = ColumnArray::ColumnOffsets::create();
    for (int64_t offset : {2, 3, 5}) {
        inner_offsets->insert_value(offset);
    }
    auto inner_array_column =
            ColumnArray::create(std::move(nullable_int_column), std::move(inner_offsets));
    auto inner_array_null_map = ColumnUInt8::create(inner_array_column->size(), 0);
    auto nullable_inner_array_column =
            ColumnNullable::create(std::move(inner_array_column), std::move(inner_array_null_map));

    auto outer_offsets = ColumnArray::ColumnOffsets::create();
    for (int64_t offset : {2, 3}) {
        outer_offsets->insert_value(offset);
    }
    return ColumnArray::create(std::move(nullable_inner_array_column), std::move(outer_offsets));
}

static ColumnPtr make_nested_unsorted_int_array_column() {
    // Two input rows:
    //   row 0: [[2, 1], [3]]
    //   row 1: [[5, 4]]
    auto int_column = ColumnInt32::create();
    for (int32_t value : {2, 1, 3, 5, 4}) {
        int_column->insert_value(value);
    }
    auto int_null_map = ColumnUInt8::create(int_column->size(), 0);
    auto nullable_int_column =
            ColumnNullable::create(std::move(int_column), std::move(int_null_map));

    auto inner_offsets = ColumnArray::ColumnOffsets::create();
    for (int64_t offset : {2, 3, 5}) {
        inner_offsets->insert_value(offset);
    }
    auto inner_array_column =
            ColumnArray::create(std::move(nullable_int_column), std::move(inner_offsets));
    auto inner_array_null_map = ColumnUInt8::create(inner_array_column->size(), 0);
    auto nullable_inner_array_column =
            ColumnNullable::create(std::move(inner_array_column), std::move(inner_array_null_map));

    auto outer_offsets = ColumnArray::ColumnOffsets::create();
    for (int64_t offset : {2, 3}) {
        outer_offsets->insert_value(offset);
    }
    return ColumnArray::create(std::move(nullable_inner_array_column), std::move(outer_offsets));
}

static void open_expr(const VExprSPtr& expr, VExprContext* context) {
    RuntimeState state;
    RowDescriptor row_desc;
    ASSERT_TRUE(expr->prepare(&state, row_desc, context).ok());
    ASSERT_TRUE(expr->open(&state, context, FunctionContext::THREAD_LOCAL).ok());
}

static void open_expr_with_batch_size(const VExprSPtr& expr, VExprContext* context,
                                      int batch_size) {
    RuntimeState state;
    TQueryOptions query_options;
    query_options.__set_batch_size(batch_size);
    state.set_query_options(query_options);
    RowDescriptor row_desc;
    ASSERT_TRUE(expr->prepare(&state, row_desc, context).ok());
    ASSERT_TRUE(expr->open(&state, context, FunctionContext::THREAD_LOCAL).ok());
}

static void open_expr_with_block_budget(const VExprSPtr& expr, VExprContext* context,
                                        int batch_size, int64_t preferred_block_size_bytes) {
    RuntimeState state;
    TQueryOptions query_options;
    query_options.__set_batch_size(batch_size);
    query_options.__set_preferred_block_size_bytes(preferred_block_size_bytes);
    state.set_query_options(query_options);
    RowDescriptor row_desc;
    ASSERT_TRUE(expr->prepare(&state, row_desc, context).ok());
    ASSERT_TRUE(expr->open(&state, context, FunctionContext::THREAD_LOCAL).ok());
}

static const ColumnInt32& get_int_array_values(const ColumnPtr& result) {
    const auto& result_array = assert_cast<const ColumnArray&>(*result);
    const auto& nullable_values = assert_cast<const ColumnNullable&>(*result_array.get_data_ptr());
    return assert_cast<const ColumnInt32&>(nullable_values.get_nested_column());
}

TEST(ArrayMapFunctionTest, IdentityLambdaSharesNestedInputColumn) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto input = make_int_array_column({{1, 2}, {3}});
    const auto& input_array = assert_cast<const ColumnArray&>(*input);
    const IColumn* nested_input = input_array.get_data_ptr().get();

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));
    lambda->add_child(VColumnRef::create_shared(make_column_ref_node(0, "x", int_type)));
    root->add_child(lambda);
    root->add_child(
            std::make_shared<MockColumnExpr>(std::move(input), array_int_type, "input_array"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, 2, result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& result_array = assert_cast<const ColumnArray&>(*result);
    EXPECT_EQ(result_array.get_data_ptr().get(), nested_input);
}

TEST(ArrayMapFunctionTest, LambdaWithConstantCaptureUsesNestedColumnDirectly) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto input = make_int_array_column({{1, 2}, {3}});
    const auto& input_array = assert_cast<const ColumnArray&>(*input);
    const IColumn* nested_input = input_array.get_data_ptr().get();

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));
    bool used_direct_input = false;
    bool kept_capture_const = false;
    auto body = std::make_shared<MockCapturedInputExpr>(int_type, nested_input, &used_direct_input,
                                                        &kept_capture_const);
    body->add_child(make_slot_ref(0, "captured", int_type));
    body->add_child(VColumnRef::create_shared(make_column_ref_node(0, "x", int_type)));
    lambda->add_child(body);
    root->add_child(lambda);
    root->add_child(
            std::make_shared<MockColumnExpr>(std::move(input), array_int_type, "input_array"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    block.insert({ColumnConst::create(make_int_column({10}), 2), int_type, "captured"});

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, block.rows(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(used_direct_input);
    EXPECT_TRUE(kept_capture_const);

    const auto& values = get_int_array_values(result);
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(values.get_element(0), 11);
    EXPECT_EQ(values.get_element(1), 12);
    EXPECT_EQ(values.get_element(2), 13);
}

TEST(ArrayMapFunctionTest, LargeLambdaProducesCorrectResult) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    std::vector<int32_t> input_values(100000);
    for (size_t i = 0; i < input_values.size(); ++i) {
        input_values[i] = static_cast<int32_t>(i);
    }

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));
    auto body = std::make_shared<MockIncrementExpr>(int_type);
    body->add_child(VColumnRef::create_shared(make_column_ref_node(0, "x", int_type)));
    lambda->add_child(body);
    root->add_child(lambda);
    root->add_child(std::make_shared<MockColumnExpr>(
            make_int_array_column({std::move(input_values)}), array_int_type, "input_array"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, 1, result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& values = get_int_array_values(result);
    ASSERT_EQ(values.size(), 100000);
    EXPECT_EQ(values.get_element(0), 1);
    EXPECT_EQ(values.get_element(99999), 100000);
}

TEST(ArrayMapFunctionTest, VariableLengthCaptureUsesLargerBatch) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto string_type = std::make_shared<DataTypeString>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    std::vector<size_t> observed_batch_sizes;

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));
    auto body = std::make_shared<MockBatchSizeExpr>(int_type, &observed_batch_sizes);
    body->add_child(make_slot_ref(0, "captured", string_type));
    lambda->add_child(body);
    root->add_child(lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{1, 2, 3, 4, 5}}),
                                                     array_int_type, "input_array"));

    VExprContext context(root);
    open_expr_with_batch_size(root, &context, 2);

    auto captured = ColumnString::create();
    std::string captured_value(5000, 'a');
    captured->insert_data(captured_value.data(), captured_value.size());
    Block block;
    block.insert({std::move(captured), string_type, "captured"});

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, block.rows(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(observed_batch_sizes.size(), 1);
    EXPECT_EQ(observed_batch_sizes[0], 5);
    EXPECT_EQ(get_int_array_values(result).size(), 5);
}

TEST(ArrayMapFunctionTest, FixedLengthComplexLambdaUsesLargerBatch) {
    constexpr size_t intermediate_count = 100;
    constexpr size_t nested_count = 1000;
    constexpr int outer_batch_size = 256;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    std::vector<size_t> observed_batch_sizes;
    std::vector<int32_t> input_values(nested_count);
    for (size_t i = 0; i < nested_count; ++i) {
        input_values[i] = static_cast<int32_t>(i);
    }

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));
    auto body = std::make_shared<MockGreatestExpr>(int_type, &observed_batch_sizes);
    for (size_t i = 0; i < intermediate_count; ++i) {
        auto increment = std::make_shared<MockIncrementExpr>(int_type);
        increment->add_child(VColumnRef::create_shared(make_column_ref_node(0, "x", int_type)));
        body->add_child(increment);
    }
    lambda->add_child(body);
    root->add_child(lambda);
    root->add_child(std::make_shared<MockColumnExpr>(
            make_int_array_column({std::move(input_values)}), array_int_type, "input_array"));

    VExprContext context(root);
    open_expr_with_batch_size(root, &context, outer_batch_size);

    Block block;
    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, 1, result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(observed_batch_sizes.size(), 1);
    EXPECT_EQ(observed_batch_sizes[0], nested_count);

    const auto& values = get_int_array_values(result);
    ASSERT_EQ(values.size(), nested_count);
    EXPECT_EQ(values.get_element(0), 1);
    EXPECT_EQ(values.get_element(nested_count - 1), nested_count);
}

TEST(ArrayMapFunctionTest, FixedLengthInputsUseLargerBatch) {
    constexpr size_t capture_count = 64;
    constexpr size_t nested_count = 1000;
    constexpr int outer_batch_size = 128;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int64_type = std::make_shared<DataTypeInt64>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    std::vector<size_t> observed_batch_sizes;

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));
    auto body = std::make_shared<MockBatchSizeExpr>(int_type, &observed_batch_sizes);
    for (size_t i = 0; i < capture_count; ++i) {
        body->add_child(
                make_slot_ref(static_cast<int>(i), "captured_" + std::to_string(i), int64_type));
    }
    lambda->add_child(body);
    root->add_child(lambda);
    root->add_child(std::make_shared<MockColumnExpr>(
            make_int_array_column({std::vector<int32_t>(nested_count, 1)}), array_int_type,
            "input_array"));

    VExprContext context(root);
    open_expr_with_batch_size(root, &context, outer_batch_size);

    Block block;
    for (size_t i = 0; i < capture_count; ++i) {
        auto captured = ColumnInt64::create();
        captured->insert_value(static_cast<int64_t>(i));
        block.insert({std::move(captured), int64_type, "captured_" + std::to_string(i)});
    }

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, block.rows(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_EQ(observed_batch_sizes.size(), 1);
    EXPECT_EQ(observed_batch_sizes[0], nested_count);
    EXPECT_EQ(get_int_array_values(result).size(), nested_count);
}

TEST(ArrayMapFunctionTest, FixedLengthInputsUsePreferredBlockSizeBudget) {
    constexpr size_t capture_count = 64;
    constexpr size_t nested_count = 3000;
    constexpr int outer_batch_size = 65535;
    constexpr int64_t preferred_block_size_bytes = 1024 * 1024;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int64_type = std::make_shared<DataTypeInt64>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    std::vector<size_t> observed_batch_sizes;

    const bool old_enable_adaptive_batch_size = config::enable_adaptive_batch_size;
    config::enable_adaptive_batch_size = true;
    Defer restore_adaptive_batch_size {[old_enable_adaptive_batch_size] {
        config::enable_adaptive_batch_size = old_enable_adaptive_batch_size;
    }};

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));
    auto body = std::make_shared<MockBatchSizeExpr>(int_type, &observed_batch_sizes);
    for (size_t i = 0; i < capture_count; ++i) {
        body->add_child(
                make_slot_ref(static_cast<int>(i), "captured_" + std::to_string(i), int64_type));
    }
    lambda->add_child(body);
    root->add_child(lambda);
    root->add_child(std::make_shared<MockColumnExpr>(
            make_int_array_column({std::vector<int32_t>(nested_count, 1)}), array_int_type,
            "input_array"));

    VExprContext context(root);
    open_expr_with_block_budget(root, &context, outer_batch_size, preferred_block_size_bytes);

    Block block;
    for (size_t i = 0; i < capture_count; ++i) {
        auto captured = ColumnInt64::create();
        captured->insert_value(static_cast<int64_t>(i));
        block.insert({std::move(captured), int64_type, "captured_" + std::to_string(i)});
    }

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, block.rows(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    ASSERT_GT(observed_batch_sizes.size(), 1);
    size_t observed_rows = 0;
    for (size_t batch_rows : observed_batch_sizes) {
        EXPECT_LE(batch_rows, preferred_block_size_bytes / (capture_count * sizeof(int64_t)));
        observed_rows += batch_rows;
    }
    EXPECT_EQ(observed_rows, nested_count);
    EXPECT_EQ(get_int_array_values(result).size(), nested_count);
}

TEST(ArrayMapFunctionTest, MultiBatchPreservesCaptureMappingAcrossSelectedArrayRows) {
    constexpr size_t selected_array_size = 300;
    constexpr int outer_batch_size = 64;
    constexpr int lambda_batch_size = outer_batch_size;
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    std::vector<size_t> observed_batch_sizes;

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));
    auto body = std::make_shared<MockAddExpr>(int_type, &observed_batch_sizes);
    body->add_child(make_slot_ref(0, "captured_0", int_type));
    body->add_child(VColumnRef::create_shared(make_column_ref_node(0, "x", int_type)));
    lambda->add_child(body);
    root->add_child(lambda);
    root->add_child(make_slot_ref(1, "input_array", array_int_type));

    VExprContext context(root);
    open_expr_with_batch_size(root, &context, outer_batch_size);

    std::vector<std::vector<int32_t>> input_rows(6);
    input_rows[0] = {-1};
    input_rows[1].resize(selected_array_size);
    for (size_t i = 0; i < selected_array_size; ++i) {
        input_rows[1][i] = static_cast<int32_t>(i);
    }
    input_rows[2] = {-2, -3};
    input_rows[4] = {-4};
    input_rows[5].resize(selected_array_size);
    for (size_t i = 0; i < selected_array_size; ++i) {
        input_rows[5][i] = 1000 + static_cast<int32_t>(i);
    }

    Block block;
    block.insert({make_int_column({100, 10, 200, 30, 300, 50}), int_type, "captured_0"});
    block.insert({make_int_array_column(input_rows), array_int_type, "input_array"});

    Selector selector;
    selector.push_back(1);
    selector.push_back(3);
    selector.push_back(5);

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, &selector, selector.size(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const size_t total_nested_rows = 2 * selected_array_size;
    const size_t expected_full_batches = total_nested_rows / lambda_batch_size;
    const size_t expected_last_batch_size = total_nested_rows % lambda_batch_size;
    ASSERT_EQ(observed_batch_sizes.size(), expected_full_batches + (expected_last_batch_size > 0));
    for (size_t i = 0; i < expected_full_batches; ++i) {
        EXPECT_EQ(observed_batch_sizes[i], lambda_batch_size);
    }
    if (expected_last_batch_size > 0) {
        EXPECT_EQ(observed_batch_sizes.back(), expected_last_batch_size);
    }

    const auto& result_array = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(result_array.size(), 3);
    EXPECT_EQ(result_array.get_offsets()[0], selected_array_size);
    EXPECT_EQ(result_array.get_offsets()[1], selected_array_size);
    EXPECT_EQ(result_array.get_offsets()[2], total_nested_rows);

    const auto& values = get_int_array_values(result);
    ASSERT_EQ(values.size(), total_nested_rows);
    EXPECT_EQ(values.get_element(0), 10);
    EXPECT_EQ(values.get_element(selected_array_size - 1), 309);
    EXPECT_EQ(values.get_element(selected_array_size), 1050);
    EXPECT_EQ(values.get_element(lambda_batch_size - 1),
              10 + static_cast<int32_t>(lambda_batch_size - 1));
    EXPECT_EQ(values.get_element(lambda_batch_size), 10 + lambda_batch_size);
    EXPECT_EQ(values.get_element(total_nested_rows - 1), 1349);
}

TEST(ArrayMapFunctionTest, SparseCapturedColumnUsesColumnNothingForUnusedSlots) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto uint8_type = std::make_shared<DataTypeUInt8>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    constexpr size_t captured_column_position = 999;
    std::vector<int32_t> input_values(1000, 1);

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));
    bool sparse_slots_use_column_nothing = false;
    auto body = std::make_shared<MockSparseCapturedInputExpr>(int_type, captured_column_position,
                                                              &sparse_slots_use_column_nothing);
    body->add_child(make_slot_ref(captured_column_position, "captured", int_type));
    body->add_child(VColumnRef::create_shared(make_column_ref_node(0, "x", int_type)));
    lambda->add_child(body);
    root->add_child(lambda);
    root->add_child(std::make_shared<MockColumnExpr>(
            make_int_array_column({std::move(input_values)}), array_int_type, "input_array"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    for (size_t i = 0; i < captured_column_position; ++i) {
        block.insert({ColumnUInt8::create(1, 0), uint8_type, "unused"});
    }
    block.insert({make_int_column({10}), int_type, "captured"});

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, 1, result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(sparse_slots_use_column_nothing);

    const auto& values = get_int_array_values(result);
    ASSERT_EQ(values.size(), 1000);
    EXPECT_EQ(values.get_element(0), 11);
    EXPECT_EQ(values.get_element(999), 11);
}

TEST(ArrayMapFunctionTest, CapturedColumnExpansionUsesOuterSelector) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));
    auto add = std::make_shared<MockAddExpr>(int_type);
    add->add_child(make_slot_ref(0, "captured", int_type));
    add->add_child(VColumnRef::create_shared(make_column_ref_node(0, "x", int_type)));
    lambda->add_child(add);
    root->add_child(lambda);
    root->add_child(make_slot_ref(1, "input_array", array_int_type));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    block.insert({make_int_column({10, 20, 30, 40}), int_type, "captured"});
    block.insert(
            {make_int_array_column({{1, 2}, {3}, {}, {7, 8, 9}}), array_int_type, "input_array"});
    Selector selector;
    selector.push_back(1);
    selector.push_back(3);

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, &selector, selector.size(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& result_array = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(result_array.size(), 2);
    ASSERT_EQ(result_array.get_offsets()[0], 1);
    ASSERT_EQ(result_array.get_offsets()[1], 4);
    const auto& values = get_int_array_values(result);
    ASSERT_EQ(values.size(), 4);
    EXPECT_EQ(values.get_element(0), 23);
    EXPECT_EQ(values.get_element(1), 47);
    EXPECT_EQ(values.get_element(2), 48);
    EXPECT_EQ(values.get_element(3), 49);
}

TEST(ArrayMapFunctionTest, NestedLambdaWithSameArgumentNameUsesInnerScope) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);

    auto root =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 2));
    auto outer_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(array_int_type, {"x"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));

    // This mirrors array_map(x -> array_map(x -> x, x), nested_array). Both
    // lambda arguments are named "x"; only their scopes and element types are
    // different. The non-ordinal column ids cover FE plans where a lambda
    // argument ColumnRef id is not the same as its argument position.
    auto inner_x = VColumnRef::create_shared(make_column_ref_node(2, "x", int_type));
    auto outer_x = VColumnRef::create_shared(make_column_ref_node(1, "x", array_int_type));

    inner_lambda->add_child(inner_x);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(outer_x);
    outer_lambda->add_child(inner_call);
    root->add_child(outer_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_nested_int_array_column(),
                                                     nested_array_int_type, "nested_array"));

    VExprContext context(root);
    open_expr(root, &context);

    // Keep one ordinary input column before the array_map argument. The test
    // verifies nested lambda gap calculation when lambda blocks need to preserve
    // existing input columns as well as append current lambda arguments.
    Block block;
    block.insert({make_int_column({10, 20}), int_type, "ordinary_input"});

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, block.rows(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& outer_array = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(outer_array.size(), 2);
    ASSERT_EQ(outer_array.get_offsets()[0], 2);
    ASSERT_EQ(outer_array.get_offsets()[1], 3);

    const auto* nullable_inner_arrays =
            check_and_get_column<ColumnNullable>(&outer_array.get_data());
    ASSERT_NE(nullable_inner_arrays, nullptr);
    for (size_t i = 0; i < nullable_inner_arrays->size(); ++i) {
        EXPECT_FALSE(nullable_inner_arrays->is_null_at(i));
    }

    const auto& inner_arrays =
            assert_cast<const ColumnArray&>(nullable_inner_arrays->get_nested_column());
    ASSERT_EQ(inner_arrays.size(), 3);
    ASSERT_EQ(inner_arrays.get_offsets()[0], 2);
    ASSERT_EQ(inner_arrays.get_offsets()[1], 3);
    ASSERT_EQ(inner_arrays.get_offsets()[2], 5);

    const auto* nullable_values = check_and_get_column<ColumnNullable>(&inner_arrays.get_data());
    ASSERT_NE(nullable_values, nullptr);
    for (size_t i = 0; i < nullable_values->size(); ++i) {
        EXPECT_FALSE(nullable_values->is_null_at(i));
    }

    const auto& values = assert_cast<const ColumnInt32&>(nullable_values->get_nested_column());
    ASSERT_EQ(values.size(), 5);
    EXPECT_EQ(values.get_element(0), 1);
    EXPECT_EQ(values.get_element(1), 2);
    EXPECT_EQ(values.get_element(2), 3);
    EXPECT_EQ(values.get_element(3), 4);
    EXPECT_EQ(values.get_element(4), 5);
}

TEST(ArrayMapFunctionTest, NamedLambdaWithFewerArgumentsThanArraysUsesDeclaredBindings) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 3));
    auto lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));

    // This mirrors a BE-compatible plan shape like array_map(x -> x, arr1, arr2).
    // Only x is part of the lambda binding frame; the extra array input is still
    // materialized for size/offset validation but must not require a lambda name.
    auto x = VColumnRef::create_shared(make_column_ref_node(5, "x", int_type));

    lambda->add_child(x);
    root->add_child(lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{10, 20}, {30}}),
                                                     array_int_type, "arr1"));
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{1, 2}, {3}}),
                                                     array_int_type, "arr2"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, 2, result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& result_array = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(result_array.size(), 2);
    ASSERT_EQ(result_array.get_offsets()[0], 2);
    ASSERT_EQ(result_array.get_offsets()[1], 3);
    const auto* nullable_values = check_and_get_column<ColumnNullable>(&result_array.get_data());
    ASSERT_NE(nullable_values, nullptr);
    const auto& values = assert_cast<const ColumnInt32&>(nullable_values->get_nested_column());
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(values.get_element(0), 10);
    EXPECT_EQ(values.get_element(1), 20);
    EXPECT_EQ(values.get_element(2), 30);
}

TEST(ArrayMapFunctionTest, NestedArraySortInsideArrayMapSkipsArrayMapArgumentInference) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int8_type = std::make_shared<DataTypeInt8>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nullable_array_int_type = std::make_shared<DataTypeNullable>(array_int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);

    auto root =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 2));
    auto outer_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(array_int_type, {"a"}));
    auto sort_call = VLambdaFunctionCallExpr::create_shared(
            make_lambda_call_node(nullable_array_int_type, 2, "array_sort"));
    auto sort_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int8_type, {"a", "b"}));
    auto compare = std::make_shared<MockCompareExpr>(int8_type);

    // This mirrors array_map(a -> array_sort((a, b) -> a - b, a), nested_array).
    // FE represents the second comparator argument by cloning the first
    // ColumnRef and only changing column_id to 1, so both comparator ColumnRefs
    // can have the same name. array_sort must keep its comparator arguments
    // position-based instead of using array_map's name-based binding.
    auto sort_a = VColumnRef::create_shared(make_column_ref_node(0, "a", int_type));
    auto sort_b = VColumnRef::create_shared(make_column_ref_node(1, "a", int_type));
    auto outer_a = VColumnRef::create_shared(make_column_ref_node(1, "a", array_int_type));

    compare->add_child(sort_a);
    compare->add_child(sort_b);
    sort_lambda->add_child(compare);
    sort_call->add_child(sort_lambda);
    sort_call->add_child(outer_a);
    outer_lambda->add_child(sort_call);
    root->add_child(outer_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_nested_unsorted_int_array_column(),
                                                     nested_array_int_type, "nested_array"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    block.insert({make_int_column({10, 20}), int_type, "ordinary_input"});

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, block.rows(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& outer_array = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(outer_array.size(), 2);
    ASSERT_EQ(outer_array.get_offsets()[0], 2);
    ASSERT_EQ(outer_array.get_offsets()[1], 3);

    const auto* nullable_inner_arrays =
            check_and_get_column<ColumnNullable>(&outer_array.get_data());
    ASSERT_NE(nullable_inner_arrays, nullptr);

    const auto& inner_arrays =
            assert_cast<const ColumnArray&>(nullable_inner_arrays->get_nested_column());
    ASSERT_EQ(inner_arrays.size(), 3);
    ASSERT_EQ(inner_arrays.get_offsets()[0], 2);
    ASSERT_EQ(inner_arrays.get_offsets()[1], 3);
    ASSERT_EQ(inner_arrays.get_offsets()[2], 5);

    const auto* nullable_values = check_and_get_column<ColumnNullable>(&inner_arrays.get_data());
    ASSERT_NE(nullable_values, nullptr);
    const auto& values = assert_cast<const ColumnInt32&>(nullable_values->get_nested_column());
    ASSERT_EQ(values.size(), 5);
    EXPECT_EQ(values.get_element(0), 1);
    EXPECT_EQ(values.get_element(1), 2);
    EXPECT_EQ(values.get_element(2), 3);
    EXPECT_EQ(values.get_element(3), 4);
    EXPECT_EQ(values.get_element(4), 5);
}

TEST(ArrayMapFunctionTest, NestedArraySortComparatorCapturingOuterArgumentReturnsError) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int8_type = std::make_shared<DataTypeInt8>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);

    auto root =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 2));
    auto outer_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(array_int_type, {"i"}));
    auto sort_call = VLambdaFunctionCallExpr::create_shared(
            make_lambda_call_node(array_int_type, 2, "array_sort"));
    auto sort_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int8_type, {"x", "y"}));
    auto add = std::make_shared<MockAddExpr>(int_type);

    // This mirrors:
    //   array_map(i -> array_sort((x, y) -> x + i, arr2), arr1)
    // array_sort's comparator executes against a two-column temporary block that
    // only contains x and y. Capturing i from the outer array_map would otherwise
    // be silently resolved by column id as x.
    auto sort_x = VColumnRef::create_shared(make_column_ref_node(0, "x", int_type));
    auto outer_i = VColumnRef::create_shared(make_column_ref_node(0, "i", int_type));

    add->add_child(sort_x);
    add->add_child(outer_i);
    sort_lambda->add_child(add);
    sort_call->add_child(sort_lambda);
    sort_call->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{2, 1}, {4, 3}}),
                                                          array_int_type, "arr2"));
    outer_lambda->add_child(sort_call);
    root->add_child(outer_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{10, 20}, {30}}),
                                                     array_int_type, "arr1"));

    VExprContext context(root);
    RuntimeState state;
    RowDescriptor row_desc;
    auto status = root->prepare(&state, row_desc, &context);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(
            status.to_string().find("array_sort comparator only supports its own lambda arguments"),
            std::string::npos);
    EXPECT_NE(status.to_string().find("captured column ref 'i'"), std::string::npos);
}

TEST(ArrayMapFunctionTest,
     NestedArraySortComparatorNestedLambdaCapturingComparatorArgumentReturnsError) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int8_type = std::make_shared<DataTypeInt8>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(
            make_lambda_call_node(array_int_type, 2, "array_sort"));
    auto sort_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int8_type, {"x", "y"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"z"}));
    auto add = std::make_shared<MockAddExpr>(int_type);

    // This mirrors:
    //   array_sort((x, y) -> array_map(z -> z + x, [1, 2]), arr)
    // The inner array_map's lambda frame cannot see array_sort comparator-local x/y because
    // array_sort intentionally uses a position-based anonymous comparator frame.
    auto inner_z = VColumnRef::create_shared(make_column_ref_node(0, "z", int_type));
    auto comparator_x = VColumnRef::create_shared(make_column_ref_node(0, "x", int_type));

    add->add_child(inner_z);
    add->add_child(comparator_x);
    inner_lambda->add_child(add);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    sort_lambda->add_child(inner_call);
    root->add_child(sort_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{2, 1}, {4, 3}}),
                                                     array_int_type, "arr"));

    VExprContext context(root);
    RuntimeState state;
    RowDescriptor row_desc;
    auto status = root->prepare(&state, row_desc, &context);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(
                      "array_sort comparator does not support nested lambda capturing comparator "
                      "argument 'x'"),
              std::string::npos);
}

TEST(ArrayMapFunctionTest, NestedArraySortComparatorNestedLambdaCanShadowComparatorArgument) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int8_type = std::make_shared<DataTypeInt8>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(
            make_lambda_call_node(array_int_type, 2, "array_sort"));
    auto sort_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int8_type, {"x", "y"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"x"}));

    // This mirrors:
    //   array_sort((x, y) -> array_map(x -> x, [1, 2]), arr)
    // The inner array_map argument named x shadows the array_sort comparator argument x.
    auto inner_x = VColumnRef::create_shared(make_column_ref_node(0, "x", int_type));

    inner_lambda->add_child(inner_x);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    sort_lambda->add_child(inner_call);
    root->add_child(sort_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{2, 1}, {4, 3}}),
                                                     array_int_type, "arr"));

    VExprContext context(root);
    RuntimeState state;
    RowDescriptor row_desc;
    auto status = root->prepare(&state, row_desc, &context);
    EXPECT_TRUE(status.ok()) << status.to_string();
}

TEST(ArrayMapFunctionTest,
     NestedArraySortComparatorNestedLambdaCapturingNonComparatorColumnReturnsError) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int8_type = std::make_shared<DataTypeInt8>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(
            make_lambda_call_node(array_int_type, 2, "array_sort"));
    auto sort_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int8_type, {"x", "y"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"z"}));
    auto add = std::make_shared<MockAddExpr>(int_type);

    // This mirrors:
    //   array_sort((x, y) -> array_map(z -> z + outer_col, [1, 2]), arr)
    // Only the nested lambda's own arguments are visible in nested lambda bodies.
    auto inner_z = VColumnRef::create_shared(make_column_ref_node(0, "z", int_type));
    auto outer_col = VColumnRef::create_shared(make_column_ref_node(2, "outer_col", int_type));

    add->add_child(inner_z);
    add->add_child(outer_col);
    inner_lambda->add_child(add);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    sort_lambda->add_child(inner_call);
    root->add_child(sort_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{2, 1}, {4, 3}}),
                                                     array_int_type, "arr"));

    VExprContext context(root);
    RuntimeState state;
    RowDescriptor row_desc;
    auto status = root->prepare(&state, row_desc, &context);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(
                      "array_sort comparator only supports nested lambda arguments inside nested "
                      "lambda bodies, but found captured column ref 'outer_col'"),
              std::string::npos);
}

TEST(ArrayMapFunctionTest, NestedArraySortComparatorNestedLambdaCapturingSlotRefReturnsError) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int8_type = std::make_shared<DataTypeInt8>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(
            make_lambda_call_node(array_int_type, 2, "array_sort"));
    auto sort_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int8_type, {"x", "y"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"z"}));
    auto add = std::make_shared<MockAddExpr>(int_type);

    // This mirrors:
    //   array_sort((x, y) -> array_map(z -> z + k1, [1, 2]), arr)
    // SlotRefs from outside the nested lambda are not available inside array_sort's comparator.
    auto inner_z = VColumnRef::create_shared(make_column_ref_node(0, "z", int_type));
    auto k1 = make_slot_ref(0, "k1", int_type);

    add->add_child(inner_z);
    add->add_child(k1);
    inner_lambda->add_child(add);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    sort_lambda->add_child(inner_call);
    root->add_child(sort_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{2, 1}, {4, 3}}),
                                                     array_int_type, "arr"));

    VExprContext context(root);
    RuntimeState state;
    RowDescriptor row_desc;
    auto status = root->prepare(&state, row_desc, &context);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(
                      "array_sort comparator only supports nested lambda arguments inside nested "
                      "lambda bodies, but found captured slot ref 'k1'"),
              std::string::npos);
}

TEST(ArrayMapFunctionTest,
     NestedArraySortComparatorNestedLambdaWithoutArgumentMetadataReturnsError) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int8_type = std::make_shared<DataTypeInt8>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(
            make_lambda_call_node(array_int_type, 2, "array_sort"));
    auto sort_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int8_type, {"x", "y"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(
            make_lambda_expr_node(int_type, {"z"}, false /*set_argument_names*/));

    // This mirrors an old FE plan where the nested array_map lambda has no argument metadata.
    auto inner_z = VColumnRef::create_shared(make_column_ref_node(0, "z", int_type));

    inner_lambda->add_child(inner_z);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    sort_lambda->add_child(inner_call);
    root->add_child(sort_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{2, 1}, {4, 3}}),
                                                     array_int_type, "arr"));

    VExprContext context(root);
    RuntimeState state;
    RowDescriptor row_desc;
    auto status = root->prepare(&state, row_desc, &context);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(
                      "Cannot validate nested lambda capture in array_sort comparator without "
                      "lambda metadata"),
              std::string::npos);
}

TEST(ArrayMapFunctionTest,
     NestedArraySortComparatorTwoLevelNestedLambdaCanCaptureOuterNestedArgument) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int8_type = std::make_shared<DataTypeInt8>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(
            make_lambda_call_node(array_int_type, 2, "array_sort"));
    auto sort_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int8_type, {"x", "y"}));
    auto outer_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 2));
    auto outer_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(array_int_type, {"z"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"q"}));
    auto add = std::make_shared<MockAddExpr>(int_type);

    // This mirrors:
    //   array_sort((x, y) -> array_map(z -> array_map(q -> q + z, [1, 2]), [3, 4]), arr)
    // q is local to the inner array_map, while z is declared by the enclosing nested array_map.
    auto inner_q = VColumnRef::create_shared(make_column_ref_node(0, "q", int_type));
    auto outer_z = VColumnRef::create_shared(make_column_ref_node(0, "z", int_type));

    add->add_child(inner_q);
    add->add_child(outer_z);
    inner_lambda->add_child(add);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    outer_lambda->add_child(inner_call);
    outer_call->add_child(outer_lambda);
    outer_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{3, 4}}),
                                                                array_int_type, "outer_array"));
    sort_lambda->add_child(outer_call);
    root->add_child(sort_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{2, 1}, {4, 3}}),
                                                     array_int_type, "arr"));

    VExprContext context(root);
    RuntimeState state;
    RowDescriptor row_desc;
    auto status = root->prepare(&state, row_desc, &context);
    EXPECT_TRUE(status.ok()) << status.to_string();
}

TEST(ArrayMapFunctionTest,
     NestedArraySortComparatorNestedLambdaCallWithoutArgumentMetadataReturnsError) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto int8_type = std::make_shared<DataTypeInt8>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(
            make_lambda_call_node(array_int_type, 2, "array_sort"));
    auto sort_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int8_type, {"x", "y"}));
    auto outer_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 2));
    auto outer_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(array_int_type, {"z"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(
            make_lambda_expr_node(int_type, {"q"}, false /*set_argument_names*/));
    auto add = std::make_shared<MockAddExpr>(int_type);

    // This mirrors an old FE plan where a nested lambda call under another nested lambda has no
    // argument metadata.
    auto inner_q = VColumnRef::create_shared(make_column_ref_node(0, "q", int_type));
    auto outer_z = VColumnRef::create_shared(make_column_ref_node(0, "z", int_type));

    add->add_child(inner_q);
    add->add_child(outer_z);
    inner_lambda->add_child(add);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    outer_lambda->add_child(inner_call);
    outer_call->add_child(outer_lambda);
    outer_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{3, 4}}),
                                                                array_int_type, "outer_array"));
    sort_lambda->add_child(outer_call);
    root->add_child(sort_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{2, 1}, {4, 3}}),
                                                     array_int_type, "arr"));

    VExprContext context(root);
    RuntimeState state;
    RowDescriptor row_desc;
    auto status = root->prepare(&state, row_desc, &context);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(
                      "Cannot validate nested lambda capture in array_sort comparator without "
                      "lambda metadata"),
              std::string::npos);
}

TEST(ArrayMapFunctionTest, NestedLambdaCapturesOuterSlotRefFromInnerBody) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);

    auto root =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 2));
    auto outer_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(array_int_type, {"x"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"y"}));
    auto add = std::make_shared<MockAddExpr>(int_type);

    // This mirrors:
    //   array_map(x -> array_map(y -> y + k1, [1, 2]), arr)
    // The outer array_map body does not reference k1 directly. The BE still
    // needs to carry the full input block through the outer lambda block and
    // inherit it into the inner lambda block.
    auto inner_y = VColumnRef::create_shared(make_column_ref_node(0, "y", int_type));
    auto k1 = make_slot_ref(0, "k1", int_type);

    add->add_child(inner_y);
    add->add_child(k1);
    inner_lambda->add_child(add);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    outer_lambda->add_child(inner_call);
    root->add_child(outer_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{10, 20}, {30}}),
                                                     array_int_type, "outer_array"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    block.insert({make_int_column({100, 200}), int_type, "k1"});

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, block.rows(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& outer_array = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(outer_array.size(), 2);
    ASSERT_EQ(outer_array.get_offsets()[0], 2);
    ASSERT_EQ(outer_array.get_offsets()[1], 3);

    const auto* nullable_inner_arrays =
            check_and_get_column<ColumnNullable>(&outer_array.get_data());
    ASSERT_NE(nullable_inner_arrays, nullptr);
    const auto& inner_arrays =
            assert_cast<const ColumnArray&>(nullable_inner_arrays->get_nested_column());
    ASSERT_EQ(inner_arrays.size(), 3);
    ASSERT_EQ(inner_arrays.get_offsets()[0], 2);
    ASSERT_EQ(inner_arrays.get_offsets()[1], 4);
    ASSERT_EQ(inner_arrays.get_offsets()[2], 6);

    const auto* nullable_values = check_and_get_column<ColumnNullable>(&inner_arrays.get_data());
    ASSERT_NE(nullable_values, nullptr);
    const auto& values = assert_cast<const ColumnInt32&>(nullable_values->get_nested_column());
    ASSERT_EQ(values.size(), 6);
    EXPECT_EQ(values.get_element(0), 101);
    EXPECT_EQ(values.get_element(1), 102);
    EXPECT_EQ(values.get_element(2), 101);
    EXPECT_EQ(values.get_element(3), 102);
    EXPECT_EQ(values.get_element(4), 201);
    EXPECT_EQ(values.get_element(5), 202);
}

TEST(ArrayMapFunctionTest, LegacySingleLambdaWithoutArgumentMetadataUsesColumnId) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto lambda = VLambdaFunctionExpr::create_shared(
            make_lambda_expr_node(int_type, {"x"}, false /*set_argument_names*/));

    // Simulate an old FE plan without lambda_argument_names. Single-layer
    // lambda can still use the ColumnRef id to bind argument position 0.
    auto x = VColumnRef::create_shared(make_column_ref_node(0, "legacy_x", int_type));
    lambda->add_child(x);
    root->add_child(lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{10, 20}}),
                                                     array_int_type, "array"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, 1, result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& result_array = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(result_array.size(), 1);
    ASSERT_EQ(result_array.get_offsets()[0], 2);
    const auto* nullable_values = check_and_get_column<ColumnNullable>(&result_array.get_data());
    ASSERT_NE(nullable_values, nullptr);
    const auto& values = assert_cast<const ColumnInt32&>(nullable_values->get_nested_column());
    ASSERT_EQ(values.size(), 2);
    EXPECT_EQ(values.get_element(0), 10);
    EXPECT_EQ(values.get_element(1), 20);
}

TEST(ArrayMapFunctionTest, LegacyNestedLambdaWithoutArgumentMetadataReturnsError) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);

    auto root =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 2));
    auto outer_lambda = VLambdaFunctionExpr::create_shared(
            make_lambda_expr_node(array_int_type, {"x"}, false /*set_argument_names*/));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(
            make_lambda_expr_node(int_type, {"y"}, false /*set_argument_names*/));
    auto subtract = std::make_shared<MockSubtractExpr>(int_type);

    auto outer_x = VColumnRef::create_shared(make_column_ref_node(0, "x", int_type));
    auto inner_y = VColumnRef::create_shared(make_column_ref_node(0, "y", int_type));

    subtract->add_child(outer_x);
    subtract->add_child(inner_y);
    inner_lambda->add_child(subtract);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    outer_lambda->add_child(inner_call);
    root->add_child(outer_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{10, 20}}),
                                                     array_int_type, "outer_array"));

    VExprContext context(root);
    RuntimeState state;
    RowDescriptor row_desc;
    auto status = root->prepare(&state, row_desc, &context);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find(
                      "Cannot resolve nested lambda argument without lambda metadata"),
              std::string::npos);
}

TEST(ArrayMapFunctionTest, NestedLambdaUsesOuterArgumentsAndInputSlotRefArray) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);

    auto root =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 3));
    auto outer_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(array_int_type, {"x", "y"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"z"}));
    auto subtract = std::make_shared<MockSubtractExpr>(int_type);
    auto add = std::make_shared<MockAddExpr>(int_type);
    auto multiply = std::make_shared<MockMultiplyExpr>(int_type);

    // This mirrors:
    //   array_map((x, y) -> array_map(z -> (y - z) * (x + z), arr3), arr1, arr2)
    // arr3 is a sparse ordinary input SlotRef. Each lambda block must keep required
    // input positions before appending its own arguments, so the inner array_map can
    // use arr3 while resolving x/y/z from the nested lambda context by name.
    auto y_for_subtract = VColumnRef::create_shared(make_column_ref_node(10, "y", int_type));
    auto z_for_subtract = VColumnRef::create_shared(make_column_ref_node(0, "z", int_type));
    auto x_for_add = VColumnRef::create_shared(make_column_ref_node(9, "x", int_type));
    auto z_for_add = VColumnRef::create_shared(make_column_ref_node(0, "z", int_type));
    auto arr3 = make_slot_ref(4, "arr3", array_int_type);

    subtract->add_child(y_for_subtract);
    subtract->add_child(z_for_subtract);
    add->add_child(x_for_add);
    add->add_child(z_for_add);
    multiply->add_child(subtract);
    multiply->add_child(add);
    inner_lambda->add_child(multiply);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(arr3);
    outer_lambda->add_child(inner_call);
    root->add_child(outer_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{10, 20}, {30}}),
                                                     array_int_type, "arr1"));
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{100, 200}, {300}}),
                                                     array_int_type, "arr2"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    block.insert({make_int_column({0, 0}), int_type, "unused0"});
    block.insert({make_int_column({1, 1}), int_type, "unused1"});
    block.insert({make_int_column({2, 2}), int_type, "unused2"});
    block.insert({make_int_column({3, 3}), int_type, "unused3"});
    block.insert({make_int_array_column({{1, 2}, {3}}), array_int_type, "arr3"});

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, block.rows(), result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& outer_array = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(outer_array.size(), 2);
    ASSERT_EQ(outer_array.get_offsets()[0], 2);
    ASSERT_EQ(outer_array.get_offsets()[1], 3);

    const auto* nullable_inner_arrays =
            check_and_get_column<ColumnNullable>(&outer_array.get_data());
    ASSERT_NE(nullable_inner_arrays, nullptr);
    const auto& inner_arrays =
            assert_cast<const ColumnArray&>(nullable_inner_arrays->get_nested_column());
    ASSERT_EQ(inner_arrays.size(), 3);
    ASSERT_EQ(inner_arrays.get_offsets()[0], 2);
    ASSERT_EQ(inner_arrays.get_offsets()[1], 4);
    ASSERT_EQ(inner_arrays.get_offsets()[2], 5);

    const auto* nullable_values = check_and_get_column<ColumnNullable>(&inner_arrays.get_data());
    ASSERT_NE(nullable_values, nullptr);
    const auto& values = assert_cast<const ColumnInt32&>(nullable_values->get_nested_column());
    ASSERT_EQ(values.size(), 5);
    EXPECT_EQ(values.get_element(0), 1089);
    EXPECT_EQ(values.get_element(1), 1176);
    EXPECT_EQ(values.get_element(2), 4179);
    EXPECT_EQ(values.get_element(3), 4356);
    EXPECT_EQ(values.get_element(4), 9801);
}

TEST(ArrayMapFunctionTest, ThreeLevelNestedLambdaCapturesAllOuterArguments) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);
    auto three_level_array_int_type = std::make_shared<DataTypeArray>(nested_array_int_type);

    auto root = VLambdaFunctionCallExpr::create_shared(
            make_lambda_call_node(three_level_array_int_type, 2));
    auto outer_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(nested_array_int_type, {"x"}));
    auto middle_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 2));
    auto middle_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(array_int_type, {"y"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"z"}));
    auto add_zy = std::make_shared<MockAddExpr>(int_type);
    auto add_zyx = std::make_shared<MockAddExpr>(int_type);

    // This mirrors:
    //   array_map(x -> array_map(y -> array_map(z -> z + y + x, [1, 2]),
    //                           [10, 20]),
    //             [100])
    // It verifies LambdaExecutionContext works as a stack: the innermost z
    // shadows nothing, y is resolved from the middle frame, and x is resolved
    // from the outer frame.
    auto z = VColumnRef::create_shared(make_column_ref_node(0, "z", int_type));
    auto y = VColumnRef::create_shared(make_column_ref_node(7, "y", int_type));
    auto x = VColumnRef::create_shared(make_column_ref_node(9, "x", int_type));

    add_zy->add_child(z);
    add_zy->add_child(y);
    add_zyx->add_child(add_zy);
    add_zyx->add_child(x);
    inner_lambda->add_child(add_zyx);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    middle_lambda->add_child(inner_call);
    middle_call->add_child(middle_lambda);
    middle_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{10, 20}}),
                                                                 array_int_type, "middle_array"));
    outer_lambda->add_child(middle_call);
    root->add_child(outer_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{100}}), array_int_type,
                                                     "outer_array"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;
    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, 1, result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& level3_arrays = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(level3_arrays.size(), 1);
    ASSERT_EQ(level3_arrays.get_offsets()[0], 1);

    const auto* nullable_level2_arrays =
            check_and_get_column<ColumnNullable>(&level3_arrays.get_data());
    ASSERT_NE(nullable_level2_arrays, nullptr);
    const auto& level2_arrays =
            assert_cast<const ColumnArray&>(nullable_level2_arrays->get_nested_column());
    ASSERT_EQ(level2_arrays.size(), 1);
    ASSERT_EQ(level2_arrays.get_offsets()[0], 2);

    const auto* nullable_level1_arrays =
            check_and_get_column<ColumnNullable>(&level2_arrays.get_data());
    ASSERT_NE(nullable_level1_arrays, nullptr);
    const auto& level1_arrays =
            assert_cast<const ColumnArray&>(nullable_level1_arrays->get_nested_column());
    ASSERT_EQ(level1_arrays.size(), 2);
    ASSERT_EQ(level1_arrays.get_offsets()[0], 2);
    ASSERT_EQ(level1_arrays.get_offsets()[1], 4);

    const auto* nullable_values = check_and_get_column<ColumnNullable>(&level1_arrays.get_data());
    ASSERT_NE(nullable_values, nullptr);
    const auto& values = assert_cast<const ColumnInt32&>(nullable_values->get_nested_column());
    ASSERT_EQ(values.size(), 4);
    EXPECT_EQ(values.get_element(0), 111);
    EXPECT_EQ(values.get_element(1), 112);
    EXPECT_EQ(values.get_element(2), 121);
    EXPECT_EQ(values.get_element(3), 122);
}

TEST(ArrayMapFunctionTest, NestedLambdaCapturesOuterArgumentWithSameElementType) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);

    auto root =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 2));
    auto outer_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(array_int_type, {"x"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"y"}));
    auto subtract = std::make_shared<MockSubtractExpr>(int_type);

    // This mirrors array_map(x -> array_map(y -> y - x, [1, 2]), [10, 20]).
    // The captured outer x and the inner y both use INT type, so only names
    // plus lambda scope can disambiguate them. The non-ordinal id of outer x
    // covers FE plans where lambda argument ColumnRef ids are not 0-based.
    auto outer_x = VColumnRef::create_shared(make_column_ref_node(1, "x", int_type));
    auto inner_y = VColumnRef::create_shared(make_column_ref_node(0, "y", int_type));

    subtract->add_child(inner_y);
    subtract->add_child(outer_x);
    inner_lambda->add_child(subtract);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    outer_lambda->add_child(inner_call);
    root->add_child(outer_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{10, 20}}),
                                                     array_int_type, "outer_array"));

    VExprContext context(root);
    open_expr(root, &context);

    // No ordinary input column is needed by this expression. The outer lambda
    // block only appends its own x argument, while the inner lambda resolves y
    // from the nearest frame and x from the outer frame.
    Block block;

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, 1, result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& outer_array = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(outer_array.size(), 1);
    ASSERT_EQ(outer_array.get_offsets()[0], 2);

    const auto* nullable_inner_arrays =
            check_and_get_column<ColumnNullable>(&outer_array.get_data());
    ASSERT_NE(nullable_inner_arrays, nullptr);
    const auto& inner_arrays =
            assert_cast<const ColumnArray&>(nullable_inner_arrays->get_nested_column());
    ASSERT_EQ(inner_arrays.size(), 2);
    ASSERT_EQ(inner_arrays.get_offsets()[0], 2);
    ASSERT_EQ(inner_arrays.get_offsets()[1], 4);

    const auto* nullable_values = check_and_get_column<ColumnNullable>(&inner_arrays.get_data());
    ASSERT_NE(nullable_values, nullptr);
    const auto& values = assert_cast<const ColumnInt32&>(nullable_values->get_nested_column());
    ASSERT_EQ(values.size(), 4);
    EXPECT_EQ(values.get_element(0), -9);
    EXPECT_EQ(values.get_element(1), -8);
    EXPECT_EQ(values.get_element(2), -19);
    EXPECT_EQ(values.get_element(3), -18);
}

TEST(ArrayMapFunctionTest, NestedLambdaCapturesOuterArgumentBeforeInnerArgument) {
    auto int_type = std::make_shared<DataTypeInt32>();
    auto array_int_type = std::make_shared<DataTypeArray>(int_type);
    auto nested_array_int_type = std::make_shared<DataTypeArray>(array_int_type);

    auto root =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(nested_array_int_type, 2));
    auto outer_lambda =
            VLambdaFunctionExpr::create_shared(make_lambda_expr_node(array_int_type, {"x"}));
    auto inner_call =
            VLambdaFunctionCallExpr::create_shared(make_lambda_call_node(array_int_type, 2));
    auto inner_lambda = VLambdaFunctionExpr::create_shared(make_lambda_expr_node(int_type, {"y"}));
    auto subtract = std::make_shared<MockSubtractExpr>(int_type);

    // This mirrors array_map(x -> array_map(y -> x - y, [1, 2]), [10, 20]).
    // The outer x and inner y both use INT type. FE-provided lambda argument
    // names must make x bind to the outer lambda even when x is visited before
    // y in the inner lambda body. The non-ordinal id of outer x covers FE plans
    // where lambda argument ColumnRef ids are not 0-based.
    auto outer_x = VColumnRef::create_shared(make_column_ref_node(1, "x", int_type));
    auto inner_y = VColumnRef::create_shared(make_column_ref_node(0, "y", int_type));

    subtract->add_child(outer_x);
    subtract->add_child(inner_y);
    inner_lambda->add_child(subtract);
    inner_call->add_child(inner_lambda);
    inner_call->add_child(std::make_shared<MockConstColumnExpr>(make_int_array_column({{1, 2}}),
                                                                array_int_type, "inner_array"));
    outer_lambda->add_child(inner_call);
    root->add_child(outer_lambda);
    root->add_child(std::make_shared<MockColumnExpr>(make_int_array_column({{10, 20}}),
                                                     array_int_type, "outer_array"));

    VExprContext context(root);
    open_expr(root, &context);

    Block block;

    ColumnPtr result;
    auto status = root->execute_column(&context, &block, nullptr, 1, result);
    ASSERT_TRUE(status.ok()) << status.to_string();

    const auto& outer_array = assert_cast<const ColumnArray&>(*result);
    ASSERT_EQ(outer_array.size(), 1);
    ASSERT_EQ(outer_array.get_offsets()[0], 2);

    const auto* nullable_inner_arrays =
            check_and_get_column<ColumnNullable>(&outer_array.get_data());
    ASSERT_NE(nullable_inner_arrays, nullptr);
    const auto& inner_arrays =
            assert_cast<const ColumnArray&>(nullable_inner_arrays->get_nested_column());
    ASSERT_EQ(inner_arrays.size(), 2);
    ASSERT_EQ(inner_arrays.get_offsets()[0], 2);
    ASSERT_EQ(inner_arrays.get_offsets()[1], 4);

    const auto* nullable_values = check_and_get_column<ColumnNullable>(&inner_arrays.get_data());
    ASSERT_NE(nullable_values, nullptr);
    const auto& values = assert_cast<const ColumnInt32&>(nullable_values->get_nested_column());
    ASSERT_EQ(values.size(), 4);
    EXPECT_EQ(values.get_element(0), 9);
    EXPECT_EQ(values.get_element(1), 8);
    EXPECT_EQ(values.get_element(2), 19);
    EXPECT_EQ(values.get_element(3), 18);
}

} // namespace doris
