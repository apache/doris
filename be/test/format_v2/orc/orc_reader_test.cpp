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

#include "format_v2/orc/orc_reader.h"

#include <cctz/time_zone.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <orc/OrcFile.hh>
#include <set>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_date.h"
#include "core/data_type/data_type_date_time.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_varbinary.h"
#include "core/data_type/primitive_type.h"
#include "core/value/timestamptz_value.h"
#include "exprs/create_predicate_function.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vdirect_in_predicate.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format/orc/orc_memory_stream_test.h"
#include "format_v2/expr/cast.h"
#include "format_v2/expr/delete_predicate.h"
#include "format_v2/file_reader.h"
#include "gen_cpp/Types_types.h"
#include "io/io_common.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/segment/condition_cache.h"
#include "storage/utils.h"

namespace doris {
namespace {

std::filesystem::path unique_test_dir(std::string_view prefix) {
    static std::atomic<uint64_t> next_dir_id {0};
    std::string name(prefix);
    if (const auto* test_info = testing::UnitTest::GetInstance()->current_test_info();
        test_info != nullptr) {
        name += "_";
        name += test_info->test_suite_name();
        name += "_";
        name += test_info->name();
    }
    name += "_";
    name += std::to_string(getpid());
    name += "_";
    name += std::to_string(next_dir_id.fetch_add(1));
    for (auto& ch : name) {
        if (!std::isalnum(static_cast<unsigned char>(ch)) && ch != '_' && ch != '-' && ch != '.') {
            ch = '_';
        }
    }
    return std::filesystem::temp_directory_path() / name;
}

std::filesystem::path find_repo_file(std::string_view relative_path) {
    auto dir = std::filesystem::current_path();
    while (true) {
        auto candidate = dir / relative_path;
        if (std::filesystem::exists(candidate)) {
            return candidate;
        }
        if (!dir.has_parent_path() || dir.parent_path() == dir) {
            return candidate;
        }
        dir = dir.parent_path();
    }
}

DateV2Value<DateV2ValueType> make_date_v2(uint16_t year, uint8_t month, uint8_t day) {
    DateV2Value<DateV2ValueType> value;
    value.unchecked_set_time(year, month, day, 0, 0, 0, 0);
    return value;
}

int64_t orc_date_offset(uint16_t year, uint8_t month, uint8_t day) {
    static constexpr int32_t DATE_THRESHOLD = 719528;
    return make_date_v2(year, month, day).daynr() - DATE_THRESHOLD;
}

DateV2Value<DateTimeV2ValueType> make_datetime_v2(uint16_t year, uint8_t month, uint8_t day,
                                                  uint8_t hour = 0, uint8_t minute = 0,
                                                  uint8_t second = 0, uint32_t microsecond = 0) {
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(year, month, day, hour, minute, second, microsecond);
    return value;
}

class TableLiteral : public VLiteral {
public:
    template <typename... Args>
    static std::shared_ptr<TableLiteral> create_shared(Args&&... args) {
        return std::make_shared<TableLiteral>(std::forward<Args>(args)...);
    }

    TableLiteral(const DataTypePtr& type, const Field& field) : VLiteral(type) {
        _node_type = node_type_for_field(field);
        _data_type = type;
        _column_ptr = _data_type->create_column_const(1, field);
        _expr_name = _data_type->get_name();
    }

private:
    static TExprNodeType::type node_type_for_field(const Field& field) {
        switch (field.get_type()) {
        case TYPE_BOOLEAN:
            return TExprNodeType::BOOL_LITERAL;
        case TYPE_TINYINT:
        case TYPE_SMALLINT:
        case TYPE_INT:
        case TYPE_BIGINT:
            return TExprNodeType::INT_LITERAL;
        case TYPE_LARGEINT:
            return TExprNodeType::LARGE_INT_LITERAL;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return TExprNodeType::FLOAT_LITERAL;
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_DATEV2:
        case TYPE_DATETIMEV2:
        case TYPE_TIMESTAMPTZ:
            return TExprNodeType::DATE_LITERAL;
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128I:
        case TYPE_DECIMAL256:
            return TExprNodeType::DECIMAL_LITERAL;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING:
            return TExprNodeType::STRING_LITERAL;
        case TYPE_IPV4:
            return TExprNodeType::IPV4_LITERAL;
        case TYPE_IPV6:
            return TExprNodeType::IPV6_LITERAL;
        case TYPE_TIMEV2:
            return TExprNodeType::TIMEV2_LITERAL;
        case TYPE_VARBINARY:
            return TExprNodeType::VARBINARY_LITERAL;
        case TYPE_ARRAY:
            return TExprNodeType::ARRAY_LITERAL;
        case TYPE_MAP:
            return TExprNodeType::MAP_LITERAL;
        case TYPE_STRUCT:
            return TExprNodeType::STRUCT_LITERAL;
        case TYPE_NULL:
            return TExprNodeType::NULL_LITERAL;
        default:
            return TExprNodeType::LITERAL_PRED;
        }
    }
};

class TableSlotRef : public VSlotRef {
public:
    template <typename... Args>
    static std::shared_ptr<TableSlotRef> create_shared(Args&&... args) {
        return std::make_shared<TableSlotRef>(std::forward<Args>(args)...);
    }

    TableSlotRef(int slot_id, int column_id, int column_uniq_id, const DataTypePtr& type,
                 const std::string& column_name)
            : VSlotRef(slot_id, column_id, column_uniq_id), _cname(column_name) {
        _data_type = type;
    }

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        if (_prepared) {
            return Status::OK();
        }
        _prepared = true;
        _prepare_finished = true;
        return Status::OK();
    }

    const std::string& expr_name() const override { return _cname; }
    const std::string& column_name() const override { return _cname; }

private:
    const std::string _cname;
};

constexpr int64_t ROW_COUNT = 5;
constexpr int64_t PRIMITIVE_ROW_COUNT = 3;
constexpr int64_t COMPLEX_ROW_COUNT = 3;
constexpr int64_t DEEP_NESTED_ROW_COUNT = 4;
constexpr int64_t DEEP_NESTED_BATCH_CAPACITY = 16;
constexpr int64_t NULL_ROW = 1;

VExprSPtr function_expr(const std::string& function_name, const DataTypePtr& return_type,
                        const std::vector<DataTypePtr>& arg_types,
                        TExprNodeType::type node_type = TExprNodeType::FUNCTION_CALL,
                        TExprOpcode::type opcode = TExprOpcode::INVALID_OPCODE) {
    TFunctionName fn_name;
    fn_name.__set_function_name(function_name);
    TFunction fn;
    fn.__set_name(fn_name);
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    std::vector<TTypeDesc> thrift_arg_types;
    thrift_arg_types.reserve(arg_types.size());
    for (const auto& arg_type : arg_types) {
        thrift_arg_types.push_back(arg_type->to_thrift());
    }
    fn.__set_arg_types(thrift_arg_types);
    fn.__set_ret_type(return_type->to_thrift());
    fn.__set_has_var_args(false);

    TExprNode node;
    node.__set_node_type(node_type);
    node.__set_opcode(opcode);
    node.__set_type(return_type->to_thrift());
    node.__set_fn(fn);
    node.__set_num_children(static_cast<int16_t>(arg_types.size()));
    node.__set_is_nullable(return_type->is_nullable());
    return VectorizedFnCall::create_shared(node);
}

TExprNode make_filter_in_node(TExprNodeType::type node_type) {
    TExprNode node;
    node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    node.__set_node_type(node_type);
    node.__set_opcode(TExprOpcode::FILTER_IN);
    node.__set_num_children(1);
    node.__set_is_nullable(false);
    node.in_predicate.__set_is_not_in(false);
    return node;
}

class NullableInt32GreaterThanExpr final : public VExpr {
public:
    NullableInt32GreaterThanExpr(int column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int_type = std::make_shared<DataTypeInt32>();
        add_child(TableSlotRef::create_shared(column_id, column_id, -1, make_nullable(int_type),
                                              "id"));
        add_child(TableLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] =
                    !nullable_column.is_null_at(input_row) && input.get_element(input_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableInt32GreaterThanExpr";
};

class NullableInt32LessThanExpr final : public VExpr {
public:
    NullableInt32LessThanExpr(int column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::LT;
        const auto int_type = std::make_shared<DataTypeInt32>();
        add_child(TableSlotRef::create_shared(column_id, column_id, -1, make_nullable(int_type),
                                              "id"));
        add_child(TableLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] =
                    !nullable_column.is_null_at(input_row) && input.get_element(input_row) < _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableInt32LessThanExpr";
};

class NullableInt32EqualsExpr final : public VExpr {
public:
    NullableInt32EqualsExpr(int column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {}

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_element(input_row) == _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableInt32EqualsExpr";
};

class NullableInt32NullSafeEqualsNullExpr final : public VExpr {
public:
    NullableInt32NullSafeEqualsNullExpr(int column_id, bool literal_on_left)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _column_id(column_id) {
        _node_type = TExprNodeType::NULL_AWARE_BINARY_PRED;
        _opcode = TExprOpcode::EQ_FOR_NULL;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        auto slot = TableSlotRef::create_shared(column_id, column_id, -1, make_nullable(int32_type),
                                                "id");
        auto null_literal = TableLiteral::create_shared(make_nullable(int32_type),
                                                        Field::create_field<TYPE_NULL>(Null()));
        if (literal_on_left) {
            add_child(std::move(null_literal));
            add_child(std::move(slot));
        } else {
            add_child(std::move(slot));
            add_child(std::move(null_literal));
        }
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = nullable_column.is_null_at(input_row);
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::string _expr_name = "NullableInt32NullSafeEqualsNullExpr";
};

class NullableInt32NullSafeEqualsLiteralExpr final : public VExpr {
public:
    NullableInt32NullSafeEqualsLiteralExpr(int column_id, int32_t value, bool literal_on_left)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::NULL_AWARE_BINARY_PRED;
        _opcode = TExprOpcode::EQ_FOR_NULL;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        auto slot = TableSlotRef::create_shared(column_id, column_id, -1, make_nullable(int32_type),
                                                "id");
        auto literal =
                TableLiteral::create_shared(int32_type, Field::create_field<TYPE_INT>(value));
        if (literal_on_left) {
            add_child(std::move(literal));
            add_child(std::move(slot));
        } else {
            add_child(std::move(slot));
            add_child(std::move(literal));
        }
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_element(input_row) == _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableInt32NullSafeEqualsLiteralExpr";
};

class NullableInt32NullSafeEqualsSlotExpr final : public VExpr {
public:
    NullableInt32NullSafeEqualsSlotExpr(int left_column_id, int right_column_id)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _left_column_id(left_column_id),
              _right_column_id(right_column_id) {
        _node_type = TExprNodeType::NULL_AWARE_BINARY_PRED;
        _opcode = TExprOpcode::EQ_FOR_NULL;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        add_child(TableSlotRef::create_shared(left_column_id, left_column_id, -1,
                                              make_nullable(int32_type), "lhs"));
        add_child(TableSlotRef::create_shared(right_column_id, right_column_id, -1,
                                              make_nullable(int32_type), "rhs"));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& left_nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_left_column_id).column);
        const auto& right_nullable = assert_cast<const ColumnNullable&>(
                *block->get_by_position(_right_column_id).column);
        const auto& left = assert_cast<const ColumnInt32&>(left_nullable.get_nested_column());
        const auto& right = assert_cast<const ColumnInt32&>(right_nullable.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            const bool left_is_null = left_nullable.is_null_at(input_row);
            const bool right_is_null = right_nullable.is_null_at(input_row);
            result_data[row] =
                    left_is_null == right_is_null &&
                    (left_is_null || left.get_element(input_row) == right.get_element(input_row));
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _left_column_id;
    const int _right_column_id;
    const std::string _expr_name = "NullableInt32NullSafeEqualsSlotExpr";
};

class NullableInt32IsNullExpr final : public VExpr {
public:
    explicit NullableInt32IsNullExpr(int column_id)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _column_id(column_id) {
        _node_type = TExprNodeType::FUNCTION_CALL;
        _opcode = TExprOpcode::INVALID_OPCODE;
        TFunctionName fn_name;
        fn_name.__set_function_name("is_null_pred");
        _fn.__set_name(fn_name);
        const auto int32_type = std::make_shared<DataTypeInt32>();
        add_child(TableSlotRef::create_shared(column_id, column_id, -1, make_nullable(int32_type),
                                              "id"));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = nullable_column.is_null_at(input_row);
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::string _expr_name = "NullableInt32IsNullExpr";
};

class CompoundPredicateExpr final : public VExpr {
public:
    CompoundPredicateExpr(TExprOpcode::type opcode, VExprSPtrs children)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _expr_name(opcode == TExprOpcode::COMPOUND_OR ? "CompoundOrPredicateExpr"
                                                            : "CompoundNotPredicateExpr") {
        _node_type = TExprNodeType::COMPOUND_PRED;
        _opcode = opcode;
        set_children(std::move(children));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        if (_opcode == TExprOpcode::COMPOUND_NOT) {
            DORIS_CHECK(_children.size() == 1);
            ColumnPtr child_column;
            RETURN_IF_ERROR(
                    _children[0]->execute_column(context, block, selector, count, child_column));
            for (size_t row = 0; row < count; ++row) {
                result_data[row] = !bool_value(*child_column, row);
            }
            result_column = std::move(result);
            return Status::OK();
        }

        DORIS_CHECK(_opcode == TExprOpcode::COMPOUND_OR);
        DORIS_CHECK(!_children.empty());
        std::vector<ColumnPtr> child_columns;
        child_columns.reserve(_children.size());
        for (const auto& child : _children) {
            ColumnPtr child_column;
            RETURN_IF_ERROR(child->execute_column(context, block, selector, count, child_column));
            child_columns.push_back(std::move(child_column));
        }
        for (size_t row = 0; row < count; ++row) {
            result_data[row] = 0;
            for (const auto& child_column : child_columns) {
                if (bool_value(*child_column, row)) {
                    result_data[row] = 1;
                    break;
                }
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    static bool bool_value(const IColumn& column, size_t row) {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
            if (nullable->is_null_at(row)) {
                return false;
            }
            const auto& nested = assert_cast<const ColumnUInt8&>(nullable->get_nested_column());
            return nested.get_element(row) != 0;
        }
        const auto& data = assert_cast<const ColumnUInt8&>(column);
        return data.get_element(row) != 0;
    }

    std::string _expr_name;
};

class RuntimeFilterWrapperExpr final : public VExpr {
public:
    explicit RuntimeFilterWrapperExpr(VExprSPtr impl)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _impl(std::move(impl)),
              _expr_name("RuntimeFilterWrapperExpr") {
        DORIS_CHECK(_impl != nullptr);
        _node_type = _impl->node_type();
        _opcode = _impl->op();
    }

    bool is_rf_wrapper() const override { return true; }

    VExprSPtr get_impl() const override { return _impl; }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        return _impl->execute_column(context, block, selector, count, result_column);
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    VExprSPtr _impl;
    const std::string _expr_name;
};

class NullableInt32CastToInt64GreaterThanExpr final : public VExpr {
public:
    NullableInt32CastToInt64GreaterThanExpr(int column_id, int64_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        const auto int64_type = std::make_shared<DataTypeInt64>();
        auto cast_expr = format::Cast::create_shared(make_nullable(int64_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int32_type), "id"));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(int64_type, Field::create_field<TYPE_BIGINT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               static_cast<int64_t>(input.get_element(input_row)) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int64_t _value;
    const std::string _expr_name = "NullableInt32CastToInt64GreaterThanExpr";
};

class NullableInt32CastToInt64NullSafeEqualsNullExpr final : public VExpr {
public:
    explicit NullableInt32CastToInt64NullSafeEqualsNullExpr(int column_id)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _column_id(column_id) {
        _node_type = TExprNodeType::NULL_AWARE_BINARY_PRED;
        _opcode = TExprOpcode::EQ_FOR_NULL;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        const auto int64_type = std::make_shared<DataTypeInt64>();
        auto cast_expr = format::Cast::create_shared(make_nullable(int64_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int32_type), "id"));
        auto null_literal = TableLiteral::create_shared(make_nullable(int64_type),
                                                        Field::create_field<TYPE_NULL>(Null()));
        add_child(std::move(cast_expr));
        add_child(std::move(null_literal));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = nullable_column.is_null_at(input_row);
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::string _expr_name = "NullableInt32CastToInt64NullSafeEqualsNullExpr";
};

class NullableInt32CastToInt64NullSafeEqualsLiteralExpr final : public VExpr {
public:
    NullableInt32CastToInt64NullSafeEqualsLiteralExpr(int column_id, int64_t value,
                                                      bool literal_on_left)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::NULL_AWARE_BINARY_PRED;
        _opcode = TExprOpcode::EQ_FOR_NULL;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        const auto int64_type = std::make_shared<DataTypeInt64>();
        auto cast_expr = format::Cast::create_shared(make_nullable(int64_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int32_type), "id"));
        auto literal =
                TableLiteral::create_shared(int64_type, Field::create_field<TYPE_BIGINT>(value));
        if (literal_on_left) {
            add_child(std::move(literal));
            add_child(std::move(cast_expr));
        } else {
            add_child(std::move(cast_expr));
            add_child(std::move(literal));
        }
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               static_cast<int64_t>(input.get_element(input_row)) == _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int64_t _value;
    const std::string _expr_name = "NullableInt32CastToInt64NullSafeEqualsLiteralExpr";
};

class NullableInt32CastToDoubleGreaterThanExpr final : public VExpr {
public:
    NullableInt32CastToDoubleGreaterThanExpr(int column_id, double value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        const auto double_type = std::make_shared<DataTypeFloat64>();
        auto cast_expr = format::Cast::create_shared(make_nullable(double_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int32_type), "id"));
        add_child(std::move(cast_expr));
        add_child(
                TableLiteral::create_shared(double_type, Field::create_field<TYPE_DOUBLE>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               static_cast<double>(input.get_element(input_row)) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const double _value;
    const std::string _expr_name = "NullableInt32CastToDoubleGreaterThanExpr";
};

class NullableInt32CastToFloatGreaterThanExpr final : public VExpr {
public:
    NullableInt32CastToFloatGreaterThanExpr(int column_id, float value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        const auto float_type = std::make_shared<DataTypeFloat32>();
        auto cast_expr = format::Cast::create_shared(make_nullable(float_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int32_type), "id"));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(float_type, Field::create_field<TYPE_FLOAT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               static_cast<float>(input.get_element(input_row)) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const float _value;
    const std::string _expr_name = "NullableInt32CastToFloatGreaterThanExpr";
};

class NullableInt64CastToDoubleGreaterThanExpr final : public VExpr {
public:
    NullableInt64CastToDoubleGreaterThanExpr(int column_id, double value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int64_type = std::make_shared<DataTypeInt64>();
        const auto double_type = std::make_shared<DataTypeFloat64>();
        auto cast_expr = format::Cast::create_shared(make_nullable(double_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int64_type), "id"));
        add_child(std::move(cast_expr));
        add_child(
                TableLiteral::create_shared(double_type, Field::create_field<TYPE_DOUBLE>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt64&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               static_cast<double>(input.get_element(input_row)) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const double _value;
    const std::string _expr_name = "NullableInt64CastToDoubleGreaterThanExpr";
};

class NullableInt32CastToDoubleLessThanExpr final : public VExpr {
public:
    NullableInt32CastToDoubleLessThanExpr(int column_id, double value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::LT;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        const auto double_type = std::make_shared<DataTypeFloat64>();
        auto cast_expr = format::Cast::create_shared(make_nullable(double_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int32_type), "id"));
        add_child(std::move(cast_expr));
        add_child(
                TableLiteral::create_shared(double_type, Field::create_field<TYPE_DOUBLE>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               static_cast<double>(input.get_element(input_row)) < _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const double _value;
    const std::string _expr_name = "NullableInt32CastToDoubleLessThanExpr";
};

class NullableInt32CastToDoubleInExpr final : public VExpr {
public:
    NullableInt32CastToDoubleInExpr(int column_id, std::vector<double> values)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _values(std::move(values)) {
        _node_type = TExprNodeType::IN_PRED;
        _opcode = TExprOpcode::FILTER_IN;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        const auto double_type = std::make_shared<DataTypeFloat64>();
        auto cast_expr = format::Cast::create_shared(make_nullable(double_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int32_type), "id"));
        add_child(std::move(cast_expr));
        for (const auto value : _values) {
            add_child(TableLiteral::create_shared(double_type,
                                                  Field::create_field<TYPE_DOUBLE>(value)));
        }
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = 0;
            if (nullable_column.is_null_at(input_row)) {
                continue;
            }
            const auto value = static_cast<double>(input.get_element(input_row));
            for (const auto literal : _values) {
                if (value == literal) {
                    result_data[row] = 1;
                    break;
                }
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::vector<double> _values;
    const std::string _expr_name = "NullableInt32CastToDoubleInExpr";
};

class NullableInt32CastToFloatInExpr final : public VExpr {
public:
    NullableInt32CastToFloatInExpr(int column_id, std::vector<float> values)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _values(std::move(values)) {
        _node_type = TExprNodeType::IN_PRED;
        _opcode = TExprOpcode::FILTER_IN;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        const auto float_type = std::make_shared<DataTypeFloat32>();
        auto cast_expr = format::Cast::create_shared(make_nullable(float_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int32_type), "id"));
        add_child(std::move(cast_expr));
        for (const auto value : _values) {
            add_child(TableLiteral::create_shared(float_type,
                                                  Field::create_field<TYPE_FLOAT>(value)));
        }
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = 0;
            if (nullable_column.is_null_at(input_row)) {
                continue;
            }
            const auto value = static_cast<float>(input.get_element(input_row));
            for (const auto literal : _values) {
                if (value == literal) {
                    result_data[row] = 1;
                    break;
                }
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::vector<float> _values;
    const std::string _expr_name = "NullableInt32CastToFloatInExpr";
};

class NullableInt64CastToDoubleInExpr final : public VExpr {
public:
    NullableInt64CastToDoubleInExpr(int column_id, std::vector<double> values)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _values(std::move(values)) {
        _node_type = TExprNodeType::IN_PRED;
        _opcode = TExprOpcode::FILTER_IN;
        const auto int64_type = std::make_shared<DataTypeInt64>();
        const auto double_type = std::make_shared<DataTypeFloat64>();
        auto cast_expr = format::Cast::create_shared(make_nullable(double_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int64_type), "id"));
        add_child(std::move(cast_expr));
        for (const auto value : _values) {
            add_child(TableLiteral::create_shared(double_type,
                                                  Field::create_field<TYPE_DOUBLE>(value)));
        }
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt64&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = 0;
            if (nullable_column.is_null_at(input_row)) {
                continue;
            }
            const auto value = static_cast<double>(input.get_element(input_row));
            for (const auto literal : _values) {
                if (value == literal) {
                    result_data[row] = 1;
                    break;
                }
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::vector<double> _values;
    const std::string _expr_name = "NullableInt64CastToDoubleInExpr";
};

class NullableInt64CastToInt32GreaterThanExpr final : public VExpr {
public:
    NullableInt64CastToInt32GreaterThanExpr(int column_id, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int64_type = std::make_shared<DataTypeInt64>();
        const auto int32_type = std::make_shared<DataTypeInt32>();
        auto cast_expr = format::Cast::create_shared(make_nullable(int32_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int64_type), "id"));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(int32_type, Field::create_field<TYPE_INT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt64&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               static_cast<int32_t>(input.get_element(input_row)) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableInt64CastToInt32GreaterThanExpr";
};

class NullableInt64CastToDoubleNullSafeEqualsLiteralExpr final : public VExpr {
public:
    NullableInt64CastToDoubleNullSafeEqualsLiteralExpr(int column_id, double value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::NULL_AWARE_BINARY_PRED;
        _opcode = TExprOpcode::EQ_FOR_NULL;
        const auto int64_type = std::make_shared<DataTypeInt64>();
        const auto double_type = std::make_shared<DataTypeFloat64>();
        auto cast_expr = format::Cast::create_shared(make_nullable(double_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int64_type), "id"));
        add_child(std::move(cast_expr));
        add_child(
                TableLiteral::create_shared(double_type, Field::create_field<TYPE_DOUBLE>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt64&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               static_cast<double>(input.get_element(input_row)) == _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const double _value;
    const std::string _expr_name = "NullableInt64CastToDoubleNullSafeEqualsLiteralExpr";
};

class NullableInt16CastToFloatGreaterThanExpr final : public VExpr {
public:
    NullableInt16CastToFloatGreaterThanExpr(int column_id, float value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int16_type = std::make_shared<DataTypeInt16>();
        const auto float_type = std::make_shared<DataTypeFloat32>();
        auto cast_expr = format::Cast::create_shared(make_nullable(float_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(int16_type), "id"));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(float_type, Field::create_field<TYPE_FLOAT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnInt16&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               static_cast<float>(input.get_element(input_row)) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const float _value;
    const std::string _expr_name = "NullableInt16CastToFloatGreaterThanExpr";
};

class NullableFloatCastToDoubleGreaterThanExpr final : public VExpr {
public:
    NullableFloatCastToDoubleGreaterThanExpr(int column_id, double value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto float_type = std::make_shared<DataTypeFloat32>();
        const auto double_type = std::make_shared<DataTypeFloat64>();
        auto cast_expr = format::Cast::create_shared(make_nullable(double_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(float_type), "float_col"));
        add_child(std::move(cast_expr));
        add_child(
                TableLiteral::create_shared(double_type, Field::create_field<TYPE_DOUBLE>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnFloat32&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               static_cast<double>(input.get_element(input_row)) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const double _value;
    const std::string _expr_name = "NullableFloatCastToDoubleGreaterThanExpr";
};

class NullableDateV2CastToDateGreaterThanExpr final : public VExpr {
public:
    NullableDateV2CastToDateGreaterThanExpr(int column_id, DateV2Value<DateV2ValueType> file_value,
                                            const VecDateTimeValue& literal)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _file_value(file_value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto date_v2_type = std::make_shared<DataTypeDateV2>();
        const auto date_type = std::make_shared<DataTypeDate>();
        auto cast_expr = format::Cast::create_shared(make_nullable(date_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(date_v2_type), "date_col"));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(date_type, Field::create_field<TYPE_DATE>(literal)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnDateV2&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_element(input_row) > _file_value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const DateV2Value<DateV2ValueType> _file_value;
    const std::string _expr_name = "NullableDateV2CastToDateGreaterThanExpr";
};

class NullableDateV2CastToDateTimeV2GreaterThanExpr final : public VExpr {
public:
    NullableDateV2CastToDateTimeV2GreaterThanExpr(int column_id,
                                                  DateV2Value<DateV2ValueType> file_value,
                                                  DateV2Value<DateTimeV2ValueType> literal)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _file_value(file_value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto date_v2_type = std::make_shared<DataTypeDateV2>();
        const auto datetime_v2_type = std::make_shared<DataTypeDateTimeV2>();
        auto cast_expr = format::Cast::create_shared(make_nullable(datetime_v2_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(date_v2_type), "date_col"));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(datetime_v2_type,
                                              Field::create_field<TYPE_DATETIMEV2>(literal)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnDateV2&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_element(input_row) > _file_value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const DateV2Value<DateV2ValueType> _file_value;
    const std::string _expr_name = "NullableDateV2CastToDateTimeV2GreaterThanExpr";
};

class NullableDateV2CastToDateTimeV2ComparisonExpr final : public VExpr {
public:
    NullableDateV2CastToDateTimeV2ComparisonExpr(int column_id, TExprOpcode::type opcode,
                                                 DateV2Value<DateTimeV2ValueType> literal)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _literal(literal) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = opcode;
        const auto date_v2_type = std::make_shared<DataTypeDateV2>();
        const auto datetime_v2_type = std::make_shared<DataTypeDateTimeV2>();
        auto cast_expr = format::Cast::create_shared(make_nullable(datetime_v2_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(date_v2_type), "date_col"));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(datetime_v2_type,
                                              Field::create_field<TYPE_DATETIMEV2>(literal)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnDateV2&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               compare(date_to_midnight(input.get_element(input_row)));
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    DateV2Value<DateTimeV2ValueType> date_to_midnight(
            const DateV2Value<DateV2ValueType>& date) const {
        return make_datetime_v2(date.year(), date.month(), date.day());
    }

    bool compare(const DateV2Value<DateTimeV2ValueType>& value) const {
        switch (_opcode) {
        case TExprOpcode::GE:
            return value >= _literal;
        case TExprOpcode::GT:
            return value > _literal;
        case TExprOpcode::LE:
            return value <= _literal;
        case TExprOpcode::LT:
            return value < _literal;
        default:
            return false;
        }
    }

    const int _column_id;
    const DateV2Value<DateTimeV2ValueType> _literal;
    const std::string _expr_name = "NullableDateV2CastToDateTimeV2ComparisonExpr";
};

class NullableDateV2CastToDateTimeV2InExpr final : public VExpr {
public:
    NullableDateV2CastToDateTimeV2InExpr(int column_id,
                                         std::vector<DateV2Value<DateTimeV2ValueType>> values)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _values(std::move(values)) {
        _node_type = TExprNodeType::IN_PRED;
        _opcode = TExprOpcode::FILTER_IN;
        const auto date_v2_type = std::make_shared<DataTypeDateV2>();
        const auto datetime_v2_type = std::make_shared<DataTypeDateTimeV2>();
        auto cast_expr = format::Cast::create_shared(make_nullable(datetime_v2_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(date_v2_type), "date_col"));
        add_child(std::move(cast_expr));
        for (const auto& value : _values) {
            add_child(TableLiteral::create_shared(datetime_v2_type,
                                                  Field::create_field<TYPE_DATETIMEV2>(value)));
        }
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnDateV2&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = 0;
            if (nullable_column.is_null_at(input_row)) {
                continue;
            }
            const auto value = date_to_midnight(input.get_element(input_row));
            for (const auto& literal : _values) {
                if (value == literal) {
                    result_data[row] = 1;
                    break;
                }
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    DateV2Value<DateTimeV2ValueType> date_to_midnight(
            const DateV2Value<DateV2ValueType>& date) const {
        return make_datetime_v2(date.year(), date.month(), date.day());
    }

    const int _column_id;
    const std::vector<DateV2Value<DateTimeV2ValueType>> _values;
    const std::string _expr_name = "NullableDateV2CastToDateTimeV2InExpr";
};

class NullableDateV2CastToDateTimeGreaterThanExpr final : public VExpr {
public:
    NullableDateV2CastToDateTimeGreaterThanExpr(int column_id,
                                                DateV2Value<DateV2ValueType> file_value,
                                                const VecDateTimeValue& literal)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _file_value(file_value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto date_v2_type = std::make_shared<DataTypeDateV2>();
        const auto datetime_type = std::make_shared<DataTypeDateTime>();
        auto cast_expr = format::Cast::create_shared(make_nullable(datetime_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(date_v2_type), "date_col"));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(datetime_type,
                                              Field::create_field<TYPE_DATETIME>(literal)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnDateV2&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_element(input_row) > _file_value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const DateV2Value<DateV2ValueType> _file_value;
    const std::string _expr_name = "NullableDateV2CastToDateTimeGreaterThanExpr";
};

class NullableStringCastToStringGreaterThanExpr final : public VExpr {
public:
    NullableStringCastToStringGreaterThanExpr(int column_id, DataTypePtr source_type,
                                              std::string value, std::string column_name)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(std::move(value)) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto string_type = std::make_shared<DataTypeString>();
        auto cast_expr = format::Cast::create_shared(string_type);
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(std::move(source_type)),
                                                         std::move(column_name)));
        add_child(std::move(cast_expr));
        add_child(
                TableLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(_value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnString&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_data_at(input_row).to_string_view().compare(_value) > 0;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::string _value;
    const std::string _expr_name = "NullableStringCastToStringGreaterThanExpr";
};

class NullableStringEqualsExpr final : public VExpr {
public:
    NullableStringEqualsExpr(int column_id, DataTypePtr source_type, std::string value,
                             std::string column_name)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(std::move(value)) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::EQ;
        const auto string_type = std::make_shared<DataTypeString>();
        add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                              make_nullable(std::move(source_type)),
                                              std::move(column_name)));
        add_child(
                TableLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(_value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnString&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_data_at(input_row).to_string_view().compare(_value) == 0;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::string _value;
    const std::string _expr_name = "NullableStringEqualsExpr";
};

class SargOnlyStringEqualsVarbinaryLiteralExpr final : public VExpr {
public:
    SargOnlyStringEqualsVarbinaryLiteralExpr(int column_id, DataTypePtr source_type,
                                             std::string value, std::string column_name)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _value(std::move(value)) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::EQ;
        add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                              make_nullable(std::move(source_type)),
                                              std::move(column_name)));
        add_child(TableLiteral::create_shared(
                std::make_shared<DataTypeVarbinary>(),
                Field::create_field<TYPE_VARBINARY>(StringView(_value))));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            result_data[row] = 1;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const std::string _value;
    const std::string _expr_name = "SargOnlyStringEqualsVarbinaryLiteralExpr";
};

class SargOnlyStringCastToStringEqualsExpr final : public VExpr {
public:
    SargOnlyStringCastToStringEqualsExpr(int column_id, DataTypePtr source_type, std::string value,
                                         std::string column_name)
            : VExpr(std::make_shared<DataTypeUInt8>(), false), _value(std::move(value)) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::EQ;
        const auto string_type = std::make_shared<DataTypeString>();
        auto cast_expr = format::Cast::create_shared(string_type);
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(std::move(source_type)),
                                                         std::move(column_name)));
        add_child(std::move(cast_expr));
        add_child(
                TableLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(_value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            result_data[row] = 1;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const std::string _value;
    const std::string _expr_name = "SargOnlyStringCastToStringEqualsExpr";
};

class NullableDateV2CastToStringGreaterThanExpr final : public VExpr {
public:
    NullableDateV2CastToStringGreaterThanExpr(int column_id, std::string value,
                                              DateV2Value<DateV2ValueType> file_value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _file_value(file_value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto date_type = std::make_shared<DataTypeDateV2>();
        const auto string_type = std::make_shared<DataTypeString>();
        auto cast_expr = format::Cast::create_shared(string_type);
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(date_type), "date_col"));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(string_type,
                                              Field::create_field<TYPE_STRING>(std::move(value))));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnDateV2&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_element(input_row) > _file_value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const DateV2Value<DateV2ValueType> _file_value;
    const std::string _expr_name = "NullableDateV2CastToStringGreaterThanExpr";
};

class NullableDateTimeV2LowerPrecisionCastGreaterThanExpr final : public VExpr {
public:
    NullableDateTimeV2LowerPrecisionCastGreaterThanExpr(int column_id, DataTypePtr source_type,
                                                        DateV2Value<DateTimeV2ValueType> literal)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _literal(literal) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto lower_precision_type = std::make_shared<DataTypeDateTimeV2>(0);
        auto cast_expr = format::Cast::create_shared(make_nullable(lower_precision_type));
        cast_expr->add_child(TableSlotRef::create_shared(
                column_id, column_id, -1, make_nullable(std::move(source_type)), "timestamp_col"));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(lower_precision_type,
                                              Field::create_field<TYPE_DATETIMEV2>(literal)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input =
                assert_cast<const ColumnDateTimeV2&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_element(input_row) > _literal;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const DateV2Value<DateTimeV2ValueType> _literal;
    const std::string _expr_name = "NullableDateTimeV2LowerPrecisionCastGreaterThanExpr";
};

class NullableStructChildInt32GreaterThanExpr final : public VExpr {
public:
    NullableStructChildInt32GreaterThanExpr(int column_id, DataTypePtr struct_type,
                                            std::string child_name, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int_type = std::make_shared<DataTypeInt32>();
        auto child_expr = function_expr("struct_element", make_nullable(int_type),
                                        {struct_type, std::make_shared<DataTypeString>()});
        child_expr->add_child(
                TableSlotRef::create_shared(column_id, column_id, -1, struct_type, "struct_col"));
        child_expr->add_child(TableLiteral::create_shared(
                std::make_shared<DataTypeString>(),
                Field::create_field<TYPE_STRING>(std::move(child_name))));
        add_child(std::move(child_expr));
        add_child(TableLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_struct =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& struct_column =
                assert_cast<const ColumnStruct&>(nullable_struct.get_nested_column());
        const auto& nullable_child =
                assert_cast<const ColumnNullable&>(struct_column.get_column(0));
        const auto& child = assert_cast<const ColumnInt32&>(nullable_child.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_struct.is_null_at(input_row) &&
                               !nullable_child.is_null_at(input_row) &&
                               child.get_element(input_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableStructChildInt32GreaterThanExpr";
};

class NullableStructChildInt32CastToInt64GreaterThanExpr final : public VExpr {
public:
    NullableStructChildInt32CastToInt64GreaterThanExpr(int column_id, DataTypePtr struct_type,
                                                       std::string child_name, int64_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int_type = std::make_shared<DataTypeInt32>();
        const auto int64_type = std::make_shared<DataTypeInt64>();
        auto child_expr = function_expr("struct_element", make_nullable(int_type),
                                        {struct_type, std::make_shared<DataTypeString>()});
        child_expr->add_child(
                TableSlotRef::create_shared(column_id, column_id, -1, struct_type, "struct_col"));
        child_expr->add_child(TableLiteral::create_shared(
                std::make_shared<DataTypeString>(),
                Field::create_field<TYPE_STRING>(std::move(child_name))));
        auto cast_expr = format::Cast::create_shared(make_nullable(int64_type));
        cast_expr->add_child(std::move(child_expr));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(int64_type, Field::create_field<TYPE_BIGINT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_struct =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& struct_column =
                assert_cast<const ColumnStruct&>(nullable_struct.get_nested_column());
        const auto& nullable_child =
                assert_cast<const ColumnNullable&>(struct_column.get_column(0));
        const auto& child = assert_cast<const ColumnInt32&>(nullable_child.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_struct.is_null_at(input_row) &&
                               !nullable_child.is_null_at(input_row) &&
                               static_cast<int64_t>(child.get_element(input_row)) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int64_t _value;
    const std::string _expr_name = "NullableStructChildInt32CastToInt64GreaterThanExpr";
};

class NullableStructChildInt32NullSafeEqualsLiteralExpr final : public VExpr {
public:
    NullableStructChildInt32NullSafeEqualsLiteralExpr(int column_id, DataTypePtr struct_type,
                                                      std::string child_name, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::NULL_AWARE_BINARY_PRED;
        _opcode = TExprOpcode::EQ_FOR_NULL;
        const auto int_type = std::make_shared<DataTypeInt32>();
        auto child_expr = function_expr("struct_element", make_nullable(int_type),
                                        {struct_type, std::make_shared<DataTypeString>()});
        child_expr->add_child(
                TableSlotRef::create_shared(column_id, column_id, -1, struct_type, "struct_col"));
        child_expr->add_child(TableLiteral::create_shared(
                std::make_shared<DataTypeString>(),
                Field::create_field<TYPE_STRING>(std::move(child_name))));
        add_child(std::move(child_expr));
        add_child(TableLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_struct =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& struct_column =
                assert_cast<const ColumnStruct&>(nullable_struct.get_nested_column());
        const auto& nullable_child =
                assert_cast<const ColumnNullable&>(struct_column.get_column(0));
        const auto& child = assert_cast<const ColumnInt32&>(nullable_child.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_struct.is_null_at(input_row) &&
                               !nullable_child.is_null_at(input_row) &&
                               child.get_element(input_row) == _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableStructChildInt32NullSafeEqualsLiteralExpr";
};

class NullableStructChildArrayNullSafeEqualsLiteralExpr final : public VExpr {
public:
    NullableStructChildArrayNullSafeEqualsLiteralExpr(int column_id, DataTypePtr struct_type,
                                                      DataTypePtr array_type,
                                                      std::string child_name, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::NULL_AWARE_BINARY_PRED;
        _opcode = TExprOpcode::EQ_FOR_NULL;
        const auto string_type = std::make_shared<DataTypeString>();
        auto child_expr = function_expr("struct_element", array_type, {struct_type, string_type});
        child_expr->add_child(
                TableSlotRef::create_shared(column_id, column_id, -1, struct_type, "struct_col"));
        child_expr->add_child(TableLiteral::create_shared(
                string_type, Field::create_field<TYPE_STRING>(std::move(child_name))));

        Array literal_values {Field::create_field<TYPE_INT>(value)};
        add_child(std::move(child_expr));
        add_child(TableLiteral::create_shared(
                std::move(array_type), Field::create_field<TYPE_ARRAY>(std::move(literal_values))));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_struct =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& struct_column =
                assert_cast<const ColumnStruct&>(nullable_struct.get_nested_column());
        const auto& nullable_array =
                assert_cast<const ColumnNullable&>(struct_column.get_column(0));
        const auto& array_column =
                assert_cast<const ColumnArray&>(nullable_array.get_nested_column());
        const auto& offsets = array_column.get_offsets();
        const auto& values_nullable = assert_cast<const ColumnNullable&>(array_column.get_data());
        const auto& values = assert_cast<const ColumnInt32&>(values_nullable.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = 0;
            if (nullable_struct.is_null_at(input_row) || nullable_array.is_null_at(input_row)) {
                continue;
            }
            const auto begin = input_row == 0 ? 0 : offsets[input_row - 1];
            const auto end = offsets[input_row];
            result_data[row] = end == begin + 1 && !values_nullable.is_null_at(begin) &&
                               values.get_element(begin) == _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableStructChildArrayNullSafeEqualsLiteralExpr";
};

class NullableNestedStructChildInt32GreaterThanExpr final : public VExpr {
public:
    NullableNestedStructChildInt32GreaterThanExpr(int column_id, DataTypePtr struct_type,
                                                  DataTypePtr nested_struct_type,
                                                  std::string nested_child_name,
                                                  std::string child_name, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int_type = std::make_shared<DataTypeInt32>();
        const auto string_type = std::make_shared<DataTypeString>();
        auto nested_expr =
                function_expr("struct_element", nested_struct_type, {struct_type, string_type});
        nested_expr->add_child(
                TableSlotRef::create_shared(column_id, column_id, -1, struct_type, "struct_col"));
        nested_expr->add_child(TableLiteral::create_shared(
                string_type, Field::create_field<TYPE_STRING>(std::move(nested_child_name))));
        auto child_expr = function_expr("struct_element", make_nullable(int_type),
                                        {nested_struct_type, std::make_shared<DataTypeString>()});
        child_expr->add_child(std::move(nested_expr));
        child_expr->add_child(TableLiteral::create_shared(
                std::make_shared<DataTypeString>(),
                Field::create_field<TYPE_STRING>(std::move(child_name))));
        add_child(std::move(child_expr));
        add_child(TableLiteral::create_shared(int_type, Field::create_field<TYPE_INT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_struct =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& struct_column =
                assert_cast<const ColumnStruct&>(nullable_struct.get_nested_column());
        const auto& nullable_nested =
                assert_cast<const ColumnNullable&>(struct_column.get_column(0));
        const auto& nested_struct =
                assert_cast<const ColumnStruct&>(nullable_nested.get_nested_column());
        const auto& nullable_child =
                assert_cast<const ColumnNullable&>(nested_struct.get_column(0));
        const auto& child = assert_cast<const ColumnInt32&>(nullable_child.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_struct.is_null_at(input_row) &&
                               !nullable_nested.is_null_at(input_row) &&
                               !nullable_child.is_null_at(input_row) &&
                               child.get_element(input_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableNestedStructChildInt32GreaterThanExpr";
};

class NullableArrayContainsInt32Expr final : public VExpr {
public:
    NullableArrayContainsInt32Expr(int column_id, DataTypePtr array_type, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::FUNCTION_CALL;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        add_child(TableSlotRef::create_shared(column_id, column_id, -1, std::move(array_type),
                                              "array_col"));
        add_child(TableLiteral::create_shared(int32_type, Field::create_field<TYPE_INT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& array_nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& array_column =
                assert_cast<const ColumnArray&>(array_nullable.get_nested_column());
        const auto& offsets = array_column.get_offsets();
        const auto& values_nullable = assert_cast<const ColumnNullable&>(array_column.get_data());
        const auto& values = assert_cast<const ColumnInt32&>(values_nullable.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = 0;
            if (array_nullable.is_null_at(input_row)) {
                continue;
            }
            const auto begin = input_row == 0 ? 0 : offsets[input_row - 1];
            const auto end = offsets[input_row];
            for (auto element = begin; element < end; ++element) {
                if (!values_nullable.is_null_at(element) && values.get_element(element) == _value) {
                    result_data[row] = 1;
                    break;
                }
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableArrayContainsInt32Expr";
};

class NullableMapContainsKeyExpr final : public VExpr {
public:
    NullableMapContainsKeyExpr(int column_id, DataTypePtr map_type, std::string key)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _key(std::move(key)) {
        _node_type = TExprNodeType::FUNCTION_CALL;
        const auto string_type = std::make_shared<DataTypeString>();
        add_child(TableSlotRef::create_shared(column_id, column_id, -1, std::move(map_type),
                                              "map_col"));
        add_child(TableLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(_key)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& map_nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& map_column = assert_cast<const ColumnMap&>(map_nullable.get_nested_column());
        const auto& offsets = map_column.get_offsets();
        const auto& keys_nullable = assert_cast<const ColumnNullable&>(map_column.get_keys());
        const auto& keys = assert_cast<const ColumnString&>(keys_nullable.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = 0;
            if (map_nullable.is_null_at(input_row)) {
                continue;
            }
            const auto begin = input_row == 0 ? 0 : offsets[input_row - 1];
            const auto end = offsets[input_row];
            for (auto entry = begin; entry < end; ++entry) {
                if (!keys_nullable.is_null_at(entry) &&
                    keys.get_data_at(entry).to_string_view() == _key) {
                    result_data[row] = 1;
                    break;
                }
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::string _key;
    const std::string _expr_name = "NullableMapContainsKeyExpr";
};

class NullableMapElementInt32GreaterThanExpr final : public VExpr {
public:
    NullableMapElementInt32GreaterThanExpr(int column_id, DataTypePtr map_type, std::string key,
                                           int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _key(std::move(key)),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        const auto string_type = std::make_shared<DataTypeString>();
        auto element_expr =
                function_expr("element_at", make_nullable(int32_type), {map_type, string_type});
        element_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                            std::move(map_type), "map_col"));
        element_expr->add_child(
                TableLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(_key)));
        add_child(std::move(element_expr));
        add_child(TableLiteral::create_shared(int32_type, Field::create_field<TYPE_INT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& map_nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& map_column = assert_cast<const ColumnMap&>(map_nullable.get_nested_column());
        const auto& offsets = map_column.get_offsets();
        const auto& keys_nullable = assert_cast<const ColumnNullable&>(map_column.get_keys());
        const auto& keys = assert_cast<const ColumnString&>(keys_nullable.get_nested_column());
        const auto& values_nullable = assert_cast<const ColumnNullable&>(map_column.get_values());
        const auto& values = assert_cast<const ColumnInt32&>(values_nullable.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = 0;
            if (map_nullable.is_null_at(input_row)) {
                continue;
            }
            const auto begin = input_row == 0 ? 0 : offsets[input_row - 1];
            const auto end = offsets[input_row];
            for (auto entry = begin; entry < end; ++entry) {
                if (!keys_nullable.is_null_at(entry) &&
                    keys.get_data_at(entry).to_string_view() == _key &&
                    !values_nullable.is_null_at(entry) && values.get_element(entry) > _value) {
                    result_data[row] = 1;
                    break;
                }
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::string _key;
    const int32_t _value;
    const std::string _expr_name = "NullableMapElementInt32GreaterThanExpr";
};

class NullableMapKeysInExpr final : public VExpr {
public:
    NullableMapKeysInExpr(int column_id, DataTypePtr map_type, std::string key)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _key(std::move(key)) {
        _node_type = TExprNodeType::IN_PRED;
        _opcode = TExprOpcode::FILTER_IN;
        const auto string_type = std::make_shared<DataTypeString>();
        auto keys_expr = function_expr("map_keys",
                                       std::make_shared<DataTypeArray>(make_nullable(string_type)),
                                       {map_type});
        keys_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         std::move(map_type), "map_col"));
        add_child(std::move(keys_expr));
        add_child(TableLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(_key)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& map_nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& map_column = assert_cast<const ColumnMap&>(map_nullable.get_nested_column());
        const auto& offsets = map_column.get_offsets();
        const auto& keys_nullable = assert_cast<const ColumnNullable&>(map_column.get_keys());
        const auto& keys = assert_cast<const ColumnString&>(keys_nullable.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = 0;
            if (map_nullable.is_null_at(input_row)) {
                continue;
            }
            const auto begin = input_row == 0 ? 0 : offsets[input_row - 1];
            const auto end = offsets[input_row];
            for (auto entry = begin; entry < end; ++entry) {
                if (!keys_nullable.is_null_at(entry) &&
                    keys.get_data_at(entry).to_string_view() == _key) {
                    result_data[row] = 1;
                    break;
                }
            }
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::string _key;
    const std::string _expr_name = "NullableMapKeysInExpr";
};

class NullableArraySizeGreaterThanExpr final : public VExpr {
public:
    NullableArraySizeGreaterThanExpr(int column_id, DataTypePtr array_type, int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        auto size_expr = function_expr("size", int32_type, {array_type});
        size_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         std::move(array_type), "array_col"));
        add_child(std::move(size_expr));
        add_child(TableLiteral::create_shared(int32_type, Field::create_field<TYPE_INT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& array_nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& array_column =
                assert_cast<const ColumnArray&>(array_nullable.get_nested_column());
        const auto& offsets = array_column.get_offsets();
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            const auto begin = input_row == 0 ? 0 : offsets[input_row - 1];
            const auto end = offsets[input_row];
            result_data[row] = !array_nullable.is_null_at(input_row) &&
                               cast_set<int32_t>(end - begin) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableArraySizeGreaterThanExpr";
};

class NullableArrayStructChildInt32GreaterThanExpr final : public VExpr {
public:
    NullableArrayStructChildInt32GreaterThanExpr(int column_id, DataTypePtr array_type,
                                                 DataTypePtr struct_type, std::string child_name,
                                                 int32_t value)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto int32_type = std::make_shared<DataTypeInt32>();
        const auto string_type = std::make_shared<DataTypeString>();
        auto element_expr = function_expr("element_at", struct_type, {array_type, int32_type});
        element_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                            std::move(array_type), "array_col"));
        element_expr->add_child(
                TableLiteral::create_shared(int32_type, Field::create_field<TYPE_INT>(1)));
        auto child_expr = function_expr("struct_element", make_nullable(int32_type),
                                        {struct_type, string_type});
        child_expr->add_child(std::move(element_expr));
        child_expr->add_child(TableLiteral::create_shared(
                string_type, Field::create_field<TYPE_STRING>(std::move(child_name))));
        add_child(std::move(child_expr));
        add_child(TableLiteral::create_shared(int32_type, Field::create_field<TYPE_INT>(value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& array_nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& array_column =
                assert_cast<const ColumnArray&>(array_nullable.get_nested_column());
        const auto& offsets = array_column.get_offsets();
        const auto& values_nullable = assert_cast<const ColumnNullable&>(array_column.get_data());
        const auto& struct_values =
                assert_cast<const ColumnStruct&>(values_nullable.get_nested_column());
        const auto& child_nullable =
                assert_cast<const ColumnNullable&>(struct_values.get_column(0));
        const auto& child = assert_cast<const ColumnInt32&>(child_nullable.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            const auto begin = input_row == 0 ? 0 : offsets[input_row - 1];
            const auto end = offsets[input_row];
            result_data[row] = 0;
            if (array_nullable.is_null_at(input_row) || begin == end ||
                values_nullable.is_null_at(begin) || child_nullable.is_null_at(begin)) {
                continue;
            }
            result_data[row] = child.get_element(begin) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _value;
    const std::string _expr_name = "NullableArrayStructChildInt32GreaterThanExpr";
};

template <PrimitiveType Primitive>
class NullableGreaterThanExpr final : public VExpr {
public:
    using ColumnType = typename PrimitiveTypeTraits<Primitive>::ColumnType;
    using ValueType = typename PrimitiveTypeTraits<Primitive>::CppType;

    NullableGreaterThanExpr(int column_id, DataTypePtr type, const Field& value,
                            std::string column_name, bool literal_on_left = false)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(value.get<Primitive>()),
              _expr_name("NullableGreaterThanExpr") {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = literal_on_left ? TExprOpcode::LT : TExprOpcode::GT;
        auto slot = TableSlotRef::create_shared(column_id, column_id, -1, make_nullable(type),
                                                column_name);
        auto literal = TableLiteral::create_shared(std::move(type), value);
        if (literal_on_left) {
            add_child(literal);
            add_child(slot);
        } else {
            add_child(slot);
            add_child(literal);
        }
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnType&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] =
                    !nullable_column.is_null_at(input_row) && input.get_element(input_row) > _value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const ValueType _value;
    const std::string _expr_name;
};

template <PrimitiveType Primitive>
class NullableInExpr final : public VExpr {
public:
    using ColumnType = typename PrimitiveTypeTraits<Primitive>::ColumnType;
    using ValueType = typename PrimitiveTypeTraits<Primitive>::CppType;

    NullableInExpr(int column_id, DataTypePtr type, std::vector<Field> values,
                   std::string column_name, bool not_in = false)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _not_in(not_in) {
        _node_type = TExprNodeType::IN_PRED;
        _opcode = not_in ? TExprOpcode::FILTER_NOT_IN : TExprOpcode::FILTER_IN;
        add_child(TableSlotRef::create_shared(column_id, column_id, -1, make_nullable(type),
                                              column_name));
        _values.reserve(values.size());
        for (const auto& value : values) {
            _values.push_back(value.get<Primitive>());
            add_child(TableLiteral::create_shared(type, value));
        }
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input = assert_cast<const ColumnType&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            if (nullable_column.is_null_at(input_row)) {
                result_data[row] = 0;
                continue;
            }
            const auto value = input.get_element(input_row);
            const auto contains = std::find(_values.begin(), _values.end(), value) != _values.end();
            result_data[row] = _not_in ? !contains : contains;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const bool _not_in;
    std::vector<ValueType> _values;
    const std::string _expr_name = "NullableInExpr";
};

class NullableDecimalCastToStringGreaterThanExpr final : public VExpr {
public:
    NullableDecimalCastToStringGreaterThanExpr(int column_id, DataTypePtr slot_type,
                                               std::string value, std::string column_name,
                                               uint32_t scale)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _value(std::move(value)),
              _scale(scale) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        const auto string_type = std::make_shared<DataTypeString>();
        auto cast_expr = format::Cast::create_shared(string_type);
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(std::move(slot_type)),
                                                         std::move(column_name)));
        add_child(std::move(cast_expr));
        add_child(
                TableLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(_value)));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input =
                assert_cast<const ColumnDecimal128V3&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_element(input_row).to_string(_scale).compare(_value) > 0;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const std::string _value;
    const uint32_t _scale;
    const std::string _expr_name = "NullableDecimalCastToStringGreaterThanExpr";
};

class NullableDecimalGreaterThanExpr final : public VExpr {
public:
    NullableDecimalGreaterThanExpr(int column_id, DataTypePtr slot_type, DataTypePtr literal_type,
                                   const Field& literal, Decimal128V3 file_scale_value,
                                   std::string column_name)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _file_scale_value(file_scale_value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        add_child(TableSlotRef::create_shared(column_id, column_id, -1, make_nullable(slot_type),
                                              column_name));
        add_child(TableLiteral::create_shared(std::move(literal_type), literal));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input =
                assert_cast<const ColumnDecimal128V3&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_element(input_row) > _file_scale_value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const Decimal128V3 _file_scale_value;
    const std::string _expr_name = "NullableDecimalGreaterThanExpr";
};

class NullableDecimalCastGreaterThanExpr final : public VExpr {
public:
    NullableDecimalCastGreaterThanExpr(int column_id, DataTypePtr slot_type, DataTypePtr cast_type,
                                       const Field& literal, Decimal128V3 file_scale_value,
                                       std::string column_name)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _file_scale_value(file_scale_value) {
        _node_type = TExprNodeType::BINARY_PRED;
        _opcode = TExprOpcode::GT;
        auto cast_expr = format::Cast::create_shared(make_nullable(cast_type));
        cast_expr->add_child(TableSlotRef::create_shared(column_id, column_id, -1,
                                                         make_nullable(std::move(slot_type)),
                                                         std::move(column_name)));
        add_child(std::move(cast_expr));
        add_child(TableLiteral::create_shared(std::move(cast_type), literal));
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& input =
                assert_cast<const ColumnDecimal128V3&>(nullable_column.get_nested_column());
        auto result = ColumnUInt8::create();
        auto& result_data = result->get_data();
        result_data.resize(count);
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            result_data[row] = !nullable_column.is_null_at(input_row) &&
                               input.get_element(input_row) > _file_scale_value;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const Decimal128V3 _file_scale_value;
    const std::string _expr_name = "NullableDecimalCastGreaterThanExpr";
};

void set_nullable_batch(::orc::ColumnVectorBatch& batch) {
    batch.hasNulls = true;
    batch.numElements = PRIMITIVE_ROW_COUNT;
    for (int64_t row = 0; row < PRIMITIVE_ROW_COUNT; ++row) {
        batch.notNull[row] = 1;
    }
    batch.notNull[NULL_ROW] = 0;
}

void set_string_value(::orc::StringVectorBatch& batch, int64_t row, std::string_view value) {
    static constexpr uint64_t MAX_TEST_STRING_BYTES_PER_ROW = 4096;
    // Reserve once per batch so previously assigned data pointers remain stable.
    const auto reserve_bytes = std::max<uint64_t>(batch.capacity * MAX_TEST_STRING_BYTES_PER_ROW,
                                                  MAX_TEST_STRING_BYTES_PER_ROW);
    batch.blob.reserve(reserve_bytes);
    const auto offset = batch.blob.size();
    ASSERT_LE(offset + value.size(), reserve_bytes);
    batch.blob.resize(offset + value.size());
    auto* stored_value = batch.blob.data() + offset;
    if (!value.empty()) {
        memcpy(stored_value, value.data(), value.size());
    }
    batch.data[row] = stored_value;
    batch.length[row] = static_cast<int64_t>(value.size());
}

template <typename Offsets>
void expect_offsets(const Offsets& offsets, std::initializer_list<size_t> expected) {
    ASSERT_EQ(offsets.size(), expected.size());
    size_t idx = 0;
    for (const auto value : expected) {
        EXPECT_EQ(offsets[idx++], value);
    }
}

void expect_string_values(const ColumnString& column,
                          std::initializer_list<std::string_view> expected) {
    ASSERT_EQ(column.size(), expected.size());
    size_t idx = 0;
    for (const auto value : expected) {
        EXPECT_EQ(column.get_data_at(idx++).to_string(), value);
    }
}

void expect_int32_values(const ColumnInt32& column, std::initializer_list<int32_t> expected) {
    ASSERT_EQ(column.size(), expected.size());
    size_t idx = 0;
    for (const auto value : expected) {
        EXPECT_EQ(column.get_element(idx++), value);
    }
}

void expect_deep_nested_column(const IColumn& column, std::initializer_list<bool> expected_nulls,
                               std::initializer_list<size_t> expected_array_offsets,
                               std::initializer_list<std::string_view> expected_names,
                               std::initializer_list<size_t> expected_map_offsets,
                               std::initializer_list<std::string_view> expected_keys,
                               std::initializer_list<size_t> expected_value_array_offsets,
                               std::initializer_list<int32_t> expected_values,
                               std::initializer_list<std::string_view> expected_labels) {
    const auto& deep_nullable = assert_cast<const ColumnNullable&>(column);
    ASSERT_EQ(deep_nullable.size(), expected_nulls.size());
    size_t row = 0;
    for (const auto expected_null : expected_nulls) {
        EXPECT_EQ(deep_nullable.is_null_at(row++), expected_null);
    }

    const auto& deep_array = assert_cast<const ColumnArray&>(deep_nullable.get_nested_column());
    expect_offsets(deep_array.get_offsets(), expected_array_offsets);

    const auto& element_struct = assert_cast<const ColumnStruct&>(
            assert_cast<const ColumnNullable&>(deep_array.get_data()).get_nested_column());
    ASSERT_EQ(element_struct.tuple_size(), 2);
    const auto& names = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(element_struct.get_column(0)).get_nested_column());
    expect_string_values(names, expected_names);

    const auto& nested_map = assert_cast<const ColumnMap&>(
            assert_cast<const ColumnNullable&>(element_struct.get_column(1)).get_nested_column());
    expect_offsets(nested_map.get_offsets(), expected_map_offsets);
    const auto& keys = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(nested_map.get_keys()).get_nested_column());
    expect_string_values(keys, expected_keys);

    const auto& value_array = assert_cast<const ColumnArray&>(
            assert_cast<const ColumnNullable&>(nested_map.get_values()).get_nested_column());
    expect_offsets(value_array.get_offsets(), expected_value_array_offsets);
    const auto& leaf_struct = assert_cast<const ColumnStruct&>(
            assert_cast<const ColumnNullable&>(value_array.get_data()).get_nested_column());
    ASSERT_EQ(leaf_struct.tuple_size(), 2);
    const auto& values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(leaf_struct.get_column(0)).get_nested_column());
    expect_int32_values(values, expected_values);
    const auto& labels = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(leaf_struct.get_column(1)).get_nested_column());
    expect_string_values(labels, expected_labels);
}

void write_orc_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<id:int,value:string>"));

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(ROW_COUNT);
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
    auto& value_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);

    std::vector<std::string> values {"one", "two", "three", "four", "five"};
    for (int64_t row = 0; row < ROW_COUNT; ++row) {
        id_batch.data[row] = row + 1;
        set_string_value(value_batch, row, values[row]);
    }
    struct_batch.numElements = ROW_COUNT;
    id_batch.numElements = ROW_COUNT;
    value_batch.numElements = ROW_COUNT;

    writer->add(*batch);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_primitive_orc_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<bool_col:boolean,byte_col:tinyint,short_col:smallint,int_col:int,"
            "long_col:bigint,float_col:float,double_col:double,string_col:string,"
            "binary_col:binary,varchar_col:varchar(8),char_col:char(6),date_col:date,"
            "timestamp_col:timestamp,timestamp_instant_col:timestamp with local time zone,"
            "decimal64_col:decimal(12,2),decimal128_col:decimal(30,6)>"));

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setTimezoneName("UTC");
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(PRIMITIVE_ROW_COUNT);
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);

    auto& bool_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
    auto& byte_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[1]);
    auto& short_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[2]);
    auto& int_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[3]);
    auto& long_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[4]);
    auto& float_batch = dynamic_cast<::orc::DoubleVectorBatch&>(*struct_batch.fields[5]);
    auto& double_batch = dynamic_cast<::orc::DoubleVectorBatch&>(*struct_batch.fields[6]);
    auto& string_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[7]);
    auto& binary_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[8]);
    auto& varchar_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[9]);
    auto& char_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[10]);
    auto& date_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[11]);
    auto& timestamp_batch = dynamic_cast<::orc::TimestampVectorBatch&>(*struct_batch.fields[12]);
    auto& timestamp_instant_batch =
            dynamic_cast<::orc::TimestampVectorBatch&>(*struct_batch.fields[13]);
    auto& decimal64_batch = dynamic_cast<::orc::Decimal64VectorBatch&>(*struct_batch.fields[14]);
    auto& decimal128_batch = dynamic_cast<::orc::Decimal128VectorBatch&>(*struct_batch.fields[15]);

    std::array<::orc::ColumnVectorBatch*, 16> fields {
            &bool_batch,      &byte_batch,
            &short_batch,     &int_batch,
            &long_batch,      &float_batch,
            &double_batch,    &string_batch,
            &binary_batch,    &varchar_batch,
            &char_batch,      &date_batch,
            &timestamp_batch, &timestamp_instant_batch,
            &decimal64_batch, &decimal128_batch,
    };
    for (auto* field : fields) {
        set_nullable_batch(*field);
    }

    bool_batch.data[0] = 1;
    bool_batch.data[2] = 0;
    byte_batch.data[0] = -7;
    byte_batch.data[2] = 8;
    short_batch.data[0] = -300;
    short_batch.data[2] = 301;
    int_batch.data[0] = -70000;
    int_batch.data[2] = 70001;
    long_batch.data[0] = -9000000000L;
    long_batch.data[2] = 9000000001L;
    float_batch.data[0] = 1.25;
    float_batch.data[2] = -2.5;
    double_batch.data[0] = 10.5;
    double_batch.data[2] = -20.25;

    static constexpr std::array<std::string_view, PRIMITIVE_ROW_COUNT> string_values {"alpha", "",
                                                                                      "gamma"};
    static constexpr std::array<std::string_view, PRIMITIVE_ROW_COUNT> binary_values {"bin_a", "",
                                                                                      "bin_c"};
    static constexpr std::array<std::string_view, PRIMITIVE_ROW_COUNT> varchar_values {"varchar",
                                                                                       "", "tail"};
    static constexpr std::array<std::string_view, PRIMITIVE_ROW_COUNT> char_values {"ab    ", "",
                                                                                    "xy    "};
    set_string_value(string_batch, 0, string_values[0]);
    set_string_value(string_batch, 2, string_values[2]);
    set_string_value(binary_batch, 0, binary_values[0]);
    set_string_value(binary_batch, 2, binary_values[2]);
    set_string_value(varchar_batch, 0, varchar_values[0]);
    set_string_value(varchar_batch, 2, varchar_values[2]);
    set_string_value(char_batch, 0, char_values[0]);
    set_string_value(char_batch, 2, char_values[2]);

    date_batch.data[0] = 0;
    date_batch.data[2] = 18628;
    timestamp_batch.data[0] = 86401;
    timestamp_batch.nanoseconds[0] = 234567000;
    timestamp_batch.data[2] = 1609459200;
    timestamp_batch.nanoseconds[2] = 123456000;
    timestamp_instant_batch.data[0] = 2;
    timestamp_instant_batch.nanoseconds[0] = 345678000;
    timestamp_instant_batch.data[2] = 1609459201;
    timestamp_instant_batch.nanoseconds[2] = 654321000;
    decimal64_batch.values[0] = 12345;
    decimal64_batch.values[2] = -6700;
    decimal128_batch.values[0] = ::orc::Int128("123456789012345678");
    decimal128_batch.values[2] = ::orc::Int128("-987654321000000");

    struct_batch.numElements = PRIMITIVE_ROW_COUNT;
    writer->add(*batch);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void fill_complex_struct_column(::orc::StructVectorBatch& struct_col) {
    auto& struct_a = dynamic_cast<::orc::LongVectorBatch&>(*struct_col.fields[0]);
    auto& struct_b = dynamic_cast<::orc::StringVectorBatch&>(*struct_col.fields[1]);
    std::array<int64_t, COMPLEX_ROW_COUNT> struct_a_values {10, 20, 30};
    static constexpr std::array<std::string_view, COMPLEX_ROW_COUNT> struct_b_values {
            "ten", "twenty", "thirty"};
    for (int64_t row = 0; row < COMPLEX_ROW_COUNT; ++row) {
        struct_a.data[row] = struct_a_values[row];
        set_string_value(struct_b, row, struct_b_values[row]);
    }
    struct_col.numElements = COMPLEX_ROW_COUNT;
    struct_a.numElements = COMPLEX_ROW_COUNT;
    struct_b.numElements = COMPLEX_ROW_COUNT;
}

void fill_complex_array_column(::orc::ListVectorBatch& array_col) {
    auto& array_values = dynamic_cast<::orc::LongVectorBatch&>(*array_col.elements);
    std::array<int64_t, 4> array_offsets {0, 1, 1, 3};
    std::array<int64_t, 3> array_data {1, 2, 3};
    for (int64_t row = 0; row <= COMPLEX_ROW_COUNT; ++row) {
        array_col.offsets[row] = array_offsets[row];
    }
    for (size_t idx = 0; idx < array_data.size(); ++idx) {
        array_values.data[idx] = array_data[idx];
    }
    array_col.numElements = COMPLEX_ROW_COUNT;
    array_values.numElements = array_data.size();
}

void fill_complex_map_column(::orc::MapVectorBatch& map_col) {
    auto& map_keys = dynamic_cast<::orc::StringVectorBatch&>(*map_col.keys);
    auto& map_values = dynamic_cast<::orc::LongVectorBatch&>(*map_col.elements);
    std::array<int64_t, 4> map_offsets {0, 2, 2, 3};
    static constexpr std::array<std::string_view, 3> map_key_values {"a", "b", "c"};
    std::array<int64_t, 3> map_data {100, 200, 300};
    for (int64_t row = 0; row <= COMPLEX_ROW_COUNT; ++row) {
        map_col.offsets[row] = map_offsets[row];
    }
    for (size_t idx = 0; idx < map_key_values.size(); ++idx) {
        set_string_value(map_keys, static_cast<int64_t>(idx), map_key_values[idx]);
        map_values.data[idx] = map_data[idx];
    }
    map_col.numElements = COMPLEX_ROW_COUNT;
    map_keys.numElements = map_key_values.size();
    map_values.numElements = map_data.size();
}

void fill_complex_array_struct_column(::orc::ListVectorBatch& array_struct_col) {
    auto& array_struct_values = dynamic_cast<::orc::StructVectorBatch&>(*array_struct_col.elements);
    auto& array_struct_a = dynamic_cast<::orc::LongVectorBatch&>(*array_struct_values.fields[0]);
    auto& array_struct_b = dynamic_cast<::orc::StringVectorBatch&>(*array_struct_values.fields[1]);
    std::array<int64_t, 4> array_struct_offsets {0, 1, 3, 3};
    std::array<int64_t, 3> array_struct_a_values {1, 2, 3};
    static constexpr std::array<std::string_view, 3> array_struct_b_values {"one", "two", "three"};
    for (int64_t row = 0; row <= COMPLEX_ROW_COUNT; ++row) {
        array_struct_col.offsets[row] = array_struct_offsets[row];
    }
    for (size_t idx = 0; idx < array_struct_a_values.size(); ++idx) {
        array_struct_a.data[idx] = array_struct_a_values[idx];
        set_string_value(array_struct_b, static_cast<int64_t>(idx), array_struct_b_values[idx]);
    }
    array_struct_col.numElements = COMPLEX_ROW_COUNT;
    array_struct_values.numElements = array_struct_a_values.size();
    array_struct_a.numElements = array_struct_a_values.size();
    array_struct_b.numElements = array_struct_b_values.size();
}

void fill_complex_map_struct_column(::orc::MapVectorBatch& map_struct_col) {
    auto& map_struct_keys = dynamic_cast<::orc::StringVectorBatch&>(*map_struct_col.keys);
    auto& map_struct_values = dynamic_cast<::orc::StructVectorBatch&>(*map_struct_col.elements);
    auto& map_struct_a = dynamic_cast<::orc::LongVectorBatch&>(*map_struct_values.fields[0]);
    auto& map_struct_b = dynamic_cast<::orc::StringVectorBatch&>(*map_struct_values.fields[1]);
    std::array<int64_t, 4> map_struct_offsets {0, 1, 1, 2};
    static constexpr std::array<std::string_view, 2> map_struct_key_values {"first", "second"};
    std::array<int64_t, 2> map_struct_a_values {7, 8};
    static constexpr std::array<std::string_view, 2> map_struct_b_values {"seven", "eight"};
    for (int64_t row = 0; row <= COMPLEX_ROW_COUNT; ++row) {
        map_struct_col.offsets[row] = map_struct_offsets[row];
    }
    for (size_t idx = 0; idx < map_struct_key_values.size(); ++idx) {
        set_string_value(map_struct_keys, static_cast<int64_t>(idx), map_struct_key_values[idx]);
        map_struct_a.data[idx] = map_struct_a_values[idx];
        set_string_value(map_struct_b, static_cast<int64_t>(idx), map_struct_b_values[idx]);
    }
    map_struct_col.numElements = COMPLEX_ROW_COUNT;
    map_struct_keys.numElements = map_struct_key_values.size();
    map_struct_values.numElements = map_struct_a_values.size();
    map_struct_a.numElements = map_struct_a_values.size();
    map_struct_b.numElements = map_struct_b_values.size();
}

void fill_nullable_struct_column(::orc::StructVectorBatch& struct_col) {
    auto& struct_a = dynamic_cast<::orc::LongVectorBatch&>(*struct_col.fields[0]);
    auto& struct_b = dynamic_cast<::orc::StringVectorBatch&>(*struct_col.fields[1]);
    set_nullable_batch(struct_col);
    std::array<int64_t, COMPLEX_ROW_COUNT> struct_a_values {10, 20, 30};
    static constexpr std::array<std::string_view, COMPLEX_ROW_COUNT> struct_b_values {
            "ten", "ignored", ""};
    for (int64_t row = 0; row < COMPLEX_ROW_COUNT; ++row) {
        struct_a.data[row] = struct_a_values[row];
        set_string_value(struct_b, row, struct_b_values[row]);
    }
    struct_b.hasNulls = true;
    struct_b.notNull[0] = 1;
    struct_b.notNull[1] = 1;
    struct_b.notNull[2] = 0;
    struct_a.numElements = COMPLEX_ROW_COUNT;
    struct_b.numElements = COMPLEX_ROW_COUNT;
}

void fill_nullable_array_struct_column(::orc::ListVectorBatch& array_struct_col) {
    auto& array_struct_values = dynamic_cast<::orc::StructVectorBatch&>(*array_struct_col.elements);
    auto& array_struct_a = dynamic_cast<::orc::LongVectorBatch&>(*array_struct_values.fields[0]);
    auto& array_struct_b = dynamic_cast<::orc::StringVectorBatch&>(*array_struct_values.fields[1]);
    std::array<int64_t, 4> array_struct_offsets {0, 1, 1, 2};
    std::array<int64_t, 2> array_struct_a_values {11, 22};
    static constexpr std::array<std::string_view, 2> array_struct_b_values {"eleven", "twenty_two"};
    set_nullable_batch(array_struct_col);
    for (int64_t row = 0; row <= COMPLEX_ROW_COUNT; ++row) {
        array_struct_col.offsets[row] = array_struct_offsets[row];
    }
    for (size_t idx = 0; idx < array_struct_a_values.size(); ++idx) {
        array_struct_a.data[idx] = array_struct_a_values[idx];
        set_string_value(array_struct_b, static_cast<int64_t>(idx), array_struct_b_values[idx]);
    }
    array_struct_values.numElements = array_struct_a_values.size();
    array_struct_a.numElements = array_struct_a_values.size();
    array_struct_b.numElements = array_struct_b_values.size();
}

void fill_nullable_map_struct_column(::orc::MapVectorBatch& map_struct_col) {
    auto& map_struct_keys = dynamic_cast<::orc::StringVectorBatch&>(*map_struct_col.keys);
    auto& map_struct_values = dynamic_cast<::orc::StructVectorBatch&>(*map_struct_col.elements);
    auto& map_struct_a = dynamic_cast<::orc::LongVectorBatch&>(*map_struct_values.fields[0]);
    auto& map_struct_b = dynamic_cast<::orc::StringVectorBatch&>(*map_struct_values.fields[1]);
    std::array<int64_t, 4> map_struct_offsets {0, 1, 1, 2};
    static constexpr std::array<std::string_view, 2> map_struct_key_values {"left", "right"};
    std::array<int64_t, 2> map_struct_a_values {101, 202};
    static constexpr std::array<std::string_view, 2> map_struct_b_values {"one_zero_one",
                                                                          "two_zero_two"};
    set_nullable_batch(map_struct_col);
    for (int64_t row = 0; row <= COMPLEX_ROW_COUNT; ++row) {
        map_struct_col.offsets[row] = map_struct_offsets[row];
    }
    for (size_t idx = 0; idx < map_struct_key_values.size(); ++idx) {
        set_string_value(map_struct_keys, static_cast<int64_t>(idx), map_struct_key_values[idx]);
        map_struct_a.data[idx] = map_struct_a_values[idx];
        set_string_value(map_struct_b, static_cast<int64_t>(idx), map_struct_b_values[idx]);
    }
    map_struct_keys.numElements = map_struct_key_values.size();
    map_struct_values.numElements = map_struct_a_values.size();
    map_struct_a.numElements = map_struct_a_values.size();
    map_struct_b.numElements = map_struct_b_values.size();
}

void write_complex_orc_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<struct_col:struct<a:int,b:string>,array_col:array<int>,"
            "map_col:map<string,int>,array_struct_col:array<struct<a:int,b:string>>,"
            "map_struct_col:map<string,struct<a:int,b:string>>>"));

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(COMPLEX_ROW_COUNT);
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);

    fill_complex_struct_column(dynamic_cast<::orc::StructVectorBatch&>(*struct_batch.fields[0]));
    fill_complex_array_column(dynamic_cast<::orc::ListVectorBatch&>(*struct_batch.fields[1]));
    fill_complex_map_column(dynamic_cast<::orc::MapVectorBatch&>(*struct_batch.fields[2]));
    fill_complex_array_struct_column(
            dynamic_cast<::orc::ListVectorBatch&>(*struct_batch.fields[3]));
    fill_complex_map_struct_column(dynamic_cast<::orc::MapVectorBatch&>(*struct_batch.fields[4]));

    struct_batch.numElements = COMPLEX_ROW_COUNT;
    writer->add(*batch);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_map_decimal_date_orc_file(const std::string& file_path) {
    constexpr size_t ROWS = 4;
    constexpr int64_t HIVE_012_1900_DAY_OFFSET = -719530;
    constexpr int64_t YEAR_0000_12_29_DAY_OFFSET = -719165;
    constexpr int64_t YEAR_1000_10_16_DAY_OFFSET = -353997;

    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<id:int,map_col:map<decimal(10,5),date>>"));

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_LZ4);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(ROWS);
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
    auto& map_batch = dynamic_cast<::orc::MapVectorBatch&>(*struct_batch.fields[1]);
    auto& key_batch = dynamic_cast<::orc::Decimal64VectorBatch&>(*map_batch.keys);
    auto& value_batch = dynamic_cast<::orc::LongVectorBatch&>(*map_batch.elements);

    id_batch.data[0] = 1;
    id_batch.data[1] = 2;
    id_batch.data[2] = 3;
    id_batch.data[3] = 4;
    map_batch.offsets[0] = 0;
    map_batch.offsets[1] = 1;
    map_batch.offsets[2] = 2;
    map_batch.offsets[3] = 3;
    map_batch.offsets[4] = 4;
    key_batch.values[0] = -9999999999L;
    key_batch.values[1] = 9999999999L;
    key_batch.values[2] = 0;
    key_batch.values[3] = 1;
    value_batch.data[0] = HIVE_012_1900_DAY_OFFSET;
    value_batch.data[1] = orc_date_offset(9999, 12, 31);
    value_batch.data[2] = YEAR_0000_12_29_DAY_OFFSET;
    value_batch.data[3] = YEAR_1000_10_16_DAY_OFFSET;

    struct_batch.numElements = ROWS;
    id_batch.numElements = ROWS;
    map_batch.numElements = ROWS;
    key_batch.numElements = ROWS;
    value_batch.numElements = ROWS;

    writer->add(*batch);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_two_stripe_orc_array_map_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<array_col:array<int>,map_col:map<string,int>,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int32_t array_value, std::string_view map_key, int32_t map_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& array_batch = dynamic_cast<::orc::ListVectorBatch&>(*struct_batch.fields[0]);
        auto& array_values = dynamic_cast<::orc::LongVectorBatch&>(*array_batch.elements);
        auto& map_batch = dynamic_cast<::orc::MapVectorBatch&>(*struct_batch.fields[1]);
        auto& map_keys = dynamic_cast<::orc::StringVectorBatch&>(*map_batch.keys);
        auto& map_values = dynamic_cast<::orc::LongVectorBatch&>(*map_batch.elements);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[2]);
        std::vector<std::string> keys;
        std::vector<std::string> payloads;
        keys.reserve(ROWS_PER_STRIPE);
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row <= ROWS_PER_STRIPE; ++row) {
            array_batch.offsets[row] = row;
            map_batch.offsets[row] = row;
        }
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            array_values.data[row] = array_value;
            keys.emplace_back(map_key);
            set_string_value(map_keys, row, keys.back());
            map_values.data[row] = map_value;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        array_batch.numElements = ROWS_PER_STRIPE;
        array_values.numElements = ROWS_PER_STRIPE;
        map_batch.numElements = ROWS_PER_STRIPE;
        map_keys.numElements = ROWS_PER_STRIPE;
        map_values.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(1, "a", 100);
    add_batch(2, "c", 300);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_two_stripe_orc_array_size_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<array_col:array<int>,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](bool has_element) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& array_batch = dynamic_cast<::orc::ListVectorBatch&>(*struct_batch.fields[0]);
        auto& array_values = dynamic_cast<::orc::LongVectorBatch&>(*array_batch.elements);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row <= ROWS_PER_STRIPE; ++row) {
            array_batch.offsets[row] = has_element ? row : 0;
        }
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            if (has_element) {
                array_values.data[row] = 7;
            }
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        array_batch.numElements = ROWS_PER_STRIPE;
        array_values.numElements = has_element ? ROWS_PER_STRIPE : 0;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(false);
    add_batch(true);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_two_stripe_orc_array_struct_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<array_col:array<struct<a:int,b:string>>,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t child_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& array_batch = dynamic_cast<::orc::ListVectorBatch&>(*struct_batch.fields[0]);
        auto& array_values = dynamic_cast<::orc::StructVectorBatch&>(*array_batch.elements);
        auto& child_a = dynamic_cast<::orc::LongVectorBatch&>(*array_values.fields[0]);
        auto& child_b = dynamic_cast<::orc::StringVectorBatch&>(*array_values.fields[1]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> child_values;
        std::vector<std::string> payloads;
        child_values.reserve(ROWS_PER_STRIPE);
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row <= ROWS_PER_STRIPE; ++row) {
            array_batch.offsets[row] = row;
        }
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            child_a.data[row] = child_value;
            child_values.push_back("child-" + std::to_string(child_value));
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(child_b, row, child_values.back());
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        array_batch.numElements = ROWS_PER_STRIPE;
        array_values.numElements = ROWS_PER_STRIPE;
        child_a.numElements = ROWS_PER_STRIPE;
        child_b.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(1);
    add_batch(1000);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_nullable_complex_orc_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<nullable_struct_col:struct<a:int,b:string>,"
            "nullable_array_struct_col:array<struct<a:int,b:string>>,"
            "nullable_map_struct_col:map<string,struct<a:int,b:string>>>"));

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(COMPLEX_ROW_COUNT);
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);

    fill_nullable_struct_column(dynamic_cast<::orc::StructVectorBatch&>(*struct_batch.fields[0]));
    fill_nullable_array_struct_column(
            dynamic_cast<::orc::ListVectorBatch&>(*struct_batch.fields[1]));
    fill_nullable_map_struct_column(dynamic_cast<::orc::MapVectorBatch&>(*struct_batch.fields[2]));

    struct_batch.numElements = COMPLEX_ROW_COUNT;
    writer->add(*batch);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void fill_deep_nested_complex_column(::orc::ListVectorBatch& deep_col) {
    auto& element_struct = dynamic_cast<::orc::StructVectorBatch&>(*deep_col.elements);
    auto& name_batch = dynamic_cast<::orc::StringVectorBatch&>(*element_struct.fields[0]);
    auto& nested_map = dynamic_cast<::orc::MapVectorBatch&>(*element_struct.fields[1]);
    auto& map_keys = dynamic_cast<::orc::StringVectorBatch&>(*nested_map.keys);
    auto& value_array = dynamic_cast<::orc::ListVectorBatch&>(*nested_map.elements);
    auto& leaf_struct = dynamic_cast<::orc::StructVectorBatch&>(*value_array.elements);
    auto& value_batch = dynamic_cast<::orc::LongVectorBatch&>(*leaf_struct.fields[0]);
    auto& label_batch = dynamic_cast<::orc::StringVectorBatch&>(*leaf_struct.fields[1]);

    deep_col.hasNulls = true;
    for (int64_t row = 0; row < DEEP_NESTED_ROW_COUNT; ++row) {
        deep_col.notNull[row] = 1;
    }
    deep_col.notNull[1] = 0;

    std::array<int64_t, 5> array_offsets {0, 1, 1, 3, 4};
    static constexpr std::array<std::string_view, 4> names {"row1", "row3_left", "row3_right",
                                                            "row4"};
    for (int64_t row = 0; row <= DEEP_NESTED_ROW_COUNT; ++row) {
        deep_col.offsets[row] = array_offsets[row];
    }
    for (size_t idx = 0; idx < names.size(); ++idx) {
        set_string_value(name_batch, static_cast<int64_t>(idx), names[idx]);
    }

    std::array<int64_t, 5> map_offsets {0, 1, 2, 4, 5};
    static constexpr std::array<std::string_view, 5> keys {
            "row1_key", "row3_left_key", "row3_right_a", "row3_right_empty", "row4_key"};
    for (size_t idx = 0; idx < map_offsets.size(); ++idx) {
        nested_map.offsets[idx] = map_offsets[idx];
    }
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        set_string_value(map_keys, static_cast<int64_t>(idx), keys[idx]);
    }

    std::array<int64_t, 6> value_array_offsets {0, 2, 3, 5, 5, 6};
    std::array<int64_t, 6> values {10, 11, 30, 31, 32, 40};
    static constexpr std::array<std::string_view, 6> labels {"ten",        "eleven",     "thirty",
                                                             "thirty_one", "thirty_two", "forty"};
    for (size_t idx = 0; idx < value_array_offsets.size(); ++idx) {
        value_array.offsets[idx] = value_array_offsets[idx];
    }
    for (size_t idx = 0; idx < values.size(); ++idx) {
        value_batch.data[idx] = values[idx];
        set_string_value(label_batch, static_cast<int64_t>(idx), labels[idx]);
    }

    deep_col.numElements = DEEP_NESTED_ROW_COUNT;
    element_struct.numElements = names.size();
    name_batch.numElements = names.size();
    nested_map.numElements = names.size();
    map_keys.numElements = keys.size();
    value_array.numElements = keys.size();
    leaf_struct.numElements = values.size();
    value_batch.numElements = values.size();
    label_batch.numElements = labels.size();
}

void write_deep_nested_complex_orc_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<id:int,deep:array<struct<name:string,"
            "nested:map<string,array<struct<value:int,label:string>>>>>>"));

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);
    auto batch = writer->createRowBatch(DEEP_NESTED_BATCH_CAPACITY);
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);

    for (int64_t row = 0; row < DEEP_NESTED_ROW_COUNT; ++row) {
        id_batch.data[row] = row + 1;
    }
    id_batch.numElements = DEEP_NESTED_ROW_COUNT;
    fill_deep_nested_complex_column(dynamic_cast<::orc::ListVectorBatch&>(*struct_batch.fields[1]));

    struct_batch.numElements = DEEP_NESTED_ROW_COUNT;
    writer->add(*batch);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_int_file(const std::string& file_path,
                                     std::vector<int64_t> first_values = {1, 1000}) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<id:int,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t first_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            id_batch.data[row] = first_value + row;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        id_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    for (const auto first_value : first_values) {
        add_batch(first_value);
    }
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_int_only_file(const std::string& file_path,
                                          const std::vector<int64_t>& first_values) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString("struct<id:int>"));

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t first_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            id_batch.data[row] = first_value + row;
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        id_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    for (const auto first_value : first_values) {
        add_batch(first_value);
    }
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_large_orc_int_file(const std::string& file_path, int64_t row_count) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString("struct<id:int>"));

    MemoryOutputStream memory_stream(1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto batch = writer->createRowBatch(row_count);
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
    for (int64_t row = 0; row < row_count; ++row) {
        id_batch.data[row] = row + 1;
    }
    struct_batch.numElements = row_count;
    id_batch.numElements = row_count;
    writer->add(*batch);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_sarg_skipped_row_group_orc_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<id:int,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(64 * 1024 * 1024);
    options.setRowIndexStride(ConditionCacheContext::GRANULE_SIZE);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    constexpr int64_t ROWS_PER_GROUP = ConditionCacheContext::GRANULE_SIZE;
    constexpr int64_t ROW_GROUPS = 3;
    auto batch = writer->createRowBatch(ROWS_PER_GROUP * ROW_GROUPS);
    auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
    auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
    auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
    std::vector<std::string> payloads;
    payloads.reserve(ROWS_PER_GROUP * ROW_GROUPS);
    for (int64_t group = 0; group < ROW_GROUPS; ++group) {
        for (int64_t row = 0; row < ROWS_PER_GROUP; ++row) {
            const auto batch_row = group * ROWS_PER_GROUP + row;
            id_batch.data[batch_row] = group == 1 ? 10000 + row : row + 1;
            payloads.push_back("payload_" + std::to_string(batch_row));
            set_string_value(payload_batch, batch_row, payloads.back());
        }
    }
    struct_batch.numElements = ROWS_PER_GROUP * ROW_GROUPS;
    id_batch.numElements = ROWS_PER_GROUP * ROW_GROUPS;
    payload_batch.numElements = ROWS_PER_GROUP * ROW_GROUPS;
    writer->add(*batch);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_pair_int_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<lhs:int,rhs:int,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t first_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& lhs_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
        auto& rhs_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[1]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[2]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            lhs_batch.data[row] = first_value + row;
            rhs_batch.data[row] = first_value + row;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        lhs_batch.numElements = ROWS_PER_STRIPE;
        rhs_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(1);
    add_batch(1000);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_two_stripe_orc_nullable_int_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<id:int,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](bool null_ids) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        id_batch.hasNulls = null_ids;
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            id_batch.notNull[row] = null_ids ? 0 : 1;
            id_batch.data[row] = null_ids ? 0 : row + 1;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        id_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(false);
    add_batch(true);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_short_file(const std::string& file_path,
                                       std::vector<int64_t> first_values = {1, 1000}) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<id:smallint,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t first_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            id_batch.data[row] = first_value + row;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        id_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    for (const auto first_value : first_values) {
        add_batch(first_value);
    }
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_long_file(const std::string& file_path,
                                      std::vector<int64_t> first_values = {1, 1000}) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<id:bigint,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t first_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& id_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            id_batch.data[row] = first_value + row;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        id_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    for (const auto first_value : first_values) {
        add_batch(first_value);
    }
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_varchar_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<value:varchar(16),payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](std::string_view prefix) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& value_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> values;
        std::vector<std::string> payloads;
        values.reserve(ROWS_PER_STRIPE);
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            values.push_back(std::string(prefix) + "_" + std::to_string(1000 + row));
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(value_batch, row, values.back());
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        value_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch("aaa");
    add_batch("zzz");
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_binary_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<value:binary,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setRowIndexStride(50);
    options.setDictionaryKeySizeThreshold(0);
    options.setColumnsUseBloomFilter(std::set<uint64_t> {1});
    options.setBloomFilterFPP(0.000001);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](std::string_view prefix) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& value_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> values;
        std::vector<std::string> payloads;
        values.reserve(ROWS_PER_STRIPE);
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            values.push_back(std::string(prefix) + "_" + std::to_string(1000 + row));
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(value_batch, row, values.back());
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        value_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch("aaa");
    add_batch("zzz");
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_char_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<value:char(16),payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](std::string_view prefix) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& value_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> values;
        std::vector<std::string> payloads;
        values.reserve(ROWS_PER_STRIPE);
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            values.push_back(std::string(prefix) + "_" + std::to_string(1000 + row));
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(value_batch, row, values.back());
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        value_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch("aaa");
    add_batch("zzz");
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_struct_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<struct_col:struct<a:int,b:string>,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t first_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& nested_struct = dynamic_cast<::orc::StructVectorBatch&>(*struct_batch.fields[0]);
        auto& child_a = dynamic_cast<::orc::LongVectorBatch&>(*nested_struct.fields[0]);
        auto& child_b = dynamic_cast<::orc::StringVectorBatch&>(*nested_struct.fields[1]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> child_values;
        std::vector<std::string> payloads;
        child_values.reserve(ROWS_PER_STRIPE);
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            child_a.data[row] = first_value + row;
            child_values.push_back("child-" + std::to_string(first_value + row));
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(child_b, row, child_values.back());
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        nested_struct.numElements = ROWS_PER_STRIPE;
        child_a.numElements = ROWS_PER_STRIPE;
        child_b.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(1);
    add_batch(1000);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_nested_struct_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<struct_col:struct<nested:struct<a:int>,b:string>,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t first_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& struct_col = dynamic_cast<::orc::StructVectorBatch&>(*struct_batch.fields[0]);
        auto& nested_struct = dynamic_cast<::orc::StructVectorBatch&>(*struct_col.fields[0]);
        auto& child_a = dynamic_cast<::orc::LongVectorBatch&>(*nested_struct.fields[0]);
        auto& child_b = dynamic_cast<::orc::StringVectorBatch&>(*struct_col.fields[1]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> child_values;
        std::vector<std::string> payloads;
        child_values.reserve(ROWS_PER_STRIPE);
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            child_a.data[row] = first_value + row;
            child_values.push_back("child-" + std::to_string(first_value + row));
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(child_b, row, child_values.back());
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        struct_col.numElements = ROWS_PER_STRIPE;
        nested_struct.numElements = ROWS_PER_STRIPE;
        child_a.numElements = ROWS_PER_STRIPE;
        child_b.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(1);
    add_batch(1000);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_struct_array_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<struct_col:struct<items:array<int>>,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& struct_col = dynamic_cast<::orc::StructVectorBatch&>(*struct_batch.fields[0]);
        auto& array_child = dynamic_cast<::orc::ListVectorBatch&>(*struct_col.fields[0]);
        auto& array_values = dynamic_cast<::orc::LongVectorBatch&>(*array_child.elements);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            array_child.offsets[row] = row;
            array_values.data[row] = value;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        array_child.offsets[ROWS_PER_STRIPE] = ROWS_PER_STRIPE;
        struct_batch.numElements = ROWS_PER_STRIPE;
        struct_col.numElements = ROWS_PER_STRIPE;
        array_child.numElements = ROWS_PER_STRIPE;
        array_values.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(1);
    add_batch(1000);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_float_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<float_col:float,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](double first_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& float_batch = dynamic_cast<::orc::DoubleVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            float_batch.data[row] = first_value + static_cast<double>(row);
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        float_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(1);
    add_batch(1000);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_sarg_types_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<date_col:date,timestamp_col:timestamp,decimal_col:decimal(12,2),"
            "payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t first_date_day, int64_t first_timestamp_second,
                         int64_t first_decimal_value) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& date_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
        auto& timestamp_batch = dynamic_cast<::orc::TimestampVectorBatch&>(*struct_batch.fields[1]);
        auto& decimal_batch = dynamic_cast<::orc::Decimal64VectorBatch&>(*struct_batch.fields[2]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[3]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            date_batch.data[row] = first_date_day + row;
            timestamp_batch.data[row] = first_timestamp_second + row;
            timestamp_batch.nanoseconds[row] = 123000000;
            decimal_batch.values[row] = first_decimal_value + row;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        date_batch.numElements = ROWS_PER_STRIPE;
        timestamp_batch.numElements = ROWS_PER_STRIPE;
        decimal_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(0, 0, 0);
    add_batch(18628, 1609459200, 100000);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_multi_stripe_orc_timestamp_instant_sarg_file(const std::string& file_path) {
    auto type = std::unique_ptr<::orc::Type>(::orc::Type::buildTypeFromString(
            "struct<timestamp_instant_col:timestamp with local time zone,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t first_timestamp_second) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& timestamp_batch = dynamic_cast<::orc::TimestampVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            timestamp_batch.data[row] = first_timestamp_second + row;
            timestamp_batch.nanoseconds[row] = 123000000;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        timestamp_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(0);
    add_batch(1609459200);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_two_stripe_constant_date_file(const std::string& file_path, int64_t first_date_day,
                                         int64_t second_date_day) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<date_col:date,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t date_day) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& date_batch = dynamic_cast<::orc::LongVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            date_batch.data[row] = date_day;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        date_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(first_date_day);
    add_batch(second_date_day);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

void write_two_stripe_constant_timestamp_file(const std::string& file_path,
                                              int64_t first_timestamp_second,
                                              int64_t second_timestamp_second) {
    auto type = std::unique_ptr<::orc::Type>(
            ::orc::Type::buildTypeFromString("struct<timestamp_col:timestamp,payload:string>"));

    MemoryOutputStream memory_stream(4 * 1024 * 1024);
    ::orc::WriterOptions options;
    options.setCompression(::orc::CompressionKind_NONE);
    options.setMemoryPool(::orc::getDefaultPool());
    options.setStripeSize(1);
    options.setDictionaryKeySizeThreshold(0);
    auto writer = ::orc::createWriter(*type, &memory_stream, options);

    auto add_batch = [&](int64_t timestamp_second) {
        constexpr int64_t ROWS_PER_STRIPE = 200;
        auto batch = writer->createRowBatch(ROWS_PER_STRIPE);
        auto& struct_batch = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& timestamp_batch = dynamic_cast<::orc::TimestampVectorBatch&>(*struct_batch.fields[0]);
        auto& payload_batch = dynamic_cast<::orc::StringVectorBatch&>(*struct_batch.fields[1]);
        std::vector<std::string> payloads;
        payloads.reserve(ROWS_PER_STRIPE);
        for (int64_t row = 0; row < ROWS_PER_STRIPE; ++row) {
            timestamp_batch.data[row] = timestamp_second;
            timestamp_batch.nanoseconds[row] = 123000000;
            payloads.push_back(std::string(2048, static_cast<char>('a' + row % 26)));
            set_string_value(payload_batch, row, payloads.back());
        }
        struct_batch.numElements = ROWS_PER_STRIPE;
        timestamp_batch.numElements = ROWS_PER_STRIPE;
        payload_batch.numElements = ROWS_PER_STRIPE;
        writer->add(*batch);
    };

    add_batch(first_timestamp_second);
    add_batch(second_timestamp_second);
    writer->close();

    std::ofstream out(file_path, std::ios::binary);
    out.write(memory_stream.getData(), static_cast<std::streamsize>(memory_stream.getLength()));
}

uint64_t get_orc_stripe_count(const std::string& file_path) {
    std::ifstream in(file_path, std::ios::binary | std::ios::ate);
    const auto file_size = in.tellg();
    in.seekg(0);
    std::vector<char> data(static_cast<size_t>(file_size));
    in.read(data.data(), file_size);

    ::orc::ReaderOptions options;
    options.setMemoryPool(*::orc::getDefaultPool());
    auto input_stream = std::make_unique<MemoryInputStream>(data.data(), data.size());
    auto reader = ::orc::createReader(std::move(input_stream), options);
    return reader->getNumberOfStripes();
}

struct OrcStripeLayout {
    uint64_t offset = 0;
    uint64_t length = 0;
    uint64_t rows = 0;
};

std::vector<OrcStripeLayout> get_orc_stripe_layout(const std::string& file_path) {
    std::ifstream in(file_path, std::ios::binary | std::ios::ate);
    const auto file_size = in.tellg();
    in.seekg(0);
    std::vector<char> data(static_cast<size_t>(file_size));
    in.read(data.data(), file_size);

    ::orc::ReaderOptions options;
    options.setMemoryPool(*::orc::getDefaultPool());
    auto input_stream = std::make_unique<MemoryInputStream>(data.data(), data.size());
    auto reader = ::orc::createReader(std::move(input_stream), options);

    std::vector<OrcStripeLayout> layout;
    const auto stripe_count = reader->getNumberOfStripes();
    layout.reserve(stripe_count);
    for (uint64_t i = 0; i < stripe_count; ++i) {
        const auto stripe = reader->getStripe(i);
        layout.push_back({.offset = stripe->getOffset(),
                          .length = stripe->getLength(),
                          .rows = stripe->getNumberOfRows()});
    }
    return layout;
}

Block build_file_block(const std::vector<format::ColumnDefinition>& schema) {
    Block block;
    for (const auto& field : schema) {
        block.insert({field.type->create_column(), field.type, field.name});
    }
    return block;
}

const ColumnInt64& int64_data_column(const IColumn& column) {
    if (column.is_nullable()) {
        return assert_cast<const ColumnInt64&>(
                assert_cast<const ColumnNullable&>(column).get_nested_column());
    }
    return assert_cast<const ColumnInt64&>(column);
}

format::LocalColumnIndex field_projection(int32_t field_id, bool project_all_children = true) {
    auto projection = format::LocalColumnIndex::top_level(format::LocalColumnId(field_id));
    projection.project_all_children = project_all_children;
    return projection;
}

format::LocalColumnIndex make_projection(const format::ColumnDefinition& field,
                                         bool project_all_children = true) {
    return field_projection(field.file_local_id(), project_all_children);
}

format::LocalColumnIndex struct_child_projection(int32_t root_field_id, int32_t child_field_id) {
    auto projection = format::LocalColumnIndex::partial_local(root_field_id);
    projection.children.push_back(format::LocalColumnIndex::local(child_field_id));
    return projection;
}

class NewOrcReaderTest : public testing::Test {
protected:
    void SetUp() override {
        _test_dir = unique_test_dir("doris_new_orc_reader_test");
        std::filesystem::remove_all(_test_dir);
        std::filesystem::create_directories(_test_dir);
        _file_path = (_test_dir / "reader.orc").string();
        write_orc_file(_file_path);
    }

    void TearDown() override { std::filesystem::remove_all(_test_dir); }

    std::unique_ptr<format::orc::OrcReader> create_reader(
            RuntimeProfile* profile = nullptr,
            std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt) const {
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = _file_path;
        file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(_file_path));
        return std::make_unique<format::orc::OrcReader>(system_properties, file_description,
                                                        nullptr, profile, global_rowid_context);
    }

    std::unique_ptr<format::orc::OrcReader> create_reader_for_path(
            const std::string& file_path, RuntimeProfile* profile = nullptr,
            std::shared_ptr<io::IOContext> io_ctx = nullptr,
            std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt,
            bool enable_mapping_timestamp_tz = false) const {
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = file_path;
        file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(file_path));
        return std::make_unique<format::orc::OrcReader>(system_properties, file_description, io_ctx,
                                                        profile, global_rowid_context,
                                                        enable_mapping_timestamp_tz);
    }

    // Builds a reader whose FileDescription carries the split byte window
    // [range_start_offset, range_start_offset + range_size). A negative range_size means the
    // whole file (unset sentinel), matching how FE leaves the range unspecified.
    std::unique_ptr<format::orc::OrcReader> create_reader_with_range(
            const std::string& file_path, int64_t range_start_offset, int64_t range_size,
            RuntimeProfile* profile = nullptr) const {
        auto system_properties = std::make_shared<io::FileSystemProperties>();
        system_properties->system_type = TFileType::FILE_LOCAL;
        auto file_description = std::make_unique<io::FileDescription>();
        file_description->path = file_path;
        file_description->file_size = static_cast<int64_t>(std::filesystem::file_size(file_path));
        file_description->range_start_offset = range_start_offset;
        file_description->range_size = range_size;
        return std::make_unique<format::orc::OrcReader>(system_properties, file_description,
                                                        nullptr, profile, std::nullopt);
    }

    std::filesystem::path _test_dir;
    std::string _file_path;
};

TEST_F(NewOrcReaderTest, AggregatePushdownReturnsCountFromFileMetadata) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    auto status = reader->open(request);
    ASSERT_TRUE(status.ok()) << status;

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::COUNT;
    format::FileAggregateResult aggregate_result;
    status = reader->get_aggregate_result(aggregate_request, &aggregate_result);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(aggregate_result.count, ROW_COUNT);
    EXPECT_TRUE(aggregate_result.columns.empty());
}

TEST_F(NewOrcReaderTest, AggregatePushdownCountUsesOnlySplitStripes) {
    const auto multi_stripe_file_path = (_test_dir / "aggregate_count_split.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);
    const auto layout = get_orc_stripe_layout(multi_stripe_file_path);
    ASSERT_EQ(layout.size(), 2);

    auto count_split_rows = [&](uint64_t start, uint64_t size) {
        auto reader = create_reader_with_range(multi_stripe_file_path, static_cast<int64_t>(start),
                                               static_cast<int64_t>(size));
        RuntimeState state {TQueryOptions(), TQueryGlobals()};
        EXPECT_TRUE(reader->init(&state).ok());

        auto request = std::make_shared<format::FileScanRequest>();
        EXPECT_TRUE(reader->open(request).ok());

        format::FileAggregateRequest aggregate_request;
        aggregate_request.agg_type = TPushAggOp::type::COUNT;
        format::FileAggregateResult aggregate_result;
        auto status = reader->get_aggregate_result(aggregate_request, &aggregate_result);
        EXPECT_TRUE(status.ok()) << status;
        return aggregate_result.count;
    };

    const int64_t first_split_count =
            count_split_rows(layout[0].offset, layout[1].offset - layout[0].offset);
    const int64_t second_split_count = count_split_rows(layout[1].offset, layout[1].length);

    EXPECT_EQ(first_split_count, layout[0].rows);
    EXPECT_EQ(second_split_count, layout[1].rows);
    EXPECT_EQ(first_split_count + second_split_count, layout[0].rows + layout[1].rows);
}

TEST_F(NewOrcReaderTest, OpenAcceptsDorisOffsetTimezone) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("+08:00");
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    auto status = reader->open(request);
    ASSERT_TRUE(status.ok()) << status;
}

TEST_F(NewOrcReaderTest, OpenAcceptsDorisCstTimezone) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("CST");
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    auto status = reader->open(request);
    ASSERT_TRUE(status.ok()) << status;
}

TEST_F(NewOrcReaderTest, OpenDelegatesNonHourOffsetTimezoneToOrc) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("+05:30");
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    auto status = reader->open(request);
    EXPECT_FALSE(status.is<ErrorCode::NOT_IMPLEMENTED_ERROR>()) << status.to_string();
    EXPECT_EQ(status.to_string().find("non-hour offset"), std::string::npos) << status.to_string();
}

TEST_F(NewOrcReaderTest, InitReturnsEndOfFileWhenIoContextShouldStop) {
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->should_stop = true;
    auto reader = create_reader_for_path(_file_path, nullptr, io_ctx);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};

    auto status = reader->init(&state);
    EXPECT_TRUE(status.is<ErrorCode::END_OF_FILE>()) << status;
}

TEST_F(NewOrcReaderTest, AggregatePushdownReturnsMinMaxForPrimitiveColumn) {
    const auto multi_stripe_file_path = (_test_dir / "aggregate_minmax_int.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    ASSERT_TRUE(reader->open(request).ok());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::MINMAX;
    aggregate_request.columns.push_back(
            {.projection = format::LocalColumnIndex::top_level(format::LocalColumnId(0))});
    format::FileAggregateResult aggregate_result;
    auto status = reader->get_aggregate_result(aggregate_request, &aggregate_result);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(aggregate_result.columns.size(), 1);
    EXPECT_EQ(aggregate_result.count, 400);
    EXPECT_TRUE(aggregate_result.columns[0].has_min);
    EXPECT_TRUE(aggregate_result.columns[0].has_max);
    EXPECT_EQ(aggregate_result.columns[0].min_value.get<TYPE_INT>(), 1);
    EXPECT_EQ(aggregate_result.columns[0].max_value.get<TYPE_INT>(), 1199);
}

TEST_F(NewOrcReaderTest, AggregatePushdownReturnsMinMaxForStructLeaf) {
    const auto multi_stripe_file_path = (_test_dir / "aggregate_minmax_struct.orc").string();
    write_multi_stripe_orc_struct_file(multi_stripe_file_path);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {struct_child_projection(0, 0)};
    ASSERT_TRUE(reader->open(request).ok());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::MINMAX;
    aggregate_request.columns.push_back({.projection = struct_child_projection(0, 0)});
    format::FileAggregateResult aggregate_result;
    auto status = reader->get_aggregate_result(aggregate_request, &aggregate_result);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(aggregate_result.columns.size(), 1);
    EXPECT_EQ(aggregate_result.count, 400);
    EXPECT_TRUE(aggregate_result.columns[0].has_min);
    EXPECT_TRUE(aggregate_result.columns[0].has_max);
    EXPECT_EQ(aggregate_result.columns[0].min_value.get<TYPE_INT>(), 1);
    EXPECT_EQ(aggregate_result.columns[0].max_value.get<TYPE_INT>(), 1199);
}

TEST_F(NewOrcReaderTest, AggregatePushdownUsesPrunedStripes) {
    const auto multi_stripe_file_path = (_test_dir / "aggregate_pruned_stripes.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 500)));
    ASSERT_TRUE(reader->open(request).ok());

    format::FileAggregateRequest count_request;
    count_request.agg_type = TPushAggOp::type::COUNT;
    format::FileAggregateResult count_result;
    auto status = reader->get_aggregate_result(count_request, &count_result);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(count_result.count, 200);

    format::FileAggregateRequest minmax_request;
    minmax_request.agg_type = TPushAggOp::type::MINMAX;
    minmax_request.columns.push_back(
            {.projection = format::LocalColumnIndex::top_level(format::LocalColumnId(0))});
    format::FileAggregateResult minmax_result;
    status = reader->get_aggregate_result(minmax_request, &minmax_result);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(minmax_result.columns.size(), 1);
    EXPECT_EQ(minmax_result.count, 200);
    EXPECT_TRUE(minmax_result.columns[0].has_min);
    EXPECT_TRUE(minmax_result.columns[0].has_max);
    EXPECT_EQ(minmax_result.columns[0].min_value.get<TYPE_INT>(), 1000);
    EXPECT_EQ(minmax_result.columns[0].max_value.get<TYPE_INT>(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
}

TEST_F(NewOrcReaderTest, AggregatePushdownTimestampMinMaxUsesSessionTimezone) {
    const auto multi_stripe_file_path = (_test_dir / "aggregate_timestamp_timezone.orc").string();
    write_two_stripe_constant_timestamp_file(multi_stripe_file_path, 0, 3600);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("Asia/Shanghai");
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    ASSERT_TRUE(reader->open(request).ok());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::MINMAX;
    aggregate_request.columns.push_back(
            {.projection = format::LocalColumnIndex::top_level(format::LocalColumnId(0))});
    format::FileAggregateResult aggregate_result;
    auto status = reader->get_aggregate_result(aggregate_request, &aggregate_result);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(aggregate_result.columns.size(), 1);
    EXPECT_EQ(aggregate_result.count, 400);
    EXPECT_TRUE(aggregate_result.columns[0].has_min);
    EXPECT_TRUE(aggregate_result.columns[0].has_max);
    EXPECT_EQ(aggregate_result.columns[0].min_value.get<TYPE_DATETIMEV2>(),
              make_datetime_v2(1970, 1, 1, 8, 0, 0, 123000));
    EXPECT_EQ(aggregate_result.columns[0].max_value.get<TYPE_DATETIMEV2>(),
              make_datetime_v2(1970, 1, 1, 9, 0, 0, 123000));
}

TEST_F(NewOrcReaderTest, AggregatePushdownTimestampInstantMinMaxUsesTimestampTzWhenMapped) {
    const auto multi_stripe_file_path = (_test_dir / "aggregate_timestamp_instant_tz.orc").string();
    write_multi_stripe_orc_timestamp_instant_sarg_file(multi_stripe_file_path);

    auto reader =
            create_reader_for_path(multi_stripe_file_path, nullptr, nullptr, std::nullopt, true);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("Asia/Shanghai");
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    ASSERT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_TIMESTAMPTZ);

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    ASSERT_TRUE(reader->open(request).ok());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::MINMAX;
    aggregate_request.columns.push_back(
            {.projection = format::LocalColumnIndex::top_level(format::LocalColumnId(0))});
    format::FileAggregateResult aggregate_result;
    auto status = reader->get_aggregate_result(aggregate_request, &aggregate_result);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(aggregate_result.columns.size(), 1);
    EXPECT_EQ(aggregate_result.count, 400);
    EXPECT_TRUE(aggregate_result.columns[0].has_min);
    EXPECT_TRUE(aggregate_result.columns[0].has_max);
    ASSERT_EQ(aggregate_result.columns[0].min_value.get_type(), TYPE_TIMESTAMPTZ);
    ASSERT_EQ(aggregate_result.columns[0].max_value.get_type(), TYPE_TIMESTAMPTZ);
    EXPECT_EQ(aggregate_result.columns[0].min_value.get<TYPE_TIMESTAMPTZ>().to_string(
                      state.timezone_obj(), 6),
              "1970-01-01 08:00:00.123000+08:00");
    EXPECT_EQ(aggregate_result.columns[0].max_value.get<TYPE_TIMESTAMPTZ>().to_string(
                      state.timezone_obj(), 6),
              "2021-01-01 08:03:19.123000+08:00");
}

TEST_F(NewOrcReaderTest, AggregatePushdownCharMinMaxTrimsTrailingSpaces) {
    const auto multi_stripe_file_path = (_test_dir / "aggregate_char_minmax.orc").string();
    write_multi_stripe_orc_char_file(multi_stripe_file_path);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    ASSERT_TRUE(reader->open(request).ok());

    format::FileAggregateRequest aggregate_request;
    aggregate_request.agg_type = TPushAggOp::type::MINMAX;
    aggregate_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult aggregate_result;
    auto status = reader->get_aggregate_result(aggregate_request, &aggregate_result);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(aggregate_result.columns.size(), 1);
    EXPECT_TRUE(aggregate_result.columns[0].has_min);
    EXPECT_TRUE(aggregate_result.columns[0].has_max);
    EXPECT_EQ(aggregate_result.columns[0].min_value.get<TYPE_STRING>(), "aaa_1000");
    EXPECT_EQ(aggregate_result.columns[0].max_value.get<TYPE_STRING>(), "zzz_1199");
}

TEST_F(NewOrcReaderTest, AggregatePushdownCountColumnUsesNonNullValueCount) {
    const auto multi_stripe_file_path = (_test_dir / "aggregate_count_nullable_int.orc").string();
    write_two_stripe_orc_nullable_int_file(multi_stripe_file_path);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());
    ASSERT_TRUE(reader->open(std::make_shared<format::FileScanRequest>()).ok());

    format::FileAggregateRequest count_star_request;
    count_star_request.agg_type = TPushAggOp::type::COUNT;
    format::FileAggregateResult count_star_result;
    auto status = reader->get_aggregate_result(count_star_request, &count_star_result);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(count_star_result.count, 400);

    format::FileAggregateRequest count_column_request;
    count_column_request.agg_type = TPushAggOp::type::COUNT;
    count_column_request.columns.push_back({.projection = field_projection(0)});
    format::FileAggregateResult count_column_result;
    status = reader->get_aggregate_result(count_column_request, &count_column_result);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_EQ(count_column_result.count, 200);
}

TEST_F(NewOrcReaderTest, GetSchemaReturnsFileLocalColumns) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(schema[0].local_id, 0);
    EXPECT_EQ(schema[0].name, "id");
    EXPECT_EQ(schema[0].type->get_primitive_type(), TYPE_INT);
    EXPECT_TRUE(schema[0].type->is_nullable());
    EXPECT_EQ(schema[1].local_id, 1);
    EXPECT_EQ(schema[1].name, "value");
    EXPECT_EQ(schema[1].type->get_primitive_type(), TYPE_STRING);
    EXPECT_TRUE(schema[1].type->is_nullable());
}

TEST_F(NewOrcReaderTest, GetSchemaReturnsExpectedVirtualColumnNullability) {
    const format::GlobalRowIdContext context {.version = 7, .backend_id = 12345, .file_id = 678};
    auto reader = create_reader(nullptr, context);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    const auto row_position_field = format::orc::OrcReader::row_position_column_definition();
    EXPECT_EQ(row_position_field.local_id, format::ROW_POSITION_COLUMN_ID);
    EXPECT_EQ(row_position_field.name, format::ROW_POSITION_COLUMN_NAME);
    ASSERT_FALSE(row_position_field.type->is_nullable());
    EXPECT_EQ(row_position_field.type->get_primitive_type(), TYPE_BIGINT);

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    const auto& global_rowid_field = schema.back();
    EXPECT_EQ(global_rowid_field.local_id, format::GLOBAL_ROWID_COLUMN_ID);
    EXPECT_EQ(global_rowid_field.column_type, format::ColumnType::GLOBAL_ROWID);
    ASSERT_TRUE(global_rowid_field.type->is_nullable());
    EXPECT_EQ(remove_nullable(global_rowid_field.type)->get_primitive_type(), TYPE_STRING);
}

TEST_F(NewOrcReaderTest, GetSchemaReturnsNullableMapChildren) {
    const auto file_path = (_test_dir / "complex.orc").string();
    write_complex_orc_file(file_path);
    auto reader = create_reader_for_path(file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_GE(schema.size(), 3);
    const auto& map_field = schema[2];
    ASSERT_EQ(map_field.name, "map_col");
    ASSERT_EQ(map_field.children.size(), 2);
    EXPECT_EQ(map_field.children[0].name, "key");
    EXPECT_EQ(map_field.children[1].name, "value");
    ASSERT_NE(map_field.children[0].type, nullptr);
    ASSERT_NE(map_field.children[1].type, nullptr);
    EXPECT_TRUE(map_field.children[0].type->is_nullable());
    EXPECT_TRUE(map_field.children[1].type->is_nullable());
}

TEST_F(NewOrcReaderTest, ReadGlobalRowIdVirtualColumn) {
    const format::GlobalRowIdContext context {.version = 7, .backend_id = 12345, .file_id = 678};
    auto reader = create_reader(nullptr, context);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    Block block = build_file_block({schema.back()});

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(format::GLOBAL_ROWID_COLUMN_ID)};
    request->local_positions = {
            {format::LocalColumnId(format::GLOBAL_ROWID_COLUMN_ID), format::LocalIndex(0)}};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, ROW_COUNT);

    const auto& global_rowids_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& global_rowids =
            assert_cast<const ColumnString&>(global_rowids_nullable.get_nested_column());
    for (int64_t row = 0; row < ROW_COUNT; ++row) {
        EXPECT_FALSE(global_rowids_nullable.is_null_at(row));
        const auto rowid = global_rowids.get_data_at(row);
        ASSERT_EQ(rowid.size, sizeof(GlobalRowLoacationV2));
        GlobalRowLoacationV2 location(0, 0, 0, 0);
        std::memcpy(&location, rowid.data, sizeof(location));
        EXPECT_EQ(location.version, context.version);
        EXPECT_EQ(location.backend_id, context.backend_id);
        EXPECT_EQ(location.file_id, context.file_id);
        EXPECT_EQ(location.row_id, row);
    }
}

TEST_F(NewOrcReaderTest, ReadFileLocalColumnsThenEof) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, ROW_COUNT);

    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    const auto& values_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& values = assert_cast<const ColumnString&>(values_nullable.get_nested_column());
    ASSERT_EQ(ids.size(), ROW_COUNT);
    ASSERT_EQ(values.size(), ROW_COUNT);
    EXPECT_EQ(ids.get_element(0), 1);
    EXPECT_EQ(ids.get_element(4), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "one");
    EXPECT_EQ(values.get_data_at(4).to_string(), "five");

    rows = 0;
    eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
    EXPECT_EQ(block.rows(), 0);
}

TEST_F(NewOrcReaderTest, GetBlockStopsWhenIoContextShouldStop) {
    auto io_ctx = std::make_shared<io::IOContext>();
    auto reader = create_reader_for_path(_file_path, nullptr, io_ctx);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    Block block = build_file_block({schema[0]});

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    ASSERT_TRUE(reader->open(request).ok());

    io_ctx->should_stop = true;
    size_t rows = 123;
    bool eof = false;
    auto status = reader->get_block(&block, &rows, &eof);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
    EXPECT_EQ(block.rows(), 0);
}

TEST_F(NewOrcReaderTest, ReadRowPositionVirtualColumn) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    std::vector<format::ColumnDefinition> block_schema {
            format::orc::OrcReader::row_position_column_definition(), schema[0]};
    Block block = build_file_block(block_schema);

    const auto row_position_column_id = format::ROW_POSITION_COLUMN_ID;
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(row_position_column_id)};
    request->non_predicate_columns = {field_projection(0)};
    request->local_positions = {
            {format::LocalColumnId(row_position_column_id), format::LocalIndex(0)},
            {format::LocalColumnId(0), format::LocalIndex(1)}};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, ROW_COUNT);

    const auto& row_positions = int64_data_column(*block.get_by_position(0).column);
    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    for (int64_t row = 0; row < ROW_COUNT; ++row) {
        EXPECT_EQ(row_positions.get_element(row), row);
        EXPECT_EQ(ids.get_element(row), row + 1);
    }
}

TEST_F(NewOrcReaderTest, ReadOnlyRowPositionVirtualColumn) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    const auto row_position_column_id = format::ROW_POSITION_COLUMN_ID;
    std::vector<format::ColumnDefinition> block_schema {
            format::orc::OrcReader::row_position_column_definition()};
    Block block = build_file_block(block_schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(row_position_column_id)};
    request->local_positions = {
            {format::LocalColumnId(row_position_column_id), format::LocalIndex(0)}};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, ROW_COUNT);

    const auto& row_positions = int64_data_column(*block.get_by_position(0).column);
    for (int64_t row = 0; row < ROW_COUNT; ++row) {
        EXPECT_EQ(row_positions.get_element(row), row);
    }
}

TEST_F(NewOrcReaderTest, RowPositionDeletePredicateWithoutPhysicalColumnsSkipsOrcLazy) {
    const format::GlobalRowIdContext context {.version = 7, .backend_id = 12345, .file_id = 678};
    auto reader = create_reader(nullptr, context);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    const auto row_position_column_id = format::ROW_POSITION_COLUMN_ID;
    const auto global_rowid_column_id = format::GLOBAL_ROWID_COLUMN_ID;
    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    std::vector<format::ColumnDefinition> block_schema {
            format::orc::OrcReader::row_position_column_definition(), schema.back()};
    Block block = build_file_block(block_schema);

    static const std::vector<int64_t> deleted_rows {1, 3};
    auto delete_predicate = std::make_shared<format::DeletePredicate>(deleted_rows);
    delete_predicate->add_child(TableSlotRef::create_shared(
            0, 0, -1, std::make_shared<DataTypeInt64>(), format::ROW_POSITION_COLUMN_NAME));

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(row_position_column_id)};
    request->non_predicate_columns = {field_projection(global_rowid_column_id)};
    request->local_positions = {
            {format::LocalColumnId(row_position_column_id), format::LocalIndex(0)},
            {format::LocalColumnId(global_rowid_column_id), format::LocalIndex(1)}};
    request->delete_conjuncts.push_back(VExprContext::create_shared(std::move(delete_predicate)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& row_positions = int64_data_column(*block.get_by_position(0).column);
    EXPECT_EQ(row_positions.get_element(0), 0);
    EXPECT_EQ(row_positions.get_element(1), 2);
    EXPECT_EQ(row_positions.get_element(2), 4);
    const auto& global_rowids_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& global_rowids =
            assert_cast<const ColumnString&>(global_rowids_nullable.get_nested_column());
    ASSERT_EQ(global_rowids.size(), 3);
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, 0);
}

TEST_F(NewOrcReaderTest, OrcLazyDecodesSelectedRowPositionRows) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    std::vector<format::ColumnDefinition> block_schema {
            schema[0], format::orc::OrcReader::row_position_column_definition()};
    Block block = build_file_block(block_schema);

    const auto row_position_column_id = format::ROW_POSITION_COLUMN_ID;
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(row_position_column_id)};
    request->local_positions = {
            {format::LocalColumnId(0), format::LocalIndex(0)},
            {format::LocalColumnId(row_position_column_id), format::LocalIndex(1)}};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 2)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    const auto& row_positions = int64_data_column(*block.get_by_position(1).column);
    ASSERT_EQ(ids.size(), 3);
    ASSERT_EQ(row_positions.size(), 3);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(row_positions.get_element(0), 2);
    EXPECT_EQ(row_positions.get_element(1), 3);
    EXPECT_EQ(row_positions.get_element(2), 4);
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, 2);
}

TEST_F(NewOrcReaderTest, OrcLazyKeepsRowPositionAfterRejectedBatch) {
    constexpr int64_t row_count = 4101;
    constexpr int32_t first_selected_id = 4091;
    const auto large_file_path = (_test_dir / "large_lazy_row_position.orc").string();
    write_large_orc_int_file(large_file_path, row_count);

    auto reader = create_reader_for_path(large_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);
    std::vector<format::ColumnDefinition> block_schema {
            schema[0], format::orc::OrcReader::row_position_column_definition()};

    const auto row_position_column_id = format::ROW_POSITION_COLUMN_ID;
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(row_position_column_id)};
    request->local_positions = {
            {format::LocalColumnId(0), format::LocalIndex(0)},
            {format::LocalColumnId(row_position_column_id), format::LocalIndex(1)}};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32GreaterThanExpr>(0, first_selected_id - 1)));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> result_ids;
    std::vector<int64_t> result_row_positions;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(block_schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        const auto& row_positions = int64_data_column(*block.get_by_position(1).column);
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
            result_row_positions.push_back(row_positions.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), static_cast<size_t>(row_count - first_selected_id + 1));
    ASSERT_EQ(result_row_positions.size(), result_ids.size());
    for (size_t row = 0; row < result_ids.size(); ++row) {
        EXPECT_EQ(result_ids[row], first_selected_id + static_cast<int32_t>(row));
        EXPECT_EQ(result_row_positions[row], first_selected_id - 1 + static_cast<int64_t>(row));
    }
}

TEST_F(NewOrcReaderTest, OrcLazyDeletePredicateUsesRowPositionAcrossBatches) {
    constexpr int64_t row_count = 4101;
    constexpr int32_t first_selected_id = 4091;
    constexpr int64_t deleted_row_position = 4097;
    const auto large_file_path = (_test_dir / "large_lazy_delete_position.orc").string();
    write_large_orc_int_file(large_file_path, row_count);

    auto reader = create_reader_for_path(large_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);
    std::vector<format::ColumnDefinition> block_schema {
            schema[0], format::orc::OrcReader::row_position_column_definition()};

    const auto row_position_column_id = format::ROW_POSITION_COLUMN_ID;
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0), field_projection(row_position_column_id)};
    request->non_predicate_columns = {field_projection(0)};
    request->local_positions = {
            {format::LocalColumnId(0), format::LocalIndex(0)},
            {format::LocalColumnId(row_position_column_id), format::LocalIndex(1)}};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32GreaterThanExpr>(0, first_selected_id - 1)));
    static const std::vector<int64_t> deleted_rows {deleted_row_position};
    auto delete_predicate = std::make_shared<format::DeletePredicate>(deleted_rows);
    delete_predicate->add_child(TableSlotRef::create_shared(
            1, 1, -1, std::make_shared<DataTypeInt64>(), format::ROW_POSITION_COLUMN_NAME));
    request->delete_conjuncts.push_back(VExprContext::create_shared(std::move(delete_predicate)));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> result_ids;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(block_schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), static_cast<size_t>(row_count - first_selected_id));
    EXPECT_EQ(result_ids.front(), first_selected_id);
    EXPECT_EQ(result_ids.back(), row_count);
    EXPECT_EQ(std::find(result_ids.begin(), result_ids.end(),
                        static_cast<int32_t>(deleted_row_position + 1)),
              result_ids.end());
}

TEST_F(NewOrcReaderTest, ConjunctFiltersRows) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 2)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    const auto& values_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& values = assert_cast<const ColumnString&>(values_nullable.get_nested_column());
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(1), 4);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "three");
    EXPECT_EQ(values.get_data_at(2).to_string(), "five");
}

TEST_F(NewOrcReaderTest, OrcLazyDecodesOnlySelectedNonPredicateRows) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 2)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    const auto& values_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& values = assert_cast<const ColumnString&>(values_nullable.get_nested_column());
    ASSERT_EQ(ids.size(), 3);
    ASSERT_EQ(values.size(), 3);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "three");
    EXPECT_EQ(values.get_data_at(2).to_string(), "five");
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, 2);
}

TEST_F(NewOrcReaderTest, ClosePublishesOrcLazyStatisticsToRuntimeProfile) {
    RuntimeProfile profile("new_orc_lazy_profile");
    auto reader = create_reader(&profile);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 2)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_FALSE(eof);
    ASSERT_EQ(rows, 3);
    ASSERT_TRUE(reader->close().ok());

    ASSERT_NE(profile.get_counter("FilteredRowsByLazyRead"), nullptr);
    ASSERT_NE(profile.get_counter("FilteredRowsByOrcLazyRead"), nullptr);
    EXPECT_EQ(profile.get_counter("FilteredRowsByLazyRead")->value(), 2);
    EXPECT_EQ(profile.get_counter("FilteredRowsByOrcLazyRead")->value(), 2);
}

TEST_F(NewOrcReaderTest, DisableOrcLazyMaterializationKeepsLazyProfileZero) {
    RuntimeProfile profile("new_orc_lazy_disabled_profile");
    auto reader = create_reader(&profile);
    TQueryOptions query_options;
    query_options.__set_enable_orc_lazy_mat(false);
    RuntimeState state {query_options, TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 2)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    ASSERT_FALSE(eof);
    ASSERT_EQ(rows, 3);
    ASSERT_TRUE(reader->close().ok());

    ASSERT_NE(profile.get_counter("FilteredRowsByLazyRead"), nullptr);
    ASSERT_NE(profile.get_counter("FilteredRowsByOrcLazyRead"), nullptr);
    EXPECT_EQ(profile.get_counter("FilteredRowsByLazyRead")->value(), 0);
    EXPECT_EQ(profile.get_counter("FilteredRowsByOrcLazyRead")->value(), 0);
}

TEST_F(NewOrcReaderTest, ConditionCacheMissMarksSurvivingGranules) {
    constexpr int64_t row_count = ConditionCacheContext::GRANULE_SIZE * 2;
    const auto large_file_path = (_test_dir / "condition_cache_miss.orc").string();
    write_large_orc_int_file(large_file_path, row_count);

    auto reader = create_reader_for_path(large_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(0)};
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(
                    0, ConditionCacheContext::GRANULE_SIZE)));
    ASSERT_TRUE(reader->open(request).ok());

    auto ctx = std::make_shared<ConditionCacheContext>();
    ctx->is_hit = false;
    ctx->filter_result = std::make_shared<std::vector<bool>>(3, false);
    reader->set_condition_cache_context(ctx);

    std::vector<int32_t> ids;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = assert_cast<const ColumnInt32&>(
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                        .get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
        }
    }

    ASSERT_EQ(ids.size(), static_cast<size_t>(ConditionCacheContext::GRANULE_SIZE));
    EXPECT_EQ(ids.front(), ConditionCacheContext::GRANULE_SIZE + 1);
    EXPECT_EQ(ids.back(), row_count);
    EXPECT_FALSE((*ctx->filter_result)[0]);
    EXPECT_TRUE((*ctx->filter_result)[1]);
    EXPECT_FALSE((*ctx->filter_result)[2]);
}

TEST_F(NewOrcReaderTest, ConditionCacheMissMarksSargSkippedLazyRowsByFileRow) {
    const auto file_path = (_test_dir / "condition_cache_sarg_skipped_lazy.orc").string();
    write_sarg_skipped_row_group_orc_file(file_path);

    auto reader = create_reader_for_path(file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->local_positions = {{format::LocalColumnId(0), format::LocalIndex(0)},
                                {format::LocalColumnId(1), format::LocalIndex(1)}};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 5000)));
    ASSERT_TRUE(reader->open(request).ok());

    auto ctx = std::make_shared<ConditionCacheContext>();
    ctx->is_hit = false;
    ctx->filter_result = std::make_shared<std::vector<bool>>(4, false);
    reader->set_condition_cache_context(ctx);

    std::vector<int32_t> ids;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = assert_cast<const ColumnInt32&>(
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                        .get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
        }
    }

    ASSERT_EQ(ids.size(), static_cast<size_t>(ConditionCacheContext::GRANULE_SIZE));
    EXPECT_EQ(ids.front(), 10000);
    EXPECT_EQ(ids.back(), 10000 + ConditionCacheContext::GRANULE_SIZE - 1);
    EXPECT_FALSE((*ctx->filter_result)[0]);
    EXPECT_TRUE((*ctx->filter_result)[1]);
    EXPECT_FALSE((*ctx->filter_result)[2]);
}

TEST_F(NewOrcReaderTest, OrcLazyRowPositionPredicateUsesSargSkippedFileRow) {
    const auto file_path = (_test_dir / "lazy_row_position_sarg_skipped.orc").string();
    write_sarg_skipped_row_group_orc_file(file_path);

    auto reader = create_reader_for_path(file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    std::vector<format::ColumnDefinition> block_schema {
            schema[0], format::orc::OrcReader::row_position_column_definition(), schema[1]};

    const auto row_position_column_id = format::ROW_POSITION_COLUMN_ID;
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0), field_projection(row_position_column_id)};
    request->non_predicate_columns = {field_projection(1)};
    request->local_positions = {
            {format::LocalColumnId(0), format::LocalIndex(0)},
            {format::LocalColumnId(row_position_column_id), format::LocalIndex(1)},
            {format::LocalColumnId(1), format::LocalIndex(2)}};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 5000)));
    static const std::vector<int64_t> deleted_rows {ConditionCacheContext::GRANULE_SIZE};
    auto delete_predicate = std::make_shared<format::DeletePredicate>(deleted_rows);
    delete_predicate->add_child(TableSlotRef::create_shared(
            1, 1, -1, std::make_shared<DataTypeInt64>(), format::ROW_POSITION_COLUMN_NAME));
    request->delete_conjuncts.push_back(VExprContext::create_shared(std::move(delete_predicate)));
    ASSERT_TRUE(reader->open(request).ok());

    std::vector<int32_t> ids;
    bool eof = false;
    while (!eof) {
        Block block = build_file_block(block_schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& id_column = assert_cast<const ColumnInt32&>(
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                        .get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            ids.push_back(id_column.get_element(row));
        }
    }

    ASSERT_EQ(ids.size(), static_cast<size_t>(ConditionCacheContext::GRANULE_SIZE - 1));
    EXPECT_EQ(ids.front(), 10001);
    EXPECT_EQ(ids.back(), 10000 + ConditionCacheContext::GRANULE_SIZE - 1);
}

TEST_F(NewOrcReaderTest, ConditionCacheHitSkipsFalseGranulesBeforeColumnRead) {
    constexpr int64_t row_count = ConditionCacheContext::GRANULE_SIZE * 2;
    const auto large_file_path = (_test_dir / "condition_cache_hit.orc").string();
    write_large_orc_int_file(large_file_path, row_count);

    auto io_ctx = std::make_shared<io::IOContext>();
    auto reader = create_reader_for_path(large_file_path, nullptr, io_ctx);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);
    EXPECT_EQ(reader->get_total_rows(), row_count);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(0)};
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(
                    0, ConditionCacheContext::GRANULE_SIZE)));
    ASSERT_TRUE(reader->open(request).ok());

    auto ctx = std::make_shared<ConditionCacheContext>();
    ctx->is_hit = true;
    ctx->filter_result =
            std::make_shared<std::vector<bool>>(std::vector<bool> {false, true, false});
    reader->set_condition_cache_context(ctx);

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, static_cast<size_t>(ConditionCacheContext::GRANULE_SIZE));
    EXPECT_EQ(io_ctx->condition_cache_filtered_rows, ConditionCacheContext::GRANULE_SIZE);

    const auto& ids = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                    .get_nested_column());
    EXPECT_EQ(ids.get_element(0), ConditionCacheContext::GRANULE_SIZE + 1);
    EXPECT_EQ(ids.get_element(rows - 1), row_count);

    block = build_file_block(schema);
    rows = 0;
    eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
}

TEST_F(NewOrcReaderTest, ConditionCacheHitHandlesSplitWithoutSelectedStripe) {
    const auto multi_stripe_file_path = (_test_dir / "condition_cache_empty_split.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);
    const auto layout = get_orc_stripe_layout(multi_stripe_file_path);
    ASSERT_EQ(layout.size(), 2);

    const int64_t window_start =
            layout[0].offset > 0 ? static_cast<int64_t>(layout[0].offset) - 1 : 0;
    auto reader = create_reader_with_range(multi_stripe_file_path, window_start, 1);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(0)};
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 1)));
    ASSERT_TRUE(reader->open(request).ok());
    EXPECT_EQ(reader->get_total_rows(), 0);

    auto ctx = std::make_shared<ConditionCacheContext>();
    ctx->is_hit = true;
    ctx->filter_result = std::make_shared<std::vector<bool>>(std::vector<bool> {false});
    reader->set_condition_cache_context(ctx);

    Block block = build_file_block({schema[0]});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
}

TEST_F(NewOrcReaderTest, ConditionCacheHitUsesSplitBaseGranule) {
    const auto multi_stripe_file_path = (_test_dir / "condition_cache_split_base.orc").string();
    std::vector<int64_t> first_values;
    for (int64_t stripe = 0; stripe < 24; ++stripe) {
        first_values.push_back(stripe * 1000 + 1);
    }
    write_multi_stripe_orc_int_only_file(multi_stripe_file_path, first_values);

    const auto layout = get_orc_stripe_layout(multi_stripe_file_path);
    ASSERT_FALSE(layout.empty());
    size_t stripe_index = 0;
    uint64_t stripe_first_row = 0;
    uint64_t accumulated_rows = 0;
    for (size_t i = 0; i < layout.size(); ++i) {
        if (accumulated_rows / ConditionCacheContext::GRANULE_SIZE > 0) {
            stripe_index = i;
            stripe_first_row = accumulated_rows;
            break;
        }
        accumulated_rows += layout[i].rows;
    }
    ASSERT_GT(stripe_first_row / ConditionCacheContext::GRANULE_SIZE, 0);
    ASSERT_LT(stripe_index, layout.size());
    auto reader = create_reader_with_range(multi_stripe_file_path,
                                           static_cast<int64_t>(layout[stripe_index].offset),
                                           static_cast<int64_t>(layout[stripe_index].length));
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 1);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(0)};
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 0)));
    ASSERT_TRUE(reader->open(request).ok());
    EXPECT_EQ(reader->get_total_rows(), layout[stripe_index].rows);

    auto ctx = std::make_shared<ConditionCacheContext>();
    ctx->is_hit = true;
    const auto base_granule = stripe_first_row / ConditionCacheContext::GRANULE_SIZE;
    const auto last_granule = (stripe_first_row + layout[stripe_index].rows - 1) /
                              ConditionCacheContext::GRANULE_SIZE;
    ctx->filter_result = std::make_shared<std::vector<bool>>(last_granule - base_granule + 1, true);
    reader->set_condition_cache_context(ctx);
    EXPECT_EQ(ctx->base_granule, base_granule);

    Block block = build_file_block({schema[0]});
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, layout[stripe_index].rows);

    const auto& ids = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                    .get_nested_column());
    auto value_for_file_row = [](uint64_t file_row) {
        constexpr uint64_t ROWS_PER_BATCH = 200;
        return static_cast<int32_t>((file_row / ROWS_PER_BATCH) * 1000 + 1 +
                                    file_row % ROWS_PER_BATCH);
    };
    EXPECT_EQ(ids.get_element(0), value_for_file_row(stripe_first_row));
    EXPECT_EQ(ids.get_element(rows - 1), value_for_file_row(stripe_first_row + rows - 1));
}

TEST_F(NewOrcReaderTest, OrcLazySkipsNonPredicateColumnsWhenFilterEliminatesBatch) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32EqualsExpr>(0, 999)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
    EXPECT_EQ(block.get_by_position(0).column->size(), 0);
    EXPECT_EQ(block.get_by_position(1).column->size(), 0);
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, ROW_COUNT);
}

TEST_F(NewOrcReaderTest, ConjunctFilterDoesNotDecodeDuplicateColumnsTwice) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block({schema[0]});

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(0)};
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 2)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    ASSERT_EQ(ids.size(), 3);
    EXPECT_EQ(ids.get_element(0), 3);
    EXPECT_EQ(ids.get_element(1), 4);
    EXPECT_EQ(ids.get_element(2), 5);
}

TEST_F(NewOrcReaderTest, OrcLazyDecodesSelectedComplexNonPredicateRows) {
    const auto complex_file_path = (_test_dir / "predicate_first_complex.orc").string();
    write_complex_orc_file(complex_file_path);
    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block({schema[0], schema[1], schema[2]});

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1), field_projection(2)};
    request->local_positions = {{format::LocalColumnId(0), format::LocalIndex(0)},
                                {format::LocalColumnId(1), format::LocalIndex(1)},
                                {format::LocalColumnId(2), format::LocalIndex(2)}};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableStructChildInt32GreaterThanExpr>(0, schema[0].type, "a", 15)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 2);

    const auto& struct_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(struct_nullable.get_nested_column());
    const auto& struct_a = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(struct_column.get_column(0)).get_nested_column());
    ASSERT_EQ(struct_a.size(), 2);
    EXPECT_EQ(struct_a.get_element(0), 20);
    EXPECT_EQ(struct_a.get_element(1), 30);

    const auto& array_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& array_column = assert_cast<const ColumnArray&>(array_nullable.get_nested_column());
    ASSERT_EQ(array_column.get_offsets().size(), 2);
    EXPECT_EQ(array_column.get_offsets()[0], 0);
    EXPECT_EQ(array_column.get_offsets()[1], 2);
    const auto& array_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(array_column.get_data()).get_nested_column());
    ASSERT_EQ(array_values.size(), 2);
    EXPECT_EQ(array_values.get_element(0), 2);
    EXPECT_EQ(array_values.get_element(1), 3);

    const auto& map_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    const auto& map_column = assert_cast<const ColumnMap&>(map_nullable.get_nested_column());
    ASSERT_EQ(map_column.get_offsets().size(), 2);
    EXPECT_EQ(map_column.get_offsets()[0], 0);
    EXPECT_EQ(map_column.get_offsets()[1], 1);
    const auto& map_keys = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(map_column.get_keys()).get_nested_column());
    const auto& map_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(map_column.get_values()).get_nested_column());
    ASSERT_EQ(map_keys.size(), 1);
    ASSERT_EQ(map_values.size(), 1);
    EXPECT_EQ(map_keys.get_data_at(0).to_string(), "c");
    EXPECT_EQ(map_values.get_element(0), 300);
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, 1);
}

TEST_F(NewOrcReaderTest, OrcLazySupportsNestedPredicateProjection) {
    const auto complex_file_path = (_test_dir / "predicate_nested_projection.orc").string();
    write_complex_orc_file(complex_file_path);
    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    DataTypes projected_child_types {schema[0].children[0].type};
    Strings projected_child_names {schema[0].children[0].name};
    auto projected_struct_type = make_nullable(
            std::make_shared<DataTypeStruct>(projected_child_types, projected_child_names));

    Block block;
    block.insert({projected_struct_type->create_column(), projected_struct_type, schema[0].name});
    block.insert({schema[1].type->create_column(), schema[1].type, schema[1].name});
    block.insert({schema[2].type->create_column(), schema[2].type, schema[2].name});

    auto request = std::make_shared<format::FileScanRequest>();
    auto struct_projection = make_projection(schema[0], false);
    struct_projection.children.push_back(make_projection(schema[0].children[0]));
    request->predicate_columns = {std::move(struct_projection)};
    request->non_predicate_columns = {field_projection(1), field_projection(2)};
    request->local_positions = {{format::LocalColumnId(0), format::LocalIndex(0)},
                                {format::LocalColumnId(1), format::LocalIndex(1)},
                                {format::LocalColumnId(2), format::LocalIndex(2)}};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableStructChildInt32GreaterThanExpr>(0, schema[0].type, "a", 15)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 2);

    const auto& struct_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(struct_nullable.get_nested_column());
    const auto& struct_a = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(struct_column.get_column(0)).get_nested_column());
    expect_int32_values(struct_a, {20, 30});

    const auto& map_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    const auto& map_column = assert_cast<const ColumnMap&>(map_nullable.get_nested_column());
    ASSERT_EQ(map_column.get_offsets().size(), 2);
    EXPECT_EQ(map_column.get_offsets()[0], 0);
    EXPECT_EQ(map_column.get_offsets()[1], 1);
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, 1);
}

TEST_F(NewOrcReaderTest, OrcLazyDecodesSelectedDeepNestedComplexRows) {
    const auto complex_file_path = (_test_dir / "predicate_first_deep_complex.orc").string();
    write_deep_nested_complex_orc_file(complex_file_path);
    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->local_positions = {{format::LocalColumnId(0), format::LocalIndex(0)},
                                {format::LocalColumnId(1), format::LocalIndex(1)}};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 2)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 2);

    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    expect_int32_values(ids, {3, 4});

    expect_deep_nested_column(*block.get_by_position(1).column, {false, false}, {2, 3},
                              {"row3_left", "row3_right", "row4"}, {1, 3, 4},
                              {"row3_left_key", "row3_right_a", "row3_right_empty", "row4_key"},
                              {1, 3, 3, 4}, {30, 31, 32, 40},
                              {"thirty", "thirty_one", "thirty_two", "forty"});
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, 2);
}

TEST_F(NewOrcReaderTest, DeleteConjunctFiltersRows) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->delete_conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32EqualsExpr>(0, 3)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 4);

    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    const auto& values_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& values = assert_cast<const ColumnString&>(values_nullable.get_nested_column());
    EXPECT_EQ(ids.get_element(0), 1);
    EXPECT_EQ(ids.get_element(1), 2);
    EXPECT_EQ(ids.get_element(2), 4);
    EXPECT_EQ(ids.get_element(3), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "one");
    EXPECT_EQ(values.get_data_at(2).to_string(), "four");
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, 1);
}

TEST_F(NewOrcReaderTest, DeleteConjunctFallsBackWhenItReferencesNonDecodedColumn) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block = build_file_block(schema);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(1)};
    request->local_positions = {{format::LocalColumnId(0), format::LocalIndex(0)},
                                {format::LocalColumnId(1), format::LocalIndex(1)}};
    request->delete_conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableStringCastToStringGreaterThanExpr>(
                    1, remove_nullable(schema[1].type), "o", "value")));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 2);

    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    const auto& values_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& values = assert_cast<const ColumnString&>(values_nullable.get_nested_column());
    EXPECT_EQ(ids.get_element(0), 4);
    EXPECT_EQ(ids.get_element(1), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "four");
    EXPECT_EQ(values.get_data_at(1).to_string(), "five");
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, 0);
}

TEST_F(NewOrcReaderTest, DeletePredicateFiltersRowPositions) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    const auto row_position_column_id = format::ROW_POSITION_COLUMN_ID;
    std::vector<format::ColumnDefinition> block_schema {
            schema[0], schema[1], format::orc::OrcReader::row_position_column_definition()};
    Block block = build_file_block(block_schema);

    static const std::vector<int64_t> deleted_rows {1, 3};
    auto delete_predicate = std::make_shared<format::DeletePredicate>(deleted_rows);
    delete_predicate->add_child(TableSlotRef::create_shared(
            2, 2, -1, std::make_shared<DataTypeInt64>(), format::ROW_POSITION_COLUMN_NAME));

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(row_position_column_id)};
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    request->local_positions = {
            {format::LocalColumnId(0), format::LocalIndex(0)},
            {format::LocalColumnId(1), format::LocalIndex(1)},
            {format::LocalColumnId(row_position_column_id), format::LocalIndex(2)}};
    request->delete_conjuncts.push_back(VExprContext::create_shared(std::move(delete_predicate)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    const auto& values_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& values = assert_cast<const ColumnString&>(values_nullable.get_nested_column());
    const auto& row_positions = int64_data_column(*block.get_by_position(2).column);
    EXPECT_EQ(ids.get_element(0), 1);
    EXPECT_EQ(ids.get_element(1), 3);
    EXPECT_EQ(ids.get_element(2), 5);
    EXPECT_EQ(values.get_data_at(0).to_string(), "one");
    EXPECT_EQ(values.get_data_at(1).to_string(), "three");
    EXPECT_EQ(values.get_data_at(2).to_string(), "five");
    EXPECT_EQ(row_positions.get_element(0), 0);
    EXPECT_EQ(row_positions.get_element(1), 2);
    EXPECT_EQ(row_positions.get_element(2), 4);
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, 2);
}

TEST_F(NewOrcReaderTest, SargConjunctPrunesStripesByStatistics) {
    const auto multi_stripe_file_path = (_test_dir / "multi_stripe.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }
    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

// A split window covering only the first stripe must return exactly that stripe's rows.
TEST_F(NewOrcReaderTest, SplitRangeSelectsOnlyFirstStripe) {
    const auto multi_stripe_file_path = (_test_dir / "split_first_stripe.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);
    const auto layout = get_orc_stripe_layout(multi_stripe_file_path);
    ASSERT_EQ(layout.size(), 2);

    // Window [stripe0.offset, stripe1.offset) covers only the first stripe.
    auto reader =
            create_reader_with_range(multi_stripe_file_path, static_cast<int64_t>(layout[0].offset),
                                     static_cast<int64_t>(layout[1].offset - layout[0].offset));
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }
    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1);
    EXPECT_EQ(result_ids.back(), 200);
}

TEST_F(NewOrcReaderTest, SplitRangeSelectsOnlySecondStripe) {
    const auto multi_stripe_file_path = (_test_dir / "split_second_stripe.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);
    const auto layout = get_orc_stripe_layout(multi_stripe_file_path);
    ASSERT_EQ(layout.size(), 2);

    // Window [stripe1.offset, stripe1.offset + stripe1.length) covers only the second stripe.
    auto reader =
            create_reader_with_range(multi_stripe_file_path, static_cast<int64_t>(layout[1].offset),
                                     static_cast<int64_t>(layout[1].length));
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }
    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
}

// A SARG that prunes the first stripe must not count the out-of-split first stripe in the
// filtered statistics: only the in-split stripe is considered, so nothing is pruned.
TEST_F(NewOrcReaderTest, SplitRangeWithSargStaysWithinSplit) {
    const auto multi_stripe_file_path = (_test_dir / "split_sarg_within.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);
    const auto layout = get_orc_stripe_layout(multi_stripe_file_path);
    ASSERT_EQ(layout.size(), 2);

    // Restrict to the second stripe (ids 1000..1199). id > 500 keeps every in-split row.
    auto reader =
            create_reader_with_range(multi_stripe_file_path, static_cast<int64_t>(layout[1].offset),
                                     static_cast<int64_t>(layout[1].length));
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }
    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    // The out-of-split first stripe must not be counted as filtered.
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SplitRangeCoveringNoStripeReturnsNoRows) {
    const auto multi_stripe_file_path = (_test_dir / "split_no_stripe.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);
    const auto layout = get_orc_stripe_layout(multi_stripe_file_path);
    ASSERT_EQ(layout.size(), 2);

    // A one-byte window just before the first stripe covers no stripe offset.
    const int64_t window_start =
            layout[0].offset > 0 ? static_cast<int64_t>(layout[0].offset) - 1 : 0;
    auto reader = create_reader_with_range(multi_stripe_file_path, window_start, 1);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t total_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        total_rows += rows;
    }
    EXPECT_EQ(total_rows, 0);
    EXPECT_TRUE(eof);
}

TEST_F(NewOrcReaderTest, WholeFileWhenRangeSizeNegative) {
    const auto multi_stripe_file_path = (_test_dir / "split_whole_file.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    // range_size == -1 is the unset sentinel: the reader must scan the whole file.
    auto reader = create_reader_with_range(multi_stripe_file_path, 0, -1);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }
    ASSERT_EQ(result_ids.size(), 400);
    EXPECT_EQ(result_ids.front(), 1);
    EXPECT_EQ(result_ids.back(), 1199);
}

TEST_F(NewOrcReaderTest, ClosePublishesReaderStatisticsToRuntimeProfile) {
    const auto multi_stripe_file_path = (_test_dir / "profile_sarg_pruning.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path, {1, 1000, 2000});
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 3);

    RuntimeProfile profile("new_orc_reader_profile");
    io::FileReaderStats file_reader_stats;
    auto io_ctx = std::make_shared<io::IOContext>();
    io_ctx->file_reader_stats = &file_reader_stats;
    auto reader = create_reader_for_path(multi_stripe_file_path, &profile, io_ctx);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32LessThanExpr>(0, 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }
    ASSERT_EQ(result_rows, 200);
    ASSERT_TRUE(reader->close().ok());

    ASSERT_NE(profile.get_counter("RowGroupsFiltered"), nullptr);
    ASSERT_NE(profile.get_counter("RowGroupsFilteredByMinMax"), nullptr);
    ASSERT_NE(profile.get_counter("RowGroupsReadNum"), nullptr);
    ASSERT_NE(profile.get_counter("FilteredRowsByGroup"), nullptr);
    ASSERT_NE(profile.get_counter("FilteredRowsByLazyRead"), nullptr);
    ASSERT_NE(profile.get_counter("FilteredRowsByOrcLazyRead"), nullptr);
    ASSERT_NE(profile.get_counter("FilteredBytes"), nullptr);
    ASSERT_NE(profile.get_counter("FileNum"), nullptr);
    const std::array<std::string_view, 13> orc_reader_metric_counters {
            "ReaderCall",
            "ReaderInclusiveLatencyUs",
            "DecompressionCall",
            "DecompressionLatencyUs",
            "DecodingCall",
            "DecodingLatencyUs",
            "ByteDecodingCall",
            "ByteDecodingLatencyUs",
            "IOCount",
            "IOBlockingLatencyUs",
            "SelectedRowGroupCount",
            "EvaluatedRowGroupCount",
            "ReadRowCount",
    };
    for (const auto counter_name : orc_reader_metric_counters) {
        ASSERT_NE(profile.get_counter(std::string(counter_name)), nullptr) << counter_name;
    }
    EXPECT_EQ(profile.get_counter("RowGroupsFiltered")->value(), 2);
    EXPECT_EQ(profile.get_counter("RowGroupsFilteredByMinMax")->value(), 2);
    EXPECT_EQ(profile.get_counter("RowGroupsReadNum")->value(), 1);
    EXPECT_EQ(profile.get_counter("SelectedRowGroupCount")->value(), 1);
    EXPECT_EQ(profile.get_counter("EvaluatedRowGroupCount")->value(), 3);
    EXPECT_EQ(profile.get_counter("FilteredRowsByGroup")->value(), 400);
    EXPECT_EQ(profile.get_counter("FilteredRowsByLazyRead")->value(), 0);
    EXPECT_EQ(profile.get_counter("FilteredRowsByOrcLazyRead")->value(), 0);
    EXPECT_GT(profile.get_counter("FilteredBytes")->value(), 0);
    EXPECT_EQ(profile.get_counter("FileNum")->value(), 1);
    EXPECT_EQ(profile.get_counter("ReadRowCount")->value(),
              static_cast<int64_t>(file_reader_stats.read_rows));
}

TEST_F(NewOrcReaderTest, SargConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_conjunct_pruning.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    std::vector<format::ColumnDefinition> block_schema {schema[1], schema[0]};
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1), field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(1, 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block(block_schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargLiteralOnLeftConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_literal_on_left.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableGreaterThanExpr<TYPE_INT>>(
                    0, remove_nullable(schema[0].type), Field::create_field<TYPE_INT>(500), "id",
                    true)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargRuntimeFilterWrapperConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_runtime_filter_wrapper.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    auto impl = std::make_shared<NullableGreaterThanExpr<TYPE_INT>>(
            0, remove_nullable(schema[0].type), Field::create_field<TYPE_INT>(500), "id");
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<RuntimeFilterWrapperExpr>(std::move(impl))));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargNullAwareRuntimeFilterDoesNotPruneNullStripe) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_null_aware_runtime_filter.orc").string();
    write_two_stripe_orc_nullable_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    std::shared_ptr<HybridSetBase> filter(create_set(PrimitiveType::TYPE_INT, true));
    int32_t unmatched_value = 5000;
    filter->insert(&unmatched_value);

    auto node = make_filter_in_node(TExprNodeType::NULL_AWARE_IN_PRED);
    auto direct_in = VDirectInPredicate::create_shared(node, filter, true);
    direct_in->add_child(TableSlotRef::create_shared(0, 0, -1, schema[0].type, "id"));
    auto runtime_filter = RuntimeFilterExpr::create_shared(node, direct_in, 0.0, true, 7);
    auto runtime_filter_context = VExprContext::create_shared(std::move(runtime_filter));
    ASSERT_TRUE(runtime_filter_context->prepare(&state, RowDescriptor()).ok());
    ASSERT_TRUE(runtime_filter_context->open(&state).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(std::move(runtime_filter_context));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_TRUE(ids_nullable.is_null_at(row));
        }
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
}

TEST_F(NewOrcReaderTest, SargNullSafeEqualNullConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_null_safe_equal_null.orc").string();
    write_two_stripe_orc_nullable_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32NullSafeEqualsNullExpr>(0, false)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_TRUE(ids_nullable.is_null_at(row));
        }
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargNullSafeEqualNullLiteralOnLeftConjunctPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_null_safe_equal_null_literal_on_left.orc").string();
    write_two_stripe_orc_nullable_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32NullSafeEqualsNullExpr>(0, true)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_TRUE(ids_nullable.is_null_at(row));
        }
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargIsNullConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_is_null.orc").string();
    write_two_stripe_orc_nullable_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32IsNullExpr>(0)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_TRUE(ids_nullable.is_null_at(row));
        }
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargNullSafeEqualLiteralConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_null_safe_equal_literal.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32NullSafeEqualsLiteralExpr>(0, 1000, false)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_FALSE(ids_nullable.is_null_at(row));
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 1);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargNullSafeEqualLiteralOnLeftConjunctPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_null_safe_equal_literal_on_left.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32NullSafeEqualsLiteralExpr>(0, 1000, true)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_FALSE(ids_nullable.is_null_at(row));
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 1);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargSlotToSlotNullSafeEqualDoesNotPruneStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_slot_to_slot_null_safe_equal.orc").string();
    write_multi_stripe_orc_pair_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);

    std::vector<format::ColumnDefinition> block_schema {schema[0], schema[1]};
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0), field_projection(1)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32NullSafeEqualsSlotExpr>(0, 1)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block(block_schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 400);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargNullSafeEqualInsideOrDoesNotPruneStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_null_safe_equal_inside_or.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<CompoundPredicateExpr>(
                    TExprOpcode::COMPOUND_OR,
                    VExprSPtrs {std::make_shared<NullableInt32NullSafeEqualsLiteralExpr>(0, 1000,
                                                                                         false),
                                std::make_shared<NullableInt32NullSafeEqualsLiteralExpr>(0, 1001,
                                                                                         false)})));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_FALSE(ids_nullable.is_null_at(row));
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 2);
    EXPECT_EQ(result_ids[0], 1000);
    EXPECT_EQ(result_ids[1], 1001);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargNullSafeEqualInsideNestedNotDoesNotPruneStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_null_safe_equal_inside_nested_not.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    auto null_safe_equal = std::make_shared<NullableInt32NullSafeEqualsLiteralExpr>(0, 1000, false);
    auto inner_not = std::make_shared<CompoundPredicateExpr>(TExprOpcode::COMPOUND_NOT,
                                                             VExprSPtrs {null_safe_equal});
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<CompoundPredicateExpr>(
                    TExprOpcode::COMPOUND_NOT, VExprSPtrs {inner_not})));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_FALSE(ids_nullable.is_null_at(row));
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 1);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargVarcharCastToStringConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_varchar_cast_to_string.orc").string();
    write_multi_stripe_orc_varchar_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_VARCHAR);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableStringCastToStringGreaterThanExpr>(
                    0, remove_nullable(schema[0].type), "m", "value")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargStringEqualsKeepsMatchingHiveDictRows) {
    const auto fixture_path = find_repo_file(
            "docker/thirdparties/docker-compose/hive/scripts/preinstalled_data/"
            "orc_table/test_string_dict_filter_orc/test_string_dict_filter.orc");
    ASSERT_TRUE(std::filesystem::exists(fixture_path));

    auto reader = create_reader_for_path(fixture_path.string());
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 9);
    EXPECT_EQ(schema[2].name, "o_orderstatus");
    EXPECT_EQ(remove_nullable(schema[2].type)->get_primitive_type(), TYPE_STRING);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(2)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableStringEqualsExpr>(
                    0, remove_nullable(schema[2].type), "F", schema[2].name)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<std::string> statuses;
    while (!eof) {
        Block block = build_file_block({schema[2]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& status_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& status_values =
                assert_cast<const ColumnString&>(status_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_FALSE(status_nullable.is_null_at(row));
            statuses.push_back(status_values.get_data_at(row).to_string());
        }
    }

    ASSERT_EQ(statuses.size(), 2);
    EXPECT_EQ(statuses[0], "F");
    EXPECT_EQ(statuses[1], "F");
}

TEST_F(NewOrcReaderTest, SargBinaryVarbinaryLiteralConjunctPrunesRowGroups) {
    const auto multi_stripe_file_path =
            (_test_dir / "binary_varbinary_literal_conjunct_sarg.orc").string();
    write_multi_stripe_orc_binary_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_STRING);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<SargOnlyStringEqualsVarbinaryLiteralExpr>(
                    0, std::make_shared<DataTypeVarbinary>(), "zzz_1000", "value")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 50);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargBinaryCastFromVarbinaryToStringConjunctPrunesRowGroups) {
    const auto multi_stripe_file_path =
            (_test_dir / "binary_varbinary_to_string_cast_sarg.orc").string();
    write_multi_stripe_orc_binary_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_STRING);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<SargOnlyStringCastToStringEqualsExpr>(
                    0, std::make_shared<DataTypeVarbinary>(), "zzz_1000", "value")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 50);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargVarcharToCharThenStringCastDoesNotPruneStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_varchar_to_char_then_string_cast.orc").string();
    write_multi_stripe_orc_varchar_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_VARCHAR);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableStringCastToStringGreaterThanExpr>(
                    0, std::make_shared<DataTypeString>(16, TYPE_CHAR), "m", "value")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargCharCastToStringDoesNotPruneStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_char_cast_to_string.orc").string();
    write_multi_stripe_orc_char_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_CHAR);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableStringCastToStringGreaterThanExpr>(
                    0, remove_nullable(schema[0].type), "m", "value")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargIntegerWideningCastConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_integer_widening_cast.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32CastToInt64GreaterThanExpr>(0, 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargIntegerWideningCastNullSafeEqualNullPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_integer_widening_cast_null_safe_equal_null.orc").string();
    write_two_stripe_orc_nullable_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32CastToInt64NullSafeEqualsNullExpr>(0)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_TRUE(ids_nullable.is_null_at(row));
        }
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargIntegerWideningCastNullSafeEqualLiteralOnLeftPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_integer_widening_cast_null_safe_equal_literal.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32CastToInt64NullSafeEqualsLiteralExpr>(0, 1000, true)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            EXPECT_FALSE(ids_nullable.is_null_at(row));
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 1);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargIntegerToDoubleCastConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_integer_to_double_cast.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32CastToDoubleGreaterThanExpr>(0, 500.5)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargIntegerToDoubleCastOutOfInt64BoundaryDoesNotPruneStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_integer_to_double_cast_out_of_int64_boundary.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32CastToDoubleLessThanExpr>(0, 9223372036854775808.0)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 400);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargIntegerToFloatCastDoesNotPruneStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_integer_to_float_cast.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt32CastToFloatGreaterThanExpr>(0, 500.5F)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargIntegerToFloatCastInDoesNotPruneStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_integer_to_float_cast_in.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32CastToFloatInExpr>(
                    0, std::vector<float> {1000.0F, 500.5F})));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 1);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargLongToDoubleCastDoesNotPruneStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_long_to_double_cast.orc").string();
    write_multi_stripe_orc_long_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt64CastToDoubleGreaterThanExpr>(0, 500.5)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int64_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt64&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargLongToIntCastDoesNotPruneStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_long_to_int_cast.orc").string();
    write_multi_stripe_orc_long_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt64CastToInt32GreaterThanExpr>(0, 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int64_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt64&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargLongToDoubleNullSafeEqualDoesNotPruneStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_long_to_double_null_safe_equal.orc").string();
    write_multi_stripe_orc_long_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt64CastToDoubleNullSafeEqualsLiteralExpr>(0, 1000.0)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int64_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt64&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 1);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargLongToDoubleCastInDoesNotPruneStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_long_to_double_cast_in.orc").string();
    write_multi_stripe_orc_long_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt64CastToDoubleInExpr>(
                    0, std::vector<double> {1000.0, 500.5})));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int64_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt64&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 1);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargIntegerToDoubleCastInConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_integer_to_double_cast_in.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32CastToDoubleInExpr>(
                    0, std::vector<double> {1000.0, 500.5})));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 1);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargShortToFloatCastConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_short_to_float_cast.orc").string();
    write_multi_stripe_orc_short_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableInt16CastToFloatGreaterThanExpr>(0, 500.5F)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int16_t> result_ids;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt16&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargConjunctStructChildPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_conjunct_struct_child.orc").string();
    write_multi_stripe_orc_struct_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    ASSERT_EQ(schema[0].children.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableStructChildInt32GreaterThanExpr>(
                    0, schema[0].type, "a", 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargConjunctStructChildWideningCastPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_conjunct_struct_child_widening_cast.orc").string();
    write_multi_stripe_orc_struct_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    ASSERT_EQ(schema[0].children.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableStructChildInt32CastToInt64GreaterThanExpr>(0, schema[0].type,
                                                                                 "a", 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargConjunctStructChildNullSafeEqualPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_conjunct_struct_child_null_safe_equal.orc").string();
    write_multi_stripe_orc_struct_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    ASSERT_EQ(schema[0].children.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableStructChildInt32NullSafeEqualsLiteralExpr>(0, schema[0].type,
                                                                                "a", 1000)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargConjunctStructChildComplexNullSafeEqualDoesNotPruneStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_conjunct_struct_child_complex_null_safe_equal.orc").string();
    write_multi_stripe_orc_struct_array_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    ASSERT_EQ(schema[0].children.size(), 1);
    EXPECT_EQ(remove_nullable(schema[0].children[0].type)->get_primitive_type(), TYPE_ARRAY);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableStructChildArrayNullSafeEqualsLiteralExpr>(
                    0, schema[0].type, schema[0].children[0].type, "items", 1000)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargConjunctNestedStructChildPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_conjunct_nested_struct_child.orc").string();
    write_multi_stripe_orc_nested_struct_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    ASSERT_EQ(schema[0].children.size(), 2);
    ASSERT_EQ(schema[0].children[0].children.size(), 1);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableNestedStructChildInt32GreaterThanExpr>(
                    0, schema[0].type, schema[0].children[0].type, "nested", "a", 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargConjunctArrayPredicateDoesNotPruneStripes) {
    const auto complex_file_path = (_test_dir / "sarg_array_local_conjunct.orc").string();
    write_two_stripe_orc_array_map_file(complex_file_path);
    ASSERT_EQ(get_orc_stripe_count(complex_file_path), 2);

    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_ARRAY);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableArrayContainsInt32Expr>(0, schema[0].type, 2)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargConjunctMapPredicateDoesNotPruneStripes) {
    const auto complex_file_path = (_test_dir / "sarg_map_local_conjunct.orc").string();
    write_two_stripe_orc_array_map_file(complex_file_path);
    ASSERT_EQ(get_orc_stripe_count(complex_file_path), 2);

    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    EXPECT_EQ(remove_nullable(schema[1].type)->get_primitive_type(), TYPE_MAP);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableMapContainsKeyExpr>(0, schema[1].type, "c")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[1]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargConjunctElementAtDoesNotPruneStripes) {
    const auto complex_file_path = (_test_dir / "sarg_map_element_at_local_conjunct.orc").string();
    write_two_stripe_orc_array_map_file(complex_file_path);
    ASSERT_EQ(get_orc_stripe_count(complex_file_path), 2);

    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    EXPECT_EQ(remove_nullable(schema[1].type)->get_primitive_type(), TYPE_MAP);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableMapElementInt32GreaterThanExpr>(0, schema[1].type, "c", 200)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[1]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargConjunctMapKeysInDoesNotPruneStripes) {
    const auto complex_file_path = (_test_dir / "sarg_map_keys_in_local_conjunct.orc").string();
    write_two_stripe_orc_array_map_file(complex_file_path);
    ASSERT_EQ(get_orc_stripe_count(complex_file_path), 2);

    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    EXPECT_EQ(remove_nullable(schema[1].type)->get_primitive_type(), TYPE_MAP);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableMapKeysInExpr>(0, schema[1].type, "c")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[1]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargConjunctArraySizeDoesNotPruneStripes) {
    const auto complex_file_path = (_test_dir / "sarg_array_size_local_conjunct.orc").string();
    write_two_stripe_orc_array_size_file(complex_file_path);
    ASSERT_EQ(get_orc_stripe_count(complex_file_path), 2);

    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_ARRAY);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableArraySizeGreaterThanExpr>(0, schema[0].type, 0)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargConjunctListStructChildDoesNotPruneStripes) {
    const auto complex_file_path =
            (_test_dir / "sarg_array_struct_child_local_conjunct.orc").string();
    write_two_stripe_orc_array_struct_file(complex_file_path);
    ASSERT_EQ(get_orc_stripe_count(complex_file_path), 2);

    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    ASSERT_EQ(schema[0].children.size(), 1);
    ASSERT_EQ(schema[0].children[0].children.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableArrayStructChildInt32GreaterThanExpr>(
                    0, schema[0].type, schema[0].children[0].type, "a", 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargFloatWideningCastConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_float_widening_cast.orc").string();
    write_multi_stripe_orc_float_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableFloatCastToDoubleGreaterThanExpr>(0, 500.5)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDateV2CastToDateConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_date_v2_cast_to_date.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    VecDateTimeValue literal;
    ASSERT_TRUE(literal.check_range_and_set_time(2020, 1, 1, 0, 0, 0, TIME_DATE));
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableDateV2CastToDateGreaterThanExpr>(
                    0, make_date_v2(2020, 1, 1), literal)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDateV2CastToDateTimeV2MidnightConjunctPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_date_v2_cast_to_datetime_v2_midnight.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableDateV2CastToDateTimeV2GreaterThanExpr>(
                    0, make_date_v2(2020, 1, 1), make_datetime_v2(2020, 1, 1))));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDateV2CastToDateTimeV2NonMidnightGreaterEqualPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_date_v2_cast_to_datetime_v2_non_midnight_ge.orc").string();
    constexpr int64_t DAYS_TO_2020_01_01 = 18262;
    constexpr int64_t DAYS_TO_2021_01_01 = 18628;
    write_two_stripe_constant_date_file(multi_stripe_file_path, DAYS_TO_2020_01_01,
                                        DAYS_TO_2021_01_01);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableDateV2CastToDateTimeV2ComparisonExpr>(
                    0, TExprOpcode::GE, make_datetime_v2(2020, 1, 1, 12))));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDateV2CastToDateTimeV2NonMidnightLessThanPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_date_v2_cast_to_datetime_v2_non_midnight_lt.orc").string();
    constexpr int64_t DAYS_TO_2020_01_01 = 18262;
    constexpr int64_t DAYS_TO_2021_01_01 = 18628;
    write_two_stripe_constant_date_file(multi_stripe_file_path, DAYS_TO_2020_01_01,
                                        DAYS_TO_2021_01_01);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableDateV2CastToDateTimeV2ComparisonExpr>(
                    0, TExprOpcode::LT, make_datetime_v2(2020, 1, 1, 12))));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDateV2CastToDateTimeV2InPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_date_v2_cast_to_datetime_v2_in.orc").string();
    constexpr int64_t DAYS_TO_2020_01_01 = 18262;
    constexpr int64_t DAYS_TO_2021_01_01 = 18628;
    write_two_stripe_constant_date_file(multi_stripe_file_path, DAYS_TO_2020_01_01,
                                        DAYS_TO_2021_01_01);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableDateV2CastToDateTimeV2InExpr>(
                    0, std::vector<DateV2Value<DateTimeV2ValueType>> {
                               make_datetime_v2(2021, 1, 1), make_datetime_v2(2020, 1, 1, 12)})));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDateV2CastToDateTimeMidnightConjunctPrunesStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_date_v2_cast_to_datetime_midnight.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    VecDateTimeValue literal;
    ASSERT_TRUE(literal.check_range_and_set_time(2020, 1, 1, 0, 0, 0, TIME_DATETIME));
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableDateV2CastToDateTimeGreaterThanExpr>(
                    0, make_date_v2(2020, 1, 1), literal)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDateConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_date.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    const auto literal = Field::create_field<TYPE_DATEV2>(make_date_v2(2020, 1, 1));
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableGreaterThanExpr<TYPE_DATEV2>>(
                    0, remove_nullable(schema[0].type), literal, "date_col")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDateCastToStringDoesNotPruneStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_date_cast_to_string.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableDateV2CastToStringGreaterThanExpr>(
                    0, "2020-01-01", make_date_v2(2020, 1, 1))));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargTimestampConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_timestamp.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    const auto literal = Field::create_field<TYPE_DATETIMEV2>(make_datetime_v2(2020, 1, 1));
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableGreaterThanExpr<TYPE_DATETIMEV2>>(
                    0, remove_nullable(schema[1].type), literal, "timestamp_col")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[1]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargTimestampInstantConjunctUsesSessionTimezone) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_timestamp_instant_timezone.orc").string();
    write_multi_stripe_orc_timestamp_instant_sarg_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("Asia/Shanghai");
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    const auto literal =
            Field::create_field<TYPE_DATETIMEV2>(make_datetime_v2(2021, 1, 1, 7, 59, 59));
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableGreaterThanExpr<TYPE_DATETIMEV2>>(
                    0, remove_nullable(schema[0].type), literal, "timestamp_instant_col")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargTimestampLowerPrecisionCastDoesNotPruneStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_timestamp_lower_precision_cast.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableDateTimeV2LowerPrecisionCastGreaterThanExpr>(
                    0, remove_nullable(schema[1].type), make_datetime_v2(2020, 1, 1))));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[1]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargDecimalConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_decimal.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    const auto literal = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(50000));
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(2)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableGreaterThanExpr<TYPE_DECIMAL128I>>(
                    0, remove_nullable(schema[2].type), literal, "decimal_col")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[2]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDecimalDifferentScaleConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_decimal_different_scale.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    const auto literal = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(5000000));
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(2)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableDecimalGreaterThanExpr>(
                    0, remove_nullable(schema[2].type), std::make_shared<DataTypeDecimal128>(14, 4),
                    literal, Decimal128V3(50000), "decimal_col")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[2]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDecimalWideningCastConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_decimal_widening_cast.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    const auto cast_type = std::make_shared<DataTypeDecimal128>(14, 4);
    const auto literal = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(5000000));
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(2)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableDecimalCastGreaterThanExpr>(
                    0, remove_nullable(schema[2].type), cast_type, literal, Decimal128V3(50000),
                    "decimal_col")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[2]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargDecimalScaleReducingCastDoesNotPruneStripes) {
    const auto multi_stripe_file_path =
            (_test_dir / "sarg_decimal_scale_reducing_cast.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    const auto cast_type = std::make_shared<DataTypeDecimal128>(12, 1);
    const auto literal = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(5000));
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(2)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableDecimalCastGreaterThanExpr>(
                    0, remove_nullable(schema[2].type), cast_type, literal, Decimal128V3(50000),
                    "decimal_col")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[2]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargDecimalCastToStringDoesNotPruneStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_decimal_cast_to_string.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(2)};
    request->conjuncts.push_back(VExprContext::create_shared(
            std::make_shared<NullableDecimalCastToStringGreaterThanExpr>(
                    0, remove_nullable(schema[2].type), "1.99", "decimal_col", 2)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[2]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 0);
}

TEST_F(NewOrcReaderTest, SargTimestampInListConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_timestamp_in.orc").string();
    write_multi_stripe_orc_sarg_types_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 4);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(1)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInExpr<TYPE_DATETIMEV2>>(
                    0, remove_nullable(schema[1].type),
                    std::vector<Field> {Field::create_field<TYPE_DATETIMEV2>(
                            make_datetime_v2(2021, 1, 1, 0, 0, 0, 123000))},
                    "timestamp_col")));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[1]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargTimestampNotInListConjunctPrunesStripes) {
    const auto multi_stripe_file_path = (_test_dir / "sarg_timestamp_not_in.orc").string();
    write_two_stripe_constant_timestamp_file(multi_stripe_file_path, 0, 1609459200);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInExpr<TYPE_DATETIMEV2>>(
                    0, remove_nullable(schema[0].type),
                    std::vector<Field> {Field::create_field<TYPE_DATETIMEV2>(
                            make_datetime_v2(1970, 1, 1, 0, 0, 0, 123000))},
                    "timestamp_col", true)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    size_t result_rows = 0;
    while (!eof) {
        Block block = build_file_block({schema[0]});
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        result_rows += rows;
    }

    EXPECT_EQ(result_rows, 200);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargConjunctPruningPreservesRowPosition) {
    const auto multi_stripe_file_path = (_test_dir / "row_position_after_pruning.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    const auto row_position_column_id = format::ROW_POSITION_COLUMN_ID;
    std::vector<format::ColumnDefinition> block_schema {
            format::orc::OrcReader::row_position_column_definition(), schema[0]};
    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->non_predicate_columns = {field_projection(row_position_column_id)};
    request->local_positions = {
            {format::LocalColumnId(row_position_column_id), format::LocalIndex(0)},
            {format::LocalColumnId(0), format::LocalIndex(1)}};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(1, 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int64_t> result_row_positions;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block(block_schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& row_positions = int64_data_column(*block.get_by_position(0).column);
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_row_positions.push_back(row_positions.get_element(row));
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_row_positions.size(), 200);
    ASSERT_EQ(result_ids.size(), 200);
    EXPECT_EQ(result_row_positions.front(), 200);
    EXPECT_EQ(result_row_positions.back(), 399);
    EXPECT_EQ(result_ids.front(), 1000);
    EXPECT_EQ(result_ids.back(), 1199);
}

TEST_F(NewOrcReaderTest, SargConjunctReadsNonAdjacentStripeRangesAfterPruning) {
    const auto multi_stripe_file_path = (_test_dir / "non_adjacent_stripes.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path, {1, 1000, 201});
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 3);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32LessThanExpr>(0, 500)));
    ASSERT_TRUE(reader->open(request).ok());

    bool eof = false;
    std::vector<int32_t> result_ids;
    while (!eof) {
        Block block = build_file_block(schema);
        size_t rows = 0;
        ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
        if (eof || rows == 0) {
            continue;
        }
        const auto& ids_nullable =
                assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
        const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
        for (size_t row = 0; row < rows; ++row) {
            result_ids.push_back(ids.get_element(row));
        }
    }

    ASSERT_EQ(result_ids.size(), 400);
    EXPECT_EQ(result_ids.front(), 1);
    EXPECT_EQ(result_ids[199], 200);
    EXPECT_EQ(result_ids[200], 201);
    EXPECT_EQ(result_ids.back(), 400);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 1);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 200);
}

TEST_F(NewOrcReaderTest, SargConjunctReturnsEofWhenAllStripesArePruned) {
    const auto multi_stripe_file_path = (_test_dir / "all_pruned_stripes.orc").string();
    write_multi_stripe_orc_int_file(multi_stripe_file_path);
    ASSERT_EQ(get_orc_stripe_count(multi_stripe_file_path), 2);

    auto reader = create_reader_for_path(multi_stripe_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(0)};
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32GreaterThanExpr>(0, 5000)));
    ASSERT_TRUE(reader->open(request).ok());

    Block block = build_file_block(schema);
    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(rows, 0);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups, 2);
    EXPECT_EQ(reader->reader_statistics().filtered_row_groups_by_min_max, 2);
    EXPECT_EQ(reader->reader_statistics().filtered_group_rows, 400);
}

TEST_F(NewOrcReaderTest, CloseClearsFileLocalState) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_FALSE(schema.empty());

    ASSERT_TRUE(reader->close().ok());
    ASSERT_TRUE(reader->close().ok());

    schema.clear();
    EXPECT_FALSE(reader->get_schema(&schema).ok());

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    EXPECT_FALSE(reader->open(request).ok());
}

TEST_F(NewOrcReaderTest, ReadPrimitiveTypesWithNulls) {
    const auto primitive_file_path = (_test_dir / "primitive.orc").string();
    write_primitive_orc_file(primitive_file_path);
    auto reader = create_reader_for_path(primitive_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("UTC");
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 16);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_BOOLEAN);
    EXPECT_EQ(remove_nullable(schema[1].type)->get_primitive_type(), TYPE_TINYINT);
    EXPECT_EQ(remove_nullable(schema[2].type)->get_primitive_type(), TYPE_SMALLINT);
    EXPECT_EQ(remove_nullable(schema[3].type)->get_primitive_type(), TYPE_INT);
    EXPECT_EQ(remove_nullable(schema[4].type)->get_primitive_type(), TYPE_BIGINT);
    EXPECT_EQ(remove_nullable(schema[5].type)->get_primitive_type(), TYPE_FLOAT);
    EXPECT_EQ(remove_nullable(schema[6].type)->get_primitive_type(), TYPE_DOUBLE);
    EXPECT_EQ(remove_nullable(schema[7].type)->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(remove_nullable(schema[8].type)->get_primitive_type(), TYPE_STRING);
    EXPECT_EQ(remove_nullable(schema[9].type)->get_primitive_type(), TYPE_VARCHAR);
    EXPECT_EQ(remove_nullable(schema[10].type)->get_primitive_type(), TYPE_CHAR);
    EXPECT_EQ(remove_nullable(schema[11].type)->get_primitive_type(), TYPE_DATEV2);
    EXPECT_EQ(remove_nullable(schema[12].type)->get_primitive_type(), TYPE_DATETIMEV2);
    EXPECT_EQ(remove_nullable(schema[13].type)->get_primitive_type(), TYPE_DATETIMEV2);
    EXPECT_EQ(remove_nullable(schema[14].type)->get_primitive_type(), TYPE_DECIMAL128I);
    EXPECT_EQ(remove_nullable(schema[15].type)->get_primitive_type(), TYPE_DECIMAL128I);

    Block block = build_file_block(schema);
    auto request = std::make_shared<format::FileScanRequest>();
    for (size_t column_id = 0; column_id < schema.size(); ++column_id) {
        request->non_predicate_columns.push_back(field_projection(static_cast<int32_t>(column_id)));
    }
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, PRIMITIVE_ROW_COUNT);

    for (size_t column_id = 0; column_id < schema.size(); ++column_id) {
        const auto& nullable_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(column_id).column);
        ASSERT_EQ(nullable_column.size(), PRIMITIVE_ROW_COUNT);
        EXPECT_FALSE(nullable_column.is_null_at(0));
        EXPECT_TRUE(nullable_column.is_null_at(NULL_ROW));
        EXPECT_FALSE(nullable_column.is_null_at(2));
    }

    const auto& bool_values = assert_cast<const ColumnUInt8&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                    .get_nested_column());
    const auto& byte_values = assert_cast<const ColumnInt8&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column)
                    .get_nested_column());
    const auto& short_values = assert_cast<const ColumnInt16&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(2).column)
                    .get_nested_column());
    const auto& int_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(3).column)
                    .get_nested_column());
    const auto& long_values = assert_cast<const ColumnInt64&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(4).column)
                    .get_nested_column());
    const auto& float_values = assert_cast<const ColumnFloat32&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(5).column)
                    .get_nested_column());
    const auto& double_values = assert_cast<const ColumnFloat64&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(6).column)
                    .get_nested_column());
    const auto& string_values = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(7).column)
                    .get_nested_column());
    const auto& binary_values = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(8).column)
                    .get_nested_column());
    const auto& varchar_values = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(9).column)
                    .get_nested_column());
    const auto& char_values = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(10).column)
                    .get_nested_column());

    EXPECT_EQ(bool_values.get_element(0), 1);
    EXPECT_EQ(bool_values.get_element(2), 0);
    EXPECT_EQ(byte_values.get_element(0), -7);
    EXPECT_EQ(byte_values.get_element(2), 8);
    EXPECT_EQ(short_values.get_element(0), -300);
    EXPECT_EQ(short_values.get_element(2), 301);
    EXPECT_EQ(int_values.get_element(0), -70000);
    EXPECT_EQ(int_values.get_element(2), 70001);
    EXPECT_EQ(long_values.get_element(0), -9000000000L);
    EXPECT_EQ(long_values.get_element(2), 9000000001L);
    EXPECT_FLOAT_EQ(float_values.get_element(0), 1.25F);
    EXPECT_FLOAT_EQ(float_values.get_element(2), -2.5F);
    EXPECT_DOUBLE_EQ(double_values.get_element(0), 10.5);
    EXPECT_DOUBLE_EQ(double_values.get_element(2), -20.25);
    EXPECT_EQ(string_values.get_data_at(0).to_string(), "alpha");
    EXPECT_EQ(string_values.get_data_at(2).to_string(), "gamma");
    EXPECT_EQ(binary_values.get_data_at(0).to_string(), "bin_a");
    EXPECT_EQ(binary_values.get_data_at(2).to_string(), "bin_c");
    EXPECT_EQ(varchar_values.get_data_at(0).to_string(), "varchar");
    EXPECT_EQ(varchar_values.get_data_at(2).to_string(), "tail");
    EXPECT_EQ(char_values.get_data_at(0).to_string(), "ab");
    EXPECT_EQ(char_values.get_data_at(2).to_string(), "xy");
    EXPECT_EQ(schema[11].type->to_string(*block.get_by_position(11).column, 0), "1970-01-01");
    EXPECT_EQ(schema[11].type->to_string(*block.get_by_position(11).column, 2), "2021-01-01");
    EXPECT_EQ(schema[12].type->to_string(*block.get_by_position(12).column, 0),
              "1970-01-02 00:00:01.234567");
    EXPECT_EQ(schema[12].type->to_string(*block.get_by_position(12).column, 2),
              "2021-01-01 00:00:00.123456");
    EXPECT_EQ(schema[13].type->to_string(*block.get_by_position(13).column, 0),
              "1970-01-01 00:00:02.345678");
    EXPECT_EQ(schema[13].type->to_string(*block.get_by_position(13).column, 2),
              "2021-01-01 00:00:01.654321");
    EXPECT_EQ(schema[14].type->to_string(*block.get_by_position(14).column, 0), "123.45");
    EXPECT_EQ(schema[14].type->to_string(*block.get_by_position(14).column, 2), "-67.00");
    EXPECT_EQ(schema[15].type->to_string(*block.get_by_position(15).column, 0),
              "123456789012.345678");
    EXPECT_EQ(schema[15].type->to_string(*block.get_by_position(15).column, 2),
              "-987654321.000000");
    EXPECT_EQ(schema[0].type->to_string(*block.get_by_position(0).column, NULL_ROW), "NULL");
    EXPECT_EQ(schema[11].type->to_string(*block.get_by_position(11).column, NULL_ROW), "NULL");
    EXPECT_EQ(schema[15].type->to_string(*block.get_by_position(15).column, NULL_ROW), "NULL");
}

TEST_F(NewOrcReaderTest, ReadHive11DecimalUsesBatchScale) {
    const auto file_path = find_repo_file(
            "contrib/apache-orc/java/core/src/test/resources/orc-file-11-format.orc");
    ASSERT_TRUE(std::filesystem::exists(file_path)) << file_path;
    auto reader = create_reader_for_path(file_path.string());
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    const auto decimal_it = std::ranges::find_if(
            schema, [](const format::ColumnDefinition& field) { return field.name == "decimal1"; });
    ASSERT_NE(decimal_it, schema.end());

    Block block = build_file_block({*decimal_it});
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(decimal_it->file_local_id())};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_GE(rows, 1);
    EXPECT_EQ(decimal_it->type->to_string(*block.get_by_position(0).column, 0),
              "12345678.6547450000");
}

TEST_F(NewOrcReaderTest, TimestampInstantWithoutMappingUsesSessionTimezone) {
    const auto primitive_file_path = (_test_dir / "timestamp_instant_datetime.orc").string();
    write_primitive_orc_file(primitive_file_path);
    auto reader = create_reader_for_path(primitive_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("Asia/Shanghai");
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 16);
    ASSERT_EQ(remove_nullable(schema[13].type)->get_primitive_type(), TYPE_DATETIMEV2);

    Block block = build_file_block({schema[13]});
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(13)};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, PRIMITIVE_ROW_COUNT);
    EXPECT_EQ(schema[13].type->to_string(*block.get_by_position(0).column, 0),
              "1970-01-01 08:00:02.345678");
    EXPECT_EQ(schema[13].type->to_string(*block.get_by_position(0).column, 2),
              "2021-01-01 08:00:01.654321");
}

TEST_F(NewOrcReaderTest, TimestampInstantWithMappingReadsTimestampTz) {
    const auto primitive_file_path = (_test_dir / "timestamp_instant_tz.orc").string();
    write_primitive_orc_file(primitive_file_path);
    auto reader = create_reader_for_path(primitive_file_path, nullptr, nullptr, std::nullopt, true);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    state.set_timezone("Asia/Shanghai");
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 16);
    ASSERT_EQ(remove_nullable(schema[13].type)->get_primitive_type(), TYPE_TIMESTAMPTZ);
    ASSERT_EQ(remove_nullable(schema[13].type)->get_scale(), 6);

    Block block = build_file_block({schema[13]});
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(13)};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, PRIMITIVE_ROW_COUNT);

    const auto& timestamp_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& timestamp_values =
            assert_cast<const ColumnTimeStampTz&>(timestamp_nullable.get_nested_column());
    EXPECT_FALSE(timestamp_nullable.is_null_at(0));
    EXPECT_TRUE(timestamp_nullable.is_null_at(NULL_ROW));
    EXPECT_EQ(timestamp_values.get_element(0).to_string(state.timezone_obj(), 6),
              "1970-01-01 08:00:02.345678+08:00");
    EXPECT_EQ(timestamp_values.get_element(2).to_string(state.timezone_obj(), 6),
              "2021-01-01 08:00:01.654321+08:00");
    EXPECT_EQ(timestamp_values.get_element(0).to_string(cctz::utc_time_zone(), 6),
              "1970-01-01 00:00:02.345678+00:00");
}

TEST_F(NewOrcReaderTest, ReadsProjectedColumnIntoRequestedBlockPosition) {
    auto reader = create_reader();
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    Block block;
    block.insert({schema[1].type->create_column(), schema[1].type, schema[1].name});

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(1)};
    request->local_positions.emplace(format::LocalColumnId(1), format::LocalIndex(0));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, ROW_COUNT);

    const auto& values_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& values = assert_cast<const ColumnString&>(values_nullable.get_nested_column());
    EXPECT_EQ(values.get_data_at(0).to_string(), "one");
    EXPECT_EQ(values.get_data_at(4).to_string(), "five");
}

TEST_F(NewOrcReaderTest, ReadComplexTypes) {
    const auto complex_file_path = (_test_dir / "complex.orc").string();
    write_complex_orc_file(complex_file_path);
    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 5);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_STRUCT);
    EXPECT_EQ(remove_nullable(schema[1].type)->get_primitive_type(), TYPE_ARRAY);
    EXPECT_EQ(remove_nullable(schema[2].type)->get_primitive_type(), TYPE_MAP);
    EXPECT_EQ(remove_nullable(schema[3].type)->get_primitive_type(), TYPE_ARRAY);
    EXPECT_EQ(remove_nullable(schema[4].type)->get_primitive_type(), TYPE_MAP);
    ASSERT_EQ(schema[0].children.size(), 2);
    ASSERT_EQ(schema[1].children.size(), 1);
    ASSERT_EQ(schema[2].children.size(), 2);
    ASSERT_EQ(schema[3].children.size(), 1);
    ASSERT_EQ(schema[3].children[0].children.size(), 2);
    ASSERT_EQ(schema[4].children.size(), 2);
    ASSERT_EQ(schema[4].children[1].children.size(), 2);
    EXPECT_EQ(schema[0].children[0].local_id, 0);
    EXPECT_EQ(schema[1].children[0].local_id, 0);
    EXPECT_EQ(schema[2].children[0].local_id, 0);
    EXPECT_EQ(schema[2].children[1].local_id, 1);

    Block block = build_file_block(schema);
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1), field_projection(2),
                                      field_projection(3), field_projection(4)};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, COMPLEX_ROW_COUNT);

    const auto& struct_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(struct_nullable.get_nested_column());
    ASSERT_EQ(struct_column.tuple_size(), 2);
    const auto& struct_a = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(struct_column.get_column(0)).get_nested_column());
    const auto& struct_b = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(struct_column.get_column(1)).get_nested_column());
    EXPECT_EQ(struct_a.get_element(0), 10);
    EXPECT_EQ(struct_a.get_element(2), 30);
    EXPECT_EQ(struct_b.get_data_at(0).to_string(), "ten");
    EXPECT_EQ(struct_b.get_data_at(2).to_string(), "thirty");

    const auto& array_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& array_column = assert_cast<const ColumnArray&>(array_nullable.get_nested_column());
    ASSERT_EQ(array_column.get_offsets().size(), COMPLEX_ROW_COUNT);
    EXPECT_EQ(array_column.get_offsets()[0], 1);
    EXPECT_EQ(array_column.get_offsets()[1], 1);
    EXPECT_EQ(array_column.get_offsets()[2], 3);
    const auto& array_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(array_column.get_data()).get_nested_column());
    ASSERT_EQ(array_values.size(), 3);
    EXPECT_EQ(array_values.get_element(0), 1);
    EXPECT_EQ(array_values.get_element(2), 3);

    const auto& map_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    const auto& map_column = assert_cast<const ColumnMap&>(map_nullable.get_nested_column());
    ASSERT_EQ(map_column.get_offsets().size(), COMPLEX_ROW_COUNT);
    EXPECT_EQ(map_column.get_offsets()[0], 2);
    EXPECT_EQ(map_column.get_offsets()[1], 2);
    EXPECT_EQ(map_column.get_offsets()[2], 3);
    const auto& map_keys = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(map_column.get_keys()).get_nested_column());
    const auto& map_values = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(map_column.get_values()).get_nested_column());
    EXPECT_EQ(map_keys.get_data_at(0).to_string(), "a");
    EXPECT_EQ(map_keys.get_data_at(2).to_string(), "c");
    EXPECT_EQ(map_values.get_element(0), 100);
    EXPECT_EQ(map_values.get_element(2), 300);
}

TEST_F(NewOrcReaderTest, ReadMapDecimalDateWithCenturyBoundary) {
    const auto file_path = (_test_dir / "map_decimal_date.orc").string();
    write_map_decimal_date_orc_file(file_path);
    auto reader = create_reader_for_path(file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    ASSERT_EQ(remove_nullable(schema[1].type)->get_primitive_type(), TYPE_MAP);
    ASSERT_EQ(schema[1].children.size(), 2);

    Block block = build_file_block(schema);
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 4);

    const auto& map_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    const auto& map_column = assert_cast<const ColumnMap&>(map_nullable.get_nested_column());
    ASSERT_EQ(map_column.get_offsets().size(), 4);
    ASSERT_EQ(map_column.get_values().size(), 4);
    EXPECT_EQ(schema[1].children[1].type->to_string(map_column.get_values(), 0), "1900-01-01");
    EXPECT_EQ(schema[1].children[1].type->to_string(map_column.get_values(), 1), "9999-12-31");
    EXPECT_EQ(schema[1].children[1].type->to_string(map_column.get_values(), 2), "0000-12-29");
    EXPECT_EQ(schema[1].children[1].type->to_string(map_column.get_values(), 3), "1000-10-16");
}

TEST_F(NewOrcReaderTest, ReadDeepNestedComplexTypes) {
    const auto complex_file_path = (_test_dir / "deep_complex.orc").string();
    write_deep_nested_complex_orc_file(complex_file_path);
    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 2);
    EXPECT_EQ(remove_nullable(schema[0].type)->get_primitive_type(), TYPE_INT);
    EXPECT_EQ(remove_nullable(schema[1].type)->get_primitive_type(), TYPE_ARRAY);
    ASSERT_EQ(schema[1].children.size(), 1);
    ASSERT_EQ(schema[1].children[0].children.size(), 2);
    ASSERT_EQ(schema[1].children[0].children[1].children.size(), 2);
    ASSERT_EQ(schema[1].children[0].children[1].children[1].children.size(), 1);
    ASSERT_EQ(schema[1].children[0].children[1].children[1].children[0].children.size(), 2);

    Block block = build_file_block(schema);
    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1)};
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, DEEP_NESTED_ROW_COUNT);

    const auto& ids_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    const auto& ids = assert_cast<const ColumnInt32&>(ids_nullable.get_nested_column());
    expect_int32_values(ids, {1, 2, 3, 4});

    expect_deep_nested_column(
            *block.get_by_position(1).column, {false, true, false, false}, {1, 1, 3, 4},
            {"row1", "row3_left", "row3_right", "row4"}, {1, 2, 4, 5},
            {"row1_key", "row3_left_key", "row3_right_a", "row3_right_empty", "row4_key"},
            {2, 3, 5, 5, 6}, {10, 11, 30, 31, 32, 40},
            {"ten", "eleven", "thirty", "thirty_one", "thirty_two", "forty"});
}

TEST_F(NewOrcReaderTest, ReadProjectedStructChildren) {
    const auto complex_file_path = (_test_dir / "complex_struct_projection.orc").string();
    write_complex_orc_file(complex_file_path);
    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema[0].children.size(), 2);

    DataTypes child_types {schema[0].children[0].type};
    Strings child_names {schema[0].children[0].name};
    auto projected_type = make_nullable(std::make_shared<DataTypeStruct>(child_types, child_names));
    Block block;
    block.insert({projected_type->create_column(), projected_type, schema[0].name});

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0)};
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    auto projection = make_projection(schema[0], false);
    projection.children.push_back(make_projection(schema[0].children[0]));
    request->non_predicate_columns[0] = std::move(projection);
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, COMPLEX_ROW_COUNT);

    const auto& struct_column = assert_cast<const ColumnStruct&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                    .get_nested_column());
    ASSERT_EQ(struct_column.tuple_size(), 1);
    const auto& struct_a = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(struct_column.get_column(0)).get_nested_column());
    EXPECT_EQ(struct_a.get_element(0), 10);
    EXPECT_EQ(struct_a.get_element(1), 20);
    EXPECT_EQ(struct_a.get_element(2), 30);
}

TEST_F(NewOrcReaderTest, ReadProjectedListElementStructChildren) {
    const auto complex_file_path = (_test_dir / "complex_list_projection.orc").string();
    write_complex_orc_file(complex_file_path);
    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema[3].children.size(), 1);
    ASSERT_EQ(schema[3].children[0].children.size(), 2);

    DataTypes projected_element_child_types {schema[3].children[0].children[0].type};
    Strings projected_element_child_names {schema[3].children[0].children[0].name};
    auto projected_element_type = make_nullable(std::make_shared<DataTypeStruct>(
            projected_element_child_types, projected_element_child_names));
    auto projected_type = make_nullable(std::make_shared<DataTypeArray>(projected_element_type));
    Block block;
    block.insert({projected_type->create_column(), projected_type, schema[3].name});

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(3)};
    request->local_positions.emplace(format::LocalColumnId(3), format::LocalIndex(0));
    auto projection = make_projection(schema[3], false);
    auto element_projection = make_projection(schema[3].children[0], false);
    element_projection.children.push_back(make_projection(schema[3].children[0].children[0]));
    projection.children.push_back(std::move(element_projection));
    request->non_predicate_columns[0] = std::move(projection);
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, COMPLEX_ROW_COUNT);

    const auto& array_column = assert_cast<const ColumnArray&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                    .get_nested_column());
    ASSERT_EQ(array_column.get_offsets().size(), COMPLEX_ROW_COUNT);
    EXPECT_EQ(array_column.get_offsets()[0], 1);
    EXPECT_EQ(array_column.get_offsets()[1], 3);
    EXPECT_EQ(array_column.get_offsets()[2], 3);

    const auto& element_nullable = assert_cast<const ColumnNullable&>(array_column.get_data());
    const auto& element_struct =
            assert_cast<const ColumnStruct&>(element_nullable.get_nested_column());
    ASSERT_EQ(element_struct.tuple_size(), 1);
    const auto& element_a = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(element_struct.get_column(0)).get_nested_column());
    ASSERT_EQ(element_a.size(), 3);
    EXPECT_EQ(element_a.get_element(0), 1);
    EXPECT_EQ(element_a.get_element(1), 2);
    EXPECT_EQ(element_a.get_element(2), 3);
}

TEST_F(NewOrcReaderTest, ReadProjectedMapValueStructChildren) {
    const auto complex_file_path = (_test_dir / "complex_map_projection.orc").string();
    write_complex_orc_file(complex_file_path);
    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema[4].children.size(), 2);
    ASSERT_EQ(schema[4].children[1].children.size(), 2);

    const auto& key_field = schema[4].children[0];
    const auto& value_field = schema[4].children[1];
    DataTypes projected_value_child_types {value_field.children[0].type};
    Strings projected_value_child_names {value_field.children[0].name};
    auto projected_value_type = make_nullable(std::make_shared<DataTypeStruct>(
            projected_value_child_types, projected_value_child_names));
    auto projected_type =
            make_nullable(std::make_shared<DataTypeMap>(key_field.type, projected_value_type));
    Block block;
    block.insert({projected_type->create_column(), projected_type, schema[4].name});

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(4)};
    request->local_positions.emplace(format::LocalColumnId(4), format::LocalIndex(0));
    auto projection = make_projection(schema[4], false);
    projection.children.push_back(make_projection(key_field));
    auto value_projection = make_projection(value_field, false);
    value_projection.children.push_back(make_projection(value_field.children[0]));
    projection.children.push_back(std::move(value_projection));
    request->non_predicate_columns[0] = std::move(projection);
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, COMPLEX_ROW_COUNT);

    const auto& map_column = assert_cast<const ColumnMap&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                    .get_nested_column());
    ASSERT_EQ(map_column.get_offsets().size(), COMPLEX_ROW_COUNT);
    EXPECT_EQ(map_column.get_offsets()[0], 1);
    EXPECT_EQ(map_column.get_offsets()[1], 1);
    EXPECT_EQ(map_column.get_offsets()[2], 2);

    const auto& keys = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(map_column.get_keys()).get_nested_column());
    EXPECT_EQ(keys.get_data_at(0).to_string(), "first");
    EXPECT_EQ(keys.get_data_at(1).to_string(), "second");

    const auto& value_nullable = assert_cast<const ColumnNullable&>(map_column.get_values());
    const auto& value_struct = assert_cast<const ColumnStruct&>(value_nullable.get_nested_column());
    ASSERT_EQ(value_struct.tuple_size(), 1);
    const auto& value_a = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(value_struct.get_column(0)).get_nested_column());
    ASSERT_EQ(value_a.size(), 2);
    EXPECT_EQ(value_a.get_element(0), 7);
    EXPECT_EQ(value_a.get_element(1), 8);
}

TEST_F(NewOrcReaderTest, ReadProjectedNestedStructChildFromHiveOrcFixture) {
    const auto archive = find_repo_file(
            "docker/thirdparties/docker-compose/hive/scripts/data/multi_catalog/"
            "orc_nested_types/data.tar.gz");
    ASSERT_TRUE(std::filesystem::exists(archive)) << archive;
    const auto extract_dir = _test_dir / "orc_nested_types";
    std::filesystem::create_directories(extract_dir);
    const std::string extract_cmd =
            "tar -xzf '" + archive.string() + "' -C '" + extract_dir.string() + "'";
    ASSERT_EQ(std::system(extract_cmd.c_str()), 0);
    const auto file_path =
            extract_dir /
            "data/nested_types1_orc/part-00001-af6ae8cd-ef39-4a9b-a809-bcbd8aec7ccb-c000."
            "snappy.orc";
    ASSERT_TRUE(std::filesystem::exists(file_path)) << file_path;

    auto reader = create_reader_for_path(file_path.string());
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 9);
    const auto& complex_struct = schema[8];
    ASSERT_EQ(complex_struct.name, "complex_struct_col");
    ASSERT_EQ(complex_struct.children.size(), 3);
    const auto& c_field = complex_struct.children[2];
    ASSERT_EQ(c_field.name, "c");

    auto projected_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {c_field.type}, Strings {c_field.name}));
    Block block;
    block.insert({projected_type->create_column(), projected_type, complex_struct.name});

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(complex_struct.file_local_id())};
    request->local_positions.emplace(format::LocalColumnId(complex_struct.file_local_id()),
                                     format::LocalIndex(0));
    auto projection = make_projection(complex_struct, false);
    projection.children.push_back(make_projection(c_field));
    request->non_predicate_columns[0] = std::move(projection);
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 3);

    const auto& root_struct = assert_cast<const ColumnStruct&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column)
                    .get_nested_column());
    ASSERT_EQ(root_struct.tuple_size(), 1);
    const auto& c_nullable = assert_cast<const ColumnNullable&>(root_struct.get_column(0));
    EXPECT_FALSE(c_nullable.is_null_at(0));
    const auto& c_struct = assert_cast<const ColumnStruct&>(c_nullable.get_nested_column());
    ASSERT_EQ(c_struct.tuple_size(), 2);
    const auto& y_nullable = assert_cast<const ColumnNullable&>(c_struct.get_column(1));
    const auto& y_values = assert_cast<const ColumnString&>(y_nullable.get_nested_column());
    EXPECT_FALSE(y_nullable.is_null_at(0));
    EXPECT_EQ(y_values.get_data_at(0).to_string(), "Hello");
}

TEST_F(NewOrcReaderTest, ReadLazyProjectedNestedStructChildFromHiveOrcFixture) {
    const auto archive = find_repo_file(
            "docker/thirdparties/docker-compose/hive/scripts/data/multi_catalog/"
            "orc_nested_types/data.tar.gz");
    ASSERT_TRUE(std::filesystem::exists(archive)) << archive;
    const auto extract_dir = _test_dir / "lazy_orc_nested_types";
    std::filesystem::create_directories(extract_dir);
    const std::string extract_cmd =
            "tar -xzf '" + archive.string() + "' -C '" + extract_dir.string() + "'";
    ASSERT_EQ(std::system(extract_cmd.c_str()), 0);
    const auto file_path =
            extract_dir /
            "data/nested_types1_orc/part-00001-af6ae8cd-ef39-4a9b-a809-bcbd8aec7ccb-c000."
            "snappy.orc";
    ASSERT_TRUE(std::filesystem::exists(file_path)) << file_path;

    auto reader = create_reader_for_path(file_path.string());
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 9);
    const auto& id_field = schema[0];
    const auto& complex_struct = schema[8];
    ASSERT_EQ(complex_struct.name, "complex_struct_col");
    ASSERT_EQ(complex_struct.children.size(), 3);
    const auto& c_field = complex_struct.children[2];
    ASSERT_EQ(c_field.name, "c");

    auto projected_type = make_nullable(
            std::make_shared<DataTypeStruct>(DataTypes {c_field.type}, Strings {c_field.name}));
    Block block = build_file_block({id_field});
    block.insert({projected_type->create_column(), projected_type, complex_struct.name});

    auto request = std::make_shared<format::FileScanRequest>();
    request->predicate_columns = {field_projection(id_field.file_local_id())};
    request->non_predicate_columns = {field_projection(complex_struct.file_local_id())};
    request->local_positions.emplace(format::LocalColumnId(id_field.file_local_id()),
                                     format::LocalIndex(0));
    request->local_positions.emplace(format::LocalColumnId(complex_struct.file_local_id()),
                                     format::LocalIndex(1));
    auto projection = make_projection(complex_struct, false);
    projection.children.push_back(make_projection(c_field));
    request->non_predicate_columns[0] = std::move(projection);
    request->conjuncts.push_back(
            VExprContext::create_shared(std::make_shared<NullableInt32EqualsExpr>(0, 3)));
    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, 1);

    const auto& root_struct = assert_cast<const ColumnStruct&>(
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column)
                    .get_nested_column());
    ASSERT_EQ(root_struct.tuple_size(), 1);
    const auto& c_nullable = assert_cast<const ColumnNullable&>(root_struct.get_column(0));
    EXPECT_FALSE(c_nullable.is_null_at(0));
    const auto& c_struct = assert_cast<const ColumnStruct&>(c_nullable.get_nested_column());
    ASSERT_EQ(c_struct.tuple_size(), 2);
    const auto& y_nullable = assert_cast<const ColumnNullable&>(c_struct.get_column(1));
    const auto& y_values = assert_cast<const ColumnString&>(y_nullable.get_nested_column());
    EXPECT_FALSE(y_nullable.is_null_at(0));
    EXPECT_EQ(y_values.get_data_at(0).to_string(), "Hello");
    EXPECT_EQ(reader->reader_statistics().lazy_read_filtered_rows, 2);
}

TEST_F(NewOrcReaderTest, ReadProjectedComplexChildrenWithNulls) {
    const auto complex_file_path = (_test_dir / "nullable_complex_projection.orc").string();
    write_nullable_complex_orc_file(complex_file_path);
    auto reader = create_reader_for_path(complex_file_path);
    RuntimeState state {TQueryOptions(), TQueryGlobals()};
    ASSERT_TRUE(reader->init(&state).ok());

    std::vector<format::ColumnDefinition> schema;
    ASSERT_TRUE(reader->get_schema(&schema).ok());
    ASSERT_EQ(schema.size(), 3);
    ASSERT_EQ(schema[0].children.size(), 2);
    ASSERT_EQ(schema[1].children.size(), 1);
    ASSERT_EQ(schema[1].children[0].children.size(), 2);
    ASSERT_EQ(schema[2].children.size(), 2);
    ASSERT_EQ(schema[2].children[1].children.size(), 2);

    DataTypes projected_struct_child_types {schema[0].children[1].type};
    Strings projected_struct_child_names {schema[0].children[1].name};
    auto projected_struct_type = make_nullable(std::make_shared<DataTypeStruct>(
            projected_struct_child_types, projected_struct_child_names));

    DataTypes projected_element_child_types {schema[1].children[0].children[0].type};
    Strings projected_element_child_names {schema[1].children[0].children[0].name};
    auto projected_element_type = make_nullable(std::make_shared<DataTypeStruct>(
            projected_element_child_types, projected_element_child_names));
    auto projected_array_type =
            make_nullable(std::make_shared<DataTypeArray>(projected_element_type));

    const auto& key_field = schema[2].children[0];
    const auto& value_field = schema[2].children[1];
    DataTypes projected_value_child_types {value_field.children[0].type};
    Strings projected_value_child_names {value_field.children[0].name};
    auto projected_value_type = make_nullable(std::make_shared<DataTypeStruct>(
            projected_value_child_types, projected_value_child_names));
    auto projected_map_type =
            make_nullable(std::make_shared<DataTypeMap>(key_field.type, projected_value_type));

    Block block;
    block.insert({projected_struct_type->create_column(), projected_struct_type, schema[0].name});
    block.insert({projected_array_type->create_column(), projected_array_type, schema[1].name});
    block.insert({projected_map_type->create_column(), projected_map_type, schema[2].name});

    auto request = std::make_shared<format::FileScanRequest>();
    request->non_predicate_columns = {field_projection(0), field_projection(1),
                                      field_projection(2)};
    request->local_positions.emplace(format::LocalColumnId(0), format::LocalIndex(0));
    request->local_positions.emplace(format::LocalColumnId(1), format::LocalIndex(1));
    request->local_positions.emplace(format::LocalColumnId(2), format::LocalIndex(2));

    auto struct_projection = make_projection(schema[0], false);
    struct_projection.children.push_back(make_projection(schema[0].children[1]));
    request->non_predicate_columns[0] = std::move(struct_projection);

    auto array_projection = make_projection(schema[1], false);
    auto element_projection = make_projection(schema[1].children[0], false);
    element_projection.children.push_back(make_projection(schema[1].children[0].children[0]));
    array_projection.children.push_back(std::move(element_projection));
    request->non_predicate_columns[1] = std::move(array_projection);

    auto map_projection = make_projection(schema[2], false);
    map_projection.children.push_back(make_projection(key_field));
    auto value_projection = make_projection(value_field, false);
    value_projection.children.push_back(make_projection(value_field.children[0]));
    map_projection.children.push_back(std::move(value_projection));
    request->non_predicate_columns[2] = std::move(map_projection);

    ASSERT_TRUE(reader->open(request).ok());

    size_t rows = 0;
    bool eof = false;
    ASSERT_TRUE(reader->get_block(&block, &rows, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(rows, COMPLEX_ROW_COUNT);

    const auto& struct_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    EXPECT_FALSE(struct_nullable.is_null_at(0));
    EXPECT_TRUE(struct_nullable.is_null_at(1));
    EXPECT_FALSE(struct_nullable.is_null_at(2));
    const auto& struct_column =
            assert_cast<const ColumnStruct&>(struct_nullable.get_nested_column());
    ASSERT_EQ(struct_column.tuple_size(), 1);
    const auto& struct_b = assert_cast<const ColumnNullable&>(struct_column.get_column(0));
    const auto& struct_b_values = assert_cast<const ColumnString&>(struct_b.get_nested_column());
    EXPECT_FALSE(struct_b.is_null_at(0));
    EXPECT_TRUE(struct_b.is_null_at(2));
    EXPECT_EQ(struct_b_values.get_data_at(0).to_string(), "ten");

    const auto& array_nullable =
            assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_FALSE(array_nullable.is_null_at(0));
    EXPECT_TRUE(array_nullable.is_null_at(1));
    EXPECT_FALSE(array_nullable.is_null_at(2));
    const auto& array_column = assert_cast<const ColumnArray&>(array_nullable.get_nested_column());
    ASSERT_EQ(array_column.get_offsets().size(), COMPLEX_ROW_COUNT);
    EXPECT_EQ(array_column.get_offsets()[0], 1);
    EXPECT_EQ(array_column.get_offsets()[1], 1);
    EXPECT_EQ(array_column.get_offsets()[2], 2);
    const auto& element_struct = assert_cast<const ColumnStruct&>(
            assert_cast<const ColumnNullable&>(array_column.get_data()).get_nested_column());
    ASSERT_EQ(element_struct.tuple_size(), 1);
    const auto& element_a = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(element_struct.get_column(0)).get_nested_column());
    ASSERT_EQ(element_a.size(), 2);
    EXPECT_EQ(element_a.get_element(0), 11);
    EXPECT_EQ(element_a.get_element(1), 22);

    const auto& map_nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    EXPECT_FALSE(map_nullable.is_null_at(0));
    EXPECT_TRUE(map_nullable.is_null_at(1));
    EXPECT_FALSE(map_nullable.is_null_at(2));
    const auto& map_column = assert_cast<const ColumnMap&>(map_nullable.get_nested_column());
    ASSERT_EQ(map_column.get_offsets().size(), COMPLEX_ROW_COUNT);
    EXPECT_EQ(map_column.get_offsets()[0], 1);
    EXPECT_EQ(map_column.get_offsets()[1], 1);
    EXPECT_EQ(map_column.get_offsets()[2], 2);
    const auto& map_keys = assert_cast<const ColumnString&>(
            assert_cast<const ColumnNullable&>(map_column.get_keys()).get_nested_column());
    EXPECT_EQ(map_keys.get_data_at(0).to_string(), "left");
    EXPECT_EQ(map_keys.get_data_at(1).to_string(), "right");
    const auto& value_struct = assert_cast<const ColumnStruct&>(
            assert_cast<const ColumnNullable&>(map_column.get_values()).get_nested_column());
    ASSERT_EQ(value_struct.tuple_size(), 1);
    const auto& value_a = assert_cast<const ColumnInt32&>(
            assert_cast<const ColumnNullable&>(value_struct.get_column(0)).get_nested_column());
    ASSERT_EQ(value_a.size(), 2);
    EXPECT_EQ(value_a.get_element(0), 101);
    EXPECT_EQ(value_a.get_element(1), 202);
}

} // namespace
} // namespace doris
