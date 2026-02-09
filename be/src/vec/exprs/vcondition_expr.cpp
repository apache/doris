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

#include "vec/exprs/vcondition_expr.h"

#include <glog/logging.h>

#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/exprs/function_context.h"

namespace doris::vectorized {

Status VConditionExpr::prepare(RuntimeState* state, const RowDescriptor& desc,
                               VExprContext* context) {
    RETURN_IF_ERROR_OR_PREPARED(VExpr::prepare(state, desc, context));
    _prepare_finished = true;
    return Status::OK();
}

Status VConditionExpr::open(RuntimeState* state, VExprContext* context,
                            FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    RETURN_IF_ERROR(VExpr::open(state, context, scope));
    _open_finished = true;
    return Status::OK();
}

void VConditionExpr::close(VExprContext* context, FunctionContext::FunctionStateScope scope) {
    DCHECK(_prepare_finished);
    VExpr::close(context, scope);
}

std::string VConditionExpr::debug_string() const {
    std::string result = expr_name() + "(";
    for (size_t i = 0; i < _children.size(); ++i) {
        if (i != 0) {
            result += ", ";
        }
        result += _children[i]->debug_string();
    }
    result += ")";
    return result;
}

size_t VConditionExpr::count_true_with_notnull(const ColumnPtr& col) {
    if (col->only_null()) {
        return 0;
    }

    if (const auto* const_col = check_and_get_column<ColumnConst>(col.get())) {
        // if is null , get_bool will return false
        // bool get_bool(size_t n) const override {
        //     return is_null_at(n) ? false : _nested_column->get_bool(n);
        // }
        bool is_true = const_col->get_bool(0);
        return is_true ? col->size() : 0;
    }

    auto count = col->size();
    if (col->is_nullable()) {
        const auto* nullable = assert_cast<const ColumnNullable*>(col.get());
        const auto* __restrict null_data = nullable->get_null_map_data().data();
        const auto* __restrict bool_data =
                ((const ColumnUInt8&)(nullable->get_nested_column())).get_data().data();

        size_t null_count = count - simd::count_zero_num((const int8_t*)null_data, count);

        if (null_count == count) {
            return 0;
        } else if (null_count == 0) {
            size_t true_count = count - simd::count_zero_num((const int8_t*)bool_data, count);
            return true_count;
        } else {
            // In fact, the null_count maybe is different with true_count, but it's no impact
            return null_count;
        }
    } else {
        const auto* bool_col = assert_cast<const ColumnUInt8*>(col.get());
        const auto* __restrict bool_data = bool_col->get_data().data();
        return count - simd::count_zero_num((const int8_t*)bool_data, count);
    }
}

ColumnPtr materialize_column_if_const(const ColumnPtr& column) {
    return column->convert_to_full_column_if_const();
}

ColumnPtr make_nullable_column_if_not(const ColumnPtr& column) {
    if (is_column_nullable(*column)) return column;

    return ColumnNullable::create(materialize_column_if_const(column),
                                  ColumnUInt8::create(column->size(), 0));
}

ColumnPtr get_nested_column(const ColumnPtr& column) {
    if (auto* nullable = check_and_get_column<ColumnNullable>(*column))
        return nullable->get_nested_column_ptr();
    else if (const auto* column_const = check_and_get_column<ColumnConst>(*column))
        return ColumnConst::create(get_nested_column(column_const->get_data_column_ptr()),
                                   column->size());

    return column;
}

Status VectorizedIfExpr::execute_generic(Block& block, const ColumnUInt8* cond_col,
                                         const ColumnWithTypeAndName& then_col_type_name,
                                         const ColumnWithTypeAndName& else_col_type_name,
                                         uint32_t result, size_t input_row_count) const {
    MutableColumnPtr result_column = block.get_by_position(result).type->create_column();
    result_column->reserve(input_row_count);

    const IColumn& then_col = *then_col_type_name.column;
    const IColumn& else_col = *else_col_type_name.column;
    bool then_is_const = is_column_const(then_col);
    bool else_is_const = is_column_const(else_col);

    const auto& cond_array = cond_col->get_data();

    if (then_is_const && else_is_const) {
        const IColumn& then_nested_column =
                assert_cast<const ColumnConst&>(then_col).get_data_column();
        const IColumn& else_nested_column =
                assert_cast<const ColumnConst&>(else_col).get_data_column();
        for (size_t i = 0; i < input_row_count; i++) {
            if (cond_array[i])
                result_column->insert_from(then_nested_column, 0);
            else
                result_column->insert_from(else_nested_column, 0);
        }
    } else if (then_is_const) {
        const IColumn& then_nested_column =
                assert_cast<const ColumnConst&>(then_col).get_data_column();

        for (size_t i = 0; i < input_row_count; i++) {
            if (cond_array[i])
                result_column->insert_from(then_nested_column, 0);
            else
                result_column->insert_from(else_col, i);
        }
    } else if (else_is_const) {
        const IColumn& else_nested_column =
                assert_cast<const ColumnConst&>(else_col).get_data_column();

        for (size_t i = 0; i < input_row_count; i++) {
            if (cond_array[i])
                result_column->insert_from(then_col, i);
            else
                result_column->insert_from(else_nested_column, 0);
        }
    } else {
        for (size_t i = 0; i < input_row_count; i++) {
            result_column->insert_from(cond_array[i] ? then_col : else_col, i);
        }
    }
    block.replace_by_position(result, std::move(result_column));
    return Status::OK();
}

Status VectorizedIfExpr::execute_for_null_then_else(Block& block,
                                                    const ColumnWithTypeAndName& arg_cond,
                                                    const ColumnWithTypeAndName& arg_then,
                                                    const ColumnWithTypeAndName& arg_else,
                                                    uint32_t result, size_t input_rows_count,
                                                    bool& handled) const {
    bool then_is_null = arg_then.column->only_null();
    bool else_is_null = arg_else.column->only_null();

    handled = false;
    if (!then_is_null && !else_is_null) {
        return Status::OK();
    }

    if (then_is_null && else_is_null) {
        block.get_by_position(result).column =
                block.get_by_position(result).type->create_column_const_with_default_value(
                        input_rows_count);
        handled = true;
        return Status::OK();
    }

    const auto* cond_col = typeid_cast<const ColumnUInt8*>(arg_cond.column.get());
    const ColumnConst* cond_const_col =
            check_and_get_column_const<ColumnUInt8>(arg_cond.column.get());

    /// If then is NULL, we create Nullable column with null mask OR-ed with condition.
    if (then_is_null) {
        if (cond_col) {
            if (is_column_nullable(*arg_else.column)) { // if(cond, null, nullable)
                auto arg_else_column = arg_else.column;
                auto result_column = (*std::move(arg_else_column)).mutate();
                assert_cast<ColumnNullable&>(*result_column)
                        .apply_null_map(assert_cast<const ColumnUInt8&>(*arg_cond.column));
                block.replace_by_position(result, std::move(result_column));
            } else { // if(cond, null, not_nullable)
                block.replace_by_position(
                        result, ColumnNullable::create(materialize_column_if_const(arg_else.column),
                                                       arg_cond.column));
            }
        } else if (cond_const_col) {
            if (cond_const_col->get_value<TYPE_BOOLEAN>()) { // if(true, null, else)
                block.get_by_position(result).column =
                        block.get_by_position(result).type->create_column()->clone_resized(
                                input_rows_count);
            } else { // if(false, null, else)
                block.get_by_position(result).column = make_nullable_column_if_not(arg_else.column);
            }
        } else {
            return Status::InternalError(
                    "Illegal column {} of first argument of function {}. Must be ColumnUInt8 "
                    "or ColumnConstUInt8.",
                    arg_cond.column->get_name(), expr_name());
        }
    } else { /// If else is NULL, we create Nullable column with null mask OR-ed with negated condition.
        if (cond_col) {
            size_t size = input_rows_count;

            if (is_column_nullable(*arg_then.column)) { // if(cond, nullable, NULL)
                auto arg_then_column = arg_then.column;
                auto result_column = (*std::move(arg_then_column)).mutate();
                assert_cast<ColumnNullable&>(*result_column)
                        .apply_negated_null_map(assert_cast<const ColumnUInt8&>(*arg_cond.column));
                block.replace_by_position(result, std::move(result_column));
            } else { // if(cond, not_nullable, NULL)
                const auto& null_map_data = cond_col->get_data();
                auto negated_null_map = ColumnUInt8::create();
                auto& negated_null_map_data = negated_null_map->get_data();
                negated_null_map_data.resize(size);

                for (size_t i = 0; i < size; ++i) {
                    negated_null_map_data[i] = !null_map_data[i];
                }

                block.replace_by_position(
                        result, ColumnNullable::create(materialize_column_if_const(arg_then.column),
                                                       std::move(negated_null_map)));
            }
        } else if (cond_const_col) {
            if (cond_const_col->get_value<TYPE_BOOLEAN>()) { // if(true, then, NULL)
                block.get_by_position(result).column = make_nullable_column_if_not(arg_then.column);
            } else { // if(false, then, NULL)
                block.get_by_position(result).column =
                        block.get_by_position(result).type->create_column()->clone_resized(
                                input_rows_count);
            }
        } else {
            return Status::InternalError(
                    "Illegal column {} of first argument of function {}. Must be ColumnUInt8 "
                    "or ColumnConstUInt8.",
                    arg_cond.column->get_name(), expr_name());
        }
    }
    handled = true;
    return Status::OK();
}

Status VectorizedIfExpr::execute_for_nullable_then_else(Block& block,
                                                        const ColumnWithTypeAndName& arg_cond,
                                                        const ColumnWithTypeAndName& arg_then,
                                                        const ColumnWithTypeAndName& arg_else,
                                                        uint32_t result, size_t input_rows_count,
                                                        bool& handled) const {
    auto then_type_is_nullable = arg_then.type->is_nullable();
    auto else_type_is_nullable = arg_else.type->is_nullable();
    handled = false;
    if (!then_type_is_nullable && !else_type_is_nullable) {
        return Status::OK();
    }

    const auto* then_is_nullable = check_and_get_column<ColumnNullable>(*arg_then.column);
    const auto* else_is_nullable = check_and_get_column<ColumnNullable>(*arg_else.column);
    bool then_column_is_const_nullable = false;
    bool else_column_is_const_nullable = false;
    if (then_type_is_nullable && then_is_nullable == nullptr) {
        //this case is a const(nullable column)
        const auto& const_column = assert_cast<const ColumnConst&>(*arg_then.column);
        then_is_nullable =
                assert_cast<const ColumnNullable*>(const_column.get_data_column_ptr().get());
        then_column_is_const_nullable = true;
    }

    if (else_type_is_nullable && else_is_nullable == nullptr) {
        //this case is a const(nullable column)
        const auto& const_column = assert_cast<const ColumnConst&>(*arg_else.column);
        else_is_nullable =
                assert_cast<const ColumnNullable*>(const_column.get_data_column_ptr().get());
        else_column_is_const_nullable = true;
    }

    /** Calculate null mask of result and nested column separately.
      */
    ColumnPtr result_null_mask;
    {
        // get null map from column:
        // a. get_null_map_column_ptr() : it's a real nullable column, so could get it from nullable column
        // b. create a const_nullmap_column: it's a not nullable column or a const nullable column, contain a const value
        Block temporary_block;
        temporary_block.insert(arg_cond);
        auto then_nested_null_map =
                (then_type_is_nullable && !then_column_is_const_nullable)
                        ? then_is_nullable->get_null_map_column_ptr()
                        : DataTypeUInt8().create_column_const_with_default_value(input_rows_count);
        temporary_block.insert(
                {then_nested_null_map, std::make_shared<DataTypeUInt8>(), "then_column_null_map"});

        auto else_nested_null_map =
                (else_type_is_nullable && !else_column_is_const_nullable)
                        ? else_is_nullable->get_null_map_column_ptr()
                        : DataTypeUInt8().create_column_const_with_default_value(input_rows_count);
        temporary_block.insert(
                {else_nested_null_map, std::make_shared<DataTypeUInt8>(), "else_column_null_map"});
        temporary_block.insert(
                {nullptr, std::make_shared<DataTypeUInt8>(), "result_column_null_map"});

        RETURN_IF_ERROR(
                _execute_impl_internal(temporary_block, {0, 1, 2}, 3, temporary_block.rows()));

        result_null_mask = temporary_block.get_by_position(3).column;
    }

    ColumnPtr result_nested_column;

    {
        Block temporary_block(
                {arg_cond,
                 {get_nested_column(arg_then.column), remove_nullable(arg_then.type), ""},
                 {get_nested_column(arg_else.column), remove_nullable(arg_else.type), ""},
                 {nullptr, remove_nullable(block.get_by_position(result).type), ""}});

        RETURN_IF_ERROR(
                _execute_impl_internal(temporary_block, {0, 1, 2}, 3, temporary_block.rows()));

        result_nested_column = temporary_block.get_by_position(3).column;
    }

    auto column = ColumnNullable::create(materialize_column_if_const(result_nested_column),
                                         materialize_column_if_const(result_null_mask));
    block.replace_by_position(result, std::move(column));
    handled = true;
    return Status::OK();
}

Status VectorizedIfExpr::execute_for_null_condition(Block& block, const ColumnNumbers& arguments,
                                                    const ColumnWithTypeAndName& arg_cond,
                                                    const ColumnWithTypeAndName& arg_then,
                                                    const ColumnWithTypeAndName& arg_else,
                                                    uint32_t result, bool& handled) const {
    bool cond_is_null = arg_cond.column->only_null();
    handled = false;

    if (cond_is_null) {
        block.replace_by_position(result, arg_else.column->clone_resized(arg_cond.column->size()));
        handled = true;
        return Status::OK();
    }

    if (const auto* nullable = check_and_get_column<ColumnNullable>(*arg_cond.column)) {
        DCHECK(remove_nullable(arg_cond.type)->get_primitive_type() == PrimitiveType::TYPE_BOOLEAN);

        // update nested column by null map
        const auto* __restrict null_map = nullable->get_null_map_data().data();
        auto* __restrict nested_bool_data =
                ((ColumnUInt8&)(nullable->get_nested_column())).get_data().data();
        auto rows = nullable->size();
        for (size_t i = 0; i < rows; i++) {
            nested_bool_data[i] &= !null_map[i];
        }
        auto column_size = block.columns();
        block.insert(
                {nullable->get_nested_column_ptr(), remove_nullable(arg_cond.type), arg_cond.name});

        handled = true;
        return _execute_impl_internal(block, {column_size, arguments[1], arguments[2]}, result,
                                      rows);
    }
    return Status::OK();
}

Status VectorizedIfExpr::_execute_impl_internal(Block& block, const ColumnNumbers& arguments,
                                                uint32_t result, size_t input_rows_count) const {
    const ColumnWithTypeAndName& arg_then = block.get_by_position(arguments[1]);
    const ColumnWithTypeAndName& arg_else = block.get_by_position(arguments[2]);
    ColumnWithTypeAndName& cond_column = block.get_by_position(arguments[0]);
    cond_column.column = materialize_column_if_const(cond_column.column);
    const ColumnWithTypeAndName& arg_cond = block.get_by_position(arguments[0]);

    Status ret = Status::OK();
    bool handled = false;
    RETURN_IF_ERROR(execute_for_null_condition(block, arguments, arg_cond, arg_then, arg_else,
                                               result, handled));

    if (!handled) {
        RETURN_IF_ERROR(execute_for_null_then_else(block, arg_cond, arg_then, arg_else, result,
                                                   input_rows_count, handled));
    }

    if (!handled) {
        RETURN_IF_ERROR(execute_for_nullable_then_else(block, arg_cond, arg_then, arg_else, result,
                                                       input_rows_count, handled));
    }

    if (handled) {
        return Status::OK();
    }

    const auto* cond_col = assert_cast<const ColumnUInt8*>(arg_cond.column.get());
    const ColumnConst* cond_const_col =
            check_and_get_column_const<ColumnUInt8>(arg_cond.column.get());

    if (cond_const_col) {
        block.get_by_position(result).column =
                cond_const_col->get_value<TYPE_BOOLEAN>() ? arg_then.column : arg_else.column;
        return Status::OK();
    }

    Status vec_exec;

    auto call = [&](const auto& type) -> bool {
        using DataType = std::decay_t<decltype(type)>;
        vec_exec = execute_basic_type<DataType::PType>(block, cond_col, arg_then, arg_else, result,
                                                       vec_exec);
        return true;
    };

    auto can_use_vec_exec = dispatch_switch_scalar(arg_then.type->get_primitive_type(), call);
    if (can_use_vec_exec) {
        return vec_exec;
    } else {
        return execute_generic(block, cond_col, arg_then, arg_else, result, input_rows_count);
    }
}

Status VectorizedIfExpr::execute_column(VExprContext* context, const Block* block,
                                        Selector* selector, size_t count,
                                        ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK_EQ(_children.size(), 3) << "IF expr must have three children";

    ColumnPtr cond_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, cond_column));

    ColumnPtr then_column;
    ColumnPtr else_column;

    auto true_count = count_true_with_notnull(cond_column);
    auto item_count = cond_column->size();
    if (true_count == item_count) {
        RETURN_IF_ERROR(_children[1]->execute_column(context, block, selector, count, then_column));
        result_column = _data_type->is_nullable() ? make_nullable(then_column) : then_column;
        return Status::OK();
    } else if (true_count == 0) {
        RETURN_IF_ERROR(_children[2]->execute_column(context, block, selector, count, else_column));
        result_column = _data_type->is_nullable() ? make_nullable(else_column) : else_column;
        return Status::OK();
    }

    RETURN_IF_ERROR(_children[1]->execute_column(context, block, selector, count, then_column));
    RETURN_IF_ERROR(_children[2]->execute_column(context, block, selector, count, else_column));

    Block temp_block;

    temp_block.insert({cond_column, _children[0]->execute_type(block), _children[0]->expr_name()});
    temp_block.insert({then_column, _children[1]->execute_type(block), _children[1]->expr_name()});
    temp_block.insert({else_column, _children[2]->execute_type(block), _children[2]->expr_name()});

    // prepare a column to save result
    temp_block.insert({nullptr, _data_type, IF_NAME});
    RETURN_IF_ERROR(_execute_impl_internal(temp_block, {0, 1, 2}, 3, temp_block.rows()));
    result_column = temp_block.get_by_position(3).column;
    DCHECK_EQ(result_column->size(), count);
    return Status::OK();
}

Status VectorizedIfNullExpr::execute_column(VExprContext* context, const Block* block,
                                            Selector* selector, size_t count,
                                            ColumnPtr& result_column) const {
    DCHECK(_open_finished || block == nullptr) << debug_string();
    DCHECK_EQ(_children.size(), 2) << "IFNULL expr must have two children";

    ColumnPtr first_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, selector, count, first_column));
    first_column = first_column->convert_to_full_column_if_const();

    if (!first_column->is_nullable()) {
        result_column = first_column;
        DCHECK(_data_type->is_nullable() == false);
        return Status::OK();
    }

    if (first_column->only_null()) {
        RETURN_IF_ERROR(
                _children[1]->execute_column(context, block, selector, count, result_column));
        return Status::OK();
    }

    ColumnPtr second_column;
    RETURN_IF_ERROR(_children[1]->execute_column(context, block, selector, count, second_column));

    const auto& nullable_first_column = assert_cast<const ColumnNullable&>(*first_column);

    ColumnPtr cond_column = nullable_first_column.get_null_map_column_ptr();

    ColumnPtr then_column = second_column;

    ColumnPtr else_column;
    DataTypePtr else_type;

    if (_data_type->is_nullable()) {
        else_column = first_column;
        else_type = _children[0]->execute_type(block);
    } else {
        else_column = nullable_first_column.get_nested_column_ptr();
        else_type = remove_nullable(_children[0]->execute_type(block));
    }

    Block temp_block;
    temp_block.insert({cond_column, std::make_shared<DataTypeUInt8>(), "cond column"});
    temp_block.insert({then_column, _children[1]->execute_type(block), _children[1]->expr_name()});
    temp_block.insert({else_column, else_type, _children[0]->expr_name()});

    // prepare a column to save result
    temp_block.insert({nullptr, _data_type, IF_NULL_NAME});
    RETURN_IF_ERROR(_execute_impl_internal(temp_block, {0, 1, 2}, 3, temp_block.rows()));
    result_column = temp_block.get_by_position(3).column;
    DCHECK_EQ(result_column->size(), count);
    return Status::OK();
}

template <typename ColumnType>
void insert_result_data(MutableColumnPtr& result_column, ColumnPtr& argument_column,
                        const UInt8* __restrict null_map_data, UInt8* __restrict filled_flag,
                        const size_t input_rows_count) {
    if (result_column->size() == 0 && input_rows_count) {
        result_column->resize(input_rows_count);
        auto* __restrict result_raw_data =
                assert_cast<ColumnType*>(result_column.get())->get_data().data();
        for (int i = 0; i < input_rows_count; i++) {
            result_raw_data[i] = {};
        }
    }
    auto* __restrict result_raw_data =
            assert_cast<ColumnType*>(result_column.get())->get_data().data();
    auto* __restrict column_raw_data =
            assert_cast<const ColumnType*>(argument_column.get())->get_data().data();

    // Here it's SIMD thought the compiler automatically also
    // true: null_map_data[row]==0 && filled_idx[row]==0
    // if true, could filled current row data into result column
    for (size_t row = 0; row < input_rows_count; ++row) {
        if constexpr (std::is_same_v<ColumnType, ColumnDateV2>) {
            result_raw_data[row] = binary_cast<uint32_t, DateV2Value<DateV2ValueType>>(
                    result_raw_data[row].to_date_int_val() +
                    column_raw_data[row].to_date_int_val() *
                            uint32_t(!(null_map_data[row] | filled_flag[row])));
        } else if constexpr (std::is_same_v<ColumnType, ColumnDateTimeV2>) {
            result_raw_data[row] = binary_cast<uint64_t, DateV2Value<DateTimeV2ValueType>>(
                    result_raw_data[row].to_date_int_val() +
                    column_raw_data[row].to_date_int_val() *
                            uint64_t(!(null_map_data[row] | filled_flag[row])));
        } else if constexpr (std::is_same_v<ColumnType, ColumnTimeStampTz>) {
            result_raw_data[row] = binary_cast<uint64_t, TimestampTzValue>(
                    result_raw_data[row].to_date_int_val() +
                    column_raw_data[row].to_date_int_val() *
                            uint64_t(!(null_map_data[row] | filled_flag[row])));
        } else if constexpr (std::is_same_v<ColumnType, ColumnDate> ||
                             std::is_same_v<ColumnType, ColumnDateTime>) {
            result_raw_data[row] = binary_cast<int64_t, VecDateTimeValue>(
                    binary_cast<VecDateTimeValue, int64_t>(result_raw_data[row]) +
                    binary_cast<VecDateTimeValue, int64_t>(column_raw_data[row]) *
                            int64_t(!(null_map_data[row] | filled_flag[row])));
        } else {
            result_raw_data[row] +=
                    column_raw_data[row] *
                    typename ColumnType::value_type(!(null_map_data[row] | filled_flag[row]));
        }

        filled_flag[row] += (!(null_map_data[row] | filled_flag[row]));
    }
}

Status insert_result_data_bitmap(MutableColumnPtr& result_column, ColumnPtr& argument_column,
                                 const UInt8* __restrict null_map_data,
                                 UInt8* __restrict filled_flag, const size_t input_rows_count) {
    if (result_column->size() == 0 && input_rows_count) {
        result_column->resize(input_rows_count);
    }

    auto* __restrict result_raw_data =
            reinterpret_cast<ColumnBitmap*>(result_column.get())->get_data().data();
    auto* __restrict column_raw_data =
            reinterpret_cast<const ColumnBitmap*>(argument_column.get())->get_data().data();

    // Here it's SIMD thought the compiler automatically also
    // true: null_map_data[row]==0 && filled_idx[row]==0
    // if true, could filled current row data into result column
    for (size_t row = 0; row < input_rows_count; ++row) {
        if (!(null_map_data[row] | filled_flag[row])) {
            result_raw_data[row] = column_raw_data[row];
        }
        filled_flag[row] += (!(null_map_data[row] | filled_flag[row]));
    }
    return Status::OK();
}

Status filled_result_column(const DataTypePtr& data_type, MutableColumnPtr& result_column,
                            ColumnPtr& argument_column, UInt8* __restrict null_map_data,
                            UInt8* __restrict filled_flag, const size_t input_rows_count) {
    if (data_type->get_primitive_type() == TYPE_BITMAP) {
        return insert_result_data_bitmap(result_column, argument_column, null_map_data, filled_flag,
                                         input_rows_count);
    }

    auto call = [&](const auto& type) -> bool {
        using DispatchType = std::decay_t<decltype(type)>;
        insert_result_data<typename DispatchType::ColumnType>(
                result_column, argument_column, null_map_data, filled_flag, input_rows_count);
        return true;
    };

    if (!dispatch_switch_scalar(data_type->get_primitive_type(), call)) {
        return Status::InternalError("not support type {} in coalesce", data_type->get_name());
    }
    return Status::OK();
}

Status VectorizedCoalesceExpr::execute_column(VExprContext* context, const Block* block,
                                              Selector* selector, size_t count,
                                              ColumnPtr& return_column) const {
    DataTypePtr result_type = _data_type;
    const auto input_rows_count = count;

    size_t remaining_rows = input_rows_count;
    std::vector<uint32_t> record_idx(
            input_rows_count,
            0); //used to save column idx, record the result data of each row from which column
    std::vector<uint8_t> filled_flags(
            input_rows_count,
            0); //used to save filled flag, in order to check current row whether have filled data

    MutableColumnPtr result_column;
    if (!result_type->is_nullable()) {
        result_column = result_type->create_column();
    } else {
        result_column = remove_nullable(result_type)->create_column();
    }

    // because now follow below types does not support random position writing,
    // so insert into result data have two methods, one is for these types, one is for others type remaining
    bool cannot_random_write = result_column->is_column_string() ||
                               result_type->get_primitive_type() == PrimitiveType::TYPE_MAP ||
                               result_type->get_primitive_type() == PrimitiveType::TYPE_STRUCT ||
                               result_type->get_primitive_type() == PrimitiveType::TYPE_ARRAY ||
                               result_type->get_primitive_type() == PrimitiveType::TYPE_JSONB;
    if (cannot_random_write) {
        result_column->reserve(input_rows_count);
    }

    auto return_type = std::make_shared<DataTypeUInt8>();
    auto null_map = ColumnUInt8::create(input_rows_count,
                                        1); //if null_map_data==1, the current row should be null
    auto* __restrict null_map_data = null_map->get_data().data();

    auto is_not_null = [](const ColumnPtr& column, size_t size) -> ColumnUInt8::MutablePtr {
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
            /// Return the negated null map.
            auto res_column = ColumnUInt8::create(size);
            const auto* __restrict src_data = nullable->get_null_map_data().data();
            auto* __restrict res_data = assert_cast<ColumnUInt8&>(*res_column).get_data().data();

            for (size_t i = 0; i < size; ++i) {
                res_data[i] = !src_data[i];
            }
            return res_column;
        } else {
            /// Since no element is nullable, return a constant one.
            return ColumnUInt8::create(size, 1);
        }
    };

    std::vector<ColumnPtr> original_columns(_children.size());
    std::vector<ColumnPtr> argument_not_null_columns(_children.size());

    for (size_t i = 0; i < _children.size() && remaining_rows; ++i) {
        // Execute child expression to get the argument column.
        RETURN_IF_ERROR(
                _children[i]->execute_column(context, block, selector, count, original_columns[i]));
        original_columns[i] = original_columns[i]->convert_to_full_column_if_const();
        argument_not_null_columns[i] = original_columns[i];
        if (const auto* nullable =
                    check_and_get_column<const ColumnNullable>(*argument_not_null_columns[i])) {
            argument_not_null_columns[i] = nullable->get_nested_column_ptr();
        }

        auto res_column = is_not_null(original_columns[i], input_rows_count);

        auto& res_map = res_column->get_data();
        auto* __restrict res = res_map.data();

        // Here it's SIMD thought the compiler automatically
        // true: res[j]==1 && null_map_data[j]==1, false: others
        // if true: remaining_rows--; record_idx[j]=column_idx; null_map_data[j]=0, so the current row could fill result
        for (size_t j = 0; j < input_rows_count; ++j) {
            remaining_rows -= (res[j] & null_map_data[j]);
            record_idx[j] += (res[j] & null_map_data[j]) * i;
            null_map_data[j] -= (res[j] & null_map_data[j]);
        }

        if (remaining_rows == 0) {
            //check whether all result data from the same column
            size_t is_same_column_count = 0;
            const auto data = record_idx[0];
            for (size_t row = 0; row < input_rows_count; ++row) {
                is_same_column_count += (record_idx[row] == data);
            }

            if (is_same_column_count == input_rows_count) {
                if (result_type->is_nullable()) {
                    return_column = make_nullable(argument_not_null_columns[i]);
                } else {
                    return_column = argument_not_null_columns[i];
                }
                return Status::OK();
            }
        }

        if (!cannot_random_write) {
            //if not string type, could check one column firstly,
            //and then fill the not null value in result column,
            //this method may result in higher CPU cache
            RETURN_IF_ERROR(filled_result_column(result_type, result_column,
                                                 argument_not_null_columns[i], null_map_data,
                                                 filled_flags.data(), input_rows_count));
        }
    }

    if (cannot_random_write) {
        //if string type,  should according to the record results, fill in result one by one,
        for (size_t row = 0; row < input_rows_count; ++row) {
            if (null_map_data[row]) { //should be null
                result_column->insert_default();
            } else {
                result_column->insert_from(*argument_not_null_columns[record_idx[row]].get(), row);
            }
        }
    }

    if (result_type->is_nullable()) {
        return_column = ColumnNullable::create(std::move(result_column), std::move(null_map));
    } else {
        return_column = std::move(result_column);
    }

    DCHECK_EQ(return_column->size(), count);
    return Status::OK();
}

} // namespace doris::vectorized