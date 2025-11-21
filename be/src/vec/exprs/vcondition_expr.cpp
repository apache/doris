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

#include "udf/udf.h"
#include "util/simd/bits.h"
#include "vec/columns/column.h"

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

    if (const auto* const_col = check_and_get_column_const<ColumnUInt8>(col.get())) {
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
            if (cond_const_col->get_value<UInt8>()) { // if(true, null, else)
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
            if (cond_const_col->get_value<UInt8>()) { // if(true, then, NULL)
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
                cond_const_col->get_value<UInt8>() ? arg_then.column : arg_else.column;
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
                                        ColumnPtr& result_column) const {
    DCHECK(_open_finished || _getting_const_col) << debug_string();
    DCHECK_EQ(_children.size(), 3) << "IF expr must have three children";

    ColumnPtr cond_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, cond_column));

    ColumnPtr then_column;
    ColumnPtr else_column;

    auto true_count = count_true_with_notnull(cond_column);
    auto item_count = cond_column->size();
    if (true_count == item_count) {
        RETURN_IF_ERROR(_children[1]->execute_column(context, block, then_column));
        result_column = _data_type->is_nullable() ? make_nullable(then_column) : then_column;
        return Status::OK();
    } else if (true_count == 0) {
        RETURN_IF_ERROR(_children[2]->execute_column(context, block, else_column));
        result_column = _data_type->is_nullable() ? make_nullable(else_column) : else_column;
        return Status::OK();
    }

    RETURN_IF_ERROR(_children[1]->execute_column(context, block, then_column));
    RETURN_IF_ERROR(_children[2]->execute_column(context, block, else_column));

    Block temp_block;

    temp_block.insert({cond_column, _children[0]->execute_type(block), _children[0]->expr_name()});
    temp_block.insert({then_column, _children[1]->execute_type(block), _children[1]->expr_name()});
    temp_block.insert({else_column, _children[2]->execute_type(block), _children[2]->expr_name()});

    // prepare a column to save result
    temp_block.insert({nullptr, _data_type, IF_NAME});
    RETURN_IF_ERROR(_execute_impl_internal(temp_block, {0, 1, 2}, 3, temp_block.rows()));
    result_column = temp_block.get_by_position(3).column;
    RETURN_IF_ERROR(block->check_type_and_column());
    return Status::OK();
}

Status VectorizedIfNullExpr::execute_column(VExprContext* context, const Block* block,
                                            ColumnPtr& result_column) const {
    DCHECK(_open_finished || _getting_const_col) << debug_string();
    DCHECK_EQ(_children.size(), 2) << "IFNULL expr must have two children";

    ColumnPtr first_column;
    RETURN_IF_ERROR(_children[0]->execute_column(context, block, first_column));
    first_column = first_column->convert_to_full_column_if_const();

    if (!first_column->is_nullable()) {
        result_column = first_column;
        DCHECK(_data_type->is_nullable() == false);
        return Status::OK();
    }

    if (first_column->only_null()) {
        RETURN_IF_ERROR(_children[1]->execute_column(context, block, result_column));
        return Status::OK();
    }

    ColumnPtr second_column;
    RETURN_IF_ERROR(_children[1]->execute_column(context, block, second_column));

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
    RETURN_IF_ERROR(block->check_type_and_column());
    return Status::OK();
}

} // namespace doris::vectorized