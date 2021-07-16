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

#include "vec/functions/function.h"

#include <memory>
#include <optional>

#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

ColumnPtr wrap_in_nullable(const ColumnPtr& src, const Block& block, const ColumnNumbers& args,
                           size_t result, size_t input_rows_count) {
    ColumnPtr result_null_map_column;

    /// If result is already nullable.
    ColumnPtr src_not_nullable = src;

    if (src->only_null())
        return src;
    else if (auto* nullable = check_and_get_column<ColumnNullable>(*src)) {
        src_not_nullable = nullable->get_nested_column_ptr();
        result_null_map_column = nullable->get_null_map_column_ptr();
    }

    for (const auto& arg : args) {
        const ColumnWithTypeAndName& elem = block.get_by_position(arg);
        if (!elem.type->is_nullable()) continue;

        /// Const Nullable that are NULL.
        if (elem.column->only_null())
            return block.get_by_position(result).type->create_column_const(input_rows_count,
                                                                           Null());

        if (is_column_const(*elem.column)) continue;

        if (auto* nullable = check_and_get_column<ColumnNullable>(*elem.column)) {
            const ColumnPtr& null_map_column = nullable->get_null_map_column_ptr();
            if (!result_null_map_column) {
                result_null_map_column = null_map_column;
            } else {
                MutableColumnPtr mutable_result_null_map_column =
                        (*std::move(result_null_map_column)).mutate();

                NullMap& result_null_map =
                        assert_cast<ColumnUInt8&>(*mutable_result_null_map_column).get_data();
                const NullMap& src_null_map =
                        assert_cast<const ColumnUInt8&>(*null_map_column).get_data();

                for (size_t i = 0, size = result_null_map.size(); i < size; ++i)
                    if (src_null_map[i]) result_null_map[i] = 1;

                result_null_map_column = std::move(mutable_result_null_map_column);
            }
        }
    }

    if (!result_null_map_column) return make_nullable(src);

    return ColumnNullable::create(src_not_nullable->convert_to_full_column_if_const(),
                                  result_null_map_column);
}

namespace {

struct NullPresence {
    bool has_nullable = false;
    bool has_null_constant = false;
};

NullPresence getNullPresense(const Block& block, const ColumnNumbers& args) {
    NullPresence res;

    for (const auto& arg : args) {
        const auto& elem = block.get_by_position(arg);

        if (!res.has_nullable) res.has_nullable = elem.type->is_nullable();
        if (!res.has_null_constant) res.has_null_constant = elem.type->only_null();
    }

    return res;
}

[[maybe_unused]] NullPresence getNullPresense(const ColumnsWithTypeAndName& args) {
    NullPresence res;

    for (const auto& elem : args) {
        if (!res.has_nullable) res.has_nullable = elem.type->is_nullable();
        if (!res.has_null_constant) res.has_null_constant = elem.type->only_null();
    }

    return res;
}

bool allArgumentsAreConstants(const Block& block, const ColumnNumbers& args) {
    for (auto arg : args)
        if (!is_column_const(*block.get_by_position(arg).column)) return false;
    return true;
}
} // namespace

Status PreparedFunctionImpl::default_implementation_for_constant_arguments(
        Block& block, const ColumnNumbers& args, size_t result, size_t input_rows_count,
        bool dry_run, bool* executed) {
    *executed = false;
    ColumnNumbers arguments_to_remain_constants = get_arguments_that_are_always_constant();

    /// Check that these arguments are really constant.
    for (auto arg_num : arguments_to_remain_constants)
        if (arg_num < args.size() &&
            !is_column_const(*block.get_by_position(args[arg_num]).column)) {
            return Status::RuntimeError(fmt::format(
                    "Argument at index {} for function {}  must be constant", arg_num, get_name()));
        }

    if (args.empty() || !use_default_implementation_for_constants() ||
        !allArgumentsAreConstants(block, args))
        return Status::OK();

    Block temporary_block;
    bool have_converted_columns = false;

    size_t arguments_size = args.size();
    for (size_t arg_num = 0; arg_num < arguments_size; ++arg_num) {
        const ColumnWithTypeAndName& column = block.get_by_position(args[arg_num]);

        if (arguments_to_remain_constants.end() != std::find(arguments_to_remain_constants.begin(),
                                                             arguments_to_remain_constants.end(),
                                                             arg_num)) {
            temporary_block.insert({column.column->clone_resized(1), column.type, column.name});
        } else {
            have_converted_columns = true;
            temporary_block.insert(
                    {assert_cast<const ColumnConst*>(column.column.get())->get_data_column_ptr(),
                     column.type, column.name});
        }
    }

    /** When using default implementation for constants, the function requires at least one argument
      *  not in "arguments_to_remain_constants" set. Otherwise we get infinite recursion.
      */
    if (!have_converted_columns) {
        return Status::RuntimeError(
                fmt::format("Number of arguments for function {} doesn't match: the function "
                            "requires more arguments",
                            get_name()));
    }

    temporary_block.insert(block.get_by_position(result));

    ColumnNumbers temporary_argument_numbers(arguments_size);
    for (size_t i = 0; i < arguments_size; ++i) temporary_argument_numbers[i] = i;

    RETURN_IF_ERROR(execute_without_low_cardinality_columns(
            temporary_block, temporary_argument_numbers, arguments_size, temporary_block.rows(),
            dry_run));

    ColumnPtr result_column;
    /// extremely rare case, when we have function with completely const arguments
    /// but some of them produced by non is_deterministic function
    if (temporary_block.get_by_position(arguments_size).column->size() > 1)
        result_column = temporary_block.get_by_position(arguments_size).column->clone_resized(1);
    else
        result_column = temporary_block.get_by_position(arguments_size).column;

    block.get_by_position(result).column = ColumnConst::create(result_column, input_rows_count);
    *executed = true;
    return Status::OK();
}

Status PreparedFunctionImpl::default_implementation_for_nulls(Block& block,
                                                              const ColumnNumbers& args,
                                                              size_t result,
                                                              size_t input_rows_count, bool dry_run,
                                                              bool* executed) {
    *executed = false;
    if (args.empty() || !use_default_implementation_for_nulls()) return Status::OK();

    NullPresence null_presence = getNullPresense(block, args);

    if (null_presence.has_null_constant) {
        block.get_by_position(result).column =
                block.get_by_position(result).type->create_column_const(input_rows_count, Null());
        *executed = true;
        return Status::OK();
    }

    if (null_presence.has_nullable) {
        Block temporary_block = create_block_with_nested_columns(block, args, result);
        RETURN_IF_ERROR(execute_without_low_cardinality_columns(temporary_block, args, result,
                                                                temporary_block.rows(), dry_run));
        block.get_by_position(result).column =
                wrap_in_nullable(temporary_block.get_by_position(result).column, block, args,
                                 result, input_rows_count);
        *executed = true;
        return Status::OK();
    }
    *executed = false;
    return Status::OK();
}

Status PreparedFunctionImpl::execute_without_low_cardinality_columns(Block& block,
                                                                     const ColumnNumbers& args,
                                                                     size_t result,
                                                                     size_t input_rows_count,
                                                                     bool dry_run) {
    bool executed = false;
    RETURN_IF_ERROR(default_implementation_for_constant_arguments(
            block, args, result, input_rows_count, dry_run, &executed));
    if (executed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(default_implementation_for_nulls(block, args, result, input_rows_count, dry_run,
                                                     &executed));
    if (executed) {
        return Status::OK();
    }

    if (dry_run)
        return execute_impl_dry_run(block, args, result, input_rows_count);
    else
        return execute_impl(block, args, result, input_rows_count);
}

Status PreparedFunctionImpl::execute(Block& block, const ColumnNumbers& args, size_t result,
                                     size_t input_rows_count, bool dry_run) {
    if (use_default_implementation_for_low_cardinality_columns()) {
        auto& res = block.safe_get_by_position(result);
        Block block_without_low_cardinality = block.clone_without_columns();

        for (auto arg : args)
            block_without_low_cardinality.safe_get_by_position(arg).column =
                    block.safe_get_by_position(arg).column;

        {
            RETURN_IF_ERROR(execute_without_low_cardinality_columns(
                    block_without_low_cardinality, args, result, input_rows_count, dry_run));
            res.column = block_without_low_cardinality.safe_get_by_position(result).column;
        }
    } else
        execute_without_low_cardinality_columns(block, args, result, input_rows_count, dry_run);
    return Status::OK();
}

void FunctionBuilderImpl::check_number_of_arguments(size_t number_of_arguments) const {
    if (is_variadic()) return;

    size_t expected_number_of_arguments = get_number_of_arguments();

    CHECK_EQ(number_of_arguments, expected_number_of_arguments) << fmt::format(
            "Number of arguments for function {} doesn't match: passed {} , should be {}",
            get_name(), number_of_arguments, expected_number_of_arguments);
}

DataTypePtr FunctionBuilderImpl::get_return_type_without_low_cardinality(
        const ColumnsWithTypeAndName& arguments) const {
    check_number_of_arguments(arguments.size());

    if (!arguments.empty() && use_default_implementation_for_nulls()) {
        NullPresence null_presence = getNullPresense(arguments);

        if (null_presence.has_null_constant) {
            return make_nullable(std::make_shared<DataTypeNothing>());
        }
        if (null_presence.has_nullable) {
            ColumnNumbers numbers(arguments.size());
            for (size_t i = 0; i < arguments.size(); i++) {
                numbers[i] = i;
            }
            Block nested_block = create_block_with_nested_columns(Block(arguments), numbers);
            auto return_type = get_return_type_impl(
                    ColumnsWithTypeAndName(nested_block.begin(), nested_block.end()));
            return make_nullable(return_type);
        }
    }

    return get_return_type_impl(arguments);
}

DataTypePtr FunctionBuilderImpl::get_return_type(const ColumnsWithTypeAndName& arguments) const {
    if (use_default_implementation_for_low_cardinality_columns()) {
        size_t num_full_ordinary_columns = 0;

        ColumnsWithTypeAndName args_without_low_cardinality(arguments);

        for (ColumnWithTypeAndName& arg : args_without_low_cardinality) {
            bool is_const = arg.column && is_column_const(*arg.column);
            if (is_const)
                arg.column = assert_cast<const ColumnConst&>(*arg.column).remove_low_cardinality();
            if (!is_const) ++num_full_ordinary_columns;
        }

        auto type_without_low_cardinality =
                get_return_type_without_low_cardinality(args_without_low_cardinality);

        return type_without_low_cardinality;
    }

    return get_return_type_without_low_cardinality(arguments);
}
} // namespace doris::vectorized
