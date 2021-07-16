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

#include "vec/functions/function_helpers.h"

#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

static Block create_block_with_nested_columns_impl(const Block& block,
                                                   const std::unordered_set<size_t>& args) {
    Block res;
    size_t columns = block.columns();

    for (size_t i = 0; i < columns; ++i) {
        const auto& col = block.get_by_position(i);

        if (args.count(i) && col.type->is_nullable()) {
            const DataTypePtr& nested_type =
                    static_cast<const DataTypeNullable&>(*col.type).get_nested_type();

            if (!col.column) {
                res.insert({nullptr, nested_type, col.name});
            } else if (auto* nullable = check_and_get_column<ColumnNullable>(*col.column)) {
                const auto& nested_col = nullable->get_nested_column_ptr();
                res.insert({nested_col, nested_type, col.name});
            } else if (auto* const_column = check_and_get_column<ColumnConst>(*col.column)) {
                const auto& nested_col =
                        check_and_get_column<ColumnNullable>(const_column->get_data_column())
                                ->get_nested_column_ptr();
                res.insert({ColumnConst::create(nested_col, col.column->size()), nested_type,
                            col.name});
            } else {
                LOG(FATAL) << "Illegal column for DataTypeNullable";
            }
        } else
            res.insert(col);
    }

    return res;
}

Block create_block_with_nested_columns(const Block& block, const ColumnNumbers& args) {
    std::unordered_set<size_t> args_set(args.begin(), args.end());
    return create_block_with_nested_columns_impl(block, args_set);
}

Block create_block_with_nested_columns(const Block& block, const ColumnNumbers& args,
                                       size_t result) {
    std::unordered_set<size_t> args_set(args.begin(), args.end());
    args_set.insert(result);
    return create_block_with_nested_columns_impl(block, args_set);
}

void validate_argument_type(const IFunction& func, const DataTypes& arguments,
                            size_t argument_index, bool (*validator_func)(const IDataType&),
                            const char* expected_type_description) {
    if (arguments.size() <= argument_index) {
        LOG(FATAL) << "Incorrect number of arguments of function " << func.get_name();
    }

    const auto& argument = arguments[argument_index];
    if (validator_func(*argument) == false) {
        LOG(FATAL) << fmt::format("Illegal type {} of {} argument of function {} expected {}",
                                  argument->get_name(), argument_index, func.get_name(),
                                  expected_type_description);
    }
}

} // namespace doris::vectorized
