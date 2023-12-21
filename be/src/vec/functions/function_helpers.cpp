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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionHelpers.cpp
// and modified by Doris

#include "vec/functions/function_helpers.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <vector>

#include "common/consts.h"
#include "util/string_util.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

std::tuple<Block, ColumnNumbers> create_block_with_nested_columns(
        const Block& block, const ColumnNumbers& args, const bool need_check_same,
        bool need_replace_null_data_to_default) {
    Block res;
    ColumnNumbers res_args(args.size());
    res.reserve(args.size() + 1);

    // only build temp block by args column, if args[i] == args[j]
    // just keep one
    for (size_t i = 0; i < args.size(); ++i) {
        bool is_in_res = false;
        size_t pre_loc = 0;

        if (need_check_same) {
            for (int j = 0; j < i; ++j) {
                if (args[j] == args[i]) {
                    is_in_res = true;
                    pre_loc = res_args[j];
                    break;
                }
            }
        }

        if (!is_in_res) {
            const auto& col = block.get_by_position(args[i]);
            if (col.type->is_nullable()) {
                const DataTypePtr& nested_type =
                        static_cast<const DataTypeNullable&>(*col.type).get_nested_type();

                if (!col.column) {
                    res.insert({nullptr, nested_type, col.name});
                } else if (const auto* nullable =
                                   check_and_get_column<ColumnNullable>(*col.column)) {
                    if (need_replace_null_data_to_default) {
                        const auto& null_map = nullable->get_null_map_data();
                        const auto nested_col = nullable->get_nested_column_ptr();
                        // only need to mutate nested column, avoid to copy nullmap
                        auto mutable_nested_col = (*std::move(nested_col)).mutate();
                        mutable_nested_col->replace_column_null_data(null_map.data());

                        res.insert({std::move(mutable_nested_col), nested_type, col.name});
                    } else {
                        const auto& nested_col = nullable->get_nested_column_ptr();
                        res.insert({nested_col, nested_type, col.name});
                    }
                } else if (const auto* const_column =
                                   check_and_get_column<ColumnConst>(*col.column)) {
                    const auto& nested_col =
                            check_and_get_column<ColumnNullable>(const_column->get_data_column())
                                    ->get_nested_column_ptr();
                    res.insert({ColumnConst::create(nested_col, col.column->size()), nested_type,
                                col.name});
                } else {
                    LOG(FATAL) << "Illegal column= " << col.column->get_name()
                               << " for DataTypeNullable";
                }
            } else {
                res.insert(col);
            }

            res_args[i] = res.columns() - 1;
        } else {
            res_args[i] = pre_loc;
        }
    }

    // TODO: only support match function, rethink the logic
    for (const auto& ctn : block) {
        if (ctn.name.size() > BeConsts::BLOCK_TEMP_COLUMN_PREFIX.size() &&
            starts_with(ctn.name, BeConsts::BLOCK_TEMP_COLUMN_PREFIX)) {
            res.insert(ctn);
        }
    }

    return {std::move(res), std::move(res_args)};
}

std::tuple<Block, ColumnNumbers, size_t> create_block_with_nested_columns(
        const Block& block, const ColumnNumbers& args, size_t result,
        bool need_replace_null_data_to_default) {
    auto [res, res_args] =
            create_block_with_nested_columns(block, args, true, need_replace_null_data_to_default);
    // insert result column in temp block
    res.insert(block.get_by_position(result));
    return {std::move(res), std::move(res_args), res.columns() - 1};
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

const ColumnConst* check_and_get_column_const_string_or_fixedstring(const IColumn* column) {
    if (!is_column_const(*column)) return {};

    const ColumnConst* res = assert_cast<const ColumnConst*>(column);

    if (check_column<ColumnString>(&res->get_data_column())) return res;

    return {};
}

} // namespace doris::vectorized
