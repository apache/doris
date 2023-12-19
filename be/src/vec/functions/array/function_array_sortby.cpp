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

#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class FunctionArraySortBy : public IFunction {
public:
    static constexpr auto name = "array_sortby";
    static FunctionPtr create() { return std::make_shared<FunctionArraySortBy>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(remove_nullable(arguments[0])))
                << "first argument for function: " << name << " should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr argument_columns[2] = {nullptr, nullptr};
        ColumnPtr argument_nullmap[2] = {nullptr, nullptr};
        for (int i = 0; i < 2; ++i) {
            argument_columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            if (auto* nullable = check_and_get_column<ColumnNullable>(*argument_columns[i])) {
                argument_nullmap[i] = nullable->get_null_map_column_ptr();
                argument_columns[i] = nullable->get_nested_column_ptr();
            }
        }

        const auto& src_column_array = assert_cast<const ColumnArray&>(*argument_columns[0]);
        const auto& key_column_array = assert_cast<const ColumnArray&>(*argument_columns[1]);

        const auto& src_offsets = src_column_array.get_offsets();
        const ColumnNullable& src_nested_nullable_column =
                assert_cast<const ColumnNullable&>(src_column_array.get_data());
        auto& src_nested_data_column = src_nested_nullable_column.get_nested_column();

        const auto& key_offsets = key_column_array.get_offsets();
        const ColumnNullable& key_nested_nullable_column =
                assert_cast<const ColumnNullable&>(key_column_array.get_data());

        auto result_data_column = src_nested_nullable_column.clone_empty();
        auto result_offset_column =
                src_column_array.get_offsets_column().clone_resized(input_rows_count);
        MutableColumnPtr result_nullmap = nullptr;
        const ColumnUInt8::Container* src_null_map_data = nullptr;
        if (argument_nullmap[0] != nullptr) {
            const auto& src_column_nullmap = assert_cast<const ColumnUInt8&>(*argument_nullmap[0]);
            result_nullmap = src_column_nullmap.clone_resized(input_rows_count);
            src_null_map_data = &(src_column_nullmap.get_data());
        }
        const ColumnUInt8::Container* key_null_map_data = nullptr;
        if (argument_nullmap[1] != nullptr) {
            const auto& key_column_nullmap = assert_cast<const ColumnUInt8&>(*argument_nullmap[1]);
            key_null_map_data = &(key_column_nullmap.get_data());
        }

        IColumn::Selector src_selector;
        src_selector.reserve(src_nested_data_column.size());
        IColumn::Permutation key_permutation(key_nested_nullable_column.size());
        for (size_t i = 0; i < key_nested_nullable_column.size(); ++i) {
            key_permutation[i] = i;
        }

        unsigned long null_step = 0;
        for (int row = 0; row < input_rows_count; ++row) {
            unsigned long cur_src_element_num = src_offsets[row] - src_offsets[row - 1];
            unsigned long cur_key_element_num = key_offsets[row] - key_offsets[row - 1];
            if (cur_src_element_num != cur_key_element_num) {
                if (key_null_map_data != nullptr && (*key_null_map_data)[row]) {
                    // deal with this case if one of row like: ([1,2,3], NULL) --->([1,2,3])
                    for (unsigned long pos = src_offsets[row - 1]; pos < src_offsets[row]; ++pos) {
                        src_selector.push_back(pos);
                    }
                    null_step = null_step + cur_src_element_num;
                    continue;
                } else if (src_null_map_data != nullptr && (*src_null_map_data)[row]) {
                    // deal with this case if one of row like: (NULL, [1,2,3]) --->(NULL)
                    null_step = null_step - cur_key_element_num;
                    continue;
                } else {
                    return Status::InternalError(
                            "in array sortby function, the input column nested column data rows "
                            "are not equal, the first size is {}, but with second size is {} at "
                            "row {}.",
                            src_offsets[row] - src_offsets[row - 1],
                            key_offsets[row] - key_offsets[row - 1], row);
                }
            }

            auto start = key_offsets[row - 1];
            auto end = key_offsets[row];
            std::sort(&key_permutation[start], &key_permutation[end],
                      Less(key_nested_nullable_column));

            for (unsigned long pos = start; pos < end; ++pos) {
                src_selector.push_back(key_permutation[pos] + null_step);
            }
        }
        src_nested_nullable_column.append_data_by_selector(result_data_column, src_selector);
        if (result_nullmap != nullptr) {
            block.replace_by_position(
                    result,
                    ColumnNullable::create(ColumnArray::create(std::move(result_data_column),
                                                               std::move(result_offset_column)),
                                           std::move(result_nullmap)));
        } else {
            block.replace_by_position(result, ColumnArray::create(std::move(result_data_column),
                                                                  std::move(result_offset_column)));
        }
        return Status::OK();
    }

    struct Less {
        const IColumn& column;

        explicit Less(const IColumn& column_) : column(column_) {}

        bool operator()(size_t lhs, size_t rhs) const {
            return column.compare_at(lhs, rhs, column, -1) < 0;
        }
    };
};

void register_function_array_sortby(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArraySortBy>();
}

} // namespace doris::vectorized
