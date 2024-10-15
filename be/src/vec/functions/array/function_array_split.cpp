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
//
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arraySplit.cpp
// and modified by Doris

#include <cstddef>
#include <memory>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

template <bool reverse>
class FunctionArraySplit : public IFunction {
public:
    static constexpr auto name = reverse ? "array_reverse_split" : "array_split";
    static FunctionPtr create() { return std::make_shared<FunctionArraySplit>(); }
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeArray>(make_nullable(arguments[0]));
    };

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        // <Nullable>(Array(<Nullable>(Int)))
        auto src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto spliter_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        // only change its split(i.e. offsets)
        const auto& src_data = assert_cast<const ColumnArray&>(*src_column).get_data_ptr();
        const auto& src_offsets = assert_cast<const ColumnArray&>(*src_column).get_offsets();

        auto split_col = assert_cast<const ColumnArray*>(spliter_column.get())->get_data_ptr();
        const auto& split_offsets = assert_cast<const ColumnArray&>(*spliter_column)
                                            .get_offsets(); // for check uneven array

        const NullMap* null_map = nullptr;
        if (split_col->is_nullable()) {
            if (split_col->has_null()) {
                null_map =
                        &assert_cast<const ColumnNullable*>(split_col.get())->get_null_map_data();
            }
            split_col =
                    assert_cast<const ColumnNullable*>(split_col.get())->get_nested_column_ptr();
        }

        const IColumn::Filter& cut = assert_cast<const ColumnBool*>(split_col.get())->get_data();

        auto col_offsets_inner = ColumnArray::ColumnOffsets::create();
        auto col_offsets_outer = ColumnArray::ColumnOffsets::create();
        auto& offsets_inner = col_offsets_inner->get_data();
        auto& offsets_outer = col_offsets_outer->get_data();
        offsets_inner.reserve(src_offsets.size()); // assume the actual size to be equal or larger
        offsets_outer.reserve(src_offsets.size());

        if (null_map != nullptr) {
            RETURN_IF_ERROR(do_loop<true>(src_offsets, split_offsets, cut, null_map, offsets_inner,
                                          offsets_outer));
        } else {
            RETURN_IF_ERROR(do_loop<false>(src_offsets, split_offsets, cut, null_map, offsets_inner,
                                           offsets_outer));
        }

        auto inner_result = ColumnArray::create(src_data, std::move(col_offsets_inner));
        auto outer_result = ColumnArray::create(
                ColumnNullable::create(inner_result, ColumnUInt8::create(inner_result->size(), 0)),
                std::move(col_offsets_outer));
        block.replace_by_position(result, outer_result);
        return Status::OK();
    }

    template <bool CONSIDER_NULL>
    static Status do_loop(const IColumn::Offsets64& src_offsets,
                          const IColumn::Offsets64& split_offsets, const IColumn::Filter& cut,
                          const NullMap* null_map, PaddedPODArray<IColumn::Offset64>& offsets_inner,
                          PaddedPODArray<IColumn::Offset64>& offsets_outer) {
        size_t pos = 0;
        for (auto i = 0; i < src_offsets.size(); i++) { // per cells
            auto in_offset = src_offsets[i];
            auto sp_offset = split_offsets[i];
            if (in_offset != sp_offset) [[unlikely]] {
                return Status::InvalidArgument("function {} has uneven arguments on row {}", name,
                                               i);
            }

            // [1,2,3,4,5]
            if (pos < in_offset) { // values in a cell
                pos += !reverse;
                for (; pos < in_offset - reverse; ++pos) {
                    if constexpr (CONSIDER_NULL) {
                        if (cut[pos] && !(*null_map)[pos]) {
                            offsets_inner.push_back(pos + reverse); // cut a array [1,2,3]
                        }
                    } else {
                        if (cut[pos]) {
                            offsets_inner.push_back(pos + reverse); // cut a array [1,2,3]
                        }
                    }
                }
                pos += reverse;
                // put the tail offset, always last.
                offsets_inner.push_back(pos); // put [4,5]
            }

            offsets_outer.push_back(offsets_inner.size());
        }
        return Status::OK();
    }
};

void register_function_array_splits(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArraySplit<true>>();
    factory.register_function<FunctionArraySplit<false>>();
}
} // namespace doris::vectorized
