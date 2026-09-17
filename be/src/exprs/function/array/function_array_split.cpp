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
#include <variant>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/types.h"
#include "exec/common/template_helpers.hpp"
#include "exec/common/util.hpp"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {
template <bool reverse>
class FunctionArraySplit : public IFunction {
public:
    static constexpr auto name = reverse ? "array_reverse_split" : "array_split";
    static FunctionPtr create() { return std::make_shared<FunctionArraySplit>(); }
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        auto result_type =
                std::make_shared<DataTypeArray>(make_nullable(remove_nullable(arguments[0])));
        return have_nullable(arguments) ? make_nullable(result_type) : result_type;
    };

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        ColumnUInt8::MutablePtr result_null_map;
        ColumnUInt8::Container* result_null_map_data = nullptr;
        if (block.get_by_position(result).type->is_nullable()) {
            result_null_map = ColumnUInt8::create(input_rows_count, 0);
            result_null_map_data = &result_null_map->get_data();
        }
        auto unwrap_nullable_array = [&](const ColumnPtr& input_column) {
            auto column = input_column->convert_to_full_column_if_const();
            if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
                VectorizedUtils::update_null_map(*result_null_map_data,
                                                 nullable->get_null_map_data());
                column = nullable->get_nested_column_ptr();
            }
            return column;
        };

        auto src_column = unwrap_nullable_array(block.get_by_position(arguments[0]).column);
        auto spliter_column = unwrap_nullable_array(block.get_by_position(arguments[1]).column);

        // only change its split(i.e. offsets)
        const auto& src_data = assert_cast<const ColumnArray&>(*src_column).get_data_ptr();
        const auto& src_offsets = assert_cast<const ColumnArray&>(*src_column).get_offsets();

        auto split_col = assert_cast<const ColumnArray*>(spliter_column.get())->get_data_ptr();
        const auto& split_offsets = assert_cast<const ColumnArray&>(*spliter_column)
                                            .get_offsets(); // for check uneven array

        const NullMap* split_element_null_map = nullptr;
        if (const auto* nullable_split_col =
                    check_and_get_column<ColumnNullable>(split_col.get())) {
            if (split_col->has_null()) {
                split_element_null_map = &nullable_split_col->get_null_map_data();
            }
            split_col = nullable_split_col->get_nested_column_ptr();
        }

        const IColumn::Filter& cut = assert_cast<const ColumnBool*>(split_col.get())->get_data();

        auto col_offsets_inner = ColumnArray::ColumnOffsets::create();
        auto col_offsets_outer = ColumnArray::ColumnOffsets::create();
        auto& offsets_inner = col_offsets_inner->get_data();
        auto& offsets_outer = col_offsets_outer->get_data();
        offsets_inner.reserve(src_offsets.size()); // assume the actual size to be equal or larger
        offsets_outer.reserve(src_offsets.size());

        RETURN_IF_ERROR(std::visit(
                [&](auto consider_element_null, auto consider_outer_null) {
                    return do_loop<consider_element_null, consider_outer_null>(
                            src_offsets, split_offsets, cut, split_element_null_map,
                            result_null_map_data, offsets_inner, offsets_outer);
                },
                make_bool_variant(split_element_null_map != nullptr),
                make_bool_variant(result_null_map_data != nullptr)));

        auto inner_result = ColumnArray::create(src_data, std::move(col_offsets_inner));
        ColumnPtr result_column = ColumnArray::create(
                ColumnNullable::create(std::move(inner_result),
                                       ColumnUInt8::create(inner_result->size(), 0)),
                std::move(col_offsets_outer));
        if (block.get_by_position(result).type->is_nullable()) {
            result_column =
                    ColumnNullable::create(std::move(result_column), std::move(result_null_map));
        }
        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }

    template <bool CONSIDER_ELEMENT_NULL, bool CONSIDER_OUTER_NULL>
    static Status do_loop(const IColumn::Offsets64& src_offsets,
                          const IColumn::Offsets64& split_offsets, const IColumn::Filter& cut,
                          const NullMap* split_element_null_map, const NullMap* result_null_map,
                          PaddedPODArray<IColumn::Offset64>& offsets_inner,
                          PaddedPODArray<IColumn::Offset64>& offsets_outer) {
        size_t src_begin = 0;
        size_t split_begin = 0;
        for (auto i = 0; i < src_offsets.size(); i++) { // per cells
            const size_t src_end = src_offsets[i];
            const size_t split_end = split_offsets[i];

            if constexpr (CONSIDER_OUTER_NULL) {
                if ((*result_null_map)[i]) {
                    // Preserve the hidden source payload as one segment so later rows remain aligned.
                    if (src_begin < src_end) {
                        offsets_inner.push_back(src_end);
                    }
                    offsets_outer.push_back(offsets_inner.size());
                    src_begin = src_end;
                    split_begin = split_end;
                    continue;
                }
            }

            if (src_end - src_begin != split_end - split_begin) [[unlikely]] {
                return Status::InvalidArgument("function {} has uneven arguments on row {}", name,
                                               i);
            }

            // [1,2,3,4,5]
            if (src_begin < src_end) { // values in a cell
                size_t src_pos = src_begin + !reverse;
                size_t split_pos = split_begin + !reverse;
                for (; src_pos < src_end - reverse; ++src_pos, ++split_pos) {
                    if constexpr (CONSIDER_ELEMENT_NULL) {
                        if (cut[split_pos] && !(*split_element_null_map)[split_pos]) {
                            offsets_inner.push_back(src_pos + reverse); // cut a array [1,2,3]
                        }
                    } else {
                        if (cut[split_pos]) {
                            offsets_inner.push_back(src_pos + reverse); // cut a array [1,2,3]
                        }
                    }
                }
                // put the tail offset, always last.
                offsets_inner.push_back(src_end); // put [4,5]
            }

            offsets_outer.push_back(offsets_inner.size());
            src_begin = src_end;
            split_begin = split_end;
        }
        return Status::OK();
    }
};

void register_function_array_splits(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArraySplit<true>>();
    factory.register_function<FunctionArraySplit<false>>();
}
} // namespace doris
