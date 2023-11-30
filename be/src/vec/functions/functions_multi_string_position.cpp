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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsMultiStringPosition.h
// and modified by Doris

#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <cstdint>
#include <iterator>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "function.h"
#include "function_helpers.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/common/string_searcher.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

template <typename Impl>
class FunctionMultiStringPosition : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionMultiStringPosition>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto haystack_column = block.get_by_position(arguments[0]).column;
        auto haystack_ptr = haystack_column;

        auto needles_column = block.get_by_position(arguments[1]).column;
        auto needles_ptr = needles_column;

        bool haystack_nullable = false;
        bool needles_nullable = false;

        if (haystack_column->is_nullable()) {
            haystack_ptr = check_and_get_column<ColumnNullable>(haystack_column.get())
                                   ->get_nested_column_ptr();
            haystack_nullable = true;
        }

        if (needles_column->is_nullable()) {
            needles_ptr = check_and_get_column<ColumnNullable>(needles_column.get())
                                  ->get_nested_column_ptr();
            needles_nullable = true;
        }

        const ColumnString* col_haystack_vector =
                check_and_get_column<ColumnString>(&*haystack_ptr);
        const ColumnConst* col_haystack_const =
                check_and_get_column_const<ColumnString>(&*haystack_ptr);

        const ColumnArray* col_needles_vector =
                check_and_get_column<ColumnArray>(needles_ptr.get());
        const ColumnConst* col_needles_const =
                check_and_get_column_const<ColumnArray>(needles_ptr.get());

        if (col_haystack_const && col_needles_vector) {
            return Status::InvalidArgument(
                    "function '{}' doesn't support search with non-constant needles "
                    "in constant haystack",
                    name);
        }

        using ResultType = typename Impl::ResultType;
        auto col_res = ColumnVector<ResultType>::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        auto& vec_res = col_res->get_data();
        auto& offsets_res = col_offsets->get_data();

        Status status;
        if (col_needles_const) {
            status = Impl::vector_constant(
                    col_haystack_vector->get_chars(), col_haystack_vector->get_offsets(),
                    col_needles_const->get_value<Array>(), vec_res, offsets_res);
        } else {
            status = Impl::vector_vector(col_haystack_vector->get_chars(),
                                         col_haystack_vector->get_offsets(),
                                         col_needles_vector->get_data(),
                                         col_needles_vector->get_offsets(), vec_res, offsets_res);
        }

        if (!status.ok()) {
            return status;
        }

        if (haystack_nullable) {
            auto column_nullable = check_and_get_column<ColumnNullable>(haystack_column.get());
            auto& null_map = column_nullable->get_null_map_data();
            for (size_t i = 0; i != input_rows_count; ++i) {
                if (null_map[i] == 1) {
                    for (size_t offset = offsets_res[i - 1]; offset != offsets_res[i]; ++offset) {
                        vec_res[offset] = 0;
                    }
                }
            }
        }

        if (needles_nullable) {
            auto column_nullable = check_and_get_column<ColumnNullable>(needles_column.get());
            auto& null_map = column_nullable->get_null_map_data();
            for (size_t i = 0; i != input_rows_count; ++i) {
                if (null_map[i] == 1) {
                    for (size_t offset = offsets_res[i - 1]; offset != offsets_res[i]; ++offset) {
                        vec_res[offset] = 0;
                    }
                }
            }
        }

        auto nullable_col =
                ColumnNullable::create(std::move(col_res), ColumnUInt8::create(col_res->size(), 0));
        block.get_by_position(result).column =
                ColumnArray::create(std::move(nullable_col), std::move(col_offsets));
        return status;
    }
};

struct FunctionMultiSearchAllPositionsImpl {
public:
    using ResultType = Int32;
    using SingleSearcher = ASCIICaseSensitiveStringSearcher;
    static constexpr auto name = "multi_search_all_positions";

    static Status vector_constant(const ColumnString::Chars& haystack_data,
                                  const ColumnString::Offsets& haystack_offsets,
                                  const Array& needles_arr, PaddedPODArray<Int32>& vec_res,
                                  PaddedPODArray<UInt64>& offsets_res) {
        if (needles_arr.size() > std::numeric_limits<UInt8>::max()) {
            return Status::InvalidArgument(
                    "number of arguments for function {} doesn't match: "
                    "passed {}, should be at most 255",
                    name, needles_arr.size());
        }

        const size_t needles_size = needles_arr.size();
        std::vector<SingleSearcher> searchers;
        searchers.reserve(needles_size);
        for (const auto& needle : needles_arr) {
            if (needle.get_type() != Field::Types::String) {
                return Status::InvalidArgument("invalid type of needle {}", needle.get_type_name());
            }
            searchers.emplace_back(needle.get<StringRef>().data, needle.get<StringRef>().size);
        }

        const size_t haystack_size = haystack_offsets.size();
        vec_res.resize(haystack_size * needles_size);
        offsets_res.resize(haystack_size);

        std::fill(vec_res.begin(), vec_res.end(), 0);

        // we traverse to generator answer by Vector's slot of ColumnVector, not by Vector.
        // TODO: check if the order of loop is best. The large data may make us writing across the line which size out of L2 cache.
        for (size_t ans_slot_in_row = 0; ans_slot_in_row < searchers.size(); ans_slot_in_row++) {
            //  is i.e. answer slot index in one Vector(row) of answer
            auto& searcher = searchers[ans_slot_in_row];
            size_t prev_haystack_offset = 0;

            for (size_t haystack_index = 0, res_index = ans_slot_in_row;
                 haystack_index < haystack_size; ++haystack_index, res_index += needles_size) {
                const auto* haystack = &haystack_data[prev_haystack_offset];
                const auto* haystack_end =
                        haystack - prev_haystack_offset + haystack_offsets[haystack_index];

                auto ans_now = searcher.search(haystack, haystack_end);
                vec_res[res_index] =
                        ans_now >= haystack_end ? 0 : std::distance(haystack, ans_now) + 1;
                prev_haystack_offset = haystack_offsets[haystack_index];
            }
        }

        size_t accum = needles_size;
        for (size_t i = 0; i < haystack_size; ++i) {
            offsets_res[i] = accum;
            accum += needles_size;
        }

        return Status::OK();
    }

    static Status vector_vector(const ColumnString::Chars& haystack_data,
                                const ColumnString::Offsets& haystack_offsets,
                                const IColumn& needles_data,
                                const ColumnArray::Offsets64& needles_offsets,
                                PaddedPODArray<Int32>& vec_res,
                                PaddedPODArray<UInt64>& offsets_res) {
        size_t prev_haystack_offset = 0;
        size_t prev_needles_offset = 0;

        offsets_res.reserve(haystack_data.size());
        uint64_t offset_now = 0;

        auto& nested_column =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(needles_data)
                        ->get_nested_column();
        const ColumnString* needles_data_string = check_and_get_column<ColumnString>(nested_column);

        std::vector<StringRef> needles_for_row;
        // haystack first, row by row.
        for (size_t haystack_index = 0; haystack_index < haystack_offsets.size();
             ++haystack_index) {
            // get haystack for this row.
            const auto* haystack = &haystack_data[prev_haystack_offset];
            const auto* haystack_end =
                    haystack - prev_haystack_offset + haystack_offsets[haystack_index];

            // build needles for this row.
            needles_for_row.reserve(needles_offsets[haystack_index] - prev_needles_offset);
            for (size_t j = prev_needles_offset; j < needles_offsets[haystack_index]; ++j) {
                needles_for_row.emplace_back(needles_data_string->get_data_at(j));
            }
            const size_t needles_row_size = needles_for_row.size();
            if (needles_row_size > std::numeric_limits<UInt8>::max()) {
                return Status::InvalidArgument(
                        "number of arguments for function {} doesn't match: "
                        "passed {}, should be at most 255",
                        name, needles_row_size);
            }

            // each searcher search for one needle.
            std::vector<SingleSearcher> searchers;
            searchers.clear();
            searchers.reserve(needles_row_size);
            for (auto needle : needles_for_row) {
                searchers.emplace_back(needle.data, needle.size);
            }

            // search for first so that the ans's size is constant for each row.
            auto ans_row_begin = vec_res.size();
            vec_res.resize(vec_res.size() + needles_row_size);
            offset_now += searchers.size();
            offsets_res.emplace_back(offset_now);

            //for now haystack, apply needle to search, generator answer by order.
            for (size_t ans_slot_in_row = 0; ans_slot_in_row < searchers.size();
                 ans_slot_in_row++) {
                //  is i.e. answer slot index in one Vector(row) of answer
                auto& searcher = searchers[ans_slot_in_row];

                auto ans_now = searcher.search(haystack, haystack_end);
                vec_res[ans_row_begin + ans_slot_in_row] =
                        ans_now >= haystack_end ? 0 : std::distance(haystack, ans_now) + 1;
            }

            prev_haystack_offset = haystack_offsets[haystack_index];
            prev_needles_offset = needles_offsets[haystack_index];
            needles_for_row.clear();
        }

        return Status::OK();
    }
};

using FunctionMultiSearchAllPositions =
        FunctionMultiStringPosition<FunctionMultiSearchAllPositionsImpl>;

void register_function_multi_string_position(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMultiSearchAllPositions>();
}

} // namespace doris::vectorized
