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

#include "function.h"
#include "function_helpers.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_fixed_length_object.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array.h"
#include "vec/common/volnitsky.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename Impl>
class FunctionMultiStringPosition : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionMultiStringPosition>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_constants() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        ColumnPtr haystack_ptr = block.get_by_position(arguments[0]).column;
        ColumnPtr needles_ptr = block.get_by_position(arguments[1]).column;

        const ColumnString* col_haystack_vector =
                check_and_get_column<ColumnString>(&*haystack_ptr);
        const ColumnConst* col_haystack_const =
                check_and_get_column_const<ColumnString>(&*haystack_ptr);

        const ColumnArray* col_needles_vector =
                check_and_get_column<ColumnArray>(needles_ptr.get());
        const ColumnConst* col_needles_const =
                check_and_get_column_const<ColumnArray>(needles_ptr.get());

        if (col_haystack_const && col_needles_vector)
            return Status::InvalidArgument(
                    "function '{}' doesn't support search with non-constant needles "
                    "in constant haystack",
                    name);

        using ResultType = typename Impl::ResultType;
        auto col_res = ColumnVector<ResultType>::create();
        auto col_offsets = ColumnArray::ColumnOffsets::create();

        auto& vec_res = col_res->get_data();
        auto& offsets_res = col_offsets->get_data();

        Status status;
        if (col_needles_const)
            status = Impl::vector_constant(
                    col_haystack_vector->get_chars(), col_haystack_vector->get_offsets(),
                    col_needles_const->get_value<Array>(), vec_res, offsets_res);
        else
            status = Impl::vector_vector(col_haystack_vector->get_chars(),
                                         col_haystack_vector->get_offsets(),
                                         col_needles_vector->get_data(),
                                         col_needles_vector->get_offsets(), vec_res, offsets_res);

        if (!status.ok()) return status;

        auto nullable_col =
                ColumnNullable::create(std::move(col_res), ColumnUInt8::create(col_res->size(), 0));
        block.get_by_position(result).column =
                ColumnArray::create(std::move(nullable_col), std::move(col_offsets));
        return status;
    }
};

template <typename Impl>
struct FunctionMultiSearchAllPositionsImpl {
    using ResultType = Int32;

    static constexpr auto name = "multi_search_all_positions";

    static Status vector_constant(const ColumnString::Chars& haystack_data,
                                  const ColumnString::Offsets& haystack_offsets,
                                  const Array& needles_arr, PaddedPODArray<Int32>& vec_res,
                                  PaddedPODArray<UInt64>& offsets_res) {
        if (needles_arr.size() > std::numeric_limits<UInt8>::max())
            return Status::InvalidArgument(
                    "number of arguments for function {} doesn't match: "
                    "passed {}, should be at most 255",
                    name, needles_arr.size());

        std::vector<StringRef> needles;
        needles.reserve(needles_arr.size());
        for (const auto& needle : needles_arr) needles.emplace_back(needle.get<StringRef>());

        auto res_callback = [](const UInt8* start, const UInt8* end) -> Int32 {
            return 1 + Impl::count_chars(reinterpret_cast<const char*>(start),
                                         reinterpret_cast<const char*>(end));
        };

        auto searcher = Impl::create_multi_searcher(needles);

        const size_t haystack_size = haystack_offsets.size();
        const size_t needles_size = needles.size();

        vec_res.resize(haystack_size * needles.size());
        offsets_res.resize(haystack_size);

        std::fill(vec_res.begin(), vec_res.end(), 0);

        while (searcher.hasMoreToSearch()) {
            size_t prev_haystack_offset = 0;
            for (size_t j = 0, from = 0; j < haystack_size; ++j, from += needles_size) {
                const auto* haystack = &haystack_data[prev_haystack_offset];
                const auto* haystack_end = haystack + haystack_offsets[j] - prev_haystack_offset;
                searcher.searchOneAll(haystack, haystack_end, &vec_res[from], res_callback);
                prev_haystack_offset = haystack_offsets[j];
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

        auto res_callback = [](const UInt8* start, const UInt8* end) -> Int32 {
            return 1 + Impl::count_chars(reinterpret_cast<const char*>(start),
                                         reinterpret_cast<const char*>(end));
        };

        offsets_res.reserve(haystack_offsets.size());

        auto& nested_column =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(needles_data)
                        ->get_nested_column();
        const ColumnString* needles_data_string = check_and_get_column<ColumnString>(nested_column);

        std::vector<StringRef> needles;
        for (size_t i = 0; i < haystack_offsets.size(); ++i) {
            needles.reserve(needles_offsets[i] - prev_needles_offset);

            for (size_t j = prev_needles_offset; j < needles_offsets[i]; ++j) {
                needles.emplace_back(needles_data_string->get_data_at(j));
            }

            const size_t needles_size = needles.size();
            if (needles_size > std::numeric_limits<UInt8>::max())
                return Status::InvalidArgument(
                        "number of arguments for function {} doesn't match: "
                        "passed {}, should be at most 255",
                        name, needles_size);

            vec_res.resize(vec_res.size() + needles_size);

            auto searcher = Impl::create_multi_searcher(needles);

            std::fill(vec_res.begin() + vec_res.size() - needles_size, vec_res.end(), 0);

            while (searcher.hasMoreToSearch()) {
                const auto* haystack = &haystack_data[prev_haystack_offset];
                const auto* haystack_end = haystack + haystack_offsets[i] - prev_haystack_offset;
                searcher.searchOneAll(haystack, haystack_end,
                                      &vec_res[vec_res.size() - needles_size], res_callback);
            }

            if (offsets_res.empty())
                offsets_res.push_back(needles_size);
            else
                offsets_res.push_back(offsets_res.back() + needles_size);

            prev_haystack_offset = haystack_offsets[i];
            prev_needles_offset = needles_offsets[i];
            needles.clear();
        }

        return Status::OK();
    }
};

struct MultiSearcherImpl {
    using MultiSearcher = MultiVolnitsky;

    static MultiSearcher create_multi_searcher(const std::vector<StringRef>& needles) {
        return MultiSearcher(needles);
    }

    static size_t count_chars(const char* begin, const char* end) { return end - begin; }
};

using FunctionMultiSearchAllPositions =
        FunctionMultiStringPosition<FunctionMultiSearchAllPositionsImpl<MultiSearcherImpl>>;

void register_function_multi_string_position(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMultiSearchAllPositions>();
}

} // namespace doris::vectorized
