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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsMultiStringSearch.h
// and modified by Doris

#include <hs/hs_common.h>
#include <hs/hs_runtime.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "common/status.h"
#include "function.h"
#include "function_helpers.h"
#include "regexps.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

template <typename Impl>
class FunctionsMultiStringSearch : public IFunction {
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create() { return std::make_shared<FunctionsMultiStringSearch>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return Impl::get_return_type();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto haystack_column = block.get_by_position(arguments[0]).column;
        auto needles_column = block.get_by_position(arguments[1]).column;

        bool haystack_nullable = false;
        bool needles_nullable = false;

        if (haystack_column->is_nullable()) {
            haystack_nullable = true;
        }

        if (needles_column->is_nullable()) {
            needles_nullable = true;
        }

        auto haystack_ptr = remove_nullable(haystack_column);
        auto needles_ptr = remove_nullable(needles_column);

        const ColumnString* col_haystack_vector =
                check_and_get_column<ColumnString>(&*haystack_ptr);
        const ColumnConst* col_haystack_const =
                check_and_get_column_const<ColumnString>(&*haystack_ptr);

        const ColumnArray* col_needles_vector =
                check_and_get_column<ColumnArray>(needles_ptr.get());
        const ColumnConst* col_needles_const =
                check_and_get_column_const<ColumnArray>(needles_ptr.get());

        if (!col_needles_const && !col_needles_vector)
            return Status::InvalidArgument(
                    "function '{}' encountered unsupported needles column, found {}", name,
                    needles_column->get_name());

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
        if (col_needles_const) {
            status = Impl::vector_constant(
                    col_haystack_vector->get_chars(), col_haystack_vector->get_offsets(),
                    col_needles_const->get_value<Array>(), vec_res, offsets_res, allow_hyperscan_,
                    max_hyperscan_regexp_length_, max_hyperscan_regexp_total_length_);
        } else {
            status = Impl::vector_vector(
                    col_haystack_vector->get_chars(), col_haystack_vector->get_offsets(),
                    col_needles_vector->get_data(), col_needles_vector->get_offsets(), vec_res,
                    offsets_res, allow_hyperscan_, max_hyperscan_regexp_length_,
                    max_hyperscan_regexp_total_length_);
        }

        if (!status.ok()) {
            return status;
        }

        if (haystack_nullable) {
            auto column_nullable = check_and_get_column<ColumnNullable>(haystack_column.get());
            auto& null_map = column_nullable->get_null_map_data();
            for (size_t i = 0; i != input_rows_count; ++i) {
                if (null_map[i] == 1) {
                    vec_res[i] = 0;
                }
            }
        }

        if (needles_nullable) {
            auto column_nullable = check_and_get_column<ColumnNullable>(needles_column.get());
            auto& null_map = column_nullable->get_null_map_data();
            for (size_t i = 0; i != input_rows_count; ++i) {
                if (null_map[i] == 1) {
                    vec_res[i] = 0;
                }
            }
        }

        block.replace_by_position(result, std::move(col_res));

        return status;
    }

private:
    const bool allow_hyperscan_ = true;
    const size_t max_hyperscan_regexp_length_ = 0;       // not limited
    const size_t max_hyperscan_regexp_total_length_ = 0; // not limited
};

/// For more readable instantiations of MultiMatchAnyImpl<>
struct MultiMatchTraits {
    enum class Find { Any, AnyIndex };
};

template <typename ResultType_, MultiMatchTraits::Find Find, bool WithEditDistance>
struct FunctionMultiMatchAnyImpl {
    using ResultType = ResultType_;

    static constexpr bool FindAny = (Find == MultiMatchTraits::Find::Any);
    static constexpr bool FindAnyIndex = (Find == MultiMatchTraits::Find::AnyIndex);

    static constexpr auto name = "multi_match_any";

    static auto get_return_type() { return std::make_shared<DataTypeNumber<ResultType>>(); }

    static Status vector_constant(const ColumnString::Chars& haystack_data,
                                  const ColumnString::Offsets& haystack_offsets,
                                  const Array& needles_arr, PaddedPODArray<ResultType>& res,
                                  PaddedPODArray<UInt64>& offsets, bool allow_hyperscan,
                                  size_t max_hyperscan_regexp_length,
                                  size_t max_hyperscan_regexp_total_length) {
        if (!allow_hyperscan) return Status::InvalidArgument("Hyperscan functions are disabled");

        std::vector<StringRef> needles;
        needles.reserve(needles_arr.size());
        for (const auto& needle : needles_arr) needles.emplace_back(needle.get<StringRef>());

        res.resize(haystack_offsets.size());

        if (needles_arr.empty()) {
            std::fill(res.begin(), res.end(), 0);
            return Status::OK();
        }

        multiregexps::DeferredConstructedRegexpsPtr deferred_constructed_regexps =
                multiregexps::getOrSet</*SaveIndices*/ FindAnyIndex, WithEditDistance>(
                        needles, std::nullopt);
        multiregexps::Regexps* regexps = deferred_constructed_regexps->get();

        hs_scratch_t* scratch = nullptr;
        hs_error_t err = hs_clone_scratch(regexps->getScratch(), &scratch);

        if (err != HS_SUCCESS)
            return Status::InternalError("could not clone scratch space for vectorscan");

        multiregexps::ScratchPtr smart_scratch(scratch);

        auto on_match = []([[maybe_unused]] unsigned int id,
                           unsigned long long /* from */, // NOLINT
                           unsigned long long /* to */,   // NOLINT
                           unsigned int /* flags */, void* context) -> int {
            if constexpr (FindAnyIndex)
                *reinterpret_cast<ResultType*>(context) = id;
            else if constexpr (FindAny)
                *reinterpret_cast<ResultType*>(context) = 1;
            /// Once we hit the callback, there is no need to search for others.
            return 1;
        };
        const size_t haystack_offsets_size = haystack_offsets.size();
        UInt64 offset = 0;
        for (size_t i = 0; i < haystack_offsets_size; ++i) {
            UInt64 length = haystack_offsets[i] - offset;
            /// vectorscan restriction.
            if (length > std::numeric_limits<UInt32>::max())
                return Status::InternalError("too long string to search");
            /// zero the result, scan, check, update the offset.
            res[i] = 0;
            err = hs_scan(regexps->getDB(),
                          reinterpret_cast<const char*>(haystack_data.data()) + offset,
                          static_cast<unsigned>(length), 0, smart_scratch.get(), on_match, &res[i]);
            if (err != HS_SUCCESS && err != HS_SCAN_TERMINATED)
                return Status::InternalError("failed to scan with vectorscan");
            offset = haystack_offsets[i];
        }

        return Status::OK();
    }

    static Status vector_vector(const ColumnString::Chars& haystack_data,
                                const ColumnString::Offsets& haystack_offsets,
                                const IColumn& needles_data,
                                const ColumnArray::Offsets64& needles_offsets,
                                PaddedPODArray<ResultType>& res, PaddedPODArray<UInt64>& offsets,
                                bool allow_hyperscan, size_t max_hyperscan_regexp_length,
                                size_t max_hyperscan_regexp_total_length) {
        if (!allow_hyperscan) return Status::InvalidArgument("Hyperscan functions are disabled");

        res.resize(haystack_offsets.size());

        size_t prev_haystack_offset = 0;
        size_t prev_needles_offset = 0;

        auto& nested_column =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(needles_data)
                        ->get_nested_column();
        const ColumnString* needles_data_string = check_and_get_column<ColumnString>(nested_column);

        if (!needles_data_string) {
            return Status::InvalidArgument("needles should be string");
        }

        std::vector<StringRef> needles;
        for (size_t i = 0; i < haystack_offsets.size(); ++i) {
            needles.reserve(needles_offsets[i] - prev_needles_offset);

            for (size_t j = prev_needles_offset; j < needles_offsets[i]; ++j) {
                needles.emplace_back(needles_data_string->get_data_at(j));
            }
            if (needles.empty()) {
                res[i] = 0;
                prev_haystack_offset = haystack_offsets[i];
                prev_needles_offset = needles_offsets[i];
                continue;
            }

            multiregexps::DeferredConstructedRegexpsPtr deferred_constructed_regexps =
                    multiregexps::getOrSet</*SaveIndices*/ FindAnyIndex, WithEditDistance>(
                            needles, std::nullopt);
            multiregexps::Regexps* regexps = deferred_constructed_regexps->get();

            hs_scratch_t* scratch = nullptr;
            hs_error_t err = hs_clone_scratch(regexps->getScratch(), &scratch);

            if (err != HS_SUCCESS)
                return Status::InternalError("could not clone scratch space for vectorscan");

            multiregexps::ScratchPtr smart_scratch(scratch);

            auto on_match = []([[maybe_unused]] unsigned int id,
                               unsigned long long /* from */, // NOLINT
                               unsigned long long /* to */,   // NOLINT
                               unsigned int /* flags */, void* context) -> int {
                if constexpr (FindAnyIndex)
                    *reinterpret_cast<ResultType*>(context) = id;
                else if constexpr (FindAny)
                    *reinterpret_cast<ResultType*>(context) = 1;
                /// Once we hit the callback, there is no need to search for others.
                return 1;
            };

            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset;

            /// vectorscan restriction.
            if (cur_haystack_length > std::numeric_limits<UInt32>::max())
                return Status::InternalError("too long string to search");

            /// zero the result, scan, check, update the offset.
            res[i] = 0;
            err = hs_scan(
                    regexps->getDB(),
                    reinterpret_cast<const char*>(haystack_data.data()) + prev_haystack_offset,
                    static_cast<unsigned>(cur_haystack_length), 0, smart_scratch.get(), on_match,
                    &res[i]);
            if (err != HS_SUCCESS && err != HS_SCAN_TERMINATED)
                return Status::InternalError("failed to scan with vectorscan");

            prev_haystack_offset = haystack_offsets[i];
            prev_needles_offset = needles_offsets[i];
            needles.clear();
        }

        return Status::OK();
    }
};

using FunctionMultiMatchAny = FunctionsMultiStringSearch<
        FunctionMultiMatchAnyImpl<Int8, MultiMatchTraits::Find::Any, /*WithEditDistance*/ false>>;

void register_function_multi_string_search(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionMultiMatchAny>();
}

} // namespace doris::vectorized
