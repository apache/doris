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

#pragma once

#include <hs/hs_runtime.h>

#include <algorithm>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include "common/status.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "core/pod_array_fwd.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/regexps.h"

namespace doris {

/// For more readable instantiations of MultiMatchAnyImpl<>
struct MultiMatchTraits {
    enum class Find { Any, AnyIndex };
};

template <PrimitiveType PType, MultiMatchTraits::Find Find, bool WithEditDistance>
struct FunctionMultiMatchAnyImpl {
    using ResultType = typename PrimitiveTypeTraits<PType>::CppType;
    static constexpr PrimitiveType ResultPType = PType;

    static constexpr bool FindAny = (Find == MultiMatchTraits::Find::Any);
    static constexpr bool FindAnyIndex = (Find == MultiMatchTraits::Find::AnyIndex);

    static constexpr auto name = "multi_match_any";

    static auto get_return_type() {
        return std::make_shared<typename PrimitiveTypeTraits<PType>::DataType>();
    }

    /**
     * Prepares the regular expressions and scratch space for Hyperscan.
     *
     * This function takes a vector of needles (substrings to search for) and initializes
     * the regular expressions and scratch space required for Hyperscan, a high-performance
     * regular expression matching library.
     *
     */
    static Status prepare_regexps_and_scratch(const std::vector<StringRef>& needles,
                                              multiregexps::RegexpsPtr& regexps,
                                              multiregexps::ScratchPtr& smart_scratch) {
        multiregexps::DeferredConstructedRegexpsPtr deferred_constructed_regexps =
                multiregexps::getOrSet</*SaveIndices*/
                                       FindAnyIndex, WithEditDistance>(needles, std::nullopt);
        regexps = deferred_constructed_regexps->get();

        hs_scratch_t* scratch = nullptr;
        hs_error_t err = hs_clone_scratch(regexps->getScratch(), &scratch);

        if (err != HS_SUCCESS) {
            return Status::InternalError("could not clone scratch space for vectorscan");
        }

        smart_scratch.reset(scratch);
        return Status::OK();
    }

    /**
     * Static callback function to handle the match results of the hs_scan function.
     *
     * This function is called when a matching substring is found while scanning with
     * Hyperscan. It updates the result based on the match information.
     *
     */
    static int on_match([[maybe_unused]] unsigned int id, unsigned long long /* from */, // NOLINT
                        unsigned long long /* to */,                                     // NOLINT
                        unsigned int /* flags */, void* context) {
        if constexpr (FindAnyIndex) {
            *reinterpret_cast<ResultType*>(context) = id;
        } else if constexpr (FindAny) {
            *reinterpret_cast<ResultType*>(context) = 1;
        }
        /// Once we hit the callback, there is no need to search for others.
        return 1;
    }

    static Status vector_constant(const ColumnString::Chars& haystack_data,
                                  const ColumnString::Offsets& haystack_offsets,
                                  const Array& needles_arr, PaddedPODArray<ResultType>& res,
                                  PaddedPODArray<UInt64>& offsets, bool allow_hyperscan,
                                  size_t max_hyperscan_regexp_length,
                                  size_t max_hyperscan_regexp_total_length) {
        if (!allow_hyperscan) {
            return Status::InvalidArgument("Hyperscan functions are disabled");
        }

        std::vector<StringRef> needles;
        needles.reserve(needles_arr.size());
        for (const auto& needle : needles_arr) {
            const auto& tmp = needle.get<TYPE_STRING>();
            needles.emplace_back(StringRef {tmp.data(), tmp.size()});
        }

        res.resize(haystack_offsets.size());

        if (needles_arr.empty()) {
            std::fill(res.begin(), res.end(), 0);
            return Status::OK();
        }

        multiregexps::RegexpsPtr regexps;
        multiregexps::ScratchPtr smart_scratch;
        RETURN_IF_ERROR(prepare_regexps_and_scratch(needles, regexps, smart_scratch));

        const size_t haystack_offsets_size = haystack_offsets.size();
        UInt64 offset = 0;
        for (size_t i = 0; i < haystack_offsets_size; ++i) {
            UInt64 length = haystack_offsets[i] - offset;
            /// vectorscan restriction.
            if (length > std::numeric_limits<UInt32>::max()) {
                return Status::InternalError("too long string to search");
            }
            /// zero the result, scan, check, update the offset.
            res[i] = 0;
            hs_error_t err = hs_scan(
                    regexps->getDB(), reinterpret_cast<const char*>(haystack_data.data()) + offset,
                    static_cast<unsigned>(length), 0, smart_scratch.get(), on_match, &res[i]);
            if (err != HS_SUCCESS && err != HS_SCAN_TERMINATED) {
                return Status::InternalError("failed to scan with vectorscan");
            }
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
        if (!allow_hyperscan) {
            return Status::InvalidArgument("Hyperscan functions are disabled");
        }

        res.resize(haystack_offsets.size());

        size_t prev_haystack_offset = 0;
        size_t prev_needles_offset = 0;

        const auto& nested_column =
                assert_cast<const ColumnNullable&>(needles_data).get_nested_column();
        const auto* needles_data_string = check_and_get_column<ColumnString>(nested_column);

        if (!needles_data_string) {
            return Status::InvalidArgument("needles should be string column");
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

            multiregexps::RegexpsPtr regexps;
            multiregexps::ScratchPtr smart_scratch;
            RETURN_IF_ERROR(prepare_regexps_and_scratch(needles, regexps, smart_scratch));

            const size_t cur_haystack_length = haystack_offsets[i] - prev_haystack_offset;

            /// vectorscan restriction.
            if (cur_haystack_length > std::numeric_limits<UInt32>::max()) {
                return Status::InternalError("too long string to search");
            }

            /// zero the result, scan, check, update the offset.
            res[i] = 0;
            hs_error_t err = hs_scan(
                    regexps->getDB(),
                    reinterpret_cast<const char*>(haystack_data.data()) + prev_haystack_offset,
                    static_cast<unsigned>(cur_haystack_length), 0, smart_scratch.get(), on_match,
                    &res[i]);
            if (err != HS_SUCCESS && err != HS_SCAN_TERMINATED) {
                return Status::InternalError("failed to scan with vectorscan");
            }

            prev_haystack_offset = haystack_offsets[i];
            prev_needles_offset = needles_offsets[i];
            needles.clear();
        }

        return Status::OK();
    }
};

} // namespace doris
