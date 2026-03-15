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

#include <algorithm>
#include <cstring>
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type_number.h"
#include "core/string_ref.h"
#include "exprs/function/function_totype.h"
#include "exprs/function/simple_function_factory.h"
#include "util/simd/vstring_function.h"

namespace doris {
#include "common/compile_check_begin.h"

struct NameLevenshtein {
    static constexpr auto name = "levenshtein";
};

template <typename LeftDataType, typename RightDataType>
struct LevenshteinImpl {
    using ResultDataType = DataTypeInt32;
    using ResultPaddedPODArray = PaddedPODArray<Int32>;

    static Status vector_vector(const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets,
                                const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        DCHECK_EQ(loffsets.size(), roffsets.size());

        const size_t size = loffsets.size();
        res.resize(size);
        std::vector<size_t> left_offsets;
        std::vector<size_t> right_offsets;
        for (size_t i = 0; i < size; ++i) {
            res[i] = levenshtein_distance(string_ref_at(ldata, loffsets, i),
                                          string_ref_at(rdata, roffsets, i), left_offsets,
                                          right_offsets);
        }
        return Status::OK();
    }

    static Status vector_scalar(const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets, const StringRef& rdata,
                                ResultPaddedPODArray& res) {
        const size_t size = loffsets.size();
        res.resize(size);
        const bool right_ascii = simd::VStringFunctions::is_ascii(rdata);
        std::vector<size_t> right_offsets;
        utf8_char_offsets(rdata, right_offsets);
        std::vector<size_t> left_offsets;
        for (size_t i = 0; i < size; ++i) {
            res[i] = levenshtein_distance_with_right_offsets(string_ref_at(ldata, loffsets, i),
                                                             left_offsets, rdata, right_offsets,
                                                             right_ascii);
        }
        return Status::OK();
    }

    static Status scalar_vector(const StringRef& ldata, const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        const size_t size = roffsets.size();
        res.resize(size);
        const bool left_ascii = simd::VStringFunctions::is_ascii(ldata);
        std::vector<size_t> left_offsets;
        utf8_char_offsets(ldata, left_offsets);
        std::vector<size_t> right_offsets;
        for (size_t i = 0; i < size; ++i) {
            res[i] = levenshtein_distance_with_left_offsets(ldata, left_offsets, left_ascii,
                                                            string_ref_at(rdata, roffsets, i),
                                                            right_offsets);
        }
        return Status::OK();
    }

private:
    static StringRef string_ref_at(const ColumnString::Chars& data,
                                   const ColumnString::Offsets& offsets, size_t i) {
        DCHECK_LT(i, offsets.size());
        const auto idx = static_cast<ssize_t>(i);
        return StringRef(data.data() + offsets[idx - 1], offsets[idx] - offsets[idx - 1])
                .trim_tail_padding_zero();
    }

    static void utf8_char_offsets(const StringRef& ref, std::vector<size_t>& offsets) {
        offsets.clear();
        offsets.reserve(ref.size);
        simd::VStringFunctions::get_char_len(ref.data, ref.size, offsets);
    }

    static bool utf8_char_equal(const StringRef& left, size_t left_off, size_t left_next,
                                const StringRef& right, size_t right_off, size_t right_next) {
        const size_t left_len = left_next - left_off;
        const size_t right_len = right_next - right_off;
        return left_len == right_len &&
               std::memcmp(left.data + left_off, right.data + right_off, left_len) == 0;
    }

    static Int32 levenshtein_distance_utf8(const StringRef& left,
                                           const std::vector<size_t>& left_offsets,
                                           const StringRef& right,
                                           const std::vector<size_t>& right_offsets) {
        const StringRef* left_ref = &left;
        const StringRef* right_ref = &right;
        const std::vector<size_t>* left_offsets_ref = &left_offsets;
        const std::vector<size_t>* right_offsets_ref = &right_offsets;
        if (right_offsets_ref->size() > left_offsets_ref->size()) {
            std::swap(left_offsets_ref, right_offsets_ref);
            std::swap(left_ref, right_ref);
        }

        const size_t m = left_offsets_ref->size();
        const size_t n = right_offsets_ref->size();

        std::vector<Int32> prev(n + 1);
        std::vector<Int32> curr(n + 1);
        for (size_t j = 0; j <= n; ++j) {
            prev[j] = static_cast<Int32>(j);
        }

        for (size_t i = 1; i <= m; ++i) {
            curr[0] = static_cast<Int32>(i);
            const size_t left_off = (*left_offsets_ref)[i - 1];
            const size_t left_next = i < m ? (*left_offsets_ref)[i] : left_ref->size;

            for (size_t j = 1; j <= n; ++j) {
                const size_t right_off = (*right_offsets_ref)[j - 1];
                const size_t right_next = j < n ? (*right_offsets_ref)[j] : right_ref->size;

                const Int32 cost = utf8_char_equal(*left_ref, left_off, left_next, *right_ref,
                                                   right_off, right_next)
                                           ? 0
                                           : 1;

                const Int32 insert_cost = curr[j - 1] + 1;
                const Int32 delete_cost = prev[j] + 1;
                const Int32 replace_cost = prev[j - 1] + cost;
                curr[j] = std::min(std::min(insert_cost, delete_cost), replace_cost);
            }
            std::swap(prev, curr);
        }

        return prev[n];
    }

    static Int32 levenshtein_distance_ascii(const StringRef& left, const StringRef& right) {
        const StringRef* left_ref = &left;
        const StringRef* right_ref = &right;
        size_t m = left.size;
        size_t n = right.size;

        if (n > m) {
            std::swap(left_ref, right_ref);
            std::swap(m, n);
        }

        std::vector<Int32> prev(n + 1);
        std::vector<Int32> curr(n + 1);
        for (size_t j = 0; j <= n; ++j) {
            prev[j] = static_cast<Int32>(j);
        }

        for (size_t i = 1; i <= m; ++i) {
            curr[0] = static_cast<Int32>(i);
            const char left_char = left_ref->data[i - 1];

            for (size_t j = 1; j <= n; ++j) {
                const Int32 cost = left_char == right_ref->data[j - 1] ? 0 : 1;
                const Int32 insert_cost = curr[j - 1] + 1;
                const Int32 delete_cost = prev[j] + 1;
                const Int32 replace_cost = prev[j - 1] + cost;
                curr[j] = std::min(std::min(insert_cost, delete_cost), replace_cost);
            }
            std::swap(prev, curr);
        }

        return prev[n];
    }

    static Int32 levenshtein_distance(const StringRef& left, const StringRef& right,
                                      std::vector<size_t>& left_offsets,
                                      std::vector<size_t>& right_offsets) {
        const bool left_ascii = simd::VStringFunctions::is_ascii(left);
        const bool right_ascii = simd::VStringFunctions::is_ascii(right);
        if (left_ascii && right_ascii) {
            return levenshtein_distance_ascii(left, right);
        }

        if (left.size == 0) {
            return static_cast<Int32>(simd::VStringFunctions::get_char_len(right.data, right.size));
        }
        if (right.size == 0) {
            return static_cast<Int32>(simd::VStringFunctions::get_char_len(left.data, left.size));
        }

        utf8_char_offsets(left, left_offsets);
        utf8_char_offsets(right, right_offsets);
        return levenshtein_distance_utf8(left, left_offsets, right, right_offsets);
    }

    static Int32 levenshtein_distance_with_right_offsets(const StringRef& left,
                                                         std::vector<size_t>& left_offsets,
                                                         const StringRef& right,
                                                         const std::vector<size_t>& right_offsets,
                                                         bool right_ascii) {
        const bool left_ascii = simd::VStringFunctions::is_ascii(left);
        if (left_ascii && right_ascii) {
            return levenshtein_distance_ascii(left, right);
        }

        if (left.size == 0) {
            return static_cast<Int32>(right_offsets.size());
        }
        if (right.size == 0) {
            return left_ascii ? static_cast<Int32>(left.size)
                              : static_cast<Int32>(
                                        simd::VStringFunctions::get_char_len(left.data, left.size));
        }

        utf8_char_offsets(left, left_offsets);
        return levenshtein_distance_utf8(left, left_offsets, right, right_offsets);
    }

    static Int32 levenshtein_distance_with_left_offsets(const StringRef& left,
                                                        const std::vector<size_t>& left_offsets,
                                                        bool left_ascii, const StringRef& right,
                                                        std::vector<size_t>& right_offsets) {
        const bool right_ascii = simd::VStringFunctions::is_ascii(right);
        if (left_ascii && right_ascii) {
            return levenshtein_distance_ascii(left, right);
        }

        if (left.size == 0) {
            return static_cast<Int32>(
                    right_ascii ? right.size
                                : simd::VStringFunctions::get_char_len(right.data, right.size));
        }
        if (right.size == 0) {
            return static_cast<Int32>(left_offsets.size());
        }

        utf8_char_offsets(right, right_offsets);
        return levenshtein_distance_utf8(left, left_offsets, right, right_offsets);
    }

    static Int32 levenshtein_distance(const StringRef& left, const StringRef& right) {
        std::vector<size_t> left_offsets;
        std::vector<size_t> right_offsets;
        return levenshtein_distance(left, right, left_offsets, right_offsets);
    }
};

using FunctionLevenshtein =
        FunctionBinaryToType<DataTypeString, DataTypeString, LevenshteinImpl, NameLevenshtein>;

void register_function_levenshtein(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLevenshtein>();
}

#include "common/compile_check_end.h"
} // namespace doris
