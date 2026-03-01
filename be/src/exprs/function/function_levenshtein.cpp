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
#include "util/simd/vstring_function.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
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
        for (size_t i = 0; i < size; ++i) {
            res[i] = levenshtein_distance(string_ref_at(ldata, loffsets, i),
                                          string_ref_at(rdata, roffsets, i));
        }
        return Status::OK();
    }

    static Status vector_scalar(const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets, const StringRef& rdata,
                                ResultPaddedPODArray& res) {
        const size_t size = loffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            res[i] = levenshtein_distance(string_ref_at(ldata, loffsets, i), rdata);
        }
        return Status::OK();
    }

    static Status scalar_vector(const StringRef& ldata, const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        const size_t size = roffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            res[i] = levenshtein_distance(ldata, string_ref_at(rdata, roffsets, i));
        }
        return Status::OK();
    }

private:
    static StringRef string_ref_at(const ColumnString::Chars& data,
                                   const ColumnString::Offsets& offsets, size_t i) {
        DCHECK_LT(i, offsets.size());
        const size_t begin = (i == 0) ? 0 : offsets[i - 1];
        const size_t end = offsets[i];
        if (end <= begin || end > data.size()) {
            return StringRef("", 0);
        }

        size_t str_size = end - begin;
        if (str_size > 0 && data[end - 1] == '\0') {
            --str_size;
        }
        return StringRef(reinterpret_cast<const char*>(data.data() + begin), str_size);
    }

    static void utf8_char_offsets(const StringRef& ref, std::vector<size_t>& offsets) {
        offsets.clear();
        offsets.reserve(ref.size);
        size_t i = 0;
        while (i < ref.size) {
            offsets.push_back(i);
            uint8_t char_len = doris::get_utf8_byte_length(static_cast<uint8_t>(ref.data[i]));
            if (char_len == 0) {
                char_len = 1;
            }
            if (i + char_len > ref.size) {
                char_len = static_cast<uint8_t>(ref.size - i);
            }
            i += char_len;
        }
    }

    static bool utf8_char_equal(const StringRef& left, size_t left_off, size_t left_next,
                                const StringRef& right, size_t right_off, size_t right_next) {
        const size_t left_len = left_next - left_off;
        const size_t right_len = right_next - right_off;
        return left_len == right_len &&
               std::memcmp(left.data + left_off, right.data + right_off, left_len) == 0;
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
                curr[j] = std::min({insert_cost, delete_cost, replace_cost});
            }
            std::swap(prev, curr);
        }

        return prev[n];
    }

    static Int32 levenshtein_distance(const StringRef& left, const StringRef& right) {
        if (simd::VStringFunctions::is_ascii(left) && simd::VStringFunctions::is_ascii(right)) {
            return levenshtein_distance_ascii(left, right);
        }

        if (left.size == 0) {
            return static_cast<Int32>(simd::VStringFunctions::get_char_len(right.data, right.size));
        }
        if (right.size == 0) {
            return static_cast<Int32>(simd::VStringFunctions::get_char_len(left.data, left.size));
        }

        std::vector<size_t> left_offsets;
        std::vector<size_t> right_offsets;
        utf8_char_offsets(left, left_offsets);
        utf8_char_offsets(right, right_offsets);

        const StringRef* left_ref = &left;
        const StringRef* right_ref = &right;
        if (right_offsets.size() > left_offsets.size()) {
            std::swap(left_offsets, right_offsets);
            std::swap(left_ref, right_ref);
        }

        const size_t m = left_offsets.size();
        const size_t n = right_offsets.size();

        std::vector<Int32> prev(n + 1);
        std::vector<Int32> curr(n + 1);
        for (size_t j = 0; j <= n; ++j) {
            prev[j] = static_cast<Int32>(j);
        }

        for (size_t i = 1; i <= m; ++i) {
            curr[0] = static_cast<Int32>(i);
            const size_t left_off = left_offsets[i - 1];
            const size_t left_next = i < m ? left_offsets[i] : left_ref->size;

            for (size_t j = 1; j <= n; ++j) {
                const size_t right_off = right_offsets[j - 1];
                const size_t right_next = j < n ? right_offsets[j] : right_ref->size;

                const Int32 cost = utf8_char_equal(*left_ref, left_off, left_next, *right_ref,
                                                   right_off, right_next)
                                           ? 0
                                           : 1;

                const Int32 insert_cost = curr[j - 1] + 1;
                const Int32 delete_cost = prev[j] + 1;
                const Int32 replace_cost = prev[j - 1] + cost;
                curr[j] = std::min({insert_cost, delete_cost, replace_cost});
            }
            std::swap(prev, curr);
        }

        return prev[n];
    }
};

using FunctionLevenshtein =
        FunctionBinaryToType<DataTypeString, DataTypeString, LevenshteinImpl, NameLevenshtein>;

void register_function_levenshtein(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionLevenshtein>();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
