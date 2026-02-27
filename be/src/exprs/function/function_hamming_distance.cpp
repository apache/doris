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

struct NameHammingDistance {
    static constexpr auto name = "hamming_distance";
};

template <typename LeftDataType, typename RightDataType>
struct HammingDistanceImpl {
    using ResultDataType = DataTypeInt64;
    using ResultPaddedPODArray = PaddedPODArray<Int64>;

    static Status vector_vector(const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets,
                                const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        DCHECK_EQ(loffsets.size(), roffsets.size());

        const size_t size = loffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            RETURN_IF_ERROR(hamming_distance(string_ref_at(ldata, loffsets, i),
                                             string_ref_at(rdata, roffsets, i), res[i], i));
        }
        return Status::OK();
    }

    static Status vector_scalar(const ColumnString::Chars& ldata,
                                const ColumnString::Offsets& loffsets, const StringRef& rdata,
                                ResultPaddedPODArray& res) {
        const size_t size = loffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            RETURN_IF_ERROR(hamming_distance(string_ref_at(ldata, loffsets, i), rdata, res[i], i));
        }
        return Status::OK();
    }

    static Status scalar_vector(const StringRef& ldata, const ColumnString::Chars& rdata,
                                const ColumnString::Offsets& roffsets, ResultPaddedPODArray& res) {
        const size_t size = roffsets.size();
        res.resize(size);
        for (size_t i = 0; i < size; ++i) {
            RETURN_IF_ERROR(hamming_distance(ldata, string_ref_at(rdata, roffsets, i), res[i], i));
        }
        return Status::OK();
    }

private:
    static StringRef string_ref_at(const ColumnString::Chars& data,
                                   const ColumnString::Offsets& offsets, size_t i) {
        return StringRef(reinterpret_cast<const char*>(&data[offsets[i - 1]]),
                         offsets[i] - offsets[i - 1]);
    }

    static void utf8_char_offsets(const StringRef& ref, std::vector<size_t>& offsets) {
        offsets.clear();
        offsets.reserve(ref.size);
        size_t i = 0;
        while (i < ref.size) {
            offsets.push_back(i);
            uint8_t char_len = doris::get_utf8_byte_length(static_cast<uint8_t>(ref.data[i]));
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

    static Status hamming_distance(const StringRef& left, const StringRef& right, Int64& result,
                                   size_t row) {
        if (simd::VStringFunctions::is_ascii(left) && simd::VStringFunctions::is_ascii(right)) {
            if (left.size != right.size) {
                return Status::InvalidArgument(
                        "hamming_distance requires strings of the same length at row {}", row);
            }

            Int64 distance = 0;
            for (size_t i = 0; i < left.size; ++i) {
                distance += static_cast<Int64>(left.data[i] != right.data[i]);
            }
            result = distance;
            return Status::OK();
        }

        std::vector<size_t> left_offsets;
        std::vector<size_t> right_offsets;
        utf8_char_offsets(left, left_offsets);
        utf8_char_offsets(right, right_offsets);

        if (left_offsets.size() != right_offsets.size()) {
            return Status::InvalidArgument(
                    "hamming_distance requires strings of the same length at row {}", row);
        }

        Int64 distance = 0;
        const size_t len = left_offsets.size();
        for (size_t i = 0; i + 1 < len; ++i) {
            const size_t left_off = left_offsets[i];
            const size_t left_next = left_offsets[i + 1];
            const size_t right_off = right_offsets[i];
            const size_t right_next = right_offsets[i + 1];
            distance += static_cast<Int64>(
                    !utf8_char_equal(left, left_off, left_next, right, right_off, right_next));
        }
        if (len > 0) {
            const size_t left_off = left_offsets[len - 1];
            const size_t right_off = right_offsets[len - 1];
            distance += static_cast<Int64>(
                    !utf8_char_equal(left, left_off, left.size, right, right_off, right.size));
        }

        result = distance;
        return Status::OK();
    }
};

using FunctionHammingDistance = FunctionBinaryToType<DataTypeString, DataTypeString,
                                                     HammingDistanceImpl, NameHammingDistance>;

void register_function_hamming_distance(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHammingDistance>();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
