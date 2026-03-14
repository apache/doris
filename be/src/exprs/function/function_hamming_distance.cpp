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
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/string_ref.h"
#include "exprs/function/function_totype.h"
#include "exprs/function/simple_function_factory.h"
#include "core/data_type/data_type_number.h"
#include "util/simd/vstring_function.h"

namespace doris {
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
        simd::VStringFunctions::get_char_len(ref.data, ref.size, offsets);
    }

    static bool utf8_char_equal(const StringRef& left, size_t left_off, size_t left_next,
                                const StringRef& right, size_t right_off, size_t right_next) {
        const size_t left_len = left_next - left_off;
        const size_t right_len = right_next - right_off;
        return left_len == right_len &&
               std::memcmp(left.data + left_off, right.data + right_off, left_len) == 0;
    }

public:
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

template <template <typename, typename> typename Impl, typename Name>
class FunctionBinaryStringToTypeWithNull : public IFunction {
public:
    using LeftDataType = DataTypeString;
    using RightDataType = DataTypeString;
    using ResultDataType = typename Impl<LeftDataType, RightDataType>::ResultDataType;
    using ResultColumnType = ColumnVector<ResultDataType::PType>;

    static constexpr auto name = Name::name;

    static FunctionPtr create() { return std::make_shared<FunctionBinaryStringToTypeWithNull>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        const bool has_nullable = std::ranges::any_of(
                arguments, [](const DataTypePtr& type) { return type->is_nullable(); });
        if (has_nullable) {
            return make_nullable(std::make_shared<ResultDataType>());
        }
        return std::make_shared<ResultDataType>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& [left_col, left_const] =
                unpack_if_const(block.get_by_position(arguments[0]).column);
        const auto& [right_col, right_const] =
                unpack_if_const(block.get_by_position(arguments[1]).column);

        const auto* left_nullable = check_and_get_column<ColumnNullable>(left_col.get());
        const auto* right_nullable = check_and_get_column<ColumnNullable>(right_col.get());

        const IColumn* left_nested =
                left_nullable ? &left_nullable->get_nested_column() : left_col.get();
        const IColumn* right_nested =
                right_nullable ? &right_nullable->get_nested_column() : right_col.get();

        const auto* left_str_col = check_and_get_column<ColumnString>(left_nested);
        const auto* right_str_col = check_and_get_column<ColumnString>(right_nested);
        if (!left_str_col || !right_str_col) {
            return Status::NotSupported("Illegal columns {}, {} of argument of function {}",
                                        left_col->get_name(), right_col->get_name(), get_name());
        }

        auto res_col = ResultColumnType::create(input_rows_count);
        auto& res_data = res_col->get_data();

        const NullMap* left_null_map =
                left_nullable ? &left_nullable->get_null_map_data() : nullptr;
        const NullMap* right_null_map =
                right_nullable ? &right_nullable->get_null_map_data() : nullptr;
        const bool has_nullable = left_null_map != nullptr || right_null_map != nullptr;

        if (!has_nullable) {
            if (left_const) {
                auto st = Impl<LeftDataType, RightDataType>::scalar_vector(
                        left_str_col->get_data_at(0), right_str_col->get_chars(),
                        right_str_col->get_offsets(), res_data);
                RETURN_IF_ERROR(st);
            } else if (right_const) {
                auto st = Impl<LeftDataType, RightDataType>::vector_scalar(
                        left_str_col->get_chars(), left_str_col->get_offsets(),
                        right_str_col->get_data_at(0), res_data);
                RETURN_IF_ERROR(st);
            } else {
                auto st = Impl<LeftDataType, RightDataType>::vector_vector(
                        left_str_col->get_chars(), left_str_col->get_offsets(),
                        right_str_col->get_chars(), right_str_col->get_offsets(), res_data);
                RETURN_IF_ERROR(st);
            }
            block.replace_by_position(result, std::move(res_col));
            return Status::OK();
        }

        auto null_col = ColumnUInt8::create(input_rows_count, 0);
        auto& null_map = null_col->get_data();
        for (size_t i = 0; i < input_rows_count; ++i) {
            const size_t left_idx = left_const ? 0 : i;
            const size_t right_idx = right_const ? 0 : i;

            const bool left_is_null = left_null_map && (*left_null_map)[left_idx];
            const bool right_is_null = right_null_map && (*right_null_map)[right_idx];
            if (left_is_null || right_is_null) {
                null_map[i] = 1;
                res_data[i] = 0;
                continue;
            }

            auto st = Impl<LeftDataType, RightDataType>::hamming_distance(
                    left_str_col->get_data_at(left_idx), right_str_col->get_data_at(right_idx),
                    res_data[i], i);
            RETURN_IF_ERROR(st);
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res_col), std::move(null_col)));
        return Status::OK();
    }
};

using FunctionHammingDistance =
        FunctionBinaryStringToTypeWithNull<HammingDistanceImpl, NameHammingDistance>;

void register_function_hamming_distance(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHammingDistance>();
}

#include "common/compile_check_end.h"
} // namespace doris
