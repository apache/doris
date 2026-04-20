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
#include "core/data_type/data_type_number.h"
#include "core/string_ref.h"
#include "exprs/function/simple_function_factory.h"
#include "util/simd/vstring_function.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"

class FunctionHammingDistance : public IFunction {
public:
    using ResultDataType = DataTypeInt64;
    using ResultPaddedPODArray = PaddedPODArray<Int64>;
    using ResultColumnType = ColumnVector<ResultDataType::PType>;

    static constexpr auto name = "hamming_distance";

    static FunctionPtr create() { return std::make_shared<FunctionHammingDistance>(); }

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
                RETURN_IF_ERROR(scalar_vector(left_str_col->get_data_at(0).trim_tail_padding_zero(),
                                              *right_str_col, res_data));
            } else if (right_const) {
                RETURN_IF_ERROR(vector_scalar(
                        *left_str_col, right_str_col->get_data_at(0).trim_tail_padding_zero(),
                        res_data));
            } else {
                RETURN_IF_ERROR(vector_vector(*left_str_col, *right_str_col, res_data));
            }
            block.replace_by_position(result, std::move(res_col));
            return Status::OK();
        }

        auto null_col = ColumnUInt8::create(input_rows_count, 0);
        auto& null_map = null_col->get_data();
        if (left_const) {
            if (left_null_map && (*left_null_map)[0]) {
                std::fill(null_map.begin(), null_map.end(), 1);
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(res_col), std::move(null_col)));
                return Status::OK();
            }

            const auto left = left_str_col->get_data_at(0).trim_tail_padding_zero();
            RETURN_IF_ERROR(scalar_vector_nullable(left, *right_str_col, right_null_map, res_data,
                                                   null_map));
        } else if (right_const) {
            if (right_null_map && (*right_null_map)[0]) {
                std::fill(null_map.begin(), null_map.end(), 1);
                block.replace_by_position(
                        result, ColumnNullable::create(std::move(res_col), std::move(null_col)));
                return Status::OK();
            }

            RETURN_IF_ERROR(vector_scalar_nullable(
                    *left_str_col, right_str_col->get_data_at(0).trim_tail_padding_zero(),
                    left_null_map, res_data, null_map));
        } else {
            for (size_t i = 0; i < input_rows_count; ++i) {
                const bool left_is_null = left_null_map && (*left_null_map)[i];
                const bool right_is_null = right_null_map && (*right_null_map)[i];
                if (left_is_null || right_is_null) {
                    null_map[i] = 1;
                    res_data[i] = 0;
                    continue;
                }

                RETURN_IF_ERROR(hamming_distance(
                        left_str_col->get_data_at(i).trim_tail_padding_zero(),
                        right_str_col->get_data_at(i).trim_tail_padding_zero(), res_data[i], i));
            }
        }

        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(res_col), std::move(null_col)));
        return Status::OK();
    }

private:
    static Status vector_vector(const ColumnString& lcol, const ColumnString& rcol,
                                ResultPaddedPODArray& res) {
        DCHECK_EQ(lcol.size(), rcol.size());

        const size_t size = lcol.size();
        res.resize(size);
        std::vector<size_t> left_offsets;
        std::vector<size_t> right_offsets;
        for (size_t i = 0; i < size; ++i) {
            RETURN_IF_ERROR(hamming_distance(lcol.get_data_at(i).trim_tail_padding_zero(),
                                             rcol.get_data_at(i).trim_tail_padding_zero(),
                                             left_offsets, right_offsets, res[i], i));
        }
        return Status::OK();
    }

    static Status vector_scalar(const ColumnString& lcol, const StringRef& rdata,
                                ResultPaddedPODArray& res) {
        const size_t size = lcol.size();
        res.resize(size);
        const bool right_ascii = simd::VStringFunctions::is_ascii(rdata);
        std::vector<size_t> right_offsets;
        utf8_char_offsets(rdata, right_offsets);
        std::vector<size_t> left_offsets;
        for (size_t i = 0; i < size; ++i) {
            const auto left = lcol.get_data_at(i).trim_tail_padding_zero();
            RETURN_IF_ERROR(hamming_distance_with_offsets(
                    left, left_offsets, false, simd::VStringFunctions::is_ascii(left), rdata,
                    right_offsets, true, right_ascii, res[i], i));
        }
        return Status::OK();
    }

    static Status scalar_vector(const StringRef& ldata, const ColumnString& rcol,
                                ResultPaddedPODArray& res) {
        const size_t size = rcol.size();
        res.resize(size);
        const bool left_ascii = simd::VStringFunctions::is_ascii(ldata);
        std::vector<size_t> left_offsets;
        utf8_char_offsets(ldata, left_offsets);
        std::vector<size_t> right_offsets;
        for (size_t i = 0; i < size; ++i) {
            const auto right = rcol.get_data_at(i).trim_tail_padding_zero();
            RETURN_IF_ERROR(hamming_distance_with_offsets(
                    ldata, left_offsets, true, left_ascii, right, right_offsets, false,
                    simd::VStringFunctions::is_ascii(right), res[i], i));
        }
        return Status::OK();
    }

    static Status vector_scalar_nullable(const ColumnString& lcol, const StringRef& rdata,
                                         const NullMap* left_null_map, ResultPaddedPODArray& res,
                                         NullMap& null_map) {
        const size_t size = lcol.size();
        res.resize(size);
        const bool right_ascii = simd::VStringFunctions::is_ascii(rdata);
        std::vector<size_t> right_offsets;
        utf8_char_offsets(rdata, right_offsets);
        std::vector<size_t> left_offsets;
        for (size_t i = 0; i < size; ++i) {
            if (left_null_map && (*left_null_map)[i]) {
                null_map[i] = 1;
                res[i] = 0;
                continue;
            }

            const auto left = lcol.get_data_at(i).trim_tail_padding_zero();
            RETURN_IF_ERROR(hamming_distance_with_offsets(
                    left, left_offsets, false, simd::VStringFunctions::is_ascii(left), rdata,
                    right_offsets, true, right_ascii, res[i], i));
        }
        return Status::OK();
    }

    static Status scalar_vector_nullable(const StringRef& ldata, const ColumnString& rcol,
                                         const NullMap* right_null_map, ResultPaddedPODArray& res,
                                         NullMap& null_map) {
        const size_t size = rcol.size();
        res.resize(size);
        const bool left_ascii = simd::VStringFunctions::is_ascii(ldata);
        std::vector<size_t> left_offsets;
        utf8_char_offsets(ldata, left_offsets);
        std::vector<size_t> right_offsets;
        for (size_t i = 0; i < size; ++i) {
            if (right_null_map && (*right_null_map)[i]) {
                null_map[i] = 1;
                res[i] = 0;
                continue;
            }

            const auto right = rcol.get_data_at(i).trim_tail_padding_zero();
            RETURN_IF_ERROR(hamming_distance_with_offsets(
                    ldata, left_offsets, true, left_ascii, right, right_offsets, false,
                    simd::VStringFunctions::is_ascii(right), res[i], i));
        }
        return Status::OK();
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

    static Status hamming_distance_ascii(const StringRef& left, const StringRef& right,
                                         Int64& result, size_t row) {
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

    static Status hamming_distance_utf8(const StringRef& left,
                                        const std::vector<size_t>& left_offsets,
                                        const StringRef& right,
                                        const std::vector<size_t>& right_offsets, Int64& result,
                                        size_t row) {
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

    static Status hamming_distance(const StringRef& left, const StringRef& right,
                                   std::vector<size_t>& left_offsets,
                                   std::vector<size_t>& right_offsets, Int64& result, size_t row) {
        const bool left_ascii = simd::VStringFunctions::is_ascii(left);
        const bool right_ascii = simd::VStringFunctions::is_ascii(right);
        return hamming_distance_with_offsets(left, left_offsets, false, left_ascii, right,
                                             right_offsets, false, right_ascii, result, row);
    }

    static Status hamming_distance_with_offsets(
            const StringRef& left, std::vector<size_t>& left_offsets, bool left_offsets_ready,
            bool left_ascii, const StringRef& right, std::vector<size_t>& right_offsets,
            bool right_offsets_ready, bool right_ascii, Int64& result, size_t row) {
        if (left_ascii && right_ascii) {
            return hamming_distance_ascii(left, right, result, row);
        }

        if (!left_offsets_ready) {
            utf8_char_offsets(left, left_offsets);
        }
        if (!right_offsets_ready) {
            utf8_char_offsets(right, right_offsets);
        }
        return hamming_distance_utf8(left, left_offsets, right, right_offsets, result, row);
    }

    static Status hamming_distance(const StringRef& left, const StringRef& right, Int64& result,
                                   size_t row) {
        std::vector<size_t> left_offsets;
        std::vector<size_t> right_offsets;
        return hamming_distance(left, right, left_offsets, right_offsets, result, row);
    }
};

void register_function_hamming_distance(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHammingDistance>();
}

#include "common/compile_check_avoid_end.h"
} // namespace doris
