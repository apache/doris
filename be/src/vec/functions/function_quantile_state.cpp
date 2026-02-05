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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionBitmap.h
// and modified by Doris

#include <fmt/format.h>
#include <glog/logging.h>

#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <memory>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "util/quantile_state.h"
#include "util/url_coding.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_quantilestate.h" // IWYU pragma: keep
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_const.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

struct QuantileStateEmpty {
    static constexpr auto name = "quantile_state_empty";
    using ReturnColVec = ColumnQuantileState;
    static DataTypePtr get_return_type() { return std::make_shared<DataTypeQuantileState>(); }
    static auto init_value() { return QuantileState {}; }
};

class FunctionToQuantileState : public IFunction {
public:
    static constexpr auto name = "to_quantile_state";
    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionToQuantileState>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeQuantileState>();
    }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    template <bool is_nullable>
    Status execute_internal(const ColumnPtr& column, const DataTypePtr& data_type,
                            MutableColumnPtr& column_result, float compression) const {
        auto type_error = [&]() {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        column->get_name(), get_name());
        };
        const ColumnNullable* col_nullable = nullptr;
        const ColumnUInt8* col_nullmap = nullptr;
        const ColumnFloat64* col = nullptr;
        const NullMap* nullmap = nullptr;
        if constexpr (is_nullable) {
            col_nullable = check_and_get_column<ColumnNullable>(column.get());
            col_nullmap = check_and_get_column<ColumnUInt8>(
                    col_nullable->get_null_map_column_ptr().get());
            col = check_and_get_column<ColumnFloat64>(col_nullable->get_nested_column_ptr().get());
            if (col == nullptr || col_nullmap == nullptr) {
                return type_error();
            }

            nullmap = &col_nullmap->get_data();
        } else {
            col = check_and_get_column<ColumnFloat64>(column.get());
        }
        auto* res_column = reinterpret_cast<ColumnQuantileState*>(column_result.get());
        auto& res_data = res_column->get_data();

        size_t size = col->size();
        for (size_t i = 0; i < size; ++i) {
            if constexpr (is_nullable) {
                if ((*nullmap)[i]) {
                    res_data[i].clear();
                    continue;
                }
            }
            auto value = (double)col->get_data()[i];
            res_data[i].set_compression(compression);
            res_data[i].add_value(value);
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr& column = block.get_by_position(arguments[0]).column;
        const DataTypePtr& data_type = block.get_by_position(arguments[0]).type;
        const auto* compression_arg = check_and_get_column_const<ColumnFloat32>(
                block.get_by_position(arguments.back()).column.get());
        float compression = 2048;
        if (compression_arg) {
            auto compression_arg_val = compression_arg->get_value<TYPE_FLOAT>();
            if (compression_arg_val >= QUANTILE_STATE_COMPRESSION_MIN &&
                compression_arg_val <= QUANTILE_STATE_COMPRESSION_MAX) {
                compression = compression_arg_val;
            }
        }
        MutableColumnPtr column_result = get_return_type_impl({})->create_column();
        column_result->resize(input_rows_count);

        Status status = Status::OK();
        if (data_type->is_nullable()) {
            RETURN_IF_ERROR(execute_internal<true>(column, data_type, column_result, compression));
        } else {
            RETURN_IF_ERROR(execute_internal<false>(column, data_type, column_result, compression));
        }
        if (status.ok()) {
            block.replace_by_position(result, std::move(column_result));
        }
        return status;
    }
};

class FunctionQuantileStatePercent : public IFunction {
public:
    static constexpr auto name = "quantile_percent";
    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionQuantileStatePercent>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeFloat64>();
    }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res_data_column = ColumnFloat64::create();
        auto& res = res_data_column->get_data();
        auto data_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto& null_map = data_null_map->get_data();

        auto column = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (const auto* nullable = check_and_get_column<const ColumnNullable>(*column)) {
            VectorizedUtils::update_null_map(null_map, nullable->get_null_map_data());
            column = nullable->get_nested_column_ptr();
        }
        const auto* str_col = assert_cast<const ColumnQuantileState*>(column.get());
        const auto& col_data = str_col->get_data();
        const auto* percent_arg = check_and_get_column_const<ColumnFloat32>(
                block.get_by_position(arguments.back()).column.get());

        if (!percent_arg) {
            return Status::InvalidArgument(
                    "Second argument to {} must be a constant float describing type", get_name());
        }
        auto percent_arg_value = percent_arg->get_value<TYPE_FLOAT>();
        if (percent_arg_value < 0 || percent_arg_value > 1) {
            return Status::InvalidArgument(
                    "the input argument of percentage: {} is not valid, must be in range [0,1] ",
                    percent_arg_value);
        }

        res.reserve(input_rows_count);
        for (size_t i = 0; i < input_rows_count; ++i) {
            if (null_map[i]) {
                // if null push_back meaningless result to make sure idxs can be matched
                res.push_back(0);
                continue;
            }

            res.push_back(col_data[i].get_value_by_percentile(percent_arg_value));
        }

        block.replace_by_position(result, std::move(res_data_column));
        return Status::OK();
    }
};

class FunctionQuantileStateFromBase64 : public IFunction {
public:
    static constexpr auto name = "quantile_state_from_base64";
    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionQuantileStateFromBase64>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeQuantileState>());
    }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto res_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res_data_column = ColumnQuantileState::create();
        auto& null_map = res_null_map->get_data();
        auto& res = res_data_column->get_data();

        auto& argument_column = block.get_by_position(arguments[0]).column;
        const auto& str_column = static_cast<const ColumnString&>(*argument_column);
        const ColumnString::Chars& data = str_column.get_chars();
        const ColumnString::Offsets& offsets = str_column.get_offsets();

        res.reserve(input_rows_count);

        std::string decode_buff;
        int64_t last_decode_buff_len = 0;
        int64_t curr_decode_buff_len = 0;
        for (size_t i = 0; i < input_rows_count; ++i) {
            const char* src_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int64_t src_size = offsets[i] - offsets[i - 1];

            if (src_size == 0 || 0 != src_size % 4) {
                res.emplace_back();
                null_map[i] = 1;
                continue;
            }

            curr_decode_buff_len = src_size + 3;
            if (curr_decode_buff_len > last_decode_buff_len) {
                decode_buff.resize(curr_decode_buff_len);
                last_decode_buff_len = curr_decode_buff_len;
            }
            auto outlen = base64_decode(src_str, src_size, decode_buff.data());
            if (outlen < 0) {
                res.emplace_back();
                null_map[i] = 1;
            } else {
                doris::Slice decoded_slice(decode_buff.data(), outlen);
                doris::QuantileState quantile_state;
                if (!quantile_state.deserialize(decoded_slice)) {
                    return Status::RuntimeError(fmt::format(
                            "quantile_state_from_base64 decode failed: base64: {}", src_str));
                } else {
                    res.emplace_back(std::move(quantile_state));
                }
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res_data_column), std::move(res_null_map));
        return Status::OK();
    }
};

struct NameQuantileStateToBase64 {
    static constexpr auto name = "quantile_state_to_base64";
};

struct QuantileStateToBase64 {
    using ReturnType = DataTypeString;
    static constexpr auto PrimitiveTypeImpl = PrimitiveType::TYPE_QUANTILE_STATE;
    using Type = DataTypeQuantileState::FieldType;
    using ReturnColumnType = ColumnString;
    using Chars = ColumnString::Chars;
    using Offsets = ColumnString::Offsets;

    static Status vector(const std::vector<QuantileState>& data, Chars& chars, Offsets& offsets) {
        size_t size = data.size();
        offsets.resize(size);
        size_t output_char_size = 0;
        for (size_t i = 0; i < size; ++i) {
            auto& quantile_state_val = const_cast<QuantileState&>(data[i]);
            auto ser_size = quantile_state_val.get_serialized_size();
            output_char_size += (int)(4.0 * ceil((double)ser_size / 3.0));
        }
        ColumnString::check_chars_length(output_char_size, size);
        chars.resize(output_char_size);
        auto* chars_data = chars.data();

        size_t cur_ser_size = 0;
        size_t last_ser_size = 0;
        std::string ser_buff;
        size_t encoded_offset = 0;
        for (size_t i = 0; i < size; ++i) {
            auto& quantile_state_val = const_cast<QuantileState&>(data[i]);

            cur_ser_size = quantile_state_val.get_serialized_size();
            if (cur_ser_size > last_ser_size) {
                last_ser_size = cur_ser_size;
                ser_buff.resize(cur_ser_size);
            }
            size_t real_size =
                    quantile_state_val.serialize(reinterpret_cast<uint8_t*>(ser_buff.data()));
            auto outlen = base64_encode((const unsigned char*)ser_buff.data(), real_size,
                                        chars_data + encoded_offset);
            DCHECK(outlen > 0);

            encoded_offset += outlen;
            offsets[i] = cast_set<uint32_t>(encoded_offset);
        }
        return Status::OK();
    }
};

using FunctionQuantileStateToBase64 =
        FunctionUnaryToType<QuantileStateToBase64, NameQuantileStateToBase64>;

void register_function_quantile_state(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionConst<QuantileStateEmpty, false>>();
    factory.register_function<FunctionQuantileStatePercent>();
    factory.register_function<FunctionToQuantileState>();
    factory.register_function<FunctionQuantileStateFromBase64>();
    factory.register_function<FunctionQuantileStateToBase64>();
}

} // namespace doris::vectorized
