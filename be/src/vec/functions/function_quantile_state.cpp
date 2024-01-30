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
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "util/quantile_state.h"
#include "util/string_parser.hpp"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_quantilestate.h" // IWYU pragma: keep
#include "vec/functions/function.h"
#include "vec/functions/function_const.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

template <typename InternalType>
struct QuantileStateEmpty {
    static constexpr auto name = "quantile_state_empty";
    using ReturnColVec = ColumnQuantileState<InternalType>;
    static DataTypePtr get_return_type() {
        return std::make_shared<DataTypeQuantileState<InternalType>>();
    }
    static auto init_value() { return QuantileState<InternalType> {}; }
};

template <typename InternalType>
class FunctionToQuantileState : public IFunction {
public:
    static constexpr auto name = "to_quantile_state";
    String get_name() const override { return name; }

    static FunctionPtr create() {
        return std::make_shared<FunctionToQuantileState<InternalType>>();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeQuantileState<InternalType>>();
    }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    template <typename ColumnType, bool is_nullable>
    Status execute_internal(const ColumnPtr& column, const DataTypePtr& data_type,
                            MutableColumnPtr& column_result) {
        auto type_error = [&]() {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        column->get_name(), get_name());
        };
        const ColumnNullable* col_nullable = nullptr;
        const ColumnUInt8* col_nullmap = nullptr;
        const ColumnType* col = nullptr;
        const NullMap* nullmap = nullptr;
        if constexpr (is_nullable) {
            col_nullable = check_and_get_column<ColumnNullable>(column.get());
            col_nullmap = check_and_get_column<ColumnUInt8>(
                    col_nullable->get_null_map_column_ptr().get());
            col = check_and_get_column<ColumnType>(col_nullable->get_nested_column_ptr().get());
            if (col == nullptr || col_nullmap == nullptr) {
                return type_error();
            }

            nullmap = &col_nullmap->get_data();
        } else {
            col = check_and_get_column<ColumnType>(column.get());
        }
        auto* res_column =
                reinterpret_cast<ColumnQuantileState<InternalType>*>(column_result.get());
        auto& res_data = res_column->get_data();

        size_t size = col->size();
        for (size_t i = 0; i < size; ++i) {
            if constexpr (is_nullable) {
                if ((*nullmap)[i]) {
                    continue;
                }
            }

            if constexpr (std::is_same_v<ColumnType, ColumnString>) {
                const ColumnString::Chars& data = col->get_chars();
                const ColumnString::Offsets& offsets = col->get_offsets();

                const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                size_t str_size = offsets[i] - offsets[i - 1];
                StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
                InternalType value = StringParser::string_to_float<InternalType>(raw_str, str_size,
                                                                                 &parse_result);
                if (LIKELY(parse_result == StringParser::PARSE_SUCCESS)) {
                    res_data[i].add_value(value);
                } else {
                    std::stringstream ss;
                    ss << "The input column content: " << std::string(raw_str, str_size)
                       << " is not valid in function: " << get_name();
                    LOG(WARNING) << ss.str();
                    return Status::InternalError(ss.str());
                }
            } else if constexpr (std::is_same_v<ColumnType, ColumnInt64> ||
                                 std::is_same_v<ColumnType, ColumnFloat32> ||
                                 std::is_same_v<ColumnType, ColumnFloat64>) {
                // InternalType only can be double or float, so we can cast directly
                InternalType value = (InternalType)col->get_data()[i];
                res_data[i].set_compression(compression);
                res_data[i].add_value(value);
            } else {
                type_error();
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        if constexpr (!(std::is_same_v<InternalType, float> ||
                        std::is_same_v<InternalType, double>)) {
            std::stringstream ss;
            ss << "The InternalType of quantile_state must be float or double";

            return Status::InternalError(ss.str());
        }

        const ColumnPtr& column = block.get_by_position(arguments[0]).column;
        const DataTypePtr& data_type = block.get_by_position(arguments[0]).type;
        auto compression_arg = check_and_get_column_const<ColumnFloat32>(
                block.get_by_position(arguments.back()).column);
        if (compression_arg) {
            auto compression_arg_val = compression_arg->get_value<Float32>();
            if (compression_arg_val && compression_arg_val >= QUANTILE_STATE_COMPRESSION_MIN &&
                compression_arg_val <= QUANTILE_STATE_COMPRESSION_MAX) {
                this->compression = compression_arg_val;
            }
        }
        WhichDataType which(data_type);
        MutableColumnPtr column_result = get_return_type_impl({})->create_column();
        column_result->resize(input_rows_count);

        auto type_error = [&]() {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        };
        Status status = Status::OK();
        if (which.is_nullable()) {
            const DataTypePtr& nested_data_type =
                    static_cast<const DataTypeNullable*>(data_type.get())->get_nested_type();
            WhichDataType nested_which(nested_data_type);
            if (nested_which.is_string_or_fixed_string()) {
                status = execute_internal<ColumnString, true>(column, data_type, column_result);
            } else if (nested_which.is_int64()) {
                status = execute_internal<ColumnInt64, true>(column, data_type, column_result);
            } else if (nested_which.is_float32()) {
                status = execute_internal<ColumnFloat32, true>(column, data_type, column_result);
            } else if (nested_which.is_float64()) {
                status = execute_internal<ColumnFloat64, true>(column, data_type, column_result);
            } else {
                return type_error();
            }
        } else {
            if (which.is_string_or_fixed_string()) {
                status = execute_internal<ColumnString, false>(column, data_type, column_result);
            } else if (which.is_int64()) {
                status = execute_internal<ColumnInt64, false>(column, data_type, column_result);
            } else if (which.is_float32()) {
                status = execute_internal<ColumnFloat32, false>(column, data_type, column_result);
            } else if (which.is_float64()) {
                status = execute_internal<ColumnFloat64, false>(column, data_type, column_result);
            } else {
                return type_error();
            }
        }
        if (status.ok()) {
            block.replace_by_position(result, std::move(column_result));
        }
        return status;
    }

private:
    float compression = 2048;
};

template <typename InternalType>
class FunctionQuantileStatePercent : public IFunction {
public:
    static constexpr auto name = "quantile_percent";
    String get_name() const override { return name; }

    static FunctionPtr create() {
        return std::make_shared<FunctionQuantileStatePercent<InternalType>>();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeFloat64>();
    }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto res_data_column = ColumnFloat64::create();
        auto& res = res_data_column->get_data();
        auto data_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto& null_map = data_null_map->get_data();

        auto column = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*column)) {
            VectorizedUtils::update_null_map(null_map, nullable->get_null_map_data());
            column = nullable->get_nested_column_ptr();
        }
        auto str_col = assert_cast<const ColumnQuantileState<InternalType>*>(column.get());
        auto& col_data = str_col->get_data();
        auto percent_arg = check_and_get_column_const<ColumnFloat32>(
                block.get_by_position(arguments.back()).column);

        if (!percent_arg) {
            LOG(FATAL) << fmt::format(
                    "Second argument to {} must be a constant string describing type", get_name());
        }
        float percent_arg_value = percent_arg->get_value<Float32>();
        if (percent_arg_value < 0 || percent_arg_value > 1) {
            return Status::InternalError(
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

using FunctionQuantileStateEmpty = FunctionConst<QuantileStateEmpty<double>, false>;
using FunctionQuantileStatePercentDouble = FunctionQuantileStatePercent<double>;
using FunctionToQuantileStateDouble = FunctionToQuantileState<double>;

void register_function_quantile_state(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionQuantileStateEmpty>();
    factory.register_function<FunctionQuantileStatePercentDouble>();
    factory.register_function<FunctionToQuantileStateDouble>();
}

} // namespace doris::vectorized
