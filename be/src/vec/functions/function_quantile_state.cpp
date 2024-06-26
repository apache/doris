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

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "util/quantile_state.h"
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
            double value = (double)col->get_data()[i];
            res_data[i].set_compression(compression);
            res_data[i].add_value(value);
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnPtr& column = block.get_by_position(arguments[0]).column;
        const DataTypePtr& data_type = block.get_by_position(arguments[0]).type;
        auto compression_arg = check_and_get_column_const<ColumnFloat32>(
                block.get_by_position(arguments.back()).column);
        float compression = 2048;
        if (compression_arg) {
            auto compression_arg_val = compression_arg->get_value<Float32>();
            if (compression_arg_val >= QUANTILE_STATE_COMPRESSION_MIN &&
                compression_arg_val <= QUANTILE_STATE_COMPRESSION_MAX) {
                compression = compression_arg_val;
            }
        }
        WhichDataType which(data_type);
        MutableColumnPtr column_result = get_return_type_impl({})->create_column();
        column_result->resize(input_rows_count);

        Status status = Status::OK();
        if (which.is_nullable()) {
            const DataTypePtr& nested_data_type =
                    static_cast<const DataTypeNullable*>(data_type.get())->get_nested_type();
            WhichDataType nested_which(nested_data_type);
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
                        size_t result, size_t input_rows_count) const override {
        auto res_data_column = ColumnFloat64::create();
        auto& res = res_data_column->get_data();
        auto data_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto& null_map = data_null_map->get_data();

        auto column = block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        if (auto* nullable = check_and_get_column<const ColumnNullable>(*column)) {
            VectorizedUtils::update_null_map(null_map, nullable->get_null_map_data());
            column = nullable->get_nested_column_ptr();
        }
        auto str_col = assert_cast<const ColumnQuantileState*>(column.get());
        auto& col_data = str_col->get_data();
        auto percent_arg = check_and_get_column_const<ColumnFloat32>(
                block.get_by_position(arguments.back()).column);

        if (!percent_arg) {
            return Status::InternalError(
                    "Second argument to {} must be a constant float describing type", get_name());
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

void register_function_quantile_state(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionConst<QuantileStateEmpty, false>>();
    factory.register_function<FunctionQuantileStatePercent>();
    factory.register_function<FunctionToQuantileState>();
}

} // namespace doris::vectorized
