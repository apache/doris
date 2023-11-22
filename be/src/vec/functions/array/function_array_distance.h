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

#pragma once

#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

class L1Distance {
public:
    static constexpr auto name = "l1_distance";
    struct State {
        double sum = 0;
    };
    static void accumulate(State& state, double x, double y) { state.sum += fabs(x - y); }
    static double finalize(const State& state) { return state.sum; }
};

class L2Distance {
public:
    static constexpr auto name = "l2_distance";
    struct State {
        double sum = 0;
    };
    static void accumulate(State& state, double x, double y) { state.sum += (x - y) * (x - y); }
    static double finalize(const State& state) { return sqrt(state.sum); }
};

class InnerProduct {
public:
    static constexpr auto name = "inner_product";
    struct State {
        double sum = 0;
    };
    static void accumulate(State& state, double x, double y) { state.sum += x * y; }
    static double finalize(const State& state) { return state.sum; }
};

class CosineDistance {
public:
    static constexpr auto name = "cosine_distance";
    struct State {
        double dot_prod = 0;
        double squared_x = 0;
        double squared_y = 0;
    };
    static void accumulate(State& state, double x, double y) {
        state.dot_prod += x * y;
        state.squared_x += x * x;
        state.squared_y += y * y;
    }
    static double finalize(const State& state) {
        return 1 - state.dot_prod / sqrt(state.squared_x * state.squared_y);
    }
};

template <typename DistanceImpl>
class FunctionArrayDistance : public IFunction {
public:
    static constexpr auto name = DistanceImpl::name;
    String get_name() const override { return name; }
    static FunctionPtr create() { return std::make_shared<FunctionArrayDistance<DistanceImpl>>(); }
    bool is_variadic() const override { return false; }
    size_t get_number_of_arguments() const override { return 2; }
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeFloat64>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& arg1 = block.get_by_position(arguments[0]);
        const auto& arg2 = block.get_by_position(arguments[1]);
        if (!_check_input_type(arg1.type) || !_check_input_type(arg2.type)) {
            return Status::RuntimeError(fmt::format("unsupported types for function {}({}, {})",
                                                    get_name(), arg1.type->get_name(),
                                                    arg2.type->get_name()));
        }

        auto col1 = arg1.column->convert_to_full_column_if_const();
        auto col2 = arg2.column->convert_to_full_column_if_const();
        if (col1->size() != col2->size()) {
            return Status::RuntimeError(
                    fmt::format("function {} have different input array sizes: {} and {}",
                                get_name(), col1->size(), col2->size()));
        }

        ColumnArrayExecutionData arr1;
        ColumnArrayExecutionData arr2;
        if (!extract_column_array_info(*col1, arr1) || !extract_column_array_info(*col2, arr2)) {
            return Status::RuntimeError(fmt::format("unsupported types for function {}({}, {})",
                                                    get_name(), arg1.type->get_name(),
                                                    arg2.type->get_name()));
        }

        // prepare return data
        auto dst = ColumnVector<Float64>::create(input_rows_count);
        auto& dst_data = dst->get_data();
        auto dst_null_column = ColumnUInt8::create(input_rows_count);
        auto& dst_null_data = dst_null_column->get_data();

        const auto& offsets1 = *arr1.offsets_ptr;
        const auto& offsets2 = *arr2.offsets_ptr;
        const auto& nested_col1 = arr1.nested_col;
        const auto& nested_col2 = arr2.nested_col;
        for (ssize_t row = 0; row < offsets1.size(); ++row) {
            if (arr1.array_nullmap_data && arr1.array_nullmap_data[row]) {
                dst_null_data[row] = true;
                continue;
            }
            if (arr2.array_nullmap_data && arr2.array_nullmap_data[row]) {
                dst_null_data[row] = true;
                continue;
            }

            dst_null_data[row] = false;
            if (offsets1[row] != offsets2[row]) [[unlikely]] {
                return Status::InvalidArgument(
                        "function {} have different input element sizes of array: {} and {}",
                        get_name(), offsets1[row] - offsets1[row - 1],
                        offsets2[row] - offsets2[row - 1]);
            }

            typename DistanceImpl::State st;
            for (ssize_t pos = offsets1[row - 1]; pos < offsets1[row]; ++pos) {
                if (arr1.nested_nullmap_data && arr1.nested_nullmap_data[pos]) {
                    dst_null_data[row] = true;
                    break;
                }
                if (arr2.nested_nullmap_data && arr2.nested_nullmap_data[pos]) {
                    dst_null_data[row] = true;
                    break;
                }
                DistanceImpl::accumulate(st, nested_col1->get_float64(pos),
                                         nested_col2->get_float64(pos));
            }
            if (!dst_null_data[row]) {
                dst_data[row] = DistanceImpl::finalize(st);
                dst_null_data[row] = std::isnan(dst_data[row]);
            }
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(dst), std::move(dst_null_column)));
        return Status::OK();
    }

private:
    bool _check_input_type(const DataTypePtr& type) const {
        auto array_type = remove_nullable(type);
        if (!is_array(array_type)) {
            return false;
        }
        auto nested_type =
                remove_nullable(assert_cast<const DataTypeArray&>(*array_type).get_nested_type());
        if (is_integer(nested_type) || is_float(nested_type)) {
            return true;
        }
        return false;
    }
};

} // namespace doris::vectorized
