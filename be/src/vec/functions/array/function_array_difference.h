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

#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/utils/util.hpp"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class FunctionArrayDifference : public IFunction {
public:
    static constexpr auto name = "array_difference";

    static FunctionPtr create() { return std::make_shared<FunctionArrayDifference>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "argument for function: " << name << " should be DataTypeArray but it has type "
                << arguments[0]->get_name() << ".";
        auto nested_type = assert_cast<const DataTypeArray&>(*(arguments[0])).get_nested_type();
        bool is_nullable = nested_type->is_nullable();

        WhichDataType which(remove_nullable(nested_type));
        //return type is promoted to prevent result overflow
        //like: input is int32 ---> return type will be int64
        DataTypePtr return_type = nullptr;
        if (which.is_uint8() || which.is_int8()) {
            return_type = std::make_shared<DataTypeInt16>();
        } else if (which.is_uint16() || which.is_int16()) {
            return_type = std::make_shared<DataTypeInt32>();
        } else if (which.is_uint32() || which.is_uint64() || which.is_int32()) {
            return_type = std::make_shared<DataTypeInt64>();
        } else if (which.is_int64() || which.is_int128()) {
            return_type = std::make_shared<DataTypeInt128>();
        } else if (which.is_float32() || which.is_float64()) {
            return_type = std::make_shared<DataTypeFloat64>();
        } else if (which.is_decimal()) {
            return arguments[0];
        }
        if (return_type) {
            return std::make_shared<DataTypeArray>(is_nullable ? make_nullable(return_type)
                                                               : return_type);
        } else {
            LOG(FATAL) << "Function of " << name
                       << " return type get wrong: and input argument is: "
                       << arguments[0]->get_name();
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnWithTypeAndName& arg = block.get_by_position(arguments[0]);
        auto res_column = _execute_non_nullable(arg, input_rows_count);
        if (!res_column) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name()));
        }
        DCHECK_EQ(arg.column->size(), res_column->size());
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }

private:
    template <typename Element, typename Result>
    static void impl(const Element* __restrict src, Result* __restrict dst, size_t begin,
                     size_t end) {
        size_t curr_pos = begin;
        if (curr_pos < end) {
            Element prev_element = src[curr_pos];
            dst[curr_pos] = {};
            curr_pos++;
            Element curr_element = src[curr_pos];
            for (; curr_pos < end; ++curr_pos) {
                curr_element = src[curr_pos];
                dst[curr_pos] =
                        static_cast<Result>(curr_element) - static_cast<Result>(prev_element);
                prev_element = curr_element;
            }
        }
    }

    template <typename Element, typename Result>
    ColumnPtr _execute_number_expanded(const ColumnArray::Offsets64& offsets,
                                       const IColumn& nested_column,
                                       ColumnPtr nested_null_map) const {
        using ColVecType = ColumnVectorOrDecimal<Element>;
        using ColVecResult = ColumnVectorOrDecimal<Result>;
        typename ColVecResult::MutablePtr res_nested = nullptr;

        const auto& src_data = reinterpret_cast<const ColVecType&>(nested_column).get_data();
        if constexpr (IsDecimalNumber<Result>) {
            res_nested = ColVecResult::create(0, src_data.get_scale());
        } else {
            res_nested = ColVecResult::create();
        }
        auto size = nested_column.size();
        typename ColVecResult::Container& res_values = res_nested->get_data();
        res_values.resize(size);

        size_t pos = 0;
        for (auto offset : offsets) {
            impl(src_data.data(), res_values.data(), pos, offset);
            pos = offset;
        }
        if (nested_null_map) {
            auto null_map_col = ColumnUInt8::create(size, 0);
            auto& null_map_col_data = null_map_col->get_data();
            auto nested_colum_data = static_cast<const ColumnVector<UInt8>*>(nested_null_map.get());
            VectorizedUtils::update_null_map(null_map_col_data, nested_colum_data->get_data());
            for (size_t row = 0; row < offsets.size(); ++row) {
                auto off = offsets[row - 1];
                auto len = offsets[row] - off;
                auto pos = len ? len - 1 : 0;
                for (; pos > 0; --pos) {
                    if (null_map_col_data[pos + off - 1]) {
                        null_map_col_data[pos + off] = 1;
                    }
                }
            }
            return ColumnNullable::create(std::move(res_nested), std::move(null_map_col));
        } else {
            return res_nested;
        }
    }

    ColumnPtr _execute_non_nullable(const ColumnWithTypeAndName& arg,
                                    size_t input_rows_count) const {
        // check array nested column type and get data
        auto left_column = arg.column->convert_to_full_column_if_const();
        const auto& array_column = reinterpret_cast<const ColumnArray&>(*left_column);
        const auto& offsets = array_column.get_offsets();
        DCHECK(offsets.size() == input_rows_count);

        ColumnPtr nested_column = nullptr;
        ColumnPtr nested_null_map = nullptr;
        if (is_column_nullable(array_column.get_data())) {
            const auto& nested_null_column =
                    reinterpret_cast<const ColumnNullable&>(array_column.get_data());
            nested_column = nested_null_column.get_nested_column_ptr();
            nested_null_map = nested_null_column.get_null_map_column_ptr();
        } else {
            nested_column = array_column.get_data_ptr();
        }

        ColumnPtr res = nullptr;
        auto left_element_type =
                remove_nullable(assert_cast<const DataTypeArray&>(*arg.type).get_nested_type());
        if (check_column<ColumnUInt8>(*nested_column)) {
            res = _execute_number_expanded<UInt8, Int16>(offsets, *nested_column, nested_null_map);
        } else if (check_column<ColumnInt8>(*nested_column)) {
            res = _execute_number_expanded<Int8, Int16>(offsets, *nested_column, nested_null_map);
        } else if (check_column<ColumnInt16>(*nested_column)) {
            res = _execute_number_expanded<Int16, Int32>(offsets, *nested_column, nested_null_map);
        } else if (check_column<ColumnInt32>(*nested_column)) {
            res = _execute_number_expanded<Int32, Int64>(offsets, *nested_column, nested_null_map);
        } else if (check_column<ColumnInt64>(*nested_column)) {
            res = _execute_number_expanded<Int64, Int128>(offsets, *nested_column, nested_null_map);
        } else if (check_column<ColumnInt128>(*nested_column)) {
            res = _execute_number_expanded<Int128, Int128>(offsets, *nested_column,
                                                           nested_null_map);
        } else if (check_column<ColumnFloat32>(*nested_column)) {
            res = _execute_number_expanded<Float32, Float64>(offsets, *nested_column,
                                                             nested_null_map);
        } else if (check_column<ColumnFloat64>(*nested_column)) {
            res = _execute_number_expanded<Float64, Float64>(offsets, *nested_column,
                                                             nested_null_map);
        } else if (check_column<ColumnDecimal32>(*nested_column)) {
            res = _execute_number_expanded<Decimal32, Decimal32>(offsets, *nested_column,
                                                                 nested_null_map);
        } else if (check_column<ColumnDecimal64>(*nested_column)) {
            res = _execute_number_expanded<Decimal64, Decimal64>(offsets, *nested_column,
                                                                 nested_null_map);
        } else if (check_column<ColumnDecimal128I>(*nested_column)) {
            res = _execute_number_expanded<Decimal128I, Decimal128I>(offsets, *nested_column,
                                                                     nested_null_map);
        } else if (check_column<ColumnDecimal128>(*nested_column)) {
            res = _execute_number_expanded<Decimal128, Decimal128>(offsets, *nested_column,
                                                                   nested_null_map);
        }
        return ColumnArray::create(std::move(res), array_column.get_offsets_ptr());
    }
};

} // namespace doris::vectorized
