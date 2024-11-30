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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayCumSum.cpp
// and modified by Doris

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

// array_cum_sum([1, 2, 3, 4, 5]) -> [1, 3, 6, 10, 15]
// array_cum_sum([1, NULL, 3, NULL, 5]) -> [1, NULL, 4, NULL, 9]
class FunctionArrayCumSum : public IFunction {
public:
    using NullMapType = PaddedPODArray<UInt8>;

    static constexpr auto name = "array_cum_sum";

    static FunctionPtr create() { return std::make_shared<FunctionArrayCumSum>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "argument for function: " << name << " should be DataTypeArray but it has type "
                << arguments[0]->get_name() << ".";
        auto nested_type = assert_cast<const DataTypeArray&>(*(arguments[0])).get_nested_type();

        auto non_null_nested_type = remove_nullable(nested_type);
        auto scale = get_decimal_scale(*non_null_nested_type);
        WhichDataType which(non_null_nested_type);

        //return type is promoted to prevent result overflow
        //like: input is int32 ---> return type will be int64
        DataTypePtr return_type = nullptr;

        // same cast logic as array_sum
        if (which.is_uint8() || which.is_int8() || which.is_uint16() || which.is_int16() ||
            which.is_uint32() || which.is_int32() || which.is_int64() || which.is_uint64()) {
            return_type = std::make_shared<DataTypeInt64>();
        } else if (which.is_int128()) {
            return_type = std::make_shared<DataTypeInt128>();
        } else if (which.is_float32() || which.is_float64()) {
            return_type = std::make_shared<DataTypeFloat64>();
        } else if (which.is_decimal128v2() && !which.is_decimal128v3()) {
            // decimalv2
            return_type = std::make_shared<DataTypeDecimal<Decimal128V2>>(
                    DataTypeDecimal<Decimal128V2>::max_precision(), scale);
        } else if (which.is_decimal()) {
            return_type = std::make_shared<DataTypeDecimal<Decimal128V3>>(
                    DataTypeDecimal<Decimal128V3>::max_precision(), scale);
        }
        if (return_type) {
            return std::make_shared<DataTypeArray>(make_nullable(return_type));
        } else {
            throw doris::Exception(
                    ErrorCode::INVALID_ARGUMENT,
                    "Function of {}, return type get wrong: and input argument is: {}", name,
                    arguments[0]->get_name());
        }

        return nullptr;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        const size_t result, size_t input_rows_count) const override {
        auto src_arg = block.get_by_position(arguments[0]);
        ColumnPtr src_column = src_arg.column->convert_to_full_column_if_const();

        const auto& src_column_array = check_and_get_column<ColumnArray>(src_column.get());
        if (!src_column_array) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name()));
        }

        const auto& src_offsets = src_column_array->get_offsets();
        const auto* src_nested_column = &src_column_array->get_data();
        DCHECK(src_nested_column != nullptr);

        // get src nested column
        auto src_nested_type = assert_cast<const DataTypeArray&>(*src_arg.type).get_nested_type();

        // get null map
        const ColumnNullable* src_nested_nullable_col =
                check_and_get_column<ColumnNullable>(*src_nested_column);
        src_nested_column = src_nested_nullable_col->get_nested_column_ptr();
        const NullMapType& src_null_map = src_nested_nullable_col->get_null_map_column().get_data();

        ColumnPtr res_nested_ptr;
        auto res_val = _execute_by_type(src_nested_type, *src_nested_column, src_offsets,
                                        src_null_map, res_nested_ptr);
        if (!res_val) {
            return Status::RuntimeError(
                    fmt::format("execute failed or unsupported types for function {}({})",
                                get_name(), block.get_by_position(arguments[0]).type->get_name()));
        }

        ColumnPtr res_array_ptr =
                ColumnArray::create(res_nested_ptr, src_column_array->get_offsets_ptr());

        block.replace_by_position(result, std::move(res_array_ptr));
        return Status::OK();
    }

private:
    bool _execute_by_type(DataTypePtr src_nested_type, const IColumn& src_column,
                          const ColumnArray::Offsets64& src_offsets,
                          const NullMapType& src_null_map, ColumnPtr& res_nested_ptr) const {
        bool res = false;
        WhichDataType which(remove_nullable(src_nested_type));
        if (which.is_uint8()) {
            res = _execute_number<UInt8, Int64>(src_column, src_offsets, src_null_map,
                                                res_nested_ptr);
        } else if (which.is_int8()) {
            res = _execute_number<Int8, Int64>(src_column, src_offsets, src_null_map,
                                               res_nested_ptr);
        } else if (which.is_int16()) {
            res = _execute_number<Int16, Int64>(src_column, src_offsets, src_null_map,
                                                res_nested_ptr);
        } else if (which.is_int32()) {
            res = _execute_number<Int32, Int64>(src_column, src_offsets, src_null_map,
                                                res_nested_ptr);
        } else if (which.is_int64()) {
            res = _execute_number<Int64, Int64>(src_column, src_offsets, src_null_map,
                                                res_nested_ptr);
        } else if (which.is_int128()) {
            res = _execute_number<Int128, Int128>(src_column, src_offsets, src_null_map,
                                                  res_nested_ptr);
        } else if (which.is_float32()) {
            res = _execute_number<Float32, Float64>(src_column, src_offsets, src_null_map,
                                                    res_nested_ptr);
        } else if (which.is_float64()) {
            res = _execute_number<Float64, Float64>(src_column, src_offsets, src_null_map,
                                                    res_nested_ptr);
        } else if (which.is_decimal32()) {
            res = _execute_number<Decimal32, Decimal128V3>(src_column, src_offsets, src_null_map,
                                                           res_nested_ptr);
        } else if (which.is_decimal64()) {
            res = _execute_number<Decimal64, Decimal128V3>(src_column, src_offsets, src_null_map,
                                                           res_nested_ptr);
        } else if (which.is_decimal128v3()) {
            res = _execute_number<Decimal128V3, Decimal128V3>(src_column, src_offsets, src_null_map,
                                                              res_nested_ptr);
        } else if (which.is_decimal128v2()) {
            res = _execute_number<Decimal128V2, Decimal128V2>(src_column, src_offsets, src_null_map,
                                                              res_nested_ptr);
        }

        return res;
    }

    template <typename Element, typename Result>
    bool _execute_number(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                         const NullMapType& src_null_map, ColumnPtr& res_nested_ptr) const {
        using ColVecType = ColumnVectorOrDecimal<Element>;
        using ColVecResult = ColumnVectorOrDecimal<Result>;

        // 1. get pod array from src
        auto src_column_concrete = reinterpret_cast<const ColVecType*>(&src_column);
        if (!src_column_concrete) {
            return false;
        }

        // 2. construct result data
        typename ColVecResult::MutablePtr res_nested_mut_ptr = nullptr;
        if constexpr (IsDecimalNumber<Result>) {
            res_nested_mut_ptr = ColVecResult::create(0, src_column_concrete->get_scale());
        } else {
            res_nested_mut_ptr = ColVecResult::create();
        }

        // get result data pod array
        auto size = src_column.size();
        auto& res_datas = res_nested_mut_ptr->get_data();
        res_datas.resize(size);

        _compute_cum_sum<Element, Result>(src_column_concrete->get_data(), src_offsets,
                                          src_null_map, res_datas);

        auto res_null_map_col = ColumnUInt8::create(size, 0);
        auto& res_null_map = res_null_map_col->get_data();
        VectorizedUtils::update_null_map(res_null_map, src_null_map);
        res_nested_ptr =
                ColumnNullable::create(std::move(res_nested_mut_ptr), std::move(res_null_map_col));

        return true;
    }

    template <typename Element, typename Result>
    void _compute_cum_sum(const PaddedPODArray<Element>& src_datas,
                          const ColumnArray::Offsets64& src_offsets,
                          const NullMapType& src_null_map,
                          PaddedPODArray<Result>& res_datas) const {
        size_t prev_offset = 0;
        for (auto cur_offset : src_offsets) {
            // [1, null, 2, 3] -> [1, null, 3, 6]
            // [null, null, 1, 2, 3] -> [null, null, 1, 3, 6]
            Result accumulated {};

            for (size_t pos = prev_offset; pos < cur_offset; ++pos) {
                // skip null value
                if (src_null_map[pos]) {
                    res_datas[pos] = Result {};
                    continue;
                }

                accumulated += src_datas[pos];
                res_datas[pos] = accumulated;
            }

            prev_offset = cur_offset;
        }
    }
};

void register_function_array_cum_sum(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayCumSum>();
}

} // namespace doris::vectorized