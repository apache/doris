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

#include "common/logging.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
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
template <bool enable_decimal256>
class FunctionArrayCumSum : public IFunction {
public:
    using NullMapType = PaddedPODArray<UInt8>;

    static constexpr auto name = enable_decimal256 ? "array_cum_sum_decimal256" : "array_cum_sum";

    static FunctionPtr create() {
        return std::make_shared<FunctionArrayCumSum<enable_decimal256>>();
    }

    using DecimalResultType = std::conditional_t<enable_decimal256, Decimal256, Decimal128V3>;

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments[0]->get_primitive_type() == TYPE_ARRAY)
                << "argument for function: " << name << " should be DataTypeArray but it has type "
                << arguments[0]->get_name() << ".";
        auto nested_type = assert_cast<const DataTypeArray&>(*(arguments[0])).get_nested_type();

        DataTypePtr return_type = nullptr;
        switch (nested_type->get_primitive_type()) {
        case PrimitiveType::TYPE_BOOLEAN:
        case PrimitiveType::TYPE_TINYINT:
        case PrimitiveType::TYPE_SMALLINT:
        case PrimitiveType::TYPE_INT:
        case PrimitiveType::TYPE_BIGINT: {
            return_type = std::make_shared<DataTypeInt64>();
            break;
        }
        case PrimitiveType::TYPE_LARGEINT: {
            return_type = std::make_shared<DataTypeInt128>();
            break;
        }
        case PrimitiveType::TYPE_FLOAT:
        case PrimitiveType::TYPE_DOUBLE: {
            return_type = std::make_shared<DataTypeFloat64>();
            break;
        }
        case PrimitiveType::TYPE_DECIMALV2:
            return_type = std::make_shared<DataTypeDecimalV2>(DataTypeDecimalV2::max_precision(),
                                                              nested_type->get_scale());
            break;
        case PrimitiveType::TYPE_DECIMAL32:
            return_type = std::make_shared<DataTypeDecimal32>(DataTypeDecimal32::max_precision(),
                                                              nested_type->get_scale());
            break;
        case PrimitiveType::TYPE_DECIMAL64:
            return_type = std::make_shared<DataTypeDecimal64>(DataTypeDecimal64::max_precision(),
                                                              nested_type->get_scale());
            break;
        case PrimitiveType::TYPE_DECIMAL128I:
            return_type = std::make_shared<DataTypeDecimal128>(DataTypeDecimal128::max_precision(),
                                                               nested_type->get_scale());
            break;
        case PrimitiveType::TYPE_DECIMAL256:
            return_type = std::make_shared<DataTypeDecimal256>(DataTypeDecimal256::max_precision(),
                                                               nested_type->get_scale());
            break;
        default:
            break;
        }

        if (return_type) {
            return std::make_shared<DataTypeArray>(make_nullable(return_type));
        }
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "Function of {}, return type get wrong: and input argument is: {}",
                               name, arguments[0]->get_name());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        const uint32_t result, size_t input_rows_count) const override {
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
        src_nested_column = src_nested_nullable_col->get_nested_column_ptr().get();
        const NullMapType& src_null_map = src_nested_nullable_col->get_null_map_column().get_data();

        ColumnPtr res_nested_ptr;
        auto res_val = _execute_by_type(src_nested_type, *src_nested_column, src_offsets,
                                        src_null_map, res_nested_ptr);
        if (!res_val) {
            return Status::InvalidArgument(
                    "execute failed or unsupported types for function {}({})", get_name(),
                    block.get_by_position(arguments[0]).type->get_name());
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
        switch (src_nested_type->get_primitive_type()) {
        case TYPE_BOOLEAN:
            res = _execute_number<TYPE_BOOLEAN, TYPE_BIGINT>(src_column, src_offsets, src_null_map,
                                                             res_nested_ptr);
            break;
        case TYPE_TINYINT:
            res = _execute_number<TYPE_TINYINT, TYPE_BIGINT>(src_column, src_offsets, src_null_map,
                                                             res_nested_ptr);
            break;
        case TYPE_SMALLINT:
            res = _execute_number<TYPE_SMALLINT, TYPE_BIGINT>(src_column, src_offsets, src_null_map,
                                                              res_nested_ptr);
            break;
        case TYPE_INT:
            res = _execute_number<TYPE_INT, TYPE_BIGINT>(src_column, src_offsets, src_null_map,
                                                         res_nested_ptr);
            break;
        case TYPE_BIGINT:
            res = _execute_number<TYPE_BIGINT, TYPE_BIGINT>(src_column, src_offsets, src_null_map,
                                                            res_nested_ptr);
            break;
        case TYPE_LARGEINT:
            res = _execute_number<TYPE_LARGEINT, TYPE_LARGEINT>(src_column, src_offsets,
                                                                src_null_map, res_nested_ptr);
            break;
        case TYPE_FLOAT:
            res = _execute_number<TYPE_FLOAT, TYPE_DOUBLE>(src_column, src_offsets, src_null_map,
                                                           res_nested_ptr);
            break;
        case TYPE_DOUBLE:
            res = _execute_number<TYPE_DOUBLE, TYPE_DOUBLE>(src_column, src_offsets, src_null_map,
                                                            res_nested_ptr);
            break;
        case TYPE_DECIMAL32:
            res = _execute_number<TYPE_DECIMAL32, DecimalResultType::PType>(
                    src_column, src_offsets, src_null_map, res_nested_ptr);
            break;
        case TYPE_DECIMAL64:
            res = _execute_number<TYPE_DECIMAL64, DecimalResultType::PType>(
                    src_column, src_offsets, src_null_map, res_nested_ptr);
            break;
        case TYPE_DECIMAL128I:
            res = _execute_number<TYPE_DECIMAL128I, DecimalResultType::PType>(
                    src_column, src_offsets, src_null_map, res_nested_ptr);
            break;
        case TYPE_DECIMAL256:
            res = _execute_number<TYPE_DECIMAL256, DecimalResultType::PType>(
                    src_column, src_offsets, src_null_map, res_nested_ptr);
            break;
        case TYPE_DECIMALV2:
            res = _execute_number<TYPE_DECIMALV2, TYPE_DECIMALV2>(src_column, src_offsets,
                                                                  src_null_map, res_nested_ptr);
            break;
        default:
            break;
        }
        return res;
    }

    template <PrimitiveType Element, PrimitiveType Result>
    bool _execute_number(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                         const NullMapType& src_null_map, ColumnPtr& res_nested_ptr) const {
        if constexpr (Element == TYPE_DECIMAL256 && Result != TYPE_DECIMAL256) {
            return false;
        }
        using ColVecType = typename PrimitiveTypeTraits<Element>::ColumnType;
        using ColVecResult = typename PrimitiveTypeTraits<Result>::ColumnType;

        // 1. get pod array from src
        auto src_column_concrete = assert_cast<const ColVecType*>(&src_column);
        if (!src_column_concrete) {
            return false;
        }

        // 2. construct result data
        typename ColVecResult::MutablePtr res_nested_mut_ptr = nullptr;
        if constexpr (is_decimal(Result)) {
            res_nested_mut_ptr = ColVecResult::create(0, src_column_concrete->get_scale());
        } else {
            res_nested_mut_ptr = ColVecResult::create();
        }

        // get result data pod array
        auto size = src_column.size();
        auto& res_datas = res_nested_mut_ptr->get_data();
        res_datas.resize(size);

        // 3. compute cum sum and null map
        _compute_cum_sum<Result>(src_column_concrete->get_data(), src_offsets, src_null_map,
                                 res_datas);

        // handle null value in res_datas for first null value
        auto res_null_map_col = ColumnUInt8::create(size, 0);
        size_t first_not_null_pos = VectorizedUtils::find_first_valid_simd(src_null_map, 0, size);
        VLOG_DEBUG << "first_not_null_pos: " << std::to_string(first_not_null_pos);
        VectorizedUtils::range_set_nullmap_to_true_simd(res_null_map_col->get_data(), 0,
                                                        first_not_null_pos);

        res_nested_ptr =
                ColumnNullable::create(std::move(res_nested_mut_ptr), std::move(res_null_map_col));

        return true;
    }

    template <PrimitiveType Result>
    void _compute_cum_sum(const auto& src_datas, const ColumnArray::Offsets64& src_offsets,
                          const NullMapType& src_null_map, auto& res_datas) const {
        size_t prev_offset = 0;
        for (auto cur_offset : src_offsets) {
            // [1, null, 2, 3] -> [1, 1, 3, 6]
            // [1, null, null, 3] -> [1, 1, 1, 4]
            // [null, null, 1, 2, 3] -> [null, null, 1, 3, 6]
            // [null, 1, null, 2, 3] -> [null, 1, 1, 3, 6]
            // [null, null, null, null] -> [null, null, null, null]
            typename PrimitiveTypeTraits<Result>::ColumnItemType accumulated {};

            for (size_t pos = prev_offset; pos < cur_offset; ++pos) {
                // treat null value as 0
                if (src_null_map[pos]) {
                    accumulated += typename PrimitiveTypeTraits<Result>::ColumnItemType(0);
                } else {
                    accumulated +=
                            typename PrimitiveTypeTraits<Result>::ColumnItemType(src_datas[pos]);
                }

                res_datas[pos] = accumulated;
            }

            prev_offset = cur_offset;
        }
    }
};

void register_function_array_cum_sum(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayCumSum<false>>(FunctionArrayCumSum<false>::name);
    factory.register_function<FunctionArrayCumSum<true>>(FunctionArrayCumSum<true>::name);
}

} // namespace doris::vectorized