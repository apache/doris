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

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments[0]->get_primitive_type() == TYPE_ARRAY)
                << "argument for function: " << name << " should be DataTypeArray but it has type "
                << arguments[0]->get_name() << ".";
        auto nested_type = assert_cast<const DataTypeArray&>(*(arguments[0])).get_nested_type();
        bool is_nullable = nested_type->is_nullable();

        //return type is promoted to prevent result overflow
        //like: input is int32 ---> return type will be int64
        DataTypePtr return_type = nullptr;
        switch (nested_type->get_primitive_type()) {
        case TYPE_BOOLEAN:
        case TYPE_TINYINT:
            return_type = std::make_shared<DataTypeInt16>();
            break;
        case TYPE_SMALLINT:
            return_type = std::make_shared<DataTypeInt32>();
            break;
        case TYPE_INT:
            return_type = std::make_shared<DataTypeInt64>();
            break;
        case TYPE_BIGINT:
        case TYPE_LARGEINT:
            return_type = std::make_shared<DataTypeInt128>();
            break;
        case TYPE_FLOAT:
        case TYPE_DOUBLE:
            return_type = std::make_shared<DataTypeFloat64>();
            break;
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL128I:
        case TYPE_DECIMAL256:
            return arguments[0];
        default:
            break;
        }
        if (return_type) {
            return std::make_shared<DataTypeArray>(is_nullable ? make_nullable(return_type)
                                                               : return_type);
        }
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "Function of {}, return type get wrong: and input argument is: {}",
                               name, arguments[0]->get_name());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
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
    NO_SANITIZE_UNDEFINED static void impl(const Element* __restrict src, Result* __restrict dst,
                                           size_t begin, size_t end) {
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

    template <PrimitiveType Element, PrimitiveType Result>
    ColumnPtr _execute_number_expanded(const ColumnArray::Offsets64& offsets,
                                       const IColumn& nested_column,
                                       ColumnPtr nested_null_map) const {
        using ColVecType = typename PrimitiveTypeTraits<Element>::ColumnType;
        using ColVecResult = typename PrimitiveTypeTraits<Result>::ColumnType;
        typename ColVecResult::MutablePtr res_nested = nullptr;

        const auto& src_data = reinterpret_cast<const ColVecType&>(nested_column).get_data();
        if constexpr (is_decimal(Result)) {
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
            auto nested_colum_data = static_cast<const ColumnUInt8*>(nested_null_map.get());
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
        switch (left_element_type->get_primitive_type()) {
        case TYPE_BOOLEAN:
            res = _execute_number_expanded<TYPE_BOOLEAN, TYPE_SMALLINT>(offsets, *nested_column,
                                                                        nested_null_map);
            break;
        case TYPE_TINYINT:
            res = _execute_number_expanded<TYPE_TINYINT, TYPE_SMALLINT>(offsets, *nested_column,
                                                                        nested_null_map);
            break;
        case TYPE_SMALLINT:
            res = _execute_number_expanded<TYPE_SMALLINT, TYPE_INT>(offsets, *nested_column,
                                                                    nested_null_map);
            break;
        case TYPE_INT:
            res = _execute_number_expanded<TYPE_INT, TYPE_BIGINT>(offsets, *nested_column,
                                                                  nested_null_map);
            break;
        case TYPE_BIGINT:
            res = _execute_number_expanded<TYPE_BIGINT, TYPE_LARGEINT>(offsets, *nested_column,
                                                                       nested_null_map);
            break;
        case TYPE_LARGEINT:
            res = _execute_number_expanded<TYPE_LARGEINT, TYPE_LARGEINT>(offsets, *nested_column,
                                                                         nested_null_map);
            break;
        case TYPE_FLOAT:
            res = _execute_number_expanded<TYPE_FLOAT, TYPE_DOUBLE>(offsets, *nested_column,
                                                                    nested_null_map);
            break;
        case TYPE_DOUBLE:
            res = _execute_number_expanded<TYPE_DOUBLE, TYPE_DOUBLE>(offsets, *nested_column,
                                                                     nested_null_map);
            break;
        case TYPE_DECIMAL32:
            res = _execute_number_expanded<TYPE_DECIMAL32, TYPE_DECIMAL32>(offsets, *nested_column,
                                                                           nested_null_map);
            break;
        case TYPE_DECIMAL64:
            res = _execute_number_expanded<TYPE_DECIMAL64, TYPE_DECIMAL64>(offsets, *nested_column,
                                                                           nested_null_map);
            break;
        case TYPE_DECIMAL128I:
            res = _execute_number_expanded<TYPE_DECIMAL128I, TYPE_DECIMAL128I>(
                    offsets, *nested_column, nested_null_map);
            break;
        case TYPE_DECIMALV2:
            res = _execute_number_expanded<TYPE_DECIMALV2, TYPE_DECIMALV2>(offsets, *nested_column,
                                                                           nested_null_map);
            break;
        case TYPE_DECIMAL256:
            res = _execute_number_expanded<TYPE_DECIMAL256, TYPE_DECIMAL256>(
                    offsets, *nested_column, nested_null_map);
            break;
        default:
            return nullptr;
        }
        return ColumnArray::create(res, array_column.get_offsets_ptr());
    }
};

} // namespace doris::vectorized
