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
#include <sys/types.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <new>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"

namespace doris {
class FunctionContext;

namespace vectorized {
class ColumnString;
} // namespace vectorized
} // namespace doris
template <typename, typename>
struct DefaultHash;

namespace doris::vectorized {

template <typename T>
struct OverlapSetImpl {
    using ElementNativeType = typename NativeType<typename T::value_type>::Type;
    using Set = HashSetWithStackMemory<ElementNativeType, DefaultHash<ElementNativeType>, 4>;
    Set set;
    void insert_array(const IColumn* column, size_t start, size_t size) {
        const auto& vec = assert_cast<const T&>(*column).get_data();
        for (size_t i = start; i < start + size; ++i) {
            set.insert(vec[i]);
        }
    }
    bool find_any(const IColumn* column, size_t start, size_t size) {
        const auto& vec = assert_cast<const T&>(*column).get_data();
        for (size_t i = start; i < start + size; ++i) {
            if (set.find(vec[i])) {
                return true;
            }
        }
        return false;
    }
};

template <>
struct OverlapSetImpl<ColumnString> {
    using Set = HashSetWithStackMemory<StringRef, DefaultHash<StringRef>, 4>;
    Set set;
    void insert_array(const IColumn* column, size_t start, size_t size) {
        for (size_t i = start; i < start + size; ++i) {
            set.insert(column->get_data_at(i));
        }
    }
    bool find_any(const IColumn* column, size_t start, size_t size) {
        for (size_t i = start; i < start + size; ++i) {
            if (set.find(column->get_data_at(i))) {
                return true;
            }
        }
        return false;
    }
};

class FunctionArraysOverlap : public IFunction {
public:
    static constexpr auto name = "arrays_overlap";
    static FunctionPtr create() { return std::make_shared<FunctionArraysOverlap>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool use_default_implementation_for_nulls() const override { return false; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        auto left_data_type = remove_nullable(arguments[0]);
        auto right_data_type = remove_nullable(arguments[1]);
        DCHECK(is_array(left_data_type)) << arguments[0]->get_name();
        DCHECK(is_array(right_data_type)) << arguments[1]->get_name();
        auto left_nested_type = remove_nullable(
                assert_cast<const DataTypeArray&>(*left_data_type).get_nested_type());
        auto right_nested_type = remove_nullable(
                assert_cast<const DataTypeArray&>(*right_data_type).get_nested_type());
        DCHECK(left_nested_type->equals(*right_nested_type))
                << "data type " << arguments[0]->get_name() << " not equal with "
                << arguments[1]->get_name();
        return make_nullable(std::make_shared<DataTypeUInt8>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto right_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        ColumnArrayExecutionData left_exec_data;
        ColumnArrayExecutionData right_exec_data;

        Status ret = Status::InvalidArgument(
                "execute failed, unsupported types for function {}({}, {})", get_name(),
                block.get_by_position(arguments[0]).type->get_name(),
                block.get_by_position(arguments[1]).type->get_name());

        // extract array column
        if (!extract_column_array_info(*left_column, left_exec_data) ||
            !extract_column_array_info(*right_column, right_exec_data)) {
            return ret;
        }
        // prepare return column
        auto dst_nested_col = ColumnVector<UInt8>::create(input_rows_count, 0);
        auto dst_null_map = ColumnVector<UInt8>::create(input_rows_count, 0);
        UInt8* dst_null_map_data = dst_null_map->get_data().data();

        // any array is null or any elements in array is null, return null
        RETURN_IF_ERROR(_execute_nullable(left_exec_data, dst_null_map_data));
        RETURN_IF_ERROR(_execute_nullable(right_exec_data, dst_null_map_data));

        // execute overlap check
        auto array_type = remove_nullable(block.get_by_position(arguments[0]).type);
        auto left_element_type =
                remove_nullable(assert_cast<const DataTypeArray&>(*array_type).get_nested_type());
        WhichDataType left_which_type(left_element_type);
        if (left_which_type.is_string()) {
            ret = _execute_internal<ColumnString>(left_exec_data, right_exec_data,
                                                  dst_null_map_data,
                                                  dst_nested_col->get_data().data());
        } else if (left_which_type.is_date()) {
            ret = _execute_internal<ColumnDate>(left_exec_data, right_exec_data, dst_null_map_data,
                                                dst_nested_col->get_data().data());
        } else if (left_which_type.is_date_time()) {
            ret = _execute_internal<ColumnDateTime>(left_exec_data, right_exec_data,
                                                    dst_null_map_data,
                                                    dst_nested_col->get_data().data());
        } else if (left_which_type.is_date_v2()) {
            ret = _execute_internal<ColumnDateV2>(left_exec_data, right_exec_data,
                                                  dst_null_map_data,
                                                  dst_nested_col->get_data().data());
        } else if (left_which_type.is_date_time_v2()) {
            ret = _execute_internal<ColumnDateTimeV2>(left_exec_data, right_exec_data,
                                                      dst_null_map_data,
                                                      dst_nested_col->get_data().data());
        } else if (left_which_type.is_uint8()) {
            ret = _execute_internal<ColumnUInt8>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data());
        } else if (left_which_type.is_int8()) {
            ret = _execute_internal<ColumnInt8>(left_exec_data, right_exec_data, dst_null_map_data,
                                                dst_nested_col->get_data().data());
        } else if (left_which_type.is_int16()) {
            ret = _execute_internal<ColumnInt16>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data());
        } else if (left_which_type.is_int32()) {
            ret = _execute_internal<ColumnInt32>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data());
        } else if (left_which_type.is_int64()) {
            ret = _execute_internal<ColumnInt64>(left_exec_data, right_exec_data, dst_null_map_data,
                                                 dst_nested_col->get_data().data());
        } else if (left_which_type.is_int128()) {
            ret = _execute_internal<ColumnInt128>(left_exec_data, right_exec_data,
                                                  dst_null_map_data,
                                                  dst_nested_col->get_data().data());
        } else if (left_which_type.is_float32()) {
            ret = _execute_internal<ColumnFloat32>(left_exec_data, right_exec_data,
                                                   dst_null_map_data,
                                                   dst_nested_col->get_data().data());
        } else if (left_which_type.is_float64()) {
            ret = _execute_internal<ColumnFloat64>(left_exec_data, right_exec_data,
                                                   dst_null_map_data,
                                                   dst_nested_col->get_data().data());
        } else if (left_which_type.is_decimal32()) {
            ret = _execute_internal<ColumnDecimal32>(left_exec_data, right_exec_data,
                                                     dst_null_map_data,
                                                     dst_nested_col->get_data().data());
        } else if (left_which_type.is_decimal64()) {
            ret = _execute_internal<ColumnDecimal64>(left_exec_data, right_exec_data,
                                                     dst_null_map_data,
                                                     dst_nested_col->get_data().data());
        } else if (left_which_type.is_decimal128v3()) {
            ret = _execute_internal<ColumnDecimal128V3>(left_exec_data, right_exec_data,
                                                        dst_null_map_data,
                                                        dst_nested_col->get_data().data());
        } else if (left_which_type.is_decimal128v2()) {
            ret = _execute_internal<ColumnDecimal128V2>(left_exec_data, right_exec_data,
                                                        dst_null_map_data,
                                                        dst_nested_col->get_data().data());
        } else if (left_which_type.is_decimal256()) {
            ret = _execute_internal<ColumnDecimal256>(left_exec_data, right_exec_data,
                                                      dst_null_map_data,
                                                      dst_nested_col->get_data().data());
        }

        if (ret.ok()) {
            block.replace_by_position(result, ColumnNullable::create(std::move(dst_nested_col),
                                                                     std::move(dst_null_map)));
        }

        return ret;
    }

private:
    Status _execute_nullable(const ColumnArrayExecutionData& data, UInt8* dst_nullmap_data) const {
        for (ssize_t row = 0; row < data.offsets_ptr->size(); ++row) {
            if (dst_nullmap_data[row]) {
                continue;
            }

            if (data.array_nullmap_data && data.array_nullmap_data[row]) {
                dst_nullmap_data[row] = 1;
                continue;
            }

            // any element inside array is NULL, return NULL
            if (data.nested_nullmap_data) {
                ssize_t start = (*data.offsets_ptr)[row - 1];
                ssize_t size = (*data.offsets_ptr)[row] - start;
                for (ssize_t i = start; i < start + size; ++i) {
                    if (data.nested_nullmap_data[i]) {
                        dst_nullmap_data[row] = 1;
                        break;
                    }
                }
            }
        }
        return Status::OK();
    }

    template <typename T>
    Status _execute_internal(const ColumnArrayExecutionData& left_data,
                             const ColumnArrayExecutionData& right_data,
                             const UInt8* dst_nullmap_data, UInt8* dst_data) const {
        using ExecutorImpl = OverlapSetImpl<T>;
        for (ssize_t row = 0; row < left_data.offsets_ptr->size(); ++row) {
            if (dst_nullmap_data[row]) {
                continue;
            }

            ssize_t left_start = (*left_data.offsets_ptr)[row - 1];
            ssize_t left_size = (*left_data.offsets_ptr)[row] - left_start;
            ssize_t right_start = (*right_data.offsets_ptr)[row - 1];
            ssize_t right_size = (*right_data.offsets_ptr)[row] - right_start;
            if (left_size == 0 || right_size == 0) {
                dst_data[row] = 0;
                continue;
            }

            ExecutorImpl impl;
            if (right_size < left_size) {
                impl.insert_array(right_data.nested_col, right_start, right_size);
                dst_data[row] = impl.find_any(left_data.nested_col, left_start, left_size);
            } else {
                impl.insert_array(left_data.nested_col, left_start, left_size);
                dst_data[row] = impl.find_any(right_data.nested_col, right_start, right_size);
            }
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
