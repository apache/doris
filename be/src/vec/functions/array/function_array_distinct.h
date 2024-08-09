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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayDistinct.cpp
// and modified by Doris
#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstring>
#include <memory>
#include <ostream>
#include <utility>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"

namespace doris {
class FunctionContext;
} // namespace doris
template <typename, typename>
struct DefaultHash;

namespace doris::vectorized {

class FunctionArrayDistinct : public IFunction {
public:
    static constexpr auto name = "array_distinct";
    static FunctionPtr create() { return std::make_shared<FunctionArrayDistinct>(); }
    using NullMapType = PaddedPODArray<UInt8>;

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src_column_array = check_and_get_column<ColumnArray>(*src_column);
        if (!src_column_array) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name()));
        }
        const auto& src_offsets = src_column_array->get_offsets();
        const auto* src_nested_column = &src_column_array->get_data();
        DCHECK(src_nested_column != nullptr);

        DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
        auto nested_type = assert_cast<const DataTypeArray&>(*src_column_type).get_nested_type();
        auto dest_column_ptr = ColumnArray::create(nested_type->create_column(),
                                                   ColumnArray::ColumnOffsets::create());
        IColumn* dest_nested_column = &dest_column_ptr->get_data();
        auto& dest_offsets = dest_column_ptr->get_offsets();
        DCHECK(dest_nested_column != nullptr);
        dest_nested_column->reserve(src_nested_column->size());
        dest_offsets.reserve(input_rows_count);

        const NullMapType* src_null_map = nullptr;
        if (src_nested_column->is_nullable()) {
            const auto* src_nested_nullable_col =
                    check_and_get_column<ColumnNullable>(*src_nested_column);
            src_nested_column = src_nested_nullable_col->get_nested_column_ptr();
            src_null_map = &src_nested_nullable_col->get_null_map_column().get_data();
        }

        NullMapType* dest_null_map = nullptr;
        if (dest_nested_column->is_nullable()) {
            auto* dest_nested_nullable_col = reinterpret_cast<ColumnNullable*>(dest_nested_column);
            dest_nested_column = dest_nested_nullable_col->get_nested_column_ptr();
            dest_null_map = &dest_nested_nullable_col->get_null_map_column().get_data();
        }

        auto res_val = _execute_by_type(*src_nested_column, src_offsets, *dest_nested_column,
                                        dest_offsets, src_null_map, dest_null_map, nested_type);
        if (!res_val) {
            return Status::RuntimeError(
                    fmt::format("execute failed or unsupported types for function {}({})",
                                get_name(), block.get_by_position(arguments[0]).type->get_name()));
        }

        block.replace_by_position(result, std::move(dest_column_ptr));
        return Status::OK();
    }

private:
    // Note: Here initially allocate a piece of memory for 2^5 = 32 elements.
    static constexpr size_t INITIAL_SIZE_DEGREE = 5;

    template <typename ColumnType>
    bool _execute_number(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                         IColumn& dest_column, ColumnArray::Offsets64& dest_offsets,
                         const NullMapType* src_null_map, NullMapType* dest_null_map) const {
        using NestType = typename ColumnType::value_type;
        using ElementNativeType = typename NativeType<NestType>::Type;

        const auto* src_data_concrete = reinterpret_cast<const ColumnType*>(&src_column);
        if (!src_data_concrete) {
            return false;
        }
        const PaddedPODArray<NestType>& src_datas = src_data_concrete->get_data();

        auto& dest_data_concrete = reinterpret_cast<ColumnType&>(dest_column);
        PaddedPODArray<NestType>& dest_datas = dest_data_concrete.get_data();

        using Set = HashSetWithStackMemory<ElementNativeType, DefaultHash<ElementNativeType>,
                                           INITIAL_SIZE_DEGREE>;
        Set set;

        size_t prev_src_offset = 0;
        size_t res_offset = 0;

        for (auto curr_src_offset : src_offsets) {
            set.clear();
            size_t null_size = 0;
            for (size_t j = prev_src_offset; j < curr_src_offset; ++j) {
                if (null_size != 0 && src_null_map && (*src_null_map)[j]) {
                    // ignore duplicated nulls
                    continue;
                }
                if (src_null_map && (*src_null_map)[j]) {
                    DCHECK(dest_null_map != nullptr);
                    (*dest_null_map).push_back(true);
                    // Note: here we need to add an element which will not use for output
                    // because we expand the value of each offset
                    dest_datas.push_back(NestType());
                    null_size++;
                    continue;
                }

                if (!set.find(src_datas[j])) {
                    set.insert(src_datas[j]);
                    dest_datas.push_back(src_datas[j]);
                    if (dest_null_map) {
                        (*dest_null_map).push_back(false);
                    }
                }
            }

            res_offset += set.size() + null_size;
            dest_offsets.push_back(res_offset);
            prev_src_offset = curr_src_offset;
        }

        return true;
    }

    bool _execute_string(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                         IColumn& dest_column, ColumnArray::Offsets64& dest_offsets,
                         const NullMapType* src_null_map, NullMapType* dest_null_map) const {
        const auto* src_data_concrete = reinterpret_cast<const ColumnString*>(&src_column);
        if (!src_data_concrete) {
            return false;
        }

        auto& dest_column_string = reinterpret_cast<ColumnString&>(dest_column);
        ColumnString::Chars& column_string_chars = dest_column_string.get_chars();
        ColumnString::Offsets& column_string_offsets = dest_column_string.get_offsets();
        column_string_chars.reserve(src_column.size());

        using Set = HashSetWithStackMemory<StringRef, DefaultHash<StringRef>, INITIAL_SIZE_DEGREE>;
        Set set;

        size_t prev_src_offset = 0;
        size_t res_offset = 0;

        for (auto curr_src_offset : src_offsets) {
            set.clear();
            size_t null_size = 0;
            for (size_t j = prev_src_offset; j < curr_src_offset; ++j) {
                if (null_size != 0 && src_null_map && (*src_null_map)[j]) {
                    // ignore duplicated nulls
                    continue;
                }
                if (src_null_map && (*src_null_map)[j]) {
                    DCHECK(dest_null_map != nullptr);
                    // Note: here we need to update the offset of ColumnString
                    column_string_offsets.push_back(column_string_offsets.back());
                    (*dest_null_map).push_back(true);
                    null_size++;
                    continue;
                }

                StringRef src_str_ref = src_data_concrete->get_data_at(j);
                if (!set.find(src_str_ref)) {
                    set.insert(src_str_ref);
                    // copy the src data to column_string_chars
                    const size_t old_size = column_string_chars.size();
                    const size_t new_size = old_size + src_str_ref.size;
                    column_string_chars.resize(new_size);
                    if (src_str_ref.size > 0) {
                        memcpy(column_string_chars.data() + old_size, src_str_ref.data,
                               src_str_ref.size);
                    }
                    column_string_offsets.push_back(new_size);

                    if (dest_null_map) {
                        (*dest_null_map).push_back(false);
                    }
                }
            }

            res_offset += set.size() + null_size;
            dest_offsets.push_back(res_offset);
            prev_src_offset = curr_src_offset;
        }
        return true;
    }

    bool _execute_by_type(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                          IColumn& dest_column, ColumnArray::Offsets64& dest_offsets,
                          const NullMapType* src_null_map, NullMapType* dest_null_map,
                          DataTypePtr& nested_type) const {
#define EXECUTE_NUMBER(TYPE, NAME)                                                       \
    if (which.is_##NAME()) {                                                             \
        return _execute_number<TYPE>(src_column, src_offsets, dest_column, dest_offsets, \
                                     src_null_map, dest_null_map);                       \
    }

        WhichDataType which(remove_nullable(nested_type));
        EXECUTE_NUMBER(ColumnUInt8, uint8);
        EXECUTE_NUMBER(ColumnInt8, int8);
        EXECUTE_NUMBER(ColumnInt16, int16);
        EXECUTE_NUMBER(ColumnInt32, int32);
        EXECUTE_NUMBER(ColumnInt64, int64);
        EXECUTE_NUMBER(ColumnInt128, int128);
        EXECUTE_NUMBER(ColumnFloat32, float32);
        EXECUTE_NUMBER(ColumnFloat64, float64);
        EXECUTE_NUMBER(ColumnDate, date);
        EXECUTE_NUMBER(ColumnDateTime, date_time);
        EXECUTE_NUMBER(ColumnDateV2, date_v2);
        EXECUTE_NUMBER(ColumnDateTimeV2, date_time_v2);
        EXECUTE_NUMBER(ColumnDecimal32, decimal32);
        EXECUTE_NUMBER(ColumnDecimal64, decimal64);
        EXECUTE_NUMBER(ColumnDecimal128V3, decimal128v3);
        EXECUTE_NUMBER(ColumnDecimal256, decimal256);
        EXECUTE_NUMBER(ColumnDecimal128V2, decimal128v2);
        if (which.is_string()) {
            return _execute_string(src_column, src_offsets, dest_column, dest_offsets, src_null_map,
                                   dest_null_map);
        } else {
            LOG(ERROR) << "Unsupported array's element type: "
                       << remove_nullable(nested_type)->get_name() << " for function "
                       << this->get_name();
            return false;
        }

#undef EXECUTE_NUMBER
    }
};

} // namespace doris::vectorized
