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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arraySort.cpp
// and modified by Doris
#pragma once

#include "vec/columns/column_array.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

class FunctionArraySort : public IFunction {
public:
    static constexpr auto name = "array_sort";
    static FunctionPtr create() { return std::make_shared<FunctionArraySort>(); }
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
                        size_t result, size_t input_rows_count) override {
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
            const ColumnNullable* src_nested_nullable_col =
                    check_and_get_column<ColumnNullable>(*src_nested_column);
            src_nested_column = src_nested_nullable_col->get_nested_column_ptr();
            src_null_map = &src_nested_nullable_col->get_null_map_column().get_data();
        }

        NullMapType* dest_null_map = nullptr;
        if (dest_nested_column->is_nullable()) {
            ColumnNullable* dest_nested_nullable_col =
                    reinterpret_cast<ColumnNullable*>(dest_nested_column);
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
    // sort the non-null element according to the permutation
    template <typename SrcDataType>
    void _sort_by_permutation(ColumnArray::Offset64& prev_offset,
                              const ColumnArray::Offset64& curr_offset,
                              const SrcDataType* src_data_concrete, const IColumn& src_column,
                              const NullMapType* src_null_map, IColumn::Permutation& permutation) {
        for (size_t j = prev_offset; j + 1 < curr_offset; ++j) {
            if (src_null_map && (*src_null_map)[j]) {
                continue;
            }
            for (size_t k = j + 1; k < curr_offset; ++k) {
                if (src_null_map && (*src_null_map)[k]) {
                    continue;
                }
                int result = src_data_concrete->compare_at(permutation[j], permutation[k],
                                                           src_column, 1);
                if (result > 0) {
                    auto temp = permutation[j];
                    permutation[j] = permutation[k];
                    permutation[k] = temp;
                }
            }
        }
        return;
    }

    template <typename ColumnType>
    bool _execute_number(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                         IColumn& dest_column, ColumnArray::Offsets64& dest_offsets,
                         const NullMapType* src_null_map, NullMapType* dest_null_map) {
        using NestType = typename ColumnType::value_type;
        const ColumnType* src_data_concrete = reinterpret_cast<const ColumnType*>(&src_column);
        if (!src_data_concrete) {
            return false;
        }
        const PaddedPODArray<NestType>& src_datas = src_data_concrete->get_data();

        ColumnType& dest_data_concrete = reinterpret_cast<ColumnType&>(dest_column);
        PaddedPODArray<NestType>& dest_datas = dest_data_concrete.get_data();

        ColumnArray::Offset64 prev_src_offset = 0;
        IColumn::Permutation permutation(src_column.size());
        for (size_t i = 0; i < src_column.size(); ++i) {
            permutation[i] = i;
        }

        for (auto curr_src_offset : src_offsets) {
            // filter and insert null element first
            for (size_t j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && (*src_null_map)[j]) {
                    DCHECK(dest_null_map != nullptr);
                    (*dest_null_map).push_back(true);
                    dest_datas.push_back(NestType());
                }
            }

            _sort_by_permutation<ColumnType>(prev_src_offset, curr_src_offset, src_data_concrete,
                                             src_column, src_null_map, permutation);

            // insert non-null element after sort by permutation
            for (size_t j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && (*src_null_map)[j]) {
                    continue;
                }

                dest_datas.push_back(src_datas[permutation[j]]);
                if (dest_null_map) {
                    (*dest_null_map).push_back(false);
                }
            }
            dest_offsets.push_back(curr_src_offset);
            prev_src_offset = curr_src_offset;
        }

        return true;
    }

    bool _execute_string(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                         IColumn& dest_column, ColumnArray::Offsets64& dest_offsets,
                         const NullMapType* src_null_map, NullMapType* dest_null_map) {
        const ColumnString* src_data_concrete = reinterpret_cast<const ColumnString*>(&src_column);
        if (!src_data_concrete) {
            return false;
        }

        ColumnString& dest_column_string = reinterpret_cast<ColumnString&>(dest_column);
        ColumnString::Chars& column_string_chars = dest_column_string.get_chars();
        ColumnString::Offsets& column_string_offsets = dest_column_string.get_offsets();
        column_string_chars.reserve(src_column.size());

        ColumnArray::Offset64 prev_src_offset = 0;
        IColumn::Permutation permutation(src_column.size());
        for (size_t i = 0; i < src_column.size(); ++i) {
            permutation[i] = i;
        }

        for (auto curr_src_offset : src_offsets) {
            // filter and insert null element first
            for (size_t j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && (*src_null_map)[j]) {
                    DCHECK(dest_null_map != nullptr);
                    column_string_offsets.push_back(column_string_offsets.back());
                    (*dest_null_map).push_back(true);
                }
            }

            _sort_by_permutation<ColumnString>(prev_src_offset, curr_src_offset, src_data_concrete,
                                               src_column, src_null_map, permutation);

            // insert non-null element after sort by permutation
            for (size_t j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && (*src_null_map)[j]) {
                    continue;
                }

                StringRef src_str_ref = src_data_concrete->get_data_at(permutation[j]);
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
            dest_offsets.push_back(curr_src_offset);
            prev_src_offset = curr_src_offset;
        }
        return true;
    }

    bool _execute_by_type(const IColumn& src_column, const ColumnArray::Offsets64& src_offsets,
                          IColumn& dest_column, ColumnArray::Offsets64& dest_offsets,
                          const NullMapType* src_null_map, NullMapType* dest_null_map,
                          DataTypePtr& nested_type) {
        bool res = false;
        WhichDataType which(remove_nullable(nested_type));
        if (which.is_uint8()) {
            res = _execute_number<ColumnUInt8>(src_column, src_offsets, dest_column, dest_offsets,
                                               src_null_map, dest_null_map);
        } else if (which.is_int8()) {
            res = _execute_number<ColumnInt8>(src_column, src_offsets, dest_column, dest_offsets,
                                              src_null_map, dest_null_map);
        } else if (which.is_int16()) {
            res = _execute_number<ColumnInt16>(src_column, src_offsets, dest_column, dest_offsets,
                                               src_null_map, dest_null_map);
        } else if (which.is_int32()) {
            res = _execute_number<ColumnInt32>(src_column, src_offsets, dest_column, dest_offsets,
                                               src_null_map, dest_null_map);
        } else if (which.is_int64()) {
            res = _execute_number<ColumnInt64>(src_column, src_offsets, dest_column, dest_offsets,
                                               src_null_map, dest_null_map);
        } else if (which.is_int128()) {
            res = _execute_number<ColumnInt128>(src_column, src_offsets, dest_column, dest_offsets,
                                                src_null_map, dest_null_map);
        } else if (which.is_float32()) {
            res = _execute_number<ColumnFloat32>(src_column, src_offsets, dest_column, dest_offsets,
                                                 src_null_map, dest_null_map);
        } else if (which.is_float64()) {
            res = _execute_number<ColumnFloat64>(src_column, src_offsets, dest_column, dest_offsets,
                                                 src_null_map, dest_null_map);
        } else if (which.is_date()) {
            res = _execute_number<ColumnDate>(src_column, src_offsets, dest_column, dest_offsets,
                                              src_null_map, dest_null_map);
        } else if (which.is_date_time()) {
            res = _execute_number<ColumnDateTime>(src_column, src_offsets, dest_column,
                                                  dest_offsets, src_null_map, dest_null_map);
        } else if (which.is_decimal128()) {
            res = _execute_number<ColumnDecimal128>(src_column, src_offsets, dest_column,
                                                    dest_offsets, src_null_map, dest_null_map);
        } else if (which.is_string()) {
            res = _execute_string(src_column, src_offsets, dest_column, dest_offsets, src_null_map,
                                  dest_null_map);
        }
        return res;
    }
};

} // namespace doris::vectorized
