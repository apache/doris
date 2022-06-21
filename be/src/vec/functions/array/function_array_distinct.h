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

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/sip_hash.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

class FunctionArrayDistinct : public IFunction {
public:
    static constexpr auto name = "array_distinct";
    static FunctionPtr create() { return std::make_shared<FunctionArrayDistinct>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeArray";
        return make_nullable(
                check_and_get_data_type<DataTypeArray>(arguments[0].get())->get_nested_type());
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
        const auto& src_column_data = src_column_array->get_data();

        DataTypePtr src_column_type = remove_nullable(block.get_by_position(arguments[0]).type);
        auto nested_type = assert_cast<const DataTypeArray&>(*src_column_type).get_nested_type();
        auto dest_column_ptr = ColumnArray::create(nested_type->create_column(),
                                                   ColumnArray::ColumnOffsets::create());
        IColumn& dest_column = dest_column_ptr->get_data();
        ColumnArray::Offsets& dest_offsets = dest_column_ptr->get_offsets();

        const IColumn* src_inner_column = nullptr;
        const ColumnNullable* src_nullable_column =
                check_and_get_column<ColumnNullable>(src_column_data);
        if (src_nullable_column) {
            src_inner_column = src_nullable_column->get_nested_column_ptr();
        } else {
            src_inner_column = src_column_array->get_data_ptr();
        }

        IColumn* dest_nested_column = nullptr;
        ColumnNullable* dest_nullable_column = reinterpret_cast<ColumnNullable*>(&dest_column);
        if (dest_nullable_column) {
            dest_nested_column = dest_nullable_column->get_nested_column_ptr();
        } else {
            dest_nested_column = &dest_column;
        }

        auto res_val =
                _execute_by_type(*src_inner_column, src_offsets, *dest_nested_column, dest_offsets,
                                 src_nullable_column, dest_nullable_column, nested_type);
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

    template <typename T>
    bool _execute_number(const IColumn& src_column, const ColumnArray::Offsets& src_offsets,
                         IColumn& dest_column, ColumnArray::Offsets& dest_offsets,
                         const ColumnNullable* src_nullable_col,
                         ColumnNullable* dest_nullable_col) {
        const ColumnVector<T>* src_data_concrete =
                check_and_get_column<ColumnVector<T>>(&src_column);
        if (!src_data_concrete) {
            return false;
        }
        const PaddedPODArray<T>& src_datas = src_data_concrete->get_data();

        ColumnVector<T>& dest_data_concrete = reinterpret_cast<ColumnVector<T>&>(dest_column);
        PaddedPODArray<T>& dest_datas = dest_data_concrete.get_data();

        const PaddedPODArray<UInt8>* src_null_map = nullptr;
        if (src_nullable_col) {
            src_null_map = &src_nullable_col->get_null_map_column().get_data();
        }

        PaddedPODArray<UInt8>* dest_null_map = nullptr;
        if (dest_nullable_col) {
            dest_null_map = &dest_nullable_col->get_null_map_column().get_data();
        }

        // using Set = HashSetWithSavedHashWithStackMemory<T, DefaultHash<T>, INITIAL_SIZE_DEGREE>;
        using Set = HashSetWithSavedHashWithStackMemory<UInt128, UInt128TrivialHash,
                                                        INITIAL_SIZE_DEGREE>;
        Set set;

        ColumnArray::Offset prev_src_offset = 0;
        ColumnArray::Offset res_offset = 0;

        for (auto curr_src_offset : src_offsets) {
            set.clear();
            size_t null_size = 0;
            for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_nullable_col && (*src_null_map)[j]) {
                    if (dest_nullable_col && dest_null_map) {
                        (*dest_null_map).push_back(true);
                        // Note: here we need to add an element which will not use for output
                        // because we expand the value of each offset
                        dest_datas.push_back(-1);
                        null_size++;
                    }
                    continue;
                }

                UInt128 hash;
                SipHash hash_function;
                src_column.update_hash_with_value(j, hash_function);
                hash_function.get128(reinterpret_cast<char*>(&hash));

                if (!set.find(hash)) {
                    set.insert(hash);
                    dest_datas.push_back(src_datas[j]);
                    if (dest_nullable_col && dest_null_map) {
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

    bool _execute_string(const IColumn& src_column, const ColumnArray::Offsets& src_offsets,
                         IColumn& dest_column, ColumnArray::Offsets& dest_offsets,
                         const ColumnNullable* src_nullable_col,
                         ColumnNullable* dest_nullable_col) {
        const ColumnString* src_data_concrete = check_and_get_column<ColumnString>(&src_column);
        if (!src_data_concrete) {
            return false;
        }

        ColumnString& dest_column_string = reinterpret_cast<ColumnString&>(dest_column);
        ColumnString::Offsets& column_string_offsets = dest_column_string.get_offsets();

        const PaddedPODArray<UInt8>* src_null_map = nullptr;
        if (src_nullable_col) {
            src_null_map = &src_nullable_col->get_null_map_column().get_data();
        }

        PaddedPODArray<UInt8>* dest_null_map = nullptr;
        if (dest_nullable_col) {
            dest_null_map = &dest_nullable_col->get_null_map_column().get_data();
        }

        using Set =
                HashSetWithSavedHashWithStackMemory<StringRef, StringRefHash, INITIAL_SIZE_DEGREE>;
        Set set;

        ColumnArray::Offset prev_src_offset = 0;
        ColumnArray::Offset res_offset = 0;

        for (auto curr_src_offset : src_offsets) {
            set.clear();

            size_t null_size = 0;
            for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_nullable_col && (*src_null_map)[j]) {
                    if (dest_nullable_col && dest_null_map) {
                        column_string_offsets.push_back(column_string_offsets.back());
                        (*dest_null_map).push_back(true);
                        null_size++;
                    }
                    continue;
                }
                StringRef src_str_ref = src_data_concrete->get_data_at(j);

                if (!set.find(src_str_ref)) {
                    set.insert(src_str_ref);
                    dest_column_string.insert_data(src_str_ref.data, src_str_ref.size);
                    if (dest_nullable_col && dest_null_map) {
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

    bool _execute_by_type(const IColumn& src_column, const ColumnArray::Offsets& src_offsets,
                          IColumn& dest_column, ColumnArray::Offsets& dest_offsets,
                          const ColumnNullable* src_nullable_col, ColumnNullable* dest_nullable_col,
                          DataTypePtr& nested_type) {
        bool res = false;
        WhichDataType which(remove_nullable(nested_type)->get_type_id());
        if (which.idx == TypeIndex::UInt8) {
            res = _execute_number<UInt8>(src_column, src_offsets, dest_column, dest_offsets,
                                         src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::UInt16) {
            res = _execute_number<UInt16>(src_column, src_offsets, dest_column, dest_offsets,
                                          src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::UInt32) {
            res = _execute_number<UInt32>(src_column, src_offsets, dest_column, dest_offsets,
                                          src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::UInt64) {
            res = _execute_number<UInt64>(src_column, src_offsets, dest_column, dest_offsets,
                                          src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::UInt128) {
            res = _execute_number<UInt128>(src_column, src_offsets, dest_column, dest_offsets,
                                           src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::Int8) {
            res = _execute_number<Int8>(src_column, src_offsets, dest_column, dest_offsets,
                                        src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::Int16) {
            res = _execute_number<Int16>(src_column, src_offsets, dest_column, dest_offsets,
                                         src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::Int32) {
            res = _execute_number<Int32>(src_column, src_offsets, dest_column, dest_offsets,
                                         src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::Int64) {
            res = _execute_number<Int64>(src_column, src_offsets, dest_column, dest_offsets,
                                         src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::Int128) {
            res = _execute_number<Int128>(src_column, src_offsets, dest_column, dest_offsets,
                                          src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::Float32) {
            res = _execute_number<Float32>(src_column, src_offsets, dest_column, dest_offsets,
                                           src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::Float64) {
            res = _execute_number<Float64>(src_column, src_offsets, dest_column, dest_offsets,
                                           src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::Date) {
            res = _execute_number<Date>(src_column, src_offsets, dest_column, dest_offsets,
                                        src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::DateTime) {
            res = _execute_number<DateTime>(src_column, src_offsets, dest_column, dest_offsets,
                                            src_nullable_col, dest_nullable_col);
        } else if (which.idx == TypeIndex::String) {
            res = _execute_string(src_column, src_offsets, dest_column, dest_offsets,
                                  src_nullable_col, dest_nullable_col);
        }
        return res;
    }
};

} // namespace doris::vectorized