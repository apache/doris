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

#include "vec/common/hash_table/hash_set.h"
#include "vec/common/hash_table/hash_table.h"
#include "vec/common/sip_hash.h"
#include "vec/functions/array/function_array_unary.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct NameArrayDistinct {
    static constexpr auto name = "array_distinct";
};

struct ArrayDistinctImpl {
    // Note: Here initially allocate a piece of memory for 2^5 = 32 elements.
    static constexpr size_t INITIAL_SIZE_DEGREE = 5;

    template <typename ColumnType>
    static bool _execute_number(const IColumn& src_column, const ColumnArray::Offsets& src_offsets,
                                IColumn& dest_column, ColumnArray::Offsets& dest_offsets,
                                const UInt8* src_null_map, ColumnUInt8::Container* dest_null_map) {
        using NestType = typename ColumnType::value_type;
        using ElementNativeType = typename NativeType<NestType>::Type;

        const ColumnType* src_data_concrete = reinterpret_cast<const ColumnType*>(&src_column);
        if (!src_data_concrete) {
            return false;
        }
        const PaddedPODArray<NestType>& src_datas = src_data_concrete->get_data();

        PaddedPODArray<NestType>& dest_datas =
                reinterpret_cast<ColumnType&>(dest_column).get_data();

        using Set = HashSetWithStackMemory<ElementNativeType, DefaultHash<ElementNativeType>,
                                           INITIAL_SIZE_DEGREE>;
        Set set;

        ColumnArray::Offset prev_src_offset = 0;
        ColumnArray::Offset res_offset = 0;

        for (auto curr_src_offset : src_offsets) {
            set.clear();
            size_t null_size = 0;
            for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && src_null_map[j]) {
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

    static bool _execute_string(const IColumn& src_column, const ColumnArray::Offsets& src_offsets,
                                IColumn& dest_column, ColumnArray::Offsets& dest_offsets,
                                const UInt8* src_null_map, ColumnUInt8::Container* dest_null_map) {
        const ColumnString* src_data_concrete = reinterpret_cast<const ColumnString*>(&src_column);
        if (!src_data_concrete) {
            return false;
        }

        ColumnString& dest_column_string = reinterpret_cast<ColumnString&>(dest_column);

        using Set = HashSetWithStackMemory<StringRef, DefaultHash<StringRef>, INITIAL_SIZE_DEGREE>;
        Set set;

        ColumnArray::Offset prev_src_offset = 0;
        ColumnArray::Offset res_offset = 0;

        for (auto curr_src_offset : src_offsets) {
            set.clear();
            size_t null_size = 0;
            for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && src_null_map[j]) {
                    DCHECK(dest_null_map != nullptr);
                    // Note: here we need to insert default
                    dest_column_string.insert_default();
                    (*dest_null_map).push_back(true);
                    null_size++;
                    continue;
                }

                StringRef src_str_ref = src_data_concrete->get_data_at(j);
                if (!set.find(src_str_ref)) {
                    set.insert(src_str_ref);
                    dest_column_string.insert_data(src_str_ref.data, src_str_ref.size);
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
};

using FunctionArrayDistinct = FunctionArrayUnary<ArrayDistinctImpl, NameArrayDistinct>;

void register_function_array_distinct(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayDistinct>();
}

} // namespace doris::vectorized