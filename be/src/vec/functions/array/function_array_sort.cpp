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

#include "vec/functions/array/function_array_unary.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct NameArraySort {
    static constexpr auto name = "array_sort";
};

struct ArraySortImpl {
    // sort the non-null element according to the permutation
    template <typename SrcDataType>
    static void _sort_by_permutation(ColumnArray::Offset& prev_offset,
                                     const ColumnArray::Offset& curr_offset,
                                     const SrcDataType* src_data_concrete,
                                     const IColumn& src_column, const UInt8* src_null_map,
                                     IColumn::Permutation& permutation) {
        for (ColumnArray::Offset j = prev_offset; j < curr_offset - 1; ++j) {
            if (src_null_map && src_null_map[j]) {
                continue;
            }
            for (ColumnArray::Offset k = j + 1; k < curr_offset; ++k) {
                if (src_null_map && src_null_map[k]) {
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
    static bool _execute_number(const IColumn& src_column, const ColumnArray::Offsets& src_offsets,
                                IColumn& dest_column, ColumnArray::Offsets& dest_offsets,
                                const UInt8* src_null_map, ColumnUInt8::Container* dest_null_map) {
        using NestType = typename ColumnType::value_type;
        const ColumnType* src_data_concrete = reinterpret_cast<const ColumnType*>(&src_column);
        if (!src_data_concrete) {
            return false;
        }
        const PaddedPODArray<NestType>& src_datas = src_data_concrete->get_data();

        PaddedPODArray<NestType>& dest_datas =
                reinterpret_cast<ColumnType&>(dest_column).get_data();

        ColumnArray::Offset prev_src_offset = 0;
        IColumn::Permutation permutation(src_column.size());
        for (size_t i = 0; i < src_column.size(); ++i) {
            permutation[i] = i;
        }

        for (auto curr_src_offset : src_offsets) {
            // filter and insert null element first
            for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && src_null_map[j]) {
                    DCHECK(dest_null_map != nullptr);
                    (*dest_null_map).push_back(true);
                    dest_datas.push_back(NestType());
                }
            }

            _sort_by_permutation<ColumnType>(prev_src_offset, curr_src_offset, src_data_concrete,
                                             src_column, src_null_map, permutation);

            // insert non-null element after sort by permutation
            for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && src_null_map[j]) {
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

    static bool _execute_string(const IColumn& src_column, const ColumnArray::Offsets& src_offsets,
                                IColumn& dest_column, ColumnArray::Offsets& dest_offsets,
                                const UInt8* src_null_map, ColumnUInt8::Container* dest_null_map) {
        const ColumnString* src_data_concrete = reinterpret_cast<const ColumnString*>(&src_column);
        if (!src_data_concrete) {
            return false;
        }

        ColumnString& dest_column_string = reinterpret_cast<ColumnString&>(dest_column);

        ColumnArray::Offset prev_src_offset = 0;
        IColumn::Permutation permutation(src_column.size());
        for (size_t i = 0; i < src_column.size(); ++i) {
            permutation[i] = i;
        }

        for (auto curr_src_offset : src_offsets) {
            // filter and insert null element first
            for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && src_null_map[j]) {
                    DCHECK(dest_null_map != nullptr);
                    dest_column_string.insert_default();
                    (*dest_null_map).push_back(true);
                }
            }

            _sort_by_permutation<ColumnString>(prev_src_offset, curr_src_offset, src_data_concrete,
                                               src_column, src_null_map, permutation);

            // insert non-null element after sort by permutation
            for (ColumnArray::Offset j = prev_src_offset; j < curr_src_offset; ++j) {
                if (src_null_map && src_null_map[j]) {
                    continue;
                }

                StringRef src_str_ref = src_data_concrete->get_data_at(permutation[j]);
                dest_column_string.insert_data(src_str_ref.data, src_str_ref.size);
                if (dest_null_map) {
                    (*dest_null_map).push_back(false);
                }
            }
            dest_offsets.push_back(curr_src_offset);
            prev_src_offset = curr_src_offset;
        }
        return true;
    }
};

using FunctionArraySort = FunctionArrayUnary<ArraySortImpl, NameArraySort>;

void register_function_array_sort(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArraySort>();
}

} // namespace doris::vectorized