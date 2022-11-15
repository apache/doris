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

#include "olap/hll.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/functions/function_always_not_nullable.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct HLLHash {
    static constexpr auto name = "hll_hash";

    using ReturnType = DataTypeHLL;
    template <typename ColumnType>
    static void vector(const ColumnType* col, MutableColumnPtr& col_res) {
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            const ColumnString::Chars& data = col->get_chars();
            const ColumnString::Offsets& offsets = col->get_offsets();
            auto* res_column = reinterpret_cast<ColumnHLL*>(col_res.get());
            auto& res_data = res_column->get_data();
            size_t size = offsets.size();

            for (size_t i = 0; i < size; ++i) {
                const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                size_t str_size = offsets[i] - offsets[i - 1];
                uint64_t hash_value =
                        HashUtil::murmur_hash64A(raw_str, str_size, HashUtil::MURMUR_SEED);
                res_data[i].update(hash_value);
            }
        }
    }

    template <typename ColumnType>
    static void vector_nullable(const ColumnType* col, const NullMap& nullmap,
                                MutableColumnPtr& col_res) {
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            const ColumnString::Chars& data = col->get_chars();
            const ColumnString::Offsets& offsets = col->get_offsets();
            auto* res_column = reinterpret_cast<ColumnHLL*>(col_res.get());
            auto& res_data = res_column->get_data();
            size_t size = offsets.size();

            for (size_t i = 0; i < size; ++i) {
                if (nullmap[i]) {
                    continue;
                } else {
                    const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                    size_t str_size = offsets[i] - offsets[i - 1];
                    uint64_t hash_value =
                            HashUtil::murmur_hash64A(raw_str, str_size, HashUtil::MURMUR_SEED);
                    res_data[i].update(hash_value);
                }
            }
        }
    }
};

using FunctionHLLHash = FunctionAlwaysNotNullable<HLLHash>;

void register_function_hll_hash(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHLLHash>();
}

} // namespace doris::vectorized