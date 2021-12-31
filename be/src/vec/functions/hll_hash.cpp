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

#include "exprs/hll_function.h"
#include "olap/hll.h"
#include "udf/udf.h"
#include "vec/functions/function_always_not_nullable.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct HLLHash {
    static constexpr auto name = "hll_hash";

    using ReturnType = DataTypeString;

    static void vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                       MutableColumnPtr& col_res) {
        ColumnString::Chars& res_data = reinterpret_cast<ColumnString*>(col_res.get())->get_chars();
        ColumnString::Offsets& res_offsets =
                reinterpret_cast<ColumnString*>(col_res.get())->get_offsets();

        size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(data.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < size; ++i) {
            auto hash_string = HllFunctions::hll_hash(
                    StringVal((uint8_t*)(&data[prev_offset]), offsets[i] - prev_offset - 1));

            res_data.resize(res_data.size() + hash_string.length() + 1);
            memcpy_small_allow_read_write_overflow15(&res_data[res_offset], hash_string.c_str(),
                                                     hash_string.length());
            res_offset += hash_string.length() + 1;
            res_data[res_offset - 1] = 0;

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }

    static void vector_nullable(const ColumnString::Chars& data,
                                const ColumnString::Offsets& offsets, const NullMap& nullmap,
                                MutableColumnPtr& col_res) {
        ColumnString::Chars& res_data = reinterpret_cast<ColumnString*>(col_res.get())->get_chars();
        ColumnString::Offsets& res_offsets =
                reinterpret_cast<ColumnString*>(col_res.get())->get_offsets();

        size_t size = offsets.size();
        res_offsets.resize(size);
        res_data.reserve(data.size());

        size_t prev_offset = 0;
        size_t res_offset = 0;

        for (size_t i = 0; i < size; ++i) {
            std::string hash_string;
            if (nullmap[i]) {
                hash_string = HyperLogLog::empty();
            } else {
                hash_string = HllFunctions::hll_hash(
                        StringVal((uint8_t*)(&data[prev_offset]), offsets[i] - prev_offset - 1));
            }

            res_data.resize(res_data.size() + hash_string.length() + 1);
            memcpy_small_allow_read_write_overflow15(&res_data[res_offset], hash_string.c_str(),
                                                     hash_string.length());
            res_offset += hash_string.length() + 1;
            res_data[res_offset - 1] = 0;

            res_offsets[i] = res_offset;
            prev_offset = offsets[i];
        }
    }
};

using FunctionHLLHash = FunctionAlwaysNotNullable<HLLHash>;

void register_function_hll_hash(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHLLHash>();
}

} // namespace doris::vectorized