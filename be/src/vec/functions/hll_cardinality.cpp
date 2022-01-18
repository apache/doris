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
#include "udf/udf.h"
#include "vec/data_types/number_traits.h"
#include "vec/functions/function_always_not_nullable.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct HLLCardinality {
    static constexpr auto name = "hll_cardinality";

    using ReturnType = DataTypeNumber<Int64>;

    static void vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                       MutableColumnPtr& col_res) {
        typename ColumnVector<Int64>::Container& res =
                reinterpret_cast<ColumnVector<Int64>*>(col_res.get())->get_data();

        auto size = res.size();
        for (int i = 0; i < size; ++i) {
            auto val = HllFunctions::hll_cardinality(
                    nullptr,
                    StringVal((uint8_t*)&data[offsets[i - 1]], offsets[i] - offsets[i - 1] - 1));
            res[i] = val.val;
        }
    }

    static void vector_nullable(const ColumnString::Chars& data,
                                const ColumnString::Offsets& offsets, const NullMap& nullmap,
                                MutableColumnPtr& col_res) {
        typename ColumnVector<Int64>::Container& res =
                reinterpret_cast<ColumnVector<Int64>*>(col_res.get())->get_data();

        auto size = res.size();
        for (int i = 0; i < size; ++i) {
            if (nullmap[i]) {
                res[i] = 0;
            } else {
                auto val = HllFunctions::hll_cardinality(
                        nullptr, StringVal((uint8_t*)&data[offsets[i - 1]],
                                           offsets[i] - offsets[i - 1] - 1));
                res[i] = val.val;
            }
        }
    }
};

using FunctionHLLCardinality = FunctionAlwaysNotNullable<HLLCardinality>;

void register_function_hll_cardinality(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHLLCardinality>();
}

} // namespace doris::vectorized
