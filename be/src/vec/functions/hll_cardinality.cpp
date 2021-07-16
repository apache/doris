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
#include "vec/functions/function_string_or_array_to_t.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct HLLCardinalityImpl {
    static void vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                       PaddedPODArray<Int64>& res) {
        auto size = res.size();
        for (int i = 0; i < size; ++i) {
            auto val = HllFunctions::hll_cardinality(
                    nullptr,
                    StringVal((uint8_t*)&data[offsets[i - 1]], offsets[i] - offsets[i - 1] - 1));
            res[i] = val.val;
        }
    }
};

struct NameHLLCardinality {
    static constexpr auto name = "hll_cardinality";
};

using FunctionHLLCardinality =
        FunctionStringOrArrayToT<HLLCardinalityImpl, NameHLLCardinality, Int64>;

void register_function_hll_cardinality(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHLLCardinality>();
}

} // namespace doris::vectorized
