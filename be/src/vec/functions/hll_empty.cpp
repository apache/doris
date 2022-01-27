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
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_const.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct HLLEmptyImpl {
    static constexpr auto name = "hll_empty";
    static auto get_return_type() { return std::make_shared<DataTypeString>(); }
    static Field init_value() {
        auto hll = HyperLogLog::empty();
        return {hll.c_str(), hll.size()};
    }
};

using FunctionHLLEmpty = FunctionConst<HLLEmptyImpl>;

void register_function_hll_empty(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHLLEmpty>();
}

} // namespace doris::vectorized