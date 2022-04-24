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

#include "vec/functions/function_fake.h"

#include <boost/metaparse/string.hpp>
#include <string_view>
#include <type_traits>

namespace doris::vectorized {

// We can use std::basic_fixed_string with c++20 in the future
template <const char* Name, typename ReturnType>
struct FakeFunctionBaseImpl {
    static constexpr auto name = Name;
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return std::make_shared<ReturnType>();
    }
};

#define C_STR(str_) boost::mpl::c_str<BOOST_METAPARSE_STRING(str_)>::value

using FunctionEsquery = FakeFunctionBaseImpl<C_STR("esquery"), DataTypeUInt8>;

using FunctionExplodeSplit = FakeFunctionBaseImpl<C_STR("explode_split"), DataTypeString>;
using FunctionExplodeNumbers = FakeFunctionBaseImpl<C_STR("explode_numbers"), DataTypeInt32>;
using FunctionExplodeJsonArrayInt =
        FakeFunctionBaseImpl<C_STR("explode_json_array_int"), DataTypeInt64>;
using FunctionExplodeJsonArrayString =
        FakeFunctionBaseImpl<C_STR("explode_json_array_string"), DataTypeString>;
using FunctionExplodeJsonArrayDouble =
        FakeFunctionBaseImpl<C_STR("explode_json_array_double"), DataTypeFloat64>;
using FunctionExplodeBitmap = FakeFunctionBaseImpl<C_STR("explode_bitmap"), DataTypeInt64>;

struct FunctionExplode {
    static constexpr auto name = "explode";
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DCHECK(is_array(arguments[0])) << arguments[0]->get_name() << " not supported";
        return make_nullable(
                check_and_get_data_type<DataTypeArray>(arguments[0].get())->get_nested_type());
    }
};

void register_function_fake(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionFake<FunctionEsquery>>();

    factory.register_table_function<FunctionFake<FunctionExplodeSplit>>();
    factory.register_table_function<FunctionFake<FunctionExplodeNumbers>>();
    factory.register_table_function<FunctionFake<FunctionExplodeJsonArrayDouble>>();
    factory.register_table_function<FunctionFake<FunctionExplodeJsonArrayInt>>();
    factory.register_table_function<FunctionFake<FunctionExplodeJsonArrayString>>();
    factory.register_table_function<FunctionFake<FunctionExplodeBitmap>>();
    factory.register_table_function<FunctionFake<FunctionExplode>>();
}

} // namespace doris::vectorized
