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

#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

// We can use std::basic_fixed_string with c++20 in the future
template <const char* Name, typename ReturnType, bool Nullable = false>
struct FunctionFakeBaseImpl {
    static constexpr auto name = Name;
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        if constexpr (Nullable) {
            return make_nullable(std::make_shared<ReturnType>());
        }
        return std::make_shared<ReturnType>();
    }
};

template <const char* Name>
struct FunctionExplodeImpl {
    static constexpr auto name = Name;
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DCHECK(is_array(arguments[0])) << arguments[0]->get_name() << " not supported";
        return make_nullable(
                check_and_get_data_type<DataTypeArray>(arguments[0].get())->get_nested_type());
    }
};

#define C_STR(STR) boost::mpl::c_str<BOOST_METAPARSE_STRING(#STR)>::value

using FunctionEsquery = FunctionFakeBaseImpl<C_STR("esquery"), DataTypeUInt8>;

#define REGISTER_TABLE_FUNCTION_DEFAULT(CLASS_NAME, FN_NAME, RETURN_TYPE)                      \
    using CLASS_NAME = FunctionFakeBaseImpl<C_STR(FN_NAME), RETURN_TYPE, false>;               \
    using CLASS_NAME##Outer = FunctionFakeBaseImpl<C_STR(FN_NAME##_outer), RETURN_TYPE, true>; \
    factory.register_function<FunctionFake<CLASS_NAME>>();                                     \
    factory.register_function<FunctionFake<CLASS_NAME##Outer>>();

#define REGISTER_TABLE_FUNCTION(CLASS_NAME, FN_NAME, RETURN_TYPE_IMPL)  \
    using CLASS_NAME = RETURN_TYPE_IMPL<C_STR(FN_NAME)>;                \
    using CLASS_NAME##Outer = RETURN_TYPE_IMPL<C_STR(FN_NAME##_outer)>; \
    factory.register_function<FunctionFake<CLASS_NAME>>();              \
    factory.register_function<FunctionFake<CLASS_NAME##Outer>>();

void register_function_fake(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionFake<FunctionEsquery>>();

    REGISTER_TABLE_FUNCTION_DEFAULT(FunctionExplodeSplit, explode_split, DataTypeString);
    REGISTER_TABLE_FUNCTION_DEFAULT(FunctionExplodeNumbers, explode_numbers, DataTypeInt32);
    REGISTER_TABLE_FUNCTION_DEFAULT(FunctionExplodeJsonArrayInt, explode_json_array_int,
                                    DataTypeInt64);
    REGISTER_TABLE_FUNCTION_DEFAULT(FunctionExplodeJsonArrayString, explode_json_array_string,
                                    DataTypeString);
    REGISTER_TABLE_FUNCTION_DEFAULT(FunctionExplodeJsonArrayDouble, explode_json_array_double,
                                    DataTypeFloat64);
    REGISTER_TABLE_FUNCTION_DEFAULT(FunctionExplodeBitmap, explode_bitmap, DataTypeInt64);

    REGISTER_TABLE_FUNCTION(FunctionExplode, explode, FunctionExplodeImpl);
}

} // namespace doris::vectorized
