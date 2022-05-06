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

template <typename ReturnType, bool AlwaysNullable = false>
struct FunctionFakeBaseImpl {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        if constexpr (AlwaysNullable) {
            return make_nullable(std::make_shared<ReturnType>());
        }
        return std::make_shared<ReturnType>();
    }
};

struct FunctionExplode {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DCHECK(is_array(arguments[0])) << arguments[0]->get_name() << " not supported";
        return make_nullable(
                check_and_get_data_type<DataTypeArray>(arguments[0].get())->get_nested_type());
    }
};

template <typename ReturnType, bool Nullable = false>
void register_function_default(SimpleFunctionFactory& factory, const std::string& name) {
    factory.register_function<FunctionFake<FunctionFakeBaseImpl<ReturnType, Nullable>>>(name);
};

template <typename FunctionImpl>
void register_table_function_expand(SimpleFunctionFactory& factory, const std::string& name,
                                    const std::string& suffix) {
    factory.register_function<FunctionFake<FunctionImpl>>(name);
    factory.register_function<FunctionFake<FunctionImpl>>(name + suffix);
};

template <typename ReturnType>
void register_table_function_expand_default(SimpleFunctionFactory& factory, const std::string& name,
                                            const std::string& suffix) {
    factory.register_function<FunctionFake<FunctionFakeBaseImpl<ReturnType>>>(name);
    factory.register_function<FunctionFake<FunctionFakeBaseImpl<ReturnType, true>>>(name + suffix);
};

template <typename FunctionImpl>
void register_table_function_expand_outer(SimpleFunctionFactory& factory, const std::string& name) {
    register_table_function_expand<FunctionImpl>(factory, name, COMBINATOR_SUFFIX_OUTER);
};

template <typename ReturnType>
void register_table_function_expand_outer_default(SimpleFunctionFactory& factory,
                                                  const std::string& name) {
    register_table_function_expand_default<ReturnType>(factory, name, COMBINATOR_SUFFIX_OUTER);
};

void register_function_fake(SimpleFunctionFactory& factory) {
    register_function_default<DataTypeUInt8>(factory, "esquery");

    register_table_function_expand_outer<FunctionExplode>(factory, "explode");

    register_table_function_expand_outer_default<DataTypeString>(factory, "explode_split");
    register_table_function_expand_outer_default<DataTypeInt32>(factory, "explode_numbers");
    register_table_function_expand_outer_default<DataTypeInt64>(factory, "explode_json_array_int");
    register_table_function_expand_outer_default<DataTypeString>(factory,
                                                                 "explode_json_array_string");
    register_table_function_expand_outer_default<DataTypeFloat64>(factory,
                                                                  "explode_json_array_double");
    register_table_function_expand_outer_default<DataTypeInt64>(factory, "explode_bitmap");
}

} // namespace doris::vectorized
