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

#include <glog/logging.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>
#include <string>

#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exprs/table_function/table_function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

template <typename ReturnType, bool AlwaysNullable = false, bool VARIADIC = false>
struct FunctionFakeBaseImpl {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        if constexpr (AlwaysNullable) {
            return make_nullable(std::make_shared<ReturnType>());
        }
        return std::make_shared<ReturnType>();
    }
    static DataTypes get_variadic_argument_types() {
        if constexpr (VARIADIC) {
            if constexpr (AlwaysNullable) {
                return {make_nullable(std::make_shared<ReturnType>())};
            }
            return {std::make_shared<ReturnType>()};
        } else {
            return {};
        }
    }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

struct FunctionExplode {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DCHECK(is_array(arguments[0])) << arguments[0]->get_name() << " not supported";
        return make_nullable(
                check_and_get_data_type<DataTypeArray>(arguments[0].get())->get_nested_type());
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

// explode map: make map k,v as struct field
struct FunctionExplodeMap {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DCHECK(is_map(arguments[0])) << arguments[0]->get_name() << " not supported";
        DataTypes fieldTypes(2);
        fieldTypes[0] = check_and_get_data_type<DataTypeMap>(arguments[0].get())->get_key_type();
        fieldTypes[1] = check_and_get_data_type<DataTypeMap>(arguments[0].get())->get_value_type();
        return make_nullable(std::make_shared<vectorized::DataTypeStruct>(fieldTypes));
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

template <bool AlwaysNullable = false>
struct FunctionPoseExplode {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DCHECK(is_array(arguments[0])) << arguments[0]->get_name() << " not supported";
        DataTypes fieldTypes(2);
        fieldTypes[0] = make_nullable(std::make_shared<DataTypeInt32>());
        fieldTypes[1] =
                check_and_get_data_type<DataTypeArray>(arguments[0].get())->get_nested_type();
        auto struct_type = std::make_shared<vectorized::DataTypeStruct>(fieldTypes);
        if constexpr (AlwaysNullable) {
            return make_nullable(struct_type);
        } else {
            return arguments[0]->is_nullable() ? make_nullable(struct_type) : struct_type;
        }
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

// explode json-object: expands json-object to struct with a pair of key and value in column string
struct FunctionExplodeJsonObject {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DCHECK(WhichDataType(arguments[0]).is_json())
                << " explode json object " << arguments[0]->get_name() << " not supported";
        DataTypes fieldTypes(2);
        fieldTypes[0] = make_nullable(std::make_shared<DataTypeString>());
        fieldTypes[1] = make_nullable(std::make_shared<DataTypeJsonb>());
        return make_nullable(std::make_shared<vectorized::DataTypeStruct>(fieldTypes));
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

struct FunctionEsquery {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        return FunctionFakeBaseImpl<DataTypeUInt8>::get_return_type_impl(arguments);
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "esquery only supported on es table"; }
};

template <typename FunctionImpl>
void register_function(SimpleFunctionFactory& factory, const std::string& name) {
    factory.register_function<FunctionFake<FunctionImpl>>(name);
};

template <typename FunctionImpl>
void register_table_function_expand(SimpleFunctionFactory& factory, const std::string& name,
                                    const std::string& suffix) {
    factory.register_function<FunctionFake<FunctionImpl>>(name);
    factory.register_function<FunctionFake<FunctionImpl>>(name + suffix);
};

template <typename ReturnType, bool VARIADIC>
void register_table_function_expand_default(SimpleFunctionFactory& factory, const std::string& name,
                                            const std::string& suffix) {
    factory.register_function<FunctionFake<FunctionFakeBaseImpl<ReturnType, false, VARIADIC>>>(
            name);
    factory.register_function<FunctionFake<FunctionFakeBaseImpl<ReturnType, true, VARIADIC>>>(
            name + suffix);
};

template <typename FunctionImpl>
void register_table_function_expand_outer(SimpleFunctionFactory& factory, const std::string& name) {
    register_table_function_expand<FunctionImpl>(factory, name, COMBINATOR_SUFFIX_OUTER);
};

template <typename ReturnType, bool VARIADIC>
void register_table_function_expand_outer_default(SimpleFunctionFactory& factory,
                                                  const std::string& name) {
    register_table_function_expand_default<ReturnType, VARIADIC>(factory, name,
                                                                 COMBINATOR_SUFFIX_OUTER);
};

template <typename FunctionImpl>
void register_table_function_with_impl(SimpleFunctionFactory& factory, const std::string& name,
                                       const std::string& suffix = "") {
    factory.register_function<FunctionFake<FunctionImpl>>(name + suffix);
};

void register_function_fake(SimpleFunctionFactory& factory) {
    register_function<FunctionEsquery>(factory, "esquery");

    register_table_function_expand_outer<FunctionExplode>(factory, "explode");
    register_table_function_expand_outer<FunctionExplodeMap>(factory, "explode_map");

    register_table_function_expand_outer<FunctionExplodeJsonObject>(factory, "explode_json_object");
    register_table_function_expand_outer_default<DataTypeString, false>(factory, "explode_split");
    register_table_function_expand_outer_default<DataTypeInt32, false>(factory, "explode_numbers");
    register_table_function_expand_outer_default<DataTypeInt64, false>(factory,
                                                                       "explode_json_array_int");
    register_table_function_expand_outer_default<DataTypeString, false>(
            factory, "explode_json_array_string");
    register_table_function_expand_outer_default<DataTypeJsonb, true>(factory,
                                                                      "explode_json_array_json");
    register_table_function_expand_outer_default<DataTypeString, true>(factory,
                                                                       "explode_json_array_json");
    register_table_function_expand_outer_default<DataTypeFloat64, false>(
            factory, "explode_json_array_double");
    register_table_function_expand_outer_default<DataTypeInt64, false>(factory, "explode_bitmap");
    register_table_function_with_impl<FunctionPoseExplode<false>>(factory, "posexplode");
    register_table_function_with_impl<FunctionPoseExplode<true>>(factory, "posexplode",
                                                                 COMBINATOR_SUFFIX_OUTER);
}

} // namespace doris::vectorized
