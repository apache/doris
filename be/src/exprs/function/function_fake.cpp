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

#include "exprs/function/function_fake.h"

#include <glog/logging.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <ostream>
#include <string>

#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_jsonb.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/data_type_variant.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/table_function/table_function.h"

namespace doris {

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
            return {std::make_shared<ReturnType>()};
        } else {
            return {};
        }
    }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

struct FunctionExplode {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DORIS_CHECK(arguments[0]->get_primitive_type() == TYPE_ARRAY);
        return make_nullable(
                check_and_get_data_type<DataTypeArray>(arguments[0].get())->get_nested_type());
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

struct FunctionExplodeV2 {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DataTypes fieldTypes(arguments.size());
        for (int i = 0; i < arguments.size(); i++) {
            if (arguments[i]->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
                if (arguments[i]->is_nullable()) {
                    fieldTypes[i] = arguments[i];
                } else {
                    fieldTypes[i] = make_nullable(arguments[i]);
                }
            } else {
                auto nestedType = check_and_get_data_type<DataTypeArray>(arguments[i].get())
                                          ->get_nested_type();
                if (nestedType->is_nullable()) {
                    fieldTypes[i] = nestedType;
                } else {
                    fieldTypes[i] = make_nullable(nestedType);
                }
            }
        }

        if (fieldTypes.size() > 1) {
            return make_nullable(std::make_shared<DataTypeStruct>(fieldTypes));
        } else {
            return make_nullable(fieldTypes[0]);
        }
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

// explode map: make map k,v as struct field
struct FunctionExplodeMap {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DORIS_CHECK(arguments[0]->get_primitive_type() == TYPE_MAP);
        DataTypes fieldTypes(2);
        fieldTypes[0] = check_and_get_data_type<DataTypeMap>(arguments[0].get())->get_key_type();
        fieldTypes[1] = check_and_get_data_type<DataTypeMap>(arguments[0].get())->get_value_type();
        return make_nullable(std::make_shared<DataTypeStruct>(fieldTypes));
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

template <bool AlwaysNullable = false>
struct FunctionPoseExplode {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DataTypes fieldTypes(arguments.size() + 1);
        fieldTypes[0] = std::make_shared<DataTypeInt32>();
        for (int i = 0; i < arguments.size(); i++) {
            DORIS_CHECK(arguments[i]->get_primitive_type() == TYPE_ARRAY);
            auto nestedType =
                    check_and_get_data_type<DataTypeArray>(arguments[i].get())->get_nested_type();
            fieldTypes[i + 1] = make_nullable(nestedType);
        }
        auto struct_type = std::make_shared<DataTypeStruct>(fieldTypes);
        if constexpr (AlwaysNullable) {
            return make_nullable(struct_type);
        } else {
            return struct_type;
        }
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

// explode json-object: expands json-object to struct with a pair of key and value in column string
struct FunctionExplodeJsonObject {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DORIS_CHECK(arguments[0]->get_primitive_type() == PrimitiveType::TYPE_JSONB);
        DataTypes fieldTypes(2);
        fieldTypes[0] = make_nullable(std::make_shared<DataTypeString>());
        fieldTypes[1] = make_nullable(std::make_shared<DataTypeJsonb>());
        return make_nullable(std::make_shared<DataTypeStruct>(fieldTypes));
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

// json_each(json) -> Nullable(Struct(key Nullable(String), value Nullable(JSONB)))
struct FunctionJsonEach {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DORIS_CHECK(arguments[0]->get_primitive_type() == PrimitiveType::TYPE_JSONB);
        DataTypes fieldTypes(2);
        fieldTypes[0] = make_nullable(std::make_shared<DataTypeString>());
        fieldTypes[1] = make_nullable(std::make_shared<DataTypeJsonb>());
        return make_nullable(std::make_shared<DataTypeStruct>(fieldTypes));
    }
    static DataTypes get_variadic_argument_types() { return {}; }
    static std::string get_error_msg() { return "Fake function do not support execute"; }
};

// json_each_text(json) -> Nullable(Struct(key Nullable(String), value Nullable(String)))
struct FunctionJsonEachText {
    static DataTypePtr get_return_type_impl(const DataTypes& arguments) {
        DORIS_CHECK(arguments[0]->get_primitive_type() == PrimitiveType::TYPE_JSONB);
        DataTypes fieldTypes(2);
        fieldTypes[0] = make_nullable(std::make_shared<DataTypeString>());
        fieldTypes[1] = make_nullable(std::make_shared<DataTypeString>());
        return make_nullable(std::make_shared<DataTypeStruct>(fieldTypes));
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

template <typename FunctionImpl>
void register_table_alternative_function_expand(SimpleFunctionFactory& factory,
                                                const std::string& name,
                                                const std::string& suffix) {
    factory.register_alternative_function<FunctionFake<FunctionImpl>>(name);
    factory.register_alternative_function<FunctionFake<FunctionImpl>>(name + suffix);
};

template <typename ReturnType, bool VARIADIC>
void register_table_function_expand_default(SimpleFunctionFactory& factory, const std::string& name,
                                            const std::string& suffix) {
    factory.register_function<FunctionFake<FunctionFakeBaseImpl<ReturnType, false, VARIADIC>>>(
            name);
    factory.register_function<FunctionFake<FunctionFakeBaseImpl<ReturnType, true, VARIADIC>>>(
            name + suffix);
};

template <typename ReturnType, bool VARIADIC>
void register_table_alternative_function_expand_default(SimpleFunctionFactory& factory,
                                                        const std::string& name,
                                                        const std::string& suffix) {
    factory.register_alternative_function<
            FunctionFake<FunctionFakeBaseImpl<ReturnType, false, VARIADIC>>>(name);
    factory.register_alternative_function<
            FunctionFake<FunctionFakeBaseImpl<ReturnType, true, VARIADIC>>>(name + suffix);
};

template <typename FunctionImpl>
void register_table_function_expand_outer(SimpleFunctionFactory& factory, const std::string& name) {
    register_table_function_expand<FunctionImpl>(factory, name, COMBINATOR_SUFFIX_OUTER);
};

template <typename FunctionImpl>
void register_table_alternative_function_expand_outer(SimpleFunctionFactory& factory,
                                                      const std::string& name) {
    register_table_alternative_function_expand<FunctionImpl>(factory, name,
                                                             COMBINATOR_SUFFIX_OUTER);
};

template <typename ReturnType, bool VARIADIC>
void register_table_function_expand_outer_default(SimpleFunctionFactory& factory,
                                                  const std::string& name) {
    register_table_function_expand_default<ReturnType, VARIADIC>(factory, name,
                                                                 COMBINATOR_SUFFIX_OUTER);
};

template <typename ReturnType, bool VARIADIC>
void register_table_alternative_function_expand_outer_default(SimpleFunctionFactory& factory,
                                                              const std::string& name) {
    register_table_alternative_function_expand_default<ReturnType, VARIADIC>(
            factory, name, COMBINATOR_SUFFIX_OUTER);
};

template <typename FunctionImpl>
void register_table_function_with_impl(SimpleFunctionFactory& factory, const std::string& name,
                                       const std::string& suffix = "") {
    factory.register_function<FunctionFake<FunctionImpl>>(name + suffix);
};

void register_function_fake(SimpleFunctionFactory& factory) {
    register_function<FunctionEsquery>(factory, "esquery");

    register_table_function_expand_outer<FunctionExplodeV2>(factory, "explode");
    register_table_alternative_function_expand_outer<FunctionExplode>(factory, "explode");

    register_table_function_expand_outer<FunctionExplodeMap>(factory, "explode_map");

    register_table_function_expand_outer<FunctionExplodeJsonObject>(factory, "explode_json_object");
    register_table_function_expand_outer<FunctionJsonEach>(factory, "json_each");
    register_table_function_expand_outer<FunctionJsonEachText>(factory, "json_each_text");
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
    register_table_alternative_function_expand_outer_default<DataTypeVariant, false>(
            factory, "explode_variant_array");
    register_table_function_with_impl<FunctionExplodeV2>(factory, "explode_variant_array");
}

} // namespace doris
