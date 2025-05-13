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

#include "vec/aggregate_functions/aggregate_function_context_ngrams.h"

#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

// create context_ngrams function with context pattern
AggregateFunctionPtr create_context_ngrams_with_pattern(const DataTypes& argument_types) {
    LOG(INFO) << "In create_context_ngrams_with_pattern()";
    // check second argument is array<string>
    WhichDataType which4(remove_nullable(argument_types[1]));
    if (which4.idx != TypeIndex::Array) {
        return nullptr;
    }
    const auto* context_type =
            assert_cast<const DataTypeArray*>(remove_nullable(argument_types[1]).get());

    WhichDataType which5(remove_nullable(context_type->get_nested_type()));
    if (which5.idx != TypeIndex::String && which5.idx != TypeIndex::FixedString) {
        return nullptr;
    }

    return std::make_shared<AggregateFunctionContextNgrams>(argument_types);
}

// create context_ngrams function with n parameter
AggregateFunctionPtr create_context_ngrams_with_n(const DataTypes& argument_types) {
    LOG(INFO) << "In create_context_ngrams_with_n()";
    // check second argument is integer
    WhichDataType which4(remove_nullable(argument_types[1]));
    if (!which4.is_int_or_uint()) {
        return nullptr;
    }

    return std::make_shared<AggregateFunctionContextNgrams>(argument_types);
}

AggregateFunctionPtr create_aggregate_function_context_ngrams(const String& name,
                                                              const DataTypes& argument_types,
                                                              const bool result_is_nullable,
                                                              const AggregateFunctionAttr& attr) {
    LOG(INFO) << "In create_aggregate_function_context_ngrams(), name is " << name;
    // check argument number
    if (argument_types.size() != 3 && argument_types.size() != 4) {
        throw Exception(
                ErrorCode::INVALID_ARGUMENT,
                "Number of arguments for function {} doesn't match: passed {}, should be 3 or 4",
                name, argument_types.size());
    }

    // check first argument is array<array<string>>
    WhichDataType which1(remove_nullable(argument_types[0]));
    if (which1.idx != TypeIndex::Array) {
        return nullptr;
    }
    const auto* array_type =
            assert_cast<const DataTypeArray*>(remove_nullable(argument_types[0]).get());
    WhichDataType which2(remove_nullable(array_type->get_nested_type()));
    if (which2.idx != TypeIndex::Array) {
        return nullptr;
    }
    const auto* inner_array_type =
            assert_cast<const DataTypeArray*>(remove_nullable(array_type->get_nested_type()).get());
    WhichDataType which3(remove_nullable(inner_array_type->get_nested_type()));
    if (which3.idx != TypeIndex::String && which3.idx != TypeIndex::FixedString) {
        return nullptr;
    }

    // check last argument is integer
    for (size_t i = 2; i < argument_types.size(); ++i) {
        WhichDataType which(remove_nullable(argument_types[i]));
        if (!which.is_int_or_uint()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Argument {} of function {} must be integer", i + 1, name);
        }
    }

    // choose creation method based on second argument type
    WhichDataType which(remove_nullable(argument_types[1]));
    if (which.idx == TypeIndex::Array) {
        return create_context_ngrams_with_pattern(argument_types);
    } else {
        return create_context_ngrams_with_n(argument_types);
    }
}

void register_aggregate_function_context_ngrams(AggregateFunctionSimpleFactory& factory) {
    LOG(INFO) << "register_aggregate_function_context_ngrams";
    factory.register_function_both("context_ngrams", create_aggregate_function_context_ngrams);
}

} // namespace doris::vectorized
