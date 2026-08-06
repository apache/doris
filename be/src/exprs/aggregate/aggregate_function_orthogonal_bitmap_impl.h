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

#pragma once

#include <string>

#include "core/data_type/data_type.h"
#include "core/data_type/data_type_nullable.h"
#include "exprs/aggregate/aggregate_function_orthogonal_bitmap.h"
#include "exprs/aggregate/helpers.h"

namespace doris {

template <template <PrimitiveType> class Impl>
AggregateFunctionPtr create_aggregate_function_orthogonal(const std::string& name,
                                                          const DataTypes& argument_types,
                                                          const DataTypePtr& result_type,
                                                          const bool result_is_nullable,
                                                          const AggregateFunctionAttr& attr) {
    if (argument_types.empty()) {
        LOG(WARNING) << "Incorrect number of arguments for aggregate function " << name;
        return nullptr;
    } else if (argument_types.size() == 1) {
        return creator_without_type::create<AggFunctionOrthBitmapFunc<Impl<TYPE_STRING>>>(
                argument_types, result_is_nullable, attr);
    } else {
        AggregateFunctionPtr res(
                creator_with_type_list_base<1, TYPE_TINYINT, TYPE_SMALLINT, TYPE_INT, TYPE_BIGINT,
                                            TYPE_LARGEINT>::create<AggFunctionOrthBitmapFunc,
                                                                   Impl>(argument_types,
                                                                         result_is_nullable, attr));
        if (res) {
            return res;
        } else if (is_string_type(argument_types[1]->get_primitive_type())) {
            res = creator_without_type::create<AggFunctionOrthBitmapFunc<Impl<TYPE_STRING>>>(
                    argument_types, result_is_nullable, attr);
            return res;
        }

        const IDataType& argument_type = *argument_types[1];
        LOG(WARNING) << "Incorrect Type " << argument_type.get_name()
                     << " of arguments for aggregate function " << name;
        return nullptr;
    }
}

} // namespace doris
