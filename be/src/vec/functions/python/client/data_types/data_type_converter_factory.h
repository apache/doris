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

#define PY_SSIZE_T_CLEAN
#include <vec/data_types/data_type.h>

#include "array_converter.h"
#include "boolean_converter.h"
#include "float_converter.h"
#include "int_converter.h"
#include "map_converter.h"
#include "string_converter.h"
#include "struct_converter.h"

namespace doris::pyudf {

/**
 * {@link DataTypeConverterFactory} is responsible for creating the appropriate {@link DataTypeConverter}
 * based on the given data type.
 */
class DataTypeConverterFactory {
public:
    /**
     * Creates a {@link DataTypeConverter} instance based on the given data type.
     *
     * @param type data type.
     * @return {@link DataTypeConverter} or nullptr if the type is not supported.
     */
    static std::unique_ptr<DataTypeConverter> create_converter(
            const vectorized::DataTypePtr& type) {
        if (const vectorized::WhichDataType which_data_type(type); which_data_type.is_int8()) {
            return std::make_unique<TinyIntConverter>();
        } else if (which_data_type.is_int16()) {
            return std::make_unique<SmallIntConverter>();
        } else if (which_data_type.is_int32()) {
            return std::make_unique<IntConverter>();
        } else if (which_data_type.is_int64()) {
            return std::make_unique<BigIntConverter>();
        } else if (which_data_type.is_float32()) {
            return std::make_unique<FloatConverter>();
        } else if (which_data_type.is_float64()) {
            return std::make_unique<DoubleConverter>();
        } else if (which_data_type.is_string_or_fixed_string()) {
            return std::make_unique<StringConverter>();
        } else if (which_data_type.is_array()) {
            return std::make_unique<ArrayConverter>();
        } else if (which_data_type.is_map()) {
            return std::make_unique<MapConverter>();
        } else if (which_data_type.is_boolean()) {
            return std::make_unique<BooleanConverter>();
        } else if (which_data_type.is_struct()) {
            return std::make_unique<StructConverter>();
        }
        return nullptr;
    }
};

} // namespace doris::pyudf