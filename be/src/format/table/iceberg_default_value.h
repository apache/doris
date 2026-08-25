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

#include <limits>
#include <string_view>

#include "common/status.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"

namespace doris::iceberg::detail {

inline bool parse_non_finite_default(doris::PrimitiveType type, std::string_view value,
                                     doris::Field* result) {
    DORIS_CHECK(result != nullptr);
    if (type != TYPE_FLOAT && type != TYPE_DOUBLE) {
        return false;
    }
    double parsed;
    if (value == "NaN") {
        parsed = std::numeric_limits<double>::quiet_NaN();
    } else if (value == "Infinity") {
        parsed = std::numeric_limits<double>::infinity();
    } else if (value == "-Infinity") {
        parsed = -std::numeric_limits<double>::infinity();
    } else {
        return false;
    }
    // Iceberg serializes non-finite defaults as strings, which generic numeric parsers reject.
    *result = type == TYPE_FLOAT ? Field::create_field<TYPE_FLOAT>(static_cast<float>(parsed))
                                 : Field::create_field<TYPE_DOUBLE>(parsed);
    return true;
}

} // namespace doris::iceberg::detail
