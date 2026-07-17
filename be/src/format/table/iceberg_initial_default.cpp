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

#include "format/table/iceberg_initial_default.h"

#include <string>

#include "common/consts.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "util/url_coding.h"

namespace doris::iceberg {

Status parse_initial_default(const DataTypePtr& type, std::string_view encoded_value,
                             bool is_base64, std::string_view field_name, ColumnPtr* result) {
    DORIS_CHECK(type != nullptr);
    DORIS_CHECK(result != nullptr);
    const auto nested_type = remove_nullable(type);
    const auto primitive_type = nested_type->get_primitive_type();
    Field value;
    if (is_base64 || primitive_type == TYPE_VARBINARY) {
        std::string decoded_value;
        if (!base64_decode(std::string(encoded_value), &decoded_value)) {
            return Status::InvalidArgument("Invalid Base64 Iceberg initial default for field {}",
                                           field_name);
        }
        if (primitive_type == TYPE_VARBINARY) {
            value = Field::create_field<TYPE_VARBINARY>(StringView(decoded_value));
        } else {
            DORIS_CHECK(is_string_type(primitive_type));
            value = Field::create_field<TYPE_STRING>(decoded_value);
        }
    } else {
        RETURN_IF_ERROR(
                nested_type->get_serde()->from_fe_string(std::string(encoded_value), value));
    }
    auto column = type->create_column();
    column->insert(value);
    *result = std::move(column);
    return Status::OK();
}

} // namespace doris::iceberg
