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

#include "vec/exec/format/table/iceberg/transforms.h"

#include <any>
#include <string>

#include "common/exception.h"
#include "vec/exec/format/table/iceberg/types.h"

namespace doris {
namespace iceberg {
std::string Transform::to_human_string(const Type& type, const std::any& value) const {
    if (value.has_value()) {
        switch (type.type_id()) {
        case BOOLEAN:
            return std::to_string(std::any_cast<bool>(value));
        case INTEGER:
            return std::to_string(std::any_cast<int>(value));
        case LONG:
            return std::to_string(std::any_cast<long>(value));
        case FLOAT:
            return std::to_string(std::any_cast<float>(value));
        case DOUBLE:
            return std::to_string(std::any_cast<double>(value));
        case DATE:
            std::cout << "Handling DATE type..." << std::endl;
            break;
        case TIME:
            std::cout << "Handling TIME type..." << std::endl;
            break;
        case TIMESTAMP:
            std::cout << "Handling TIMESTAMP type..." << std::endl;
            break;
        case STRING:
            return std::any_cast<std::string>(value);
            break;
        case UUID:
            std::cout << "Handling UUID type..." << std::endl;
            break;
        case FIXED:
            std::cout << "Handling FIXED type..." << std::endl;
            break;
        case BINARY:
            std::cout << "Handling BINARY type..." << std::endl;
            break;
        case DECIMAL:
            std::cout << "Handling DECIMAL type..." << std::endl;
            break;
        case STRUCT:
            std::cout << "Handling STRUCT type..." << std::endl;
            break;
        case LIST:
            std::cout << "Handling LIST type..." << std::endl;
            break;
        case MAP:
            std::cout << "Handling MAP type..." << std::endl;
            break;
        default:
            std::cout << "Unknown type!" << std::endl;
            break;
        }
    }
    return "null";
}

std::unique_ptr<Transform> Transforms::from_string(const std::string& transform) {
    std::string lower_transform_string;
    std::transform(transform.begin(), transform.end(), std::back_inserter(lower_transform_string),
                   [](unsigned char c) { return std::tolower(c); });
    if (lower_transform_string == "identity") {
        return std::make_unique<IdentityTransform>();
    } else {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Invalid transform type: {}.",
                               transform);
    }
}

std::unique_ptr<Transform> Transforms::from_string(Type* type, const std::string& transform) {
    std::string lower_transform_string;
    std::transform(transform.begin(), transform.end(), std::back_inserter(lower_transform_string),
                   [](unsigned char c) { return std::tolower(c); });
    if (lower_transform_string == "identity") {
        return std::make_unique<IdentityTransform>();
    } else {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR, "Invalid transform type: {}.",
                               transform);
    }
}

} // namespace iceberg
} // namespace doris