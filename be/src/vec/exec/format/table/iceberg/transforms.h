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
#include <any>
//#include <regex>
#include <memory>
#include <string>
//
//#include "vec/exec/format/table/iceberg/types.h"

namespace doris {
namespace iceberg {

class Type;

class Transform {
public:
    Transform() {}
    virtual ~Transform() = default;
    virtual std::string apply(const std::string& value) const = 0;
    virtual bool is_void() const { return false; }
    virtual std::string to_string() const = 0;

    virtual Type& get_result_type(Type& source_type) const = 0;

    virtual std::string to_human_string(const Type& type, const std::any& value) const;
};

class IdentityTransform : public Transform {
public:
    IdentityTransform() {} // Default constructor

    // Override pure virtual functions
    std::string apply(const std::string& value) const override { return value; }

    std::string to_string() const override { return "identity"; }

    Type& get_result_type(Type& source_type) const override { return source_type; }
};

class Transforms {
public:
    static std::unique_ptr<Transform> from_string(const std::string& transform);

    static std::unique_ptr<Transform> from_string(Type* type, const std::string& transform);
};

} // namespace iceberg
} // namespace doris