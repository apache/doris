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

#include <vector>

#include "core/field.h"

namespace doris {

class OlapTuple {
public:
    OlapTuple() {}

    void add_null() { _fields.emplace_back(PrimitiveType::TYPE_NULL); }

    void add_field(Field f) { _fields.push_back(std::move(f)); }

    size_t size() const { return _fields.size(); }

    // Return debug string for profile/logging only.
    // NOTE: this output may be inaccurate for decimal types because OlapTuple
    // does not carry decimal scale metadata and falls back to scale=0.
    std::string debug_string() const {
        std::string result;
        for (size_t i = 0; i < _fields.size(); ++i) {
            if (i > 0) {
                result.append(",");
            }
            if (_fields[i].is_null()) {
                result.append("null");
            } else {
                result.append(_fields[i].to_debug_string(0));
            }
        }
        return result;
    }

    const Field& get_field(size_t i) const { return _fields[i]; }
    Field& get_field(size_t i) { return _fields[i]; }

private:
    std::vector<Field> _fields;
};

} // namespace doris
