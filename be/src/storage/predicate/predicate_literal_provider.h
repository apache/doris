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

#include <cstdint>
#include <optional>
#include <vector>

#include "core/field.h"

namespace doris {

struct ColumnPredicateLiteralTypeInfo {
    uint32_t precision = 0;
    uint32_t scale = 0;
};

class ColumnPredicateLiteralProvider {
public:
    virtual ~ColumnPredicateLiteralProvider() = default;

    virtual std::optional<Field> predicate_value() const { return std::nullopt; }
    virtual bool predicate_values(std::vector<Field>* values) const { return false; }
    virtual std::optional<ColumnPredicateLiteralTypeInfo> predicate_literal_type_info() const {
        return std::nullopt;
    }
};

} // namespace doris
