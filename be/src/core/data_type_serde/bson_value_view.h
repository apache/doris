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

#include <bson/bson.h>

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"

namespace doris {

// Type tags and exact path segments are pinned by FE, independently of the projected Doris type.
struct BsonBinding {
    std::string name;
    std::vector<std::string> path;
    std::vector<int> types;
    std::vector<BsonBinding> children;
    int subtype = -1;
    std::string special;
    std::unordered_map<std::string, size_t> child_positions;
    const BsonBinding* child(const std::string& child_name, size_t ordinal) const;
};

// Payload pointers borrow the cursor's current document; SerDes must copy them before getMore.
struct BsonValueView {
    bson_value_t value {};
    bool missing = true;
    bool path_mismatch = false;
    static BsonValueView of(const bson_value_t& value);
    static BsonValueView document(const bson_t& document);
    BsonValueView at_path(const std::vector<std::string>& path) const;
    bool is_null() const {
        return !path_mismatch && (missing || value.value_type == BSON_TYPE_NULL);
    }
};

struct BsonDecodeContext {
    const BsonBinding* binding = nullptr;
    bool mismatch_as_null = false;
    int depth = 0;
    static constexpr int MAX_DEPTH = 64;
    static constexpr size_t MAX_DOCUMENT_BYTES = 16 * 1024 * 1024;
    static constexpr size_t MAX_ELEMENTS = 1000000;
    BsonDecodeContext child(const BsonBinding* child_binding) const;
    Status validate(const BsonValueView& value) const;
};

Status parse_bson_bindings(const std::string& json, std::vector<BsonBinding>* bindings);
Status bson_decimal_exact(const bson_decimal128_t& value, int precision, int scale,
                          std::string* text);
Status validate_bson_tree(const BsonValueView& value);
Status bson_canonical_json(const BsonValueView& value, std::string* json);

} // namespace doris
