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
    // Doris names are lowercase; source path segments retain their exact case and punctuation.
    std::string name;
    std::vector<std::string> path;
    // An empty tag list leaves physical-type validation to the target SerDe.
    std::vector<int> types;
    std::vector<BsonBinding> children;
    // BinData-to-VARBINARY requires an explicit subtype (0..255); -1 means unspecified.
    int subtype = -1;
    // Empty selects path lookup; "ejson" and "bson" materialize the entire source document.
    std::string special;
    // Derived name index built while parsing; it is not part of the serialized FE contract.
    std::unordered_map<std::string, size_t> child_positions;

    // Resolve a projected child by name, using ordinal only when its name still matches. For
    // STRUCT<a,b> projected as STRUCT<b>, ordinal zero must resolve b rather than the original a.
    // The returned pointer borrows this binding's children and is null when the name is absent.
    const BsonBinding* child(const std::string& child_name, size_t ordinal) const;
};

// A shallow view of one BSON value. Payload pointers borrow the source document; SerDes must copy
// bytes into Doris columns before the cursor advances or closes. This view never frees BSON data.
struct BsonValueView {
    bson_value_t value {};
    bool missing = true;
    bool path_mismatch = false;

    // Mark a BSON value as present, copying its scalar/tag fields and borrowing any payload pointers.
    static BsonValueView of(const bson_value_t& value);

    // View the document's original bytes as BSON_TYPE_DOCUMENT without copying or taking ownership.
    static BsonValueView document(const bson_t& document);

    // Traverse case-sensitive document keys relative to this view; ["a.b"] and ["a","b"] differ.
    // An empty path returns this view. Missing/null ancestors yield missing; a scalar ancestor sets
    // path_mismatch, so {a:1} at [a,b] cannot silently become the same SQL NULL as {a:{}}.
    BsonValueView at_path(const std::vector<std::string>& path) const;

    // Missing and BSON null both map to SQL NULL. A failed scalar traversal remains a type error.
    bool is_null() const {
        return !path_mismatch && (missing || value.value_type == BSON_TYPE_NULL);
    }
};

// Per-value decoding policy. Bindings are owned by the reader; child contexts borrow the same tree.
struct BsonDecodeContext {
    const BsonBinding* binding = nullptr;
    // Nullable SerDe applies this only to a top-level field (depth zero), after nested rollback.
    bool mismatch_as_null = false;
    int depth = 0;
    static constexpr int MAX_DEPTH = 64;
    static constexpr size_t MAX_DOCUMENT_BYTES = 16 * 1024 * 1024;
    static constexpr size_t MAX_ELEMENTS = 1000000;

    // Carry the mismatch policy into a child while replacing its binding and increasing depth.
    BsonDecodeContext child(const BsonBinding* child_binding) const;

    // Check path traversal, depth, declared BSON tags and container size before typed conversion.
    // Null/missing are permitted here; the target SerDe handles nullability, ranges and subtypes.
    Status validate(const BsonValueView& value) const;
};

// Parse FE's version-1 bindings JSON, enforcing 1 MiB, 4096-field and nesting limits and rejecting
// duplicate names. Replace bindings only on success; returned bindings own all strings and children.
Status parse_bson_bindings(const std::string& json, std::vector<BsonBinding>* bindings);

// Format a finite Decimal128 exactly for DECIMAL(precision,scale), without rounding. For scale 2,
// 1.2300 becomes 1.23; 1.234, non-finite values and precision overflow return an error.
Status bson_decimal_exact(const bson_decimal128_t& value, int precision, int scale,
                          std::string* text);

// Check document/array/code-with-scope subtrees depth-first with fixed, depth-bounded scratch
// against depth, size and element limits before conversion; this does not validate a target SQL type.
Status validate_bson_tree(const BsonValueView& value);

// Encode one present value as owned canonical Extended JSON, retaining BSON tags such as $oid and
// $numberLong. Reject missing or over-limit trees; Nullable SerDe handles SQL NULL before this call.
Status bson_canonical_json(const BsonValueView& value, std::string* json);

} // namespace doris
