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
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "core/string_ref.h"

namespace doris {

// One DECIMAL / ARRAY<DECIMAL> entry of a VARIANT Schema Template as the FE sends it
// (TScalarType.variant_decimal_paths): a dotted path for MATCH_NAME, a glob for MATCH_NAME_GLOB.
struct VariantDecimalPath {
    std::string pattern;
    bool is_glob = false;
    int32_t precision = 0;
    int32_t scale = 0;
};

// The DECIMAL template paths of one VARIANT type. Built once per type, shared by its serde and by the
// parse functions, consulted only when a JSON number token carries a fraction.
class VariantDecimalPathSet {
public:
    explicit VariantDecimalPathSet(std::vector<VariantDecimalPath> paths);

    bool empty() const noexcept { return _paths.empty(); }
    const std::vector<VariantDecimalPath>& paths() const noexcept { return _paths; }

    // True when `path` (the object keys from the root joined by '.') is a declared DECIMAL path,
    // using the same name / glob rules the shredder applies to typed paths.
    bool matches(std::string_view path) const;

private:
    std::vector<VariantDecimalPath> _paths;
    std::unordered_set<std::string> _exact;
    std::vector<std::string> _globs;
};

using VariantDecimalPathSetPtr = std::shared_ptr<const VariantDecimalPathSet>;

// Quotes every number token of the form `-?digits.digits` (a fraction, no exponent) that sits on a
// declared DECIMAL path, so the parser takes the string route and the shredder casts the text exactly;
// every other byte is copied as it is. Integers, exponent forms and undeclared paths are untouched.
// Returns true and fills `out` when at least one token was quoted; false (out unspecified) when none
// qualified or the text is not JSON this scanner understands, and the caller parses the original text.
// Object keys are matched as written between their quotes (an escaped key never matches). Never throws.
bool quote_decimal_numbers_on_paths(StringRef json, const VariantDecimalPathSet& paths,
                                    std::string* out);

} // namespace doris
