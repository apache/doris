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
// parse functions.
class VariantDecimalPathSet {
public:
    explicit VariantDecimalPathSet(std::vector<VariantDecimalPath> paths);

    bool empty() const noexcept { return _paths.empty(); }
    const std::vector<VariantDecimalPath>& paths() const noexcept { return _paths; }

    // True when `path` (the object keys from the root joined by '.') is a declared DECIMAL path, by
    // the name / glob rules the shredder applies to typed paths (fnmatch with FNM_PATHNAME).
    bool matches(std::string_view path) const;

private:
    std::vector<VariantDecimalPath> _paths;
    std::unordered_set<std::string> _exact;
    std::vector<std::string> _globs;
};

using VariantDecimalPathSetPtr = std::shared_ptr<const VariantDecimalPathSet>;

// A JSON number read exactly: the value is unscaled * 10^-scale.
struct VariantDecimalText {
    __int128 unscaled = 0;
    uint8_t scale = 0;
};

enum class JsonNumberToDecimal : uint8_t {
    OK,
    // Not a JSON number.
    MALFORMED,
    // More than 38 significant digits, or a scale outside [0, 38] once the exponent is folded in.
    OUT_OF_RANGE,
};

// Reads a JSON number token (sign, digits, fraction, exponent) into a decimal of at most 38
// significant digits with a scale in [0, 38]: an exponent is folded into the scale and a negative
// scale into the unscaled value. Never throws.
JsonNumberToDecimal parse_json_number_as_decimal(std::string_view token,
                                                 VariantDecimalText* out) noexcept;

} // namespace doris
