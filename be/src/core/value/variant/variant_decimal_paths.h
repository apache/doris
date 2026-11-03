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
#include <vector>

namespace doris {

// One field of a VARIANT Schema Template as the FE sends it (TScalarType.variant_template_paths, in
// declaration order): a dotted path for MATCH_NAME, a glob for MATCH_NAME_GLOB, and whether the field
// is DECIMAL / ARRAY<DECIMAL>.
struct VariantTemplatePath {
    std::string pattern;
    bool is_glob = false;
    bool is_decimal = false;
};

// The Schema Template of one VARIANT type, answering whether a path is a DECIMAL path. Built once per
// type, shared by its serde and by the parse functions.
class VariantDecimalPathSet {
public:
    explicit VariantDecimalPathSet(std::vector<VariantTemplatePath> paths);

    bool empty() const noexcept { return _paths.empty(); }
    const std::vector<VariantTemplatePath>& paths() const noexcept { return _paths; }

    // True when the FIRST template field matching `path` (the object keys from the root joined by
    // '.') is DECIMAL: the rule the shredder applies to typed paths (variant_util.cpp walks the
    // template in declaration order, strcmp for MATCH_NAME, fnmatch with FNM_PATHNAME for a glob),
    // so that with 'amount*':string declared before 'amount':decimal(10,2) the number under "amount"
    // is a STRING on both sides instead of a decimal 1.50 stored into a STRING column.
    bool matches(const std::string& path) const;

private:
    std::vector<VariantTemplatePath> _paths;
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
// scale into the unscaled value. Enforces the JSON number grammar itself (the caller hands it
// tokens simdjson could not read), so a token it declines is not a JSON number. Never throws.
JsonNumberToDecimal parse_json_number_as_decimal(std::string_view token,
                                                 VariantDecimalText& out) noexcept;

} // namespace doris
