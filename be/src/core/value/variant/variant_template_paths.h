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

#include "core/data_type/define_primitive_type.h"

namespace doris {

// One field of a VARIANT Schema Template as the FE sends it (TScalarType.variant_template_paths,
// in declaration order): a dotted path for MATCH_NAME, a glob for MATCH_NAME_GLOB, and the field's
// type (for an ARRAY field, its item type).
struct VariantTemplatePath {
    std::string pattern;
    bool is_glob = false;
    PrimitiveType type = INVALID_TYPE;
};

// How the JSON parser reads a number on a path, decided by the type of the template field the path
// matches.
enum class VariantNumberRead : uint8_t {
    // As the parser reads every number: int64, uint64 or binary64. No field matches the path, or
    // the field's type is one a number reaches unharmed that way.
    NATIVE,
    // A DECIMAL field: the number is read from its text, so no digit goes through a binary64.
    DECIMAL_TEXT,
    // A LARGEINT field: an integer beyond 64 bits is read from its text.
    LARGEINT_TEXT,
};

// The Schema Template of one VARIANT type, answering how a number on a path is read. Built once
// per type, shared by its serde and by the parse functions.
class VariantTemplatePaths {
public:
    explicit VariantTemplatePaths(std::vector<VariantTemplatePath> paths);

    bool empty() const noexcept { return _paths.empty(); }
    const std::vector<VariantTemplatePath>& paths() const noexcept { return _paths; }

    // Whether any field is a DECIMAL, so that some path may answer DECIMAL_TEXT. Without one no
    // fraction or exponent is read from its text, and a document the DOM parser reads is read the
    // same way with the template as without it: only an integer beyond 64 bits, which fails the
    // DOM parser, still needs the text. True even when an earlier field shadows every DECIMAL one.
    bool reads_decimal_text() const noexcept { return _reads_decimal_text; }

    // The read the FIRST template field matching `path` (the object keys from the root joined by
    // '.') asks for; NATIVE when none matches. First match is the rule the shredder applies to
    // typed paths (variant_util.cpp walks the template in declaration order, strcmp for
    // MATCH_NAME, fnmatch with FNM_PATHNAME for a glob), so that with 'amount*':string declared
    // before 'amount':decimal(10,2) the number under "amount" is a STRING on both sides instead of
    // a decimal 1.50 stored into a STRING column. An empty `path` is the path of an empty key,
    // {"":1}, which a glob can match; a value under no key at all is on no path, and the caller
    // does not ask.
    VariantNumberRead number_read(const std::string& path) const;

private:
    std::vector<VariantTemplatePath> _paths;
    bool _reads_decimal_text = false;
};

using VariantTemplatePathsPtr = std::shared_ptr<const VariantTemplatePaths>;

// A JSON number read exactly: the value is unscaled * 10^-scale, and `width` is the narrowest
// Variant decimal (4, 8 or 16 bytes) whose precision holds both the digits and the scale.
struct VariantDecimalText {
    __int128 unscaled = 0;
    uint8_t scale = 0;
    uint8_t width = 4;
};

enum class JsonNumberToDecimal : uint8_t {
    OK,
    // Not a JSON number.
    MALFORMED,
    // A value of 10^-39 or more with more than 38 significant digits, or with a scale outside
    // [0, 38] once the exponent is folded in.
    OUT_OF_RANGE,
};

// Reads a JSON number token (sign, digits, fraction, exponent) into a decimal of at most 38
// significant digits with a scale in [0, 38]: an exponent is folded into the scale and a negative
// scale into the unscaled value. The value is exact, with one exception: a nonzero value below
// 10^-39 is read as 0 at scale 38, the value it rounds to at every scale a decimal has. Enforces
// the JSON number grammar itself (the caller hands it tokens simdjson could not read), so a token
// it declines as MALFORMED is not a JSON number. Never throws.
JsonNumberToDecimal parse_json_number_as_decimal(std::string_view token,
                                                 VariantDecimalText& out) noexcept;

enum class JsonNumberToLargeInt : uint8_t {
    OK,
    // Not a JSON integer: a fraction, an exponent, a leading zero, or no JSON number at all.
    NOT_AN_INTEGER,
    // A JSON integer outside [-2^127, 2^127 - 1].
    OUT_OF_RANGE,
};

// Reads a JSON integer token (an optional '-', then "0" or a digit string that does not start with
// '0') into a 128-bit integer. Enforces that grammar itself, as parse_json_number_as_decimal does
// and for the same reason, so a token it declines as NOT_AN_INTEGER may still be a JSON number,
// 1.5 or 1e3, only not one a LARGEINT holds as written. "-0" is 0. Never throws.
JsonNumberToLargeInt parse_json_integer_as_largeint(std::string_view token, __int128& out) noexcept;

} // namespace doris
