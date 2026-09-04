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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "common/status.h"

namespace doris::segment_v2::inverted_index {

inline constexpr size_t COMMON_GRAM_MAX_ENCODED_BYTES = 16383;
inline constexpr char PLAIN_ESCAPE_PREFIX = '\x1e';
inline constexpr std::string_view INTERNAL_TERM_NAMESPACE_BEGIN {"\x1f", 1};
inline constexpr std::string_view INTERNAL_TERM_NAMESPACE_END {"\x20", 1};
inline constexpr std::string_view CG_V1_MARKER =
        "\x1f"
        "DORIS_COMMON_GRAM_V1"
        "\x1f";
inline constexpr std::string_view CG_V1_MARKER_END =
        "\x1f"
        "DORIS_COMMON_GRAM_V1"
        "\x20";

enum class PlainTermKeyVersion : uint8_t {
    kLegacyRaw = 0,
    kEscapedV1 = 1,
    kRawNoInternal = 2,
};

Result<std::string> encode_plain_term(std::string_view term, PlainTermKeyVersion version);
Result<bool> try_encode_plain_term(std::string_view term, PlainTermKeyVersion version,
                                   std::string* output);
Result<std::optional<std::string_view>> try_encode_plain_term_view(std::string_view term,
                                                                   PlainTermKeyVersion version,
                                                                   std::string* scratch);
bool try_encode_escaped_plain_term_prevalidated(std::string_view logical_term, std::string& output);
Result<std::string_view> decode_plain_term_view(std::string_view term, PlainTermKeyVersion version,
                                                std::string* scratch);
Result<std::string> decode_plain_term(std::string_view term, PlainTermKeyVersion version);
bool is_internal_term_key(std::string_view physical_term);
bool legacy_raw_exact_requires_bypass(std::string_view logical_term);
bool legacy_raw_prefix_requires_bypass(std::string_view logical_prefix);
Status validate_common_grams_logical_term(std::string_view term, std::string_view component);
Result<bool> try_encode_common_gram(std::string_view left, std::string_view right,
                                    std::string* output);
bool try_encode_common_gram_prevalidated(std::string_view left, std::string_view right,
                                         std::string& output);
bool common_gram_component_sizes_encodable(size_t left_size, size_t right_size);
bool is_common_gram_encodable_prevalidated(std::string_view left, std::string_view right);
Result<std::string> encode_common_gram(std::string_view left, std::string_view right);
bool is_common_gram_encodable(std::string_view left, std::string_view right);

} // namespace doris::segment_v2::inverted_index
