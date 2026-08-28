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

#include "storage/index/inverted/common_grams/common_grams_key_codec.h"

#include <string>
#include <string_view>

#include "util/utf8_check.h"

namespace doris::segment_v2::inverted_index {
namespace {

constexpr size_t COMMON_GRAM_LENGTH_HEX_BYTES = 8;
constexpr size_t COMMON_GRAM_SEPARATOR_BYTES = 1;
constexpr size_t COMMON_GRAM_FIXED_BYTES =
        CG_V1_MARKER.size() + COMMON_GRAM_LENGTH_HEX_BYTES + COMMON_GRAM_SEPARATOR_BYTES;
constexpr std::string_view HEX_DIGITS = "0123456789abcdef";
constexpr std::string_view LEGACY_PHRASE_BIGRAM_MARKER =
        "\x1f"
        "SNII_PHRASE_BIGRAM"
        "\x1f";

ResultError analyzer_error(std::string_view message) {
    return ResultError(Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>("{}", message));
}

bool prefixes_overlap(std::string_view left, std::string_view right) {
    return left.starts_with(right) || right.starts_with(left);
}

} // namespace

Status validate_common_grams_logical_term(std::string_view term, std::string_view component) {
    if (term.find('\0') != std::string_view::npos) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "CommonGrams {} contains NUL", component);
    }
    if (!validate_utf8(term.data(), term.size())) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "CommonGrams {} is not valid UTF-8", component);
    }
    if (term.size() > COMMON_GRAM_MAX_ENCODED_BYTES) {
        return Status::Error<ErrorCode::INVERTED_INDEX_ANALYZER_ERROR>(
                "CommonGrams {} has {} UTF-8 bytes, exceeding {}", component, term.size(),
                COMMON_GRAM_MAX_ENCODED_BYTES);
    }
    return Status::OK();
}

Result<std::string> encode_plain_term(std::string_view term, PlainTermKeyVersion version) {
    std::string encoded;
    auto encoded_result = try_encode_plain_term(term, version, &encoded);
    if (!encoded_result.has_value()) {
        return ResultError(std::move(encoded_result.error()));
    }
    if (!encoded_result.value()) {
        return analyzer_error("escaped plain term would exceed the 16383-byte key limit");
    }
    return encoded;
}

Result<bool> try_encode_plain_term(std::string_view term, PlainTermKeyVersion version,
                                   std::string* output) {
    DORIS_CHECK(output != nullptr);
    output->clear();
    auto status = validate_common_grams_logical_term(term, "plain term");
    if (!status.ok()) {
        return ResultError(std::move(status));
    }

    switch (version) {
    case PlainTermKeyVersion::kLegacyRaw:
    case PlainTermKeyVersion::kRawNoInternal:
        output->assign(term);
        return true;
    case PlainTermKeyVersion::kEscapedV1:
        if (term.empty() || (term.front() != PLAIN_ESCAPE_PREFIX && term.front() != '\x1f')) {
            output->assign(term);
            return true;
        }
        return try_encode_escaped_plain_term_prevalidated(term, *output);
    }
    return analyzer_error("unknown plain-term key version");
}

Result<std::optional<std::string_view>> try_encode_plain_term_view(std::string_view term,
                                                                   PlainTermKeyVersion version,
                                                                   std::string* scratch) {
    DORIS_CHECK(scratch != nullptr);
    scratch->clear();
    auto status = validate_common_grams_logical_term(term, "plain term");
    if (!status.ok()) {
        return ResultError(std::move(status));
    }

    switch (version) {
    case PlainTermKeyVersion::kLegacyRaw:
    case PlainTermKeyVersion::kRawNoInternal:
        return std::optional<std::string_view>(term);
    case PlainTermKeyVersion::kEscapedV1:
        if (term.empty() || (term.front() != PLAIN_ESCAPE_PREFIX && term.front() != '\x1f')) {
            return std::optional<std::string_view>(term);
        }
        if (!try_encode_escaped_plain_term_prevalidated(term, *scratch)) {
            return std::optional<std::string_view>();
        }
        return std::optional<std::string_view>(*scratch);
    }
    return analyzer_error("unknown plain-term key version");
}

bool try_encode_escaped_plain_term_prevalidated(std::string_view logical_term,
                                                std::string& output) {
    DCHECK(!logical_term.empty());
    DCHECK(logical_term.front() == PLAIN_ESCAPE_PREFIX || logical_term.front() == '\x1f');
    DCHECK(validate_common_grams_logical_term(logical_term, "plain term").ok());
    output.clear();
    if (logical_term.size() == COMMON_GRAM_MAX_ENCODED_BYTES) {
        return false;
    }
    output.reserve(logical_term.size() + 1);
    output.push_back(PLAIN_ESCAPE_PREFIX);
    output.push_back(logical_term.front() == PLAIN_ESCAPE_PREFIX ? 'E' : 'G');
    output.append(logical_term.substr(1));
    return true;
}

Result<std::string_view> decode_plain_term_view(std::string_view term, PlainTermKeyVersion version,
                                                std::string* scratch) {
    DORIS_CHECK(scratch != nullptr);
    scratch->clear();
    if (term.size() > COMMON_GRAM_MAX_ENCODED_BYTES) {
        return analyzer_error("encoded plain term exceeds the 16383-byte key limit");
    }

    switch (version) {
    case PlainTermKeyVersion::kLegacyRaw:
    case PlainTermKeyVersion::kRawNoInternal: {
        auto status = validate_common_grams_logical_term(term, "plain term");
        if (!status.ok()) {
            return ResultError(std::move(status));
        }
        return term;
    }
    case PlainTermKeyVersion::kEscapedV1: {
        if (term.empty() || term.front() != PLAIN_ESCAPE_PREFIX) {
            if (!term.empty() && term.front() == '\x1f') {
                return analyzer_error("escaped plain term enters the internal namespace");
            }
            auto status = validate_common_grams_logical_term(term, "plain term");
            if (!status.ok()) {
                return ResultError(std::move(status));
            }
            return term;
        }
        if (term.size() < 2 || (term[1] != 'E' && term[1] != 'G')) {
            return analyzer_error("invalid plain_term_escape:v1 key");
        }
        scratch->reserve(term.size() - 1);
        scratch->push_back(term[1] == 'E' ? PLAIN_ESCAPE_PREFIX : '\x1f');
        scratch->append(term.substr(2));
        auto status = validate_common_grams_logical_term(*scratch, "plain term");
        if (!status.ok()) {
            return ResultError(std::move(status));
        }
        return std::string_view(*scratch);
    }
    }
    return analyzer_error("unknown plain-term key version");
}

Result<std::string> decode_plain_term(std::string_view term, PlainTermKeyVersion version) {
    std::string scratch;
    auto decoded = decode_plain_term_view(term, version, &scratch);
    if (!decoded.has_value()) {
        return ResultError(std::move(decoded.error()));
    }
    return std::string(*decoded);
}

bool is_internal_term_key(std::string_view physical_term) {
    return physical_term.starts_with(INTERNAL_TERM_NAMESPACE_BEGIN);
}

bool legacy_raw_exact_requires_bypass(std::string_view logical_term) {
    return logical_term.starts_with(CG_V1_MARKER) ||
           logical_term.starts_with(LEGACY_PHRASE_BIGRAM_MARKER);
}

bool legacy_raw_prefix_requires_bypass(std::string_view logical_prefix) {
    return prefixes_overlap(logical_prefix, CG_V1_MARKER) ||
           prefixes_overlap(logical_prefix, LEGACY_PHRASE_BIGRAM_MARKER);
}

bool is_common_gram_encodable(std::string_view left, std::string_view right) {
    if (!validate_common_grams_logical_term(left, "left term").ok() ||
        !validate_common_grams_logical_term(right, "right term").ok()) {
        return false;
    }
    return is_common_gram_encodable_prevalidated(left, right);
}

bool is_common_gram_encodable_prevalidated(std::string_view left, std::string_view right) {
    DCHECK(validate_common_grams_logical_term(left, "left term").ok());
    DCHECK(validate_common_grams_logical_term(right, "right term").ok());
    return common_gram_component_sizes_encodable(left.size(), right.size());
}

bool common_gram_component_sizes_encodable(size_t left_size, size_t right_size) {
    return left_size <= COMMON_GRAM_MAX_ENCODED_BYTES - COMMON_GRAM_FIXED_BYTES &&
           right_size <= COMMON_GRAM_MAX_ENCODED_BYTES - COMMON_GRAM_FIXED_BYTES - left_size;
}

Result<bool> try_encode_common_gram(std::string_view left, std::string_view right,
                                    std::string* output) {
    DORIS_CHECK(output != nullptr);
    output->clear();
    auto left_status = validate_common_grams_logical_term(left, "left term");
    if (!left_status.ok()) {
        return ResultError(std::move(left_status));
    }
    auto right_status = validate_common_grams_logical_term(right, "right term");
    if (!right_status.ok()) {
        return ResultError(std::move(right_status));
    }
    return try_encode_common_gram_prevalidated(left, right, *output);
}

bool try_encode_common_gram_prevalidated(std::string_view left, std::string_view right,
                                         std::string& output) {
    DCHECK(validate_common_grams_logical_term(left, "left term").ok());
    DCHECK(validate_common_grams_logical_term(right, "right term").ok());
    output.clear();
    if (COMMON_GRAM_FIXED_BYTES + left.size() + right.size() > COMMON_GRAM_MAX_ENCODED_BYTES) {
        return false;
    }

    output.reserve(COMMON_GRAM_FIXED_BYTES + left.size() + right.size());
    output.append(CG_V1_MARKER);
    char length_and_separator[COMMON_GRAM_LENGTH_HEX_BYTES + COMMON_GRAM_SEPARATOR_BYTES];
    for (size_t i = 0; i < COMMON_GRAM_LENGTH_HEX_BYTES; ++i) {
        const size_t shift = (COMMON_GRAM_LENGTH_HEX_BYTES - i - 1) * 4;
        length_and_separator[i] = HEX_DIGITS[(left.size() >> shift) & 0xf];
    }
    length_and_separator[COMMON_GRAM_LENGTH_HEX_BYTES] = ':';
    output.append(length_and_separator, sizeof(length_and_separator));
    output.append(left);
    output.append(right);
    return true;
}

Result<std::string> encode_common_gram(std::string_view left, std::string_view right) {
    std::string encoded;
    auto encoded_result = try_encode_common_gram(left, right, &encoded);
    if (!encoded_result.has_value()) {
        return ResultError(std::move(encoded_result.error()));
    }
    if (!encoded_result.value()) {
        return analyzer_error("encoded common gram would exceed the 16383-byte key limit");
    }
    return encoded;
}

} // namespace doris::segment_v2::inverted_index
