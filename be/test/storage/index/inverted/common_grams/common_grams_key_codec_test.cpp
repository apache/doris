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

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include "util/utf8_check.h"

namespace doris::segment_v2::inverted_index {
namespace {

TEST(CommonGramsKeyCodecTest, GramGoldenBytesAndMarkerRange) {
    const std::string expected =
            "\x1f"
            "DORIS_COMMON_GRAM_V1"
            "\x1f"
            "00000001:ab";
    auto encoded = encode_common_gram("a", "b");
    ASSERT_TRUE(encoded.has_value()) << encoded.error();
    EXPECT_EQ(encoded.value(), expected);

    EXPECT_EQ(CG_V1_MARKER, std::string_view("\x1f"
                                             "DORIS_COMMON_GRAM_V1"
                                             "\x1f",
                                             22));
    EXPECT_EQ(CG_V1_MARKER_END, std::string_view("\x1f"
                                                 "DORIS_COMMON_GRAM_V1"
                                                 "\x20",
                                                 22));
    EXPECT_GE(encoded.value(), CG_V1_MARKER);
    EXPECT_LT(encoded.value(), CG_V1_MARKER_END);
    EXPECT_EQ(encoded->find('\0'), std::string::npos);
    EXPECT_TRUE(validate_utf8(encoded->data(), encoded->size()));

    auto hexadecimal_length = encode_common_gram("0123456789", "x");
    ASSERT_TRUE(hexadecimal_length.has_value()) << hexadecimal_length.error();
    EXPECT_EQ(hexadecimal_length.value(),
              "\x1f"
              "DORIS_COMMON_GRAM_V1"
              "\x1f"
              "0000000a:0123456789x");

    auto different_boundary = encode_common_gram("ab", "c");
    ASSERT_TRUE(different_boundary.has_value()) << different_boundary.error();
    auto same_text = encode_common_gram("a", "bc");
    ASSERT_TRUE(same_text.has_value()) << same_text.error();
    EXPECT_NE(different_boundary.value(), same_text.value());
}

TEST(CommonGramsKeyCodecTest, ValidatesLogicalTermsWithoutEncoding) {
    EXPECT_TRUE(validate_common_grams_logical_term("valid", "test term").ok());
    EXPECT_TRUE(validate_common_grams_logical_term("", "test term").ok());

    const std::string nul_term("a\0b", 3);
    const std::string invalid_utf8("\xc3\x28", 2);
    const std::string overlong(COMMON_GRAM_MAX_ENCODED_BYTES + 1, 'x');
    for (const auto& term : {nul_term, invalid_utf8, overlong}) {
        auto status = validate_common_grams_logical_term(term, "test term");
        EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    }
}

TEST(CommonGramsKeyCodecTest, TryEncodeDistinguishesTooLongAndReusesOutputCapacity) {
    std::string output;
    output.reserve(COMMON_GRAM_MAX_ENCODED_BYTES);
    const char* reserved_data = output.data();

    auto encoded = try_encode_common_gram("a", "b", &output);
    ASSERT_TRUE(encoded.has_value()) << encoded.error();
    EXPECT_TRUE(encoded.value());
    EXPECT_EQ(output, encode_common_gram("a", "b").value());
    EXPECT_EQ(output.data(), reserved_data);

    const std::string huge(COMMON_GRAM_MAX_ENCODED_BYTES, 'x');
    auto too_long = try_encode_common_gram("the", huge, &output);
    ASSERT_TRUE(too_long.has_value()) << too_long.error();
    EXPECT_FALSE(too_long.value());
    EXPECT_TRUE(output.empty());
    EXPECT_EQ(output.data(), reserved_data);

    const std::string invalid_utf8("\xc3\x28", 2);
    auto invalid = try_encode_common_gram(invalid_utf8, "valid", &output);
    EXPECT_FALSE(invalid.has_value());
    EXPECT_EQ(invalid.error().code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    EXPECT_TRUE(output.empty());
}

TEST(CommonGramsKeyCodecTest, PrevalidatedEncoderMatchesCheckedEncoder) {
    for (const auto& [left, right] : std::vector<std::pair<std::string, std::string>> {
                 {"a", "b"}, {"the", "term"}, {"", "right"}}) {
        std::string checked;
        auto checked_result = try_encode_common_gram(left, right, &checked);
        ASSERT_TRUE(checked_result.has_value()) << checked_result.error();

        std::string prevalidated;
        EXPECT_EQ(try_encode_common_gram_prevalidated(left, right, prevalidated),
                  checked_result.value());
        EXPECT_EQ(prevalidated, checked);
    }

    std::string too_long(COMMON_GRAM_MAX_ENCODED_BYTES, 'x');
    std::string output = "stale";
    EXPECT_FALSE(try_encode_common_gram_prevalidated("left", too_long, output));
    EXPECT_TRUE(output.empty());
}

TEST(CommonGramsKeyCodecTest, PrevalidatedPlainEncoderMatchesCheckedEncoderAndBoundaries) {
    std::string output;
    output.reserve(COMMON_GRAM_MAX_ENCODED_BYTES);
    const char* reserved_data = output.data();

    for (const char marker : {PLAIN_ESCAPE_PREFIX, '\x1f'}) {
        const std::string logical = std::string(1, marker) + "plain";
        std::string checked;
        auto checked_result =
                try_encode_plain_term(logical, PlainTermKeyVersion::kEscapedV1, &checked);
        ASSERT_TRUE(checked_result.has_value()) << checked_result.error();
        ASSERT_TRUE(checked_result.value());

        EXPECT_TRUE(try_encode_escaped_plain_term_prevalidated(logical, output));
        EXPECT_EQ(output, checked);
        EXPECT_EQ(output.data(), reserved_data);
    }

    const std::string maximum_encodable = std::string(1, PLAIN_ESCAPE_PREFIX) +
                                          std::string(COMMON_GRAM_MAX_ENCODED_BYTES - 2, 'x');
    EXPECT_TRUE(try_encode_escaped_plain_term_prevalidated(maximum_encodable, output));
    EXPECT_EQ(output.size(), COMMON_GRAM_MAX_ENCODED_BYTES);
    EXPECT_EQ(output.data(), reserved_data);

    const std::string too_long = std::string(1, PLAIN_ESCAPE_PREFIX) +
                                 std::string(COMMON_GRAM_MAX_ENCODED_BYTES - 1, 'x');
    EXPECT_FALSE(try_encode_escaped_plain_term_prevalidated(too_long, output));
    EXPECT_TRUE(output.empty());
    EXPECT_EQ(output.data(), reserved_data);
}

TEST(CommonGramsKeyCodecTest, PlainTermViewBorrowsOrdinaryKeysAndUsesScratchOnlyForEscapes) {
    const std::string ordinary = "ordinary";
    std::string scratch = "stale";
    auto borrowed = try_encode_plain_term_view(ordinary, PlainTermKeyVersion::kEscapedV1, &scratch);
    ASSERT_TRUE(borrowed.has_value()) << borrowed.error();
    ASSERT_TRUE(borrowed->has_value());
    EXPECT_EQ(**borrowed, ordinary);
    EXPECT_EQ(borrowed->value().data(), ordinary.data());
    EXPECT_TRUE(scratch.empty());

    const std::string escaped = std::string(1, '\x1f') + "literal";
    auto encoded = try_encode_plain_term_view(escaped, PlainTermKeyVersion::kEscapedV1, &scratch);
    ASSERT_TRUE(encoded.has_value()) << encoded.error();
    ASSERT_TRUE(encoded->has_value());
    EXPECT_EQ(**encoded, *encode_plain_term(escaped, PlainTermKeyVersion::kEscapedV1));
    EXPECT_EQ(encoded->value().data(), scratch.data());

    std::string unrepresentable(COMMON_GRAM_MAX_ENCODED_BYTES, 'x');
    unrepresentable.front() = PLAIN_ESCAPE_PREFIX;
    auto absent =
            try_encode_plain_term_view(unrepresentable, PlainTermKeyVersion::kEscapedV1, &scratch);
    ASSERT_TRUE(absent.has_value()) << absent.error();
    EXPECT_FALSE(absent->has_value());
    EXPECT_TRUE(scratch.empty());
}

TEST(CommonGramsKeyCodecTest, GramComponentsUseUnescapedLogicalBytes) {
    const std::array cases {
            std::tuple {std::string(1, '\x1e'), std::string(1, '\x1f'),
                        std::string("\x1f"
                                    "DORIS_COMMON_GRAM_V1"
                                    "\x1f"
                                    "00000001:\x1e\x1f")},
            std::tuple {std::string(1, '\x1f'), std::string(1, '\x1e'),
                        std::string("\x1f"
                                    "DORIS_COMMON_GRAM_V1"
                                    "\x1f"
                                    "00000001:\x1f\x1e")},
            std::tuple {std::string("\x1e"
                                    "L"),
                        std::string("\x1f"
                                    "R"),
                        std::string("\x1f"
                                    "DORIS_COMMON_GRAM_V1"
                                    "\x1f"
                                    "00000002:\x1e"
                                    "L\x1f"
                                    "R")},
            std::tuple {std::string("\x1f"
                                    "L"),
                        std::string("\x1e"
                                    "R"),
                        std::string("\x1f"
                                    "DORIS_COMMON_GRAM_V1"
                                    "\x1f"
                                    "00000002:\x1f"
                                    "L\x1e"
                                    "R")},
    };
    for (const auto& [left, right, expected] : cases) {
        auto encoded = encode_common_gram(left, right);
        ASSERT_TRUE(encoded.has_value()) << encoded.error();
        EXPECT_EQ(encoded.value(), expected);
    }
}

TEST(CommonGramsKeyCodecTest, PlainKeyVersionsHaveReversibleGoldenBytes) {
    const std::array raw_versions {PlainTermKeyVersion::kLegacyRaw,
                                   PlainTermKeyVersion::kRawNoInternal};
    const std::string escape_leading =
            "\x1e"
            "alpha";
    const std::string gram_leading =
            "\x1f"
            "alpha";

    for (PlainTermKeyVersion version : raw_versions) {
        auto escape_encoded = encode_plain_term(escape_leading, version);
        ASSERT_TRUE(escape_encoded.has_value()) << escape_encoded.error();
        EXPECT_EQ(escape_encoded.value(), escape_leading);
        auto escape_decoded = decode_plain_term(escape_encoded.value(), version);
        ASSERT_TRUE(escape_decoded.has_value()) << escape_decoded.error();
        EXPECT_EQ(escape_decoded.value(), escape_leading);

        auto gram_encoded = encode_plain_term(gram_leading, version);
        ASSERT_TRUE(gram_encoded.has_value()) << gram_encoded.error();
        EXPECT_EQ(gram_encoded.value(), gram_leading);
        auto gram_decoded = decode_plain_term(gram_encoded.value(), version);
        ASSERT_TRUE(gram_decoded.has_value()) << gram_decoded.error();
        EXPECT_EQ(gram_decoded.value(), gram_leading);
    }

    auto escape_encoded = encode_plain_term(escape_leading, PlainTermKeyVersion::kEscapedV1);
    ASSERT_TRUE(escape_encoded.has_value()) << escape_encoded.error();
    EXPECT_EQ(escape_encoded.value(),
              "\x1e"
              "Ealpha");
    auto escape_decoded =
            decode_plain_term(escape_encoded.value(), PlainTermKeyVersion::kEscapedV1);
    ASSERT_TRUE(escape_decoded.has_value()) << escape_decoded.error();
    EXPECT_EQ(escape_decoded.value(), escape_leading);

    auto gram_encoded = encode_plain_term(gram_leading, PlainTermKeyVersion::kEscapedV1);
    ASSERT_TRUE(gram_encoded.has_value()) << gram_encoded.error();
    EXPECT_EQ(gram_encoded.value(),
              "\x1e"
              "Galpha");
    auto gram_decoded = decode_plain_term(gram_encoded.value(), PlainTermKeyVersion::kEscapedV1);
    ASSERT_TRUE(gram_decoded.has_value()) << gram_decoded.error();
    EXPECT_EQ(gram_decoded.value(), gram_leading);

    EXPECT_FALSE(decode_plain_term("\x1e", PlainTermKeyVersion::kEscapedV1).has_value());
    EXPECT_FALSE(decode_plain_term("\x1e"
                                   "Xalpha",
                                   PlainTermKeyVersion::kEscapedV1)
                         .has_value());
}

TEST(CommonGramsKeyCodecTest, DecodePlainTermViewBorrowsOrdinaryInput) {
    const std::array cases {
            std::pair {PlainTermKeyVersion::kLegacyRaw, std::string("legacy")},
            std::pair {PlainTermKeyVersion::kEscapedV1, std::string("ordinary")},
    };
    for (const auto& [version, physical_term] : cases) {
        std::string scratch;
        auto decoded = decode_plain_term_view(physical_term, version, &scratch);
        ASSERT_TRUE(decoded.has_value()) << decoded.error();
        EXPECT_EQ(decoded.value(), physical_term);
        EXPECT_EQ(decoded->data(), physical_term.data());
        EXPECT_TRUE(scratch.empty());
    }
}

TEST(CommonGramsKeyCodecTest, DecodePlainTermViewUsesScratchForEscapes) {
    const std::array cases {
            std::pair {std::string("\x1e"
                                   "Ealpha"),
                       std::string("\x1e"
                                   "alpha")},
            std::pair {std::string("\x1e"
                                   "Galpha"),
                       std::string("\x1f"
                                   "alpha")},
    };
    for (const auto& [physical_term, logical_term] : cases) {
        std::string scratch = "stale";
        auto decoded =
                decode_plain_term_view(physical_term, PlainTermKeyVersion::kEscapedV1, &scratch);
        ASSERT_TRUE(decoded.has_value()) << decoded.error();
        EXPECT_EQ(decoded.value(), logical_term);
        EXPECT_EQ(scratch, logical_term);
        EXPECT_EQ(decoded->data(), scratch.data());
    }
}

TEST(CommonGramsKeyCodecTest, EscapedPlainKeysCannotEnterGramMarkerRange) {
    const std::array logical_terms {std::string("plain"),
                                    std::string("\x1e"
                                                "plain"),
                                    std::string("\x1f"
                                                "plain")};
    for (const auto& term : logical_terms) {
        auto encoded = encode_plain_term(term, PlainTermKeyVersion::kEscapedV1);
        ASSERT_TRUE(encoded.has_value()) << encoded.error();
        EXPECT_FALSE(encoded.value() >= CG_V1_MARKER && encoded.value() < CG_V1_MARKER_END);
        auto decoded = decode_plain_term(encoded.value(), PlainTermKeyVersion::kEscapedV1);
        ASSERT_TRUE(decoded.has_value()) << decoded.error();
        EXPECT_EQ(decoded.value(), term);
    }
}

TEST(CommonGramsKeyCodecTest, InternalNamespaceAndLegacyBypassAreSeparated) {
    const std::string legacy_marker =
            "\x1f"
            "SNII_PHRASE_BIGRAM"
            "\x1f";
    EXPECT_EQ(INTERNAL_TERM_NAMESPACE_BEGIN, std::string_view("\x1f", 1));
    EXPECT_EQ(INTERNAL_TERM_NAMESPACE_END, std::string_view("\x20", 1));

    EXPECT_TRUE(is_internal_term_key(encode_common_gram("a", "b").value()));
    EXPECT_TRUE(is_internal_term_key(legacy_marker + "payload"));
    EXPECT_TRUE(
            is_internal_term_key("\x1f"
                                 "FUTURE_INTERNAL_TERM"));
    EXPECT_FALSE(
            is_internal_term_key("\x1e"
                                 "Gescaped"));
    EXPECT_FALSE(is_internal_term_key("plain"));

    EXPECT_TRUE(legacy_raw_exact_requires_bypass(CG_V1_MARKER));
    EXPECT_TRUE(legacy_raw_exact_requires_bypass(legacy_marker + "payload"));
    EXPECT_FALSE(legacy_raw_exact_requires_bypass(std::string(1, '\x1f')));
    EXPECT_FALSE(
            legacy_raw_exact_requires_bypass("\x1f"
                                             "literal"));
    EXPECT_FALSE(legacy_raw_exact_requires_bypass("plain"));

    EXPECT_TRUE(legacy_raw_prefix_requires_bypass(""));
    EXPECT_TRUE(legacy_raw_prefix_requires_bypass(std::string(1, '\x1f')));
    EXPECT_TRUE(
            legacy_raw_prefix_requires_bypass("\x1f"
                                              "DORIS_COMMON"));
    EXPECT_TRUE(legacy_raw_prefix_requires_bypass(CG_V1_MARKER));
    EXPECT_TRUE(legacy_raw_prefix_requires_bypass(legacy_marker + "payload"));
    EXPECT_FALSE(
            legacy_raw_prefix_requires_bypass("\x1f"
                                              "literal"));
    EXPECT_FALSE(legacy_raw_prefix_requires_bypass("plain"));
}

TEST(CommonGramsKeyCodecTest, PlainEncodingPreservesLogicalPrefixRanges) {
    const std::array logical_prefixes {
            std::string("plain"),
            std::string("\x1e"
                        "escape"),
            std::string("\x1f"
                        "marker"),
    };
    const std::array logical_terms {
            logical_prefixes[0] + "-suffix",
            logical_prefixes[1] + "-suffix",
            logical_prefixes[2] + "-suffix",
    };
    for (const std::string& logical_prefix : logical_prefixes) {
        auto physical_prefix = encode_plain_term(logical_prefix, PlainTermKeyVersion::kEscapedV1);
        ASSERT_TRUE(physical_prefix.has_value()) << physical_prefix.error();
        for (const std::string& logical_term : logical_terms) {
            auto physical_term = encode_plain_term(logical_term, PlainTermKeyVersion::kEscapedV1);
            ASSERT_TRUE(physical_term.has_value()) << physical_term.error();
            EXPECT_EQ(logical_term.starts_with(logical_prefix),
                      physical_term->starts_with(physical_prefix.value()));
        }
    }
}

TEST(CommonGramsKeyCodecTest, EncodedLengthBoundariesAreShared) {
    EXPECT_EQ(COMMON_GRAM_MAX_ENCODED_BYTES, 16383);

    const std::array all_versions {PlainTermKeyVersion::kLegacyRaw, PlainTermKeyVersion::kEscapedV1,
                                   PlainTermKeyVersion::kRawNoInternal};
    for (PlainTermKeyVersion version : all_versions) {
        for (size_t size : {size_t {16382}, size_t {16383}}) {
            std::string term(size, 'p');
            auto encoded = encode_plain_term(term, version);
            ASSERT_TRUE(encoded.has_value()) << encoded.error();
            EXPECT_EQ(encoded->size(), size);
            auto decoded = decode_plain_term(encoded.value(), version);
            ASSERT_TRUE(decoded.has_value()) << decoded.error();
            EXPECT_EQ(decoded.value(), term);
        }
        const std::string overlong(16384, 'p');
        EXPECT_FALSE(encode_plain_term(overlong, version).has_value());
        EXPECT_FALSE(decode_plain_term(overlong, version).has_value());
    }

    for (const auto& [logical_size, encoded_size] :
         std::array {std::pair {size_t {16381}, size_t {16382}},
                     std::pair {size_t {16382}, size_t {16383}}}) {
        std::string term(logical_size, 'p');
        term.front() = '\x1e';
        auto encoded = encode_plain_term(term, PlainTermKeyVersion::kEscapedV1);
        ASSERT_TRUE(encoded.has_value()) << encoded.error();
        EXPECT_EQ(encoded->size(), encoded_size);
    }

    std::string escape_overflow(16383, 'p');
    escape_overflow.front() = '\x1f';
    EXPECT_FALSE(encode_plain_term(escape_overflow, PlainTermKeyVersion::kEscapedV1).has_value());
    std::string try_output = "stale";
    auto try_overflow =
            try_encode_plain_term(escape_overflow, PlainTermKeyVersion::kEscapedV1, &try_output);
    ASSERT_TRUE(try_overflow.has_value()) << try_overflow.error();
    EXPECT_FALSE(try_overflow.value());
    EXPECT_TRUE(try_output.empty());

    const std::string invalid_utf8("\xc3\x28", 2);
    auto try_invalid =
            try_encode_plain_term(invalid_utf8, PlainTermKeyVersion::kEscapedV1, &try_output);
    EXPECT_FALSE(try_invalid.has_value());
    EXPECT_EQ(try_invalid.error().code(), ErrorCode::INVERTED_INDEX_ANALYZER_ERROR);
    EXPECT_TRUE(try_output.empty());

    auto raw_fallback = encode_plain_term(escape_overflow, PlainTermKeyVersion::kRawNoInternal);
    ASSERT_TRUE(raw_fallback.has_value()) << raw_fallback.error();
    EXPECT_EQ(raw_fallback->size(), 16383);
    EXPECT_EQ(raw_fallback.value(), escape_overflow);

    for (const auto& [encoded_size, right_size, expected_encodable] :
         std::array {std::tuple {size_t {16382}, size_t {16350}, true},
                     std::tuple {size_t {16383}, size_t {16351}, true},
                     std::tuple {size_t {16384}, size_t {16352}, false}}) {
        std::string right(right_size, 'r');
        EXPECT_EQ(is_common_gram_encodable("a", right), expected_encodable);
        auto encoded = encode_common_gram("a", right);
        EXPECT_EQ(encoded.has_value(), expected_encodable);
        if (encoded.has_value()) {
            EXPECT_EQ(encoded->size(), encoded_size);
        }
    }
}

TEST(CommonGramsKeyCodecTest, GramLengthBoundariesCanBeConcentratedInLeft) {
    for (const auto& [encoded_size, left_size, expected_encodable] :
         std::array {std::tuple {size_t {16382}, size_t {16350}, true},
                     std::tuple {size_t {16383}, size_t {16351}, true},
                     std::tuple {size_t {16384}, size_t {16352}, false}}) {
        std::string left(left_size, 'l');
        EXPECT_EQ(is_common_gram_encodable(left, "r"), expected_encodable);
        auto encoded = encode_common_gram(left, "r");
        EXPECT_EQ(encoded.has_value(), expected_encodable);
        if (encoded.has_value()) {
            EXPECT_EQ(encoded->size(), encoded_size);
        }
    }

    const std::string e_acute = "\xc3\xa9";
    std::string multibyte_left;
    multibyte_left.reserve(16352);
    for (size_t i = 0; i < 8175; ++i) {
        multibyte_left.append(e_acute);
    }
    auto at_16382 = encode_common_gram(multibyte_left, "r");
    ASSERT_TRUE(at_16382.has_value()) << at_16382.error();
    EXPECT_EQ(at_16382->size(), 16382);

    multibyte_left.push_back('x');
    auto at_16383 = encode_common_gram(multibyte_left, "r");
    ASSERT_TRUE(at_16383.has_value()) << at_16383.error();
    EXPECT_EQ(at_16383->size(), 16383);

    multibyte_left.push_back('x');
    EXPECT_FALSE(is_common_gram_encodable(multibyte_left, "r"));
    EXPECT_FALSE(encode_common_gram(multibyte_left, "r").has_value());
}

TEST(CommonGramsKeyCodecTest, LengthUsesUtf8BytesRatherThanCodePoints) {
    const std::string chinese = "\xe4\xbd\xa0";
    const std::string e_acute = "\xc3\xa9";
    std::string right;
    right.reserve(16350);
    for (size_t i = 0; i < 8174; ++i) {
        right.append(e_acute);
    }

    auto at_16382 = encode_common_gram(chinese, right);
    ASSERT_TRUE(at_16382.has_value()) << at_16382.error();
    EXPECT_EQ(at_16382->size(), 16382);
    EXPECT_NE(at_16382->find("00000003:"), std::string::npos);

    right.push_back('x');
    auto at_16383 = encode_common_gram(chinese, right);
    ASSERT_TRUE(at_16383.has_value()) << at_16383.error();
    EXPECT_EQ(at_16383->size(), 16383);

    right.push_back('x');
    EXPECT_FALSE(is_common_gram_encodable(chinese, right));
    EXPECT_FALSE(encode_common_gram(chinese, right).has_value());
}

TEST(CommonGramsKeyCodecTest, RejectsNulAndInvalidUtf8) {
    const std::string nul_term("a\0b", 3);
    const std::string invalid_utf8("\xc3\x28", 2);
    for (const auto& term : {nul_term, invalid_utf8}) {
        EXPECT_FALSE(encode_plain_term(term, PlainTermKeyVersion::kLegacyRaw).has_value());
        EXPECT_FALSE(encode_plain_term(term, PlainTermKeyVersion::kEscapedV1).has_value());
        EXPECT_FALSE(encode_plain_term(term, PlainTermKeyVersion::kRawNoInternal).has_value());
        EXPECT_FALSE(decode_plain_term(term, PlainTermKeyVersion::kLegacyRaw).has_value());
        EXPECT_FALSE(is_common_gram_encodable(term, "valid"));
        EXPECT_FALSE(is_common_gram_encodable("valid", term));
        EXPECT_FALSE(encode_common_gram(term, "valid").has_value());
        EXPECT_FALSE(encode_common_gram("valid", term).has_value());
    }

    const std::string escaped_nul(
            "\x1e"
            "Ea\0b",
            5);
    const std::string escaped_invalid(
            "\x1e"
            "G\xc3\x28",
            4);
    EXPECT_FALSE(decode_plain_term(escaped_nul, PlainTermKeyVersion::kEscapedV1).has_value());
    EXPECT_FALSE(decode_plain_term(escaped_invalid, PlainTermKeyVersion::kEscapedV1).has_value());
    for (PlainTermKeyVersion version :
         {PlainTermKeyVersion::kLegacyRaw, PlainTermKeyVersion::kRawNoInternal}) {
        EXPECT_FALSE(decode_plain_term(nul_term, version).has_value());
        EXPECT_FALSE(decode_plain_term(invalid_utf8, version).has_value());
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index
