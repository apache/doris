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

#include "core/value/variant/variant_template_paths.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/data_type/define_primitive_type.h"

namespace doris {
namespace {

VariantTemplatePaths paths_of(std::vector<VariantTemplatePath> paths) {
    return VariantTemplatePaths(std::move(paths));
}

// gtest cannot print an __int128; compare through a decimal rendering.
std::string digits_of(__int128 value) {
    if (value == 0) {
        return "0";
    }
    const bool negative = value < 0;
    unsigned __int128 magnitude = negative ? -static_cast<unsigned __int128>(value)
                                           : static_cast<unsigned __int128>(value);
    std::string out;
    while (magnitude != 0) {
        out.insert(out.begin(), static_cast<char>('0' + static_cast<int>(magnitude % 10)));
        magnitude /= 10;
    }
    return negative ? "-" + out : out;
}

void expect_decimal_at(int finest_scale, std::string_view token, const char* unscaled, int scale,
                       int width) {
    VariantDecimalText out;
    ASSERT_EQ(JsonNumberToDecimal::OK, parse_json_number_as_decimal(token, out, finest_scale))
            << token;
    EXPECT_EQ(unscaled, digits_of(out.unscaled)) << token;
    EXPECT_EQ(scale, out.scale) << token;
    EXPECT_EQ(width, out.width) << token;
}

// On a field of up to 38 digits, the finest scale of which is 38.
void expect_decimal(std::string_view token, const char* unscaled, int scale, int width) {
    expect_decimal_at(kVariantDecimalFinestScale, token, unscaled, scale, width);
}

void expect_largeint(std::string_view token, const char* digits) {
    __int128 out = 7;
    ASSERT_EQ(JsonNumberToLargeInt::OK, parse_json_integer_as_largeint(token, out)) << token;
    EXPECT_EQ(digits, digits_of(out)) << token;
}

// A token the reader declines leaves `out` as it was.
void expect_no_largeint(std::string_view token, JsonNumberToLargeInt verdict) {
    __int128 out = 7;
    EXPECT_EQ(verdict, parse_json_integer_as_largeint(token, out)) << token;
    EXPECT_EQ("7", digits_of(out)) << token;
}

} // namespace

TEST(VariantTemplatePathsTest, SetMatchesExactAndGlobPaths) {
    const VariantTemplatePaths paths =
            paths_of({{.pattern = "money.amount", .is_glob = false, .type = TYPE_DECIMAL128I},
                      {.pattern = "ratio*", .is_glob = true, .type = TYPE_DECIMAL64},
                      {.pattern = "name", .is_glob = false, .type = TYPE_STRING}});
    EXPECT_FALSE(paths.empty());
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, paths.number_read("money.amount"));
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, paths.number_read("ratio_a"));
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, paths.number_read("ratio"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("name"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("money"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("money.amount.cents"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("amount"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read(""));
    EXPECT_TRUE(paths_of({}).empty());
    EXPECT_EQ(VariantNumberRead::NATIVE, paths_of({}).number_read("money.amount"));
}

TEST(VariantTemplatePathsTest, TheFieldsTypeSelectsTheRead) {
    const VariantTemplatePaths paths =
            paths_of({{.pattern = "d32", .is_glob = false, .type = TYPE_DECIMAL32},
                      {.pattern = "d64", .is_glob = false, .type = TYPE_DECIMAL64},
                      {.pattern = "d128", .is_glob = false, .type = TYPE_DECIMAL128I},
                      {.pattern = "d256", .is_glob = false, .type = TYPE_DECIMAL256},
                      {.pattern = "id", .is_glob = false, .type = TYPE_LARGEINT},
                      {.pattern = "ids_*", .is_glob = true, .type = TYPE_LARGEINT},
                      {.pattern = "n", .is_glob = false, .type = TYPE_BIGINT},
                      {.pattern = "s", .is_glob = false, .type = TYPE_STRING},
                      {.pattern = "f", .is_glob = false, .type = TYPE_DOUBLE},
                      {.pattern = "unset", .is_glob = false, .type = INVALID_TYPE}});
    // every DECIMAL width reads a number from its text
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, paths.number_read("d32"));
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, paths.number_read("d64"));
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, paths.number_read("d128"));
    // DECIMAL256 too, told apart for its finer scale
    EXPECT_EQ(VariantNumberRead::DECIMAL256_TEXT, paths.number_read("d256"));
    // LARGEINT, by name or by glob, reads an integer beyond 64 bits from its text
    EXPECT_EQ(VariantNumberRead::LARGEINT_TEXT, paths.number_read("id"));
    EXPECT_EQ(VariantNumberRead::LARGEINT_TEXT, paths.number_read("ids_a"));
    // every other type, the default INVALID_TYPE included, keeps the parser's own read
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("n"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("s"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("f"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("unset"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("unmatched"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("ids"));
    EXPECT_EQ(VariantNumberRead::NATIVE, paths.number_read("id.low"));
}

TEST(VariantTemplatePathsTest, ReadsDecimalTextOnlyWithADecimalField) {
    // Without a DECIMAL field the encoder lets the DOM parser go first (add_json_row).
    EXPECT_FALSE(paths_of({}).reads_decimal_text());
    EXPECT_FALSE(paths_of({{.pattern = "id", .is_glob = false, .type = TYPE_LARGEINT},
                           {.pattern = "ids_*", .is_glob = true, .type = TYPE_LARGEINT}})
                         .reads_decimal_text());
    EXPECT_FALSE(paths_of({{.pattern = "n", .is_glob = false, .type = TYPE_BIGINT},
                           {.pattern = "s", .is_glob = false, .type = TYPE_STRING},
                           {.pattern = "f", .is_glob = false, .type = TYPE_DOUBLE},
                           {.pattern = "unset", .is_glob = false, .type = INVALID_TYPE}})
                         .reads_decimal_text());
    for (const PrimitiveType type :
         {TYPE_DECIMAL32, TYPE_DECIMAL64, TYPE_DECIMAL128I, TYPE_DECIMAL256}) {
        EXPECT_TRUE(paths_of({{.pattern = "amount", .is_glob = false, .type = type}})
                            .reads_decimal_text())
                << static_cast<int>(type);
    }
    EXPECT_TRUE(paths_of({{.pattern = "id", .is_glob = false, .type = TYPE_LARGEINT},
                          {.pattern = "amount*", .is_glob = true, .type = TYPE_DECIMAL64}})
                        .reads_decimal_text());
    // The bit sums up the declared types, not the paths that reach them: a DECIMAL field that an
    // earlier field shadows on every path still counts, and the match still answers NATIVE.
    const VariantTemplatePaths shadowed =
            paths_of({{.pattern = "*", .is_glob = true, .type = TYPE_STRING},
                      {.pattern = "amount", .is_glob = false, .type = TYPE_DECIMAL64}});
    EXPECT_TRUE(shadowed.reads_decimal_text());
    EXPECT_EQ(VariantNumberRead::NATIVE, shadowed.number_read("amount"));
}

TEST(VariantTemplatePathsTest, AnEmptyPathIsThePathOfAnEmptyKey) {
    // {"":1.50} is the path "" to the shredder and {"":{"b":1.50}} the path ".b". "*" matches both
    // under fnmatch, so the matcher answers for them as for any other path. A value with no key
    // above it (the root, an element of a root array) is on no path, and the collector never asks.
    const VariantTemplatePaths every =
            paths_of({{.pattern = "*", .is_glob = true, .type = TYPE_DECIMAL128I}});
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, every.number_read(""));
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, every.number_read(".b"));
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, every.number_read("amount"));
    // a field declared 'b' is the path "b", not the path under an empty key
    const VariantTemplatePaths named =
            paths_of({{.pattern = "b", .is_glob = false, .type = TYPE_DECIMAL128I}});
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, named.number_read("b"));
    EXPECT_EQ(VariantNumberRead::NATIVE, named.number_read(".b"));
    EXPECT_EQ(VariantNumberRead::NATIVE, named.number_read(""));
}

TEST(VariantTemplatePathsTest, ANameIsComparedAsACStringLikeTheShredder) {
    // The shredder compares a MATCH_NAME field with strcmp, which stops at the first NUL of a key
    // such as "a\u0000x". With 'a':string declared before 'a*':decimal it types that path STRING,
    // so the number under it has to stay as parsed here too.
    const std::string key("a\0x", 3);
    const VariantTemplatePaths string_first =
            paths_of({{.pattern = "a", .is_glob = false, .type = TYPE_STRING},
                      {.pattern = "a*", .is_glob = true, .type = TYPE_DECIMAL128I}});
    EXPECT_EQ(VariantNumberRead::NATIVE, string_first.number_read(key));
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, string_first.number_read("ax"));
    const VariantTemplatePaths named =
            paths_of({{.pattern = "a", .is_glob = false, .type = TYPE_DECIMAL128I}});
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, named.number_read(key));
    EXPECT_EQ(VariantNumberRead::NATIVE, named.number_read("ax"));
}

TEST(VariantTemplatePathsTest, TheFirstMatchingTemplateFieldDecidesLikeTheShredder) {
    // 'amount*':string is declared before 'amount':decimal: the shredder types "amount" as a
    // STRING, so the parser must not read it as a decimal either.
    const VariantTemplatePaths string_first =
            paths_of({{.pattern = "amount*", .is_glob = true, .type = TYPE_STRING},
                      {.pattern = "amount", .is_glob = false, .type = TYPE_DECIMAL64}});
    EXPECT_EQ(VariantNumberRead::NATIVE, string_first.number_read("amount"));
    EXPECT_EQ(VariantNumberRead::NATIVE, string_first.number_read("amount_due"));
    const VariantTemplatePaths decimal_first =
            paths_of({{.pattern = "amount", .is_glob = false, .type = TYPE_DECIMAL64},
                      {.pattern = "amount*", .is_glob = true, .type = TYPE_STRING}});
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, decimal_first.number_read("amount"));
    EXPECT_EQ(VariantNumberRead::NATIVE, decimal_first.number_read("amount_due"));
    // a glob and a named field that ask for different text reads: the first one declared wins
    const VariantTemplatePaths largeint_glob_first =
            paths_of({{.pattern = "id*", .is_glob = true, .type = TYPE_LARGEINT},
                      {.pattern = "id", .is_glob = false, .type = TYPE_DECIMAL128I}});
    EXPECT_EQ(VariantNumberRead::LARGEINT_TEXT, largeint_glob_first.number_read("id"));
    EXPECT_EQ(VariantNumberRead::LARGEINT_TEXT, largeint_glob_first.number_read("id_2"));
    const VariantTemplatePaths decimal_name_first =
            paths_of({{.pattern = "id", .is_glob = false, .type = TYPE_DECIMAL128I},
                      {.pattern = "id*", .is_glob = true, .type = TYPE_LARGEINT}});
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, decimal_name_first.number_read("id"));
    EXPECT_EQ(VariantNumberRead::LARGEINT_TEXT, decimal_name_first.number_read("id_2"));
    // a BIGINT field declared first keeps a later LARGEINT field of the same path out of reach
    const VariantTemplatePaths bigint_first =
            paths_of({{.pattern = "id", .is_glob = false, .type = TYPE_BIGINT},
                      {.pattern = "id*", .is_glob = true, .type = TYPE_LARGEINT}});
    EXPECT_EQ(VariantNumberRead::NATIVE, bigint_first.number_read("id"));
    EXPECT_EQ(VariantNumberRead::LARGEINT_TEXT, bigint_first.number_read("idx"));
    // a MATCH_NAME pattern is compared literally, never as a glob
    const VariantTemplatePaths literal =
            paths_of({{.pattern = "ratio*", .is_glob = false, .type = TYPE_DECIMAL64}});
    EXPECT_EQ(VariantNumberRead::DECIMAL_TEXT, literal.number_read("ratio*"));
    EXPECT_EQ(VariantNumberRead::NATIVE, literal.number_read("ratio_a"));
}

TEST(VariantTemplatePathsTest, ReadsFractionsExactly) {
    expect_decimal("123456789012.345678", "123456789012345678", 6, 8);
    expect_decimal("-1.5", "-15", 1, 4);
    expect_decimal("0.123456789012345678901", "123456789012345678901", 21, 16);
    expect_decimal("1.50", "150", 2, 4);
    expect_decimal("0.0", "0", 1, 4);
    expect_decimal("-0", "0", 0, 4);
    // a decimal has no negative zero; the scale the text carries stays, clamped to 38
    expect_decimal("-0.0", "0", 1, 4);
    expect_decimal("0e-50", "0", 38, 16);
    expect_decimal("188118222.011121933", "188118222011121933", 9, 8);
}

TEST(VariantTemplatePathsTest, FoldsAnExponentIntoTheScale) {
    expect_decimal("1.5e3", "1500", 0, 4);
    expect_decimal("1.5E-3", "15", 4, 4);
    expect_decimal("1e2", "100", 0, 4);
    expect_decimal("-2.5e+1", "-25", 0, 4);
    expect_decimal("12e-2", "12", 2, 4);
    expect_decimal("0e100", "0", 0, 4);
}

TEST(VariantTemplatePathsTest, ReadsIntegersUpToThirtyEightDigits) {
    expect_decimal("123456789012345678901", "123456789012345678901", 0, 16);
    expect_decimal("18446744073709551616", "18446744073709551616", 0, 16);
    expect_decimal("99999999999999999999999999999999999999",
                   "99999999999999999999999999999999999999", 0, 16);
    expect_decimal("-99999999999999999999999999999999999999",
                   "-99999999999999999999999999999999999999", 0, 16);
}

TEST(VariantTemplatePathsTest, WidthHoldsTheScaleAsWellAsTheDigits) {
    // decimal4 / decimal8 carry 9 / 18 digits: a scale beyond that needs the wider encoding even
    // when the unscaled value is tiny, or a reader bound by the width's precision rejects it
    expect_decimal("0.000000001", "1", 9, 4);
    expect_decimal("0.0000000001", "1", 10, 8);
    expect_decimal("1e-18", "1", 18, 8);
    expect_decimal("1e-19", "1", 19, 16);
    expect_decimal("999999999", "999999999", 0, 4);
    expect_decimal("1000000000", "1000000000", 0, 8);
    expect_decimal("123456789.123456789", "123456789123456789", 9, 8);
    expect_decimal("999999999999999999", "999999999999999999", 0, 8);
    expect_decimal("1000000000000000000", "1000000000000000000", 0, 16);
    expect_decimal("0.0000000000", "0", 10, 8);
}

TEST(VariantTemplatePathsTest, DeclinesWhatDoesNotFit) {
    VariantDecimalText out;
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("100000000000000000000000000000000000000", out,
                                           kVariantDecimalFinestScale));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("0.1234567890123456789012345678901234567890123", out,
                                           kVariantDecimalFinestScale));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("1e-39", out, kVariantDecimalFinestScale));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("1e40", out, kVariantDecimalFinestScale));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("1e99999", out, kVariantDecimalFinestScale));
}

TEST(VariantTemplatePathsTest, AValueBelowEveryScaleReadsAsZero) {
    // Below 10^-39 a value rounds to 0 at every scale a decimal has. The cast that decides behind
    // OUT_OF_RANGE reads no exponent beyond 32 bits and answers NULL for it, so the reader answers
    // 0 itself, at scale 38, as it does for an exact zero.
    expect_decimal("1e-40", "0", 38, 16);
    expect_decimal("-1e-40", "0", 38, 16);
    expect_decimal("9.99e-40", "0", 38, 16);
    expect_decimal("1e-2147483647", "0", 38, 16);
    expect_decimal("5E-2147483648", "0", 38, 16);
    expect_decimal("1e-3000000000", "0", 38, 16);
    expect_decimal("-1e-3000000000", "0", 38, 16);
    // with more than 38 significant digits as well: the smallness is decided first
    expect_decimal("1234567890123456789012345678901234567890123e-3000000000", "0", 38, 16);
    expect_decimal("0." + std::string(60, '0') + "1234567890123456789012345678901234567890123", "0",
                   38, 16);
    // 10^-39 and up is not below every scale: 5e-39 is 10^-38 at scale 38, and the cast rounds it
    VariantDecimalText out;
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("10e-40", out, kVariantDecimalFinestScale));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("5e-39", out, kVariantDecimalFinestScale));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("0." + std::string(38, '0') + "1", out,
                                           kVariantDecimalFinestScale));
    // and a value that is too large stays too large, whatever its exponent's length
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("1e3000000000", out, kVariantDecimalFinestScale));
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("1234567890123456789012345678901234567890123e3000000000",
                                           out, kVariantDecimalFinestScale));
}

TEST(VariantTemplatePathsTest, ADecimal256FieldKeepsAValueItsScaleHolds) {
    // A DECIMAL256 field declares a scale up to 76, so "below every scale" starts below 10^-77
    // there and not below 10^-39: decimal(76,45) holds 1.25e-40 exactly. Such a value has a scale
    // beyond the 38 read here, so it is OUT_OF_RANGE and the caller keeps its text for the cast.
    VariantDecimalText out;
    for (const char* token : {"1.25e-40", "-1.25e-40", "1e-40", "1e-76", "9.9e-77", "1e-77"}) {
        EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
                  parse_json_number_as_decimal(token, out, kVariantDecimal256FinestScale))
                << token;
    }
    // below 10^-77 it is 0 at every scale a DECIMAL256 has, whatever the length of its exponent
    expect_decimal_at(kVariantDecimal256FinestScale, "1e-78", "0", 38, 16);
    expect_decimal_at(kVariantDecimal256FinestScale, "9.99e-78", "0", 38, 16);
    expect_decimal_at(kVariantDecimal256FinestScale, "1e-370", "0", 38, 16);
    // the exponent is read past this type's finest scale, not past 38: cut after its second
    // digit, 1e-450 would be read as 1e-45, which a DECIMAL256 holds
    expect_decimal_at(kVariantDecimal256FinestScale, "1e-450", "0", 38, 16);
    expect_decimal_at(kVariantDecimal256FinestScale, "1e-3000000000", "0", 38, 16);
    expect_decimal_at(kVariantDecimal256FinestScale,
                      "1234567890123456789012345678901234567890123e-3000000000", "0", 38, 16);
    // the same 1.25e-40 on a field of up to 38 digits is below every scale that field has
    expect_decimal("1.25e-40", "0", 38, 16);
    // and what fits 38 digits at a scale up to 38 reads the same on both
    expect_decimal_at(kVariantDecimal256FinestScale, "1.50", "150", 2, 4);
    expect_decimal_at(kVariantDecimal256FinestScale, "1e-38", "1", 38, 16);
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("1e40", out, kVariantDecimal256FinestScale));
}

TEST(VariantTemplatePathsTest, ReadsAnExponentOfAnyLength) {
    // A long fraction can cancel an exponent of any length, so every digit of it counts.
    // 0.<10000 zeros>1 is 10^-10001, which 10^10001 brings back to 1, and 0.<9999 zeros>1 to 10.
    expect_decimal("0." + std::string(10000, '0') + "1e10001", "1", 0, 4);
    expect_decimal("0." + std::string(9999, '0') + "1e10001", "10", 0, 4);
    expect_decimal("0e100000", "0", 0, 4);
    expect_decimal("0e-100000", "0", 38, 16);
    VariantDecimalText out;
    // 0.<999 zeros>1e10000 is 10^9000: no decimal holds it, so the text is kept for the template's
    // cast to decide. 0.<999 zeros>1e-10000 is 10^-11000, which is 0 at every scale.
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("0." + std::string(999, '0') + "1e10000", out,
                                           kVariantDecimalFinestScale));
    expect_decimal("0." + std::string(999, '0') + "1e-10000", "0", 38, 16);
    // The bound has to clear every exponent a decimal can still hold, not only the token's length:
    // cut where it first exceeds the five characters of 1e370, the exponent would be read as 37,
    // and 1e-370 as the decimal 1e-37.
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("1e370", out, kVariantDecimalFinestScale));
    expect_decimal("1e-370", "0", 38, 16);
    // an exponent past every bound stays there however many digits follow
    EXPECT_EQ(JsonNumberToDecimal::OUT_OF_RANGE,
              parse_json_number_as_decimal("1e" + std::string(400, '9'), out,
                                           kVariantDecimalFinestScale));
    expect_decimal("1e-" + std::string(400, '9'), "0", 38, 16);
}

TEST(VariantTemplatePathsTest, DeclinesMalformedTokens) {
    VariantDecimalText out;
    for (const char* token : {"", "-", "1.", ".5", "1e", "1e+", "abc", "1.5x", "--1", "1..5", " 1",
                              "-Infinity", "NaN", "+1"}) {
        EXPECT_EQ(JsonNumberToDecimal::MALFORMED,
                  parse_json_number_as_decimal(token, out, kVariantDecimalFinestScale))
                << token;
    }
    // JSON: int = "0" / digit1-9 *DIGIT. simdjson rejects these and so must the text reader.
    for (const char* token : {"01", "-01", "00", "00.5", "0001e2", "-00"}) {
        EXPECT_EQ(JsonNumberToDecimal::MALFORMED,
                  parse_json_number_as_decimal(token, out, kVariantDecimalFinestScale))
                << token;
    }
    // a lone zero before the fraction or the exponent is the grammar's one allowed leading zero
    expect_decimal("0.5", "5", 1, 4);
    expect_decimal("-0.5", "-5", 1, 4);
    expect_decimal("0e2", "0", 0, 4);
}

TEST(VariantTemplatePathsTest, ReadsAnIntegerIntoALargeInt) {
    expect_largeint("0", "0");
    // an integer has no negative zero
    expect_largeint("-0", "0");
    expect_largeint("7", "7");
    expect_largeint("-42", "-42");
    expect_largeint("9223372036854775807", "9223372036854775807");
    // 2^63 and 2^64: the first integers past int64 and past uint64
    expect_largeint("9223372036854775808", "9223372036854775808");
    expect_largeint("-9223372036854775809", "-9223372036854775809");
    expect_largeint("18446744073709551616", "18446744073709551616");
    // 10^38 - 1 and 10^38: either side of what a Variant decimal16 holds
    expect_largeint("99999999999999999999999999999999999999",
                    "99999999999999999999999999999999999999");
    expect_largeint("100000000000000000000000000000000000000",
                    "100000000000000000000000000000000000000");
}

TEST(VariantTemplatePathsTest, ReadsALargeIntUpToItsAsymmetricBounds) {
    // 2^127 - 1 and -2^127 are the largest and the smallest LARGEINT
    expect_largeint("170141183460469231731687303715884105727",
                    "170141183460469231731687303715884105727");
    expect_largeint("-170141183460469231731687303715884105727",
                    "-170141183460469231731687303715884105727");
    expect_largeint("-170141183460469231731687303715884105728",
                    "-170141183460469231731687303715884105728");
    // 2^127 and -2^127 - 1 are one past either bound
    expect_no_largeint("170141183460469231731687303715884105728",
                       JsonNumberToLargeInt::OUT_OF_RANGE);
    expect_no_largeint("-170141183460469231731687303715884105729",
                       JsonNumberToLargeInt::OUT_OF_RANGE);
    // 39 digits again, where the last multiplication by ten is what overflows
    expect_no_largeint("999999999999999999999999999999999999999",
                       JsonNumberToLargeInt::OUT_OF_RANGE);
    // 2^128 and 2^128 + 1 must not wrap around to 0 and 1
    expect_no_largeint("340282366920938463463374607431768211456",
                       JsonNumberToLargeInt::OUT_OF_RANGE);
    expect_no_largeint("340282366920938463463374607431768211457",
                       JsonNumberToLargeInt::OUT_OF_RANGE);
    // 60 digits
    expect_no_largeint("123456789012345678901234567890123456789012345678901234567890",
                       JsonNumberToLargeInt::OUT_OF_RANGE);
    expect_no_largeint("-123456789012345678901234567890123456789012345678901234567890",
                       JsonNumberToLargeInt::OUT_OF_RANGE);
}

TEST(VariantTemplatePathsTest, DeclinesWhatIsNotAJsonInteger) {
    // JSON: int = "0" / digit1-9 *DIGIT. A leading zero, a sign other than '-', or nothing at all.
    for (const char* token :
         {"", "-", "+1", "01", "-01", "00", "-00", "--1", " 1", "1 ", "abc", "-Infinity", "NaN"}) {
        expect_no_largeint(token, JsonNumberToLargeInt::NOT_AN_INTEGER);
    }
    // a fraction or an exponent is a JSON number, but not an integer as written
    for (const char* token : {"1.0", "1.5", "0.5", "-0.0", "1e3", "1E3", "0e0", "1."}) {
        expect_no_largeint(token, JsonNumberToLargeInt::NOT_AN_INTEGER);
    }
    // trailing bytes, also behind digits that are already out of range
    expect_no_largeint("1x", JsonNumberToLargeInt::NOT_AN_INTEGER);
    expect_no_largeint("123456789012345678901234567890123456789012345678901234567890x",
                       JsonNumberToLargeInt::NOT_AN_INTEGER);
    expect_no_largeint("123456789012345678901234567890123456789012345678901234567890.5",
                       JsonNumberToLargeInt::NOT_AN_INTEGER);
}

} // namespace doris
