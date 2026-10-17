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

#include "core/value/variant/variant_decimal_paths.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace doris {
namespace {

VariantDecimalPathSet paths_of(std::vector<VariantDecimalPath> paths) {
    return VariantDecimalPathSet(std::move(paths));
}

VariantDecimalPath exact(std::string pattern) {
    return {.pattern = std::move(pattern), .is_glob = false, .precision = 20, .scale = 6};
}

VariantDecimalPath glob(std::string pattern) {
    return {.pattern = std::move(pattern), .is_glob = true, .precision = 20, .scale = 6};
}

// Returns the rewritten text, or the empty string when the pre-pass declined.
std::string quoted(std::string_view json, const VariantDecimalPathSet& paths) {
    std::string out;
    if (!quote_decimal_numbers_on_paths({json.data(), json.size()}, paths, &out)) {
        return {};
    }
    return out;
}

} // namespace

TEST(VariantDecimalPathsTest, SetMatchesExactAndGlobPaths) {
    const VariantDecimalPathSet paths = paths_of({exact("money.amount"), glob("ratio*")});
    EXPECT_FALSE(paths.empty());
    EXPECT_TRUE(paths.matches("money.amount"));
    EXPECT_FALSE(paths.matches("money.amount.x"));
    EXPECT_FALSE(paths.matches("money"));
    EXPECT_TRUE(paths.matches("ratio"));
    EXPECT_TRUE(paths.matches("ratio_a"));
    EXPECT_FALSE(paths.matches("xratio"));
    EXPECT_FALSE(paths.matches(""));
    EXPECT_TRUE(paths_of({}).empty());
}

TEST(VariantDecimalPathsTest, QuotesOnlyTheFractionOnTheDeclaredPath) {
    const VariantDecimalPathSet paths = paths_of({exact("money.amount")});
    EXPECT_EQ(
            R"({"money":{"amount":"123456789012.345678","ccy":"INR"},"ratio":0.123456789012345678901})",
            quoted(R"({"money":{"amount":123456789012.345678,"ccy":"INR"},"ratio":0.123456789012345678901})",
                   paths));
    // Negative values and surrounding whitespace are preserved byte for byte.
    EXPECT_EQ("{ \"money\" : { \"amount\" : \"-0.000001\" } }\n",
              quoted("{ \"money\" : { \"amount\" : -0.000001 } }\n", paths));
}

TEST(VariantDecimalPathsTest, LeavesIntegersExponentsStringsAndOtherPathsAlone) {
    const VariantDecimalPathSet paths = paths_of({exact("a"), exact("b"), exact("c"), exact("d")});
    EXPECT_EQ(R"({"a":12,"b":1.5e3,"c":"1.5","d":"-0.25","e":2.5,"f":true,"g":null})",
              quoted(R"({"a":12,"b":1.5e3,"c":"1.5","d":-0.25,"e":2.5,"f":true,"g":null})", paths));
    // Nothing on a declared path carries a fraction: the pre-pass declines and the caller keeps the
    // original text.
    EXPECT_EQ("", quoted(R"({"a":12,"b":1.5e3,"c":"1.5","e":2.5})", paths));
    EXPECT_EQ("", quoted(R"({"money":{"amount":1.5}})", paths));
}

TEST(VariantDecimalPathsTest, ArrayElementsKeepTheirArraysPath) {
    const VariantDecimalPathSet paths = paths_of({glob("prices*"), exact("items.price")});
    EXPECT_EQ(
            R"({"prices":["1.10","2.25",3],"prices_x":"3.5","other":[4.5],"items":[{"price":"9.99"},{"price":"1.0"}]})",
            quoted(R"({"prices":[1.10,2.25,3],"prices_x":3.5,"other":[4.5],"items":[{"price":9.99},{"price":1.0}]})",
                   paths));
}

TEST(VariantDecimalPathsTest, StringsWithEscapesAndBracesAreCopied) {
    const VariantDecimalPathSet paths = paths_of({exact("v")});
    EXPECT_EQ(R"({"s":"a\"b{1.5}[2.5]\\","v":"2.5","t":"é : 3.5"})",
              quoted(R"({"s":"a\"b{1.5}[2.5]\\","v":2.5,"t":"é : 3.5"})", paths));
    // A key written with an escape never matches a template path and keeps the old behavior.
    const VariantDecimalPathSet escaped_key = paths_of({exact("a\"b")});
    EXPECT_EQ("", quoted(R"({"a\"b":1.5})", escaped_key));
}

TEST(VariantDecimalPathsTest, DeclinesMalformedTextAndEmptyInput) {
    const VariantDecimalPathSet paths = paths_of({exact("a")});
    EXPECT_EQ("", quoted(R"({"a":1.5)", paths));
    EXPECT_EQ("", quoted(R"({"a":1.5} x)", paths));
    EXPECT_EQ("", quoted(R"({"a":1.})", paths));
    EXPECT_EQ("", quoted(R"({"a":.5})", paths));
    EXPECT_EQ("", quoted(R"({"a":1.5e})", paths));
    EXPECT_EQ("", quoted(R"({"a" 1.5})", paths));
    EXPECT_EQ("", quoted(R"({"a":1.5,})", paths));
    EXPECT_EQ("", quoted(R"([1.5,])", paths));
    EXPECT_EQ("", quoted("", paths));
    EXPECT_EQ("", quoted("1.5", paths));
    std::string out = "untouched";
    EXPECT_FALSE(quote_decimal_numbers_on_paths({"{\"a\":1.5}", 9}, paths_of({}), &out));
    EXPECT_EQ("untouched", out);
    EXPECT_FALSE(quote_decimal_numbers_on_paths({"{\"a\":1.5}", 9}, paths, nullptr));
}

TEST(VariantDecimalPathsTest, DeclinesBeyondTheDepthLimit) {
    const VariantDecimalPathSet paths = paths_of({exact("a")});
    std::string deep;
    for (int i = 0; i < 600; ++i) {
        deep += "[";
    }
    deep += "1.5";
    for (int i = 0; i < 600; ++i) {
        deep += "]";
    }
    EXPECT_EQ("", quoted(deep, paths));
}

} // namespace doris
