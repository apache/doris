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

#include "core/value/variant/variant_leaf_visitor.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "core/column/variant_v2/column_variant_v2.h"
#include "core/value/variant/variant_field.h"
#include "core/value/variant/variant_scalar.h"
#include "exprs/function/parse/variant_string_parse.h"

namespace doris {
namespace {
std::vector<std::string> texts(const VariantRef& value) {
    std::vector<std::string> result;
    EXPECT_TRUE(for_each_variant_leaf_text(value, [&](std::string_view text) {
                    result.emplace_back(text);
                    return Status::OK();
                }).ok());
    return result;
}
std::vector<std::string> json_texts(std::string_view json) {
    JsonStringToVariantEncoder encoder;
    encoder.add_json({json.data(), json.size()});
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(encoder.finish_batch());
    return texts(column->read_view().value_at(0));
}
TEST(VariantLeafVisitorTest, ContainersVisitLeavesWithoutKeys) {
    for (const auto& [json, expected] :
         std::vector<std::pair<std::string, std::vector<std::string>>> {
                 {R"({"key":"hello","array":[1,true,{"child":"world"}],"null":null})",
                  {"1", "hello", "true", "world"}},
                 {R"("root")", {"root"}},
                 {R"([null,{},[],{"empty":[]}])", {}},
                 {"null", {}}}) {
        auto actual = json_texts(json);
        std::ranges::sort(actual);
        EXPECT_EQ(actual, expected) << json;
    }
}
TEST(VariantLeafVisitorTest, CanonicalScalarText) {
    std::vector<std::pair<VariantScalarRef, std::vector<std::string>>> cases = {
            {VariantScalarRef::string(StringRef("hello 汉")), {"hello 汉"}},
            {VariantScalarRef::string(StringRef("")), {""}},
            {VariantScalarRef::boolean(true), {"true"}},
            {VariantScalarRef::boolean(false), {"false"}},
            {VariantScalarRef::integer(-5), {"-5"}},
            {VariantScalarRef::integer(42), {"42"}},
            {VariantScalarRef::integer(std::numeric_limits<int64_t>::min()),
             {"-9223372036854775808"}},
            {VariantScalarRef::float32(0.1F), {"0.1"}},
            {VariantScalarRef::float32(1.5F), {"1.5"}},
            {VariantScalarRef::float32(42.0F), {"42"}},
            {VariantScalarRef::float32(-0.0F), {"0"}},
            {VariantScalarRef::float64(42.0), {"42"}},
            {VariantScalarRef::float64(-0.0), {"0"}},
            {VariantScalarRef::float64(1.5), {"1.5"}},
            {VariantScalarRef::float64(0x1p63), {"9223372036854775808"}},
            {VariantScalarRef::float64(0x1p64), {"1.8446744073709552e+19"}},
            {VariantScalarRef::null_value(), {}},
            {VariantScalarRef::date(1), {}},
            {VariantScalarRef::timestamp_micros(1, true), {}},
            {VariantScalarRef::timestamp_micros(1, false), {}},
            {VariantScalarRef::timestamp_nanos(1, true), {}},
            {VariantScalarRef::timestamp_nanos(1, false), {}},
            {VariantScalarRef::time_ntz_micros(1), {}},
            {VariantScalarRef::binary(StringRef("bytes")), {}},
            {VariantScalarRef::uuid({}), {}}};
    for (const uint8_t width : {4, 8, 16}) {
        for (const auto& [unscaled, scale, expected] :
             {std::tuple<int64_t, uint8_t, const char*> {42, 0, "42"},
              {-42, 0, "-42"},
              {4200, 2, "42"},
              {1990, 2, "19.9"},
              {-1990, 2, "-19.9"},
              {10, 3, "0.01"},
              {-10, 3, "-0.01"},
              {0, 0, "0"},
              {0, 3, "0"}}) {
            cases.push_back({VariantScalarRef::decimal(unscaled, scale, width), {expected}});
        }
    }
    const __int128 wide = std::numeric_limits<uint64_t>::max();
    cases.push_back({VariantScalarRef::decimal(wide, 0), {"18446744073709551615"}});
    cases.push_back({VariantScalarRef::decimal(wide * 100, 2), {"18446744073709551615"}});
    cases.push_back({VariantScalarRef::decimal(-wide * 100, 2), {"-18446744073709551615"}});
    for (const double value :
         {std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
          std::numeric_limits<double>::quiet_NaN()}) {
        cases.push_back({VariantScalarRef::float32(static_cast<float>(value)), {}});
        cases.push_back({VariantScalarRef::float64(value), {}});
    }
    for (const auto& [input, expected] : cases) {
        const auto field = VariantField::from_scalar(input);
        EXPECT_EQ(texts(field.ref()), expected);
    }
}
TEST(VariantLeafVisitorTest, CallbackErrorStopsTraversal) {
    JsonStringToVariantEncoder encoder;
    encoder.add_json(StringRef("[1,2,3]"));
    auto column = ColumnVariantV2::create();
    column->insert_encoded_batch(encoder.finish_batch());
    size_t calls = 0;
    auto status =
            for_each_variant_leaf_text(column->read_view().value_at(0), [&](std::string_view text) {
                ++calls;
                return text == "2" ? Status::InternalError("stop") : Status::OK();
            });
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(calls, 2);
}
} // namespace
} // namespace doris
