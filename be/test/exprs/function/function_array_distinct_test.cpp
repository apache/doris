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

#include "exprs/function/array/function_array_distinct.h"

#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <vector>

#include "core/column/column_string.h"
#include "core/data_type/data_type_string.h"

namespace doris {

class FunctionArrayDistinctTest : public testing::Test {
protected:
    using StringRow = std::vector<std::optional<std::string>>;
    using StringRows = std::vector<StringRow>;

    template <bool use_dispatch>
    // NOLINTNEXTLINE(readability-function-cognitive-complexity): GTest assertions add branches.
    static void check_strings(const StringRows& rows, const StringRows& expected, bool nullable,
                              const std::string& null_payload) {
        ASSERT_EQ(rows.size(), expected.size());
        auto source = ColumnString::create();
        ColumnArray::Offsets64 source_offsets;
        FunctionArrayDistinct::NullMapType source_nulls;
        for (const auto& row : rows) {
            for (const auto& value : row) {
                ASSERT_TRUE(nullable || value.has_value());
                const auto& payload = value ? *value : null_payload;
                source->insert_data(payload.data(), payload.size());
                source_nulls.push_back(!value.has_value());
            }
            source_offsets.push_back(source->size());
        }
        const size_t source_bytes = source->get_chars().size();

        auto result = ColumnString::create();
        ColumnArray::Offsets64 result_offsets;
        FunctionArrayDistinct::NullMapType result_nulls;
        FunctionArrayDistinct function;
        const auto* src_nulls = nullable ? &source_nulls : nullptr;
        auto* dst_nulls = nullable ? &result_nulls : nullptr;
        if constexpr (use_dispatch) {
            DataTypePtr nested_type = std::make_shared<DataTypeString>();
            ASSERT_TRUE(function._execute_by_type(*source, source_offsets, *result, result_offsets,
                                                  src_nulls, dst_nulls, nested_type));
        } else {
            ASSERT_TRUE(function._execute_string<false>(*source, source_offsets, *result,
                                                        result_offsets, src_nulls, dst_nulls));
        }

        ASSERT_EQ(result_offsets.size(), expected.size());
        size_t result_row = 0;
        size_t expected_bytes = 0;
        for (size_t row = 0; row < expected.size(); ++row) {
            for (const auto& value : expected[row]) {
                ASSERT_LT(result_row, result->size());
                if (nullable) {
                    ASSERT_LT(result_row, result_nulls.size());
                    EXPECT_EQ(result_nulls[result_row], !value.has_value());
                }
                if (value) {
                    EXPECT_EQ(result->get_data_at(result_row).to_string(), *value);
                    expected_bytes += value->size();
                } else {
                    EXPECT_EQ(result->get_data_at(result_row).size, 0);
                }
                ++result_row;
            }
            EXPECT_EQ(result_offsets[row], result_row);
        }
        EXPECT_EQ(result->size(), result_row);
        EXPECT_EQ(result->get_chars().size(), expected_bytes);
        EXPECT_EQ(result_nulls.size(), nullable ? result_row : 0);
        EXPECT_EQ(source->get_chars().size(), source_bytes);
        for (size_t row = 0; row < source->size(); ++row) {
            if (source_nulls[row]) {
                EXPECT_EQ(source->get_data_at(row).to_string(), null_payload);
            }
        }
    }

    static void check_both_paths(const StringRows& rows, const StringRows& expected, bool nullable,
                                 const std::string& null_payload = std::string(4096, 'x')) {
        check_strings<true>(rows, expected, nullable, null_payload);
        // Exercise the oversized-input fallback without allocating over 2^32 strings.
        check_strings<false>(rows, expected, nullable, null_payload);
    }
};

TEST_F(FunctionArrayDistinctTest, StringsPreserveFirstOccurrenceAcrossArrays) {
    const std::string long_string(8192, 'a');
    check_both_paths({{"b", "a", "b", "", ""},
                      {},
                      {"b", "b", "a"},
                      {"中文", "中文", "tail"},
                      {long_string, long_string, "tail"}},
                     {{"b", "a", ""}, {}, {"b", "a"}, {"中文", "tail"}, {long_string, "tail"}},
                     false);
}

TEST_F(FunctionArrayDistinctTest, NullableStringsWithHiddenPayloads) {
    check_both_paths({{std::nullopt, "a", std::nullopt, "", "a", std::nullopt},
                      {"b", std::nullopt, "b", "c", std::nullopt},
                      {std::nullopt, std::nullopt},
                      {},
                      {"", std::nullopt}},
                     {{std::nullopt, "a", ""},
                      {"b", std::nullopt, "c"},
                      {std::nullopt},
                      {},
                      {"", std::nullopt}},
                     true);
}

TEST_F(FunctionArrayDistinctTest, NullableStringsWithEmptyNullPayloads) {
    check_both_paths({{std::nullopt, "a", std::nullopt, "", "a"},
                      {std::nullopt, std::nullopt},
                      {"b", std::nullopt, "b"}},
                     {{std::nullopt, "a", ""}, {std::nullopt}, {"b", std::nullopt}}, true, "");
}

TEST_F(FunctionArrayDistinctTest, EmptyStringsInput) {
    check_both_paths({}, {}, false);
    check_both_paths({}, {}, true);
    check_both_paths({{}, {}}, {{}, {}}, true);
}

TEST_F(FunctionArrayDistinctTest, AllNullStringElements) {
    check_both_paths({{std::nullopt, std::nullopt}, {}, {std::nullopt}},
                     {{std::nullopt}, {}, {std::nullopt}}, true);
}

TEST_F(FunctionArrayDistinctTest, NullableStringsWithoutNulls) {
    check_both_paths({{"b", "a", "b"}}, {{"b", "a"}}, true);
}

TEST_F(FunctionArrayDistinctTest, NullAfterManyStrings) {
    StringRow input;
    for (size_t i = 0; i < 256; ++i) {
        input.emplace_back(std::to_string(i));
    }
    input.emplace_back(std::nullopt);
    check_both_paths({input}, {input}, true);
}

TEST_F(FunctionArrayDistinctTest, SelectorGrowsAcrossArrays) {
    StringRow input;
    StringRow expected;
    for (size_t i = 0; i < 40000; ++i) {
        auto value = std::to_string(i);
        input.emplace_back(value);
        input.emplace_back(value);
        expected.emplace_back(value);
    }
    check_both_paths({input, {}, {"0", "0", "last"}}, {expected, {}, {"0", "last"}}, false);
}

} // namespace doris
