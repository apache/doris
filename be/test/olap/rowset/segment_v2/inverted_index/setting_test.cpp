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

#include "olap/rowset/segment_v2/inverted_index/setting.h"

#include <gtest/gtest.h>

namespace doris::segment_v2::inverted_index {

class SettingsTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Common test data
        testMap = {{"bool_true", "true"},
                   {"bool_false", "false"},
                   {"bool_1", "1"},
                   {"bool_0", "0"},
                   {"bool_invalid", "maybe"},
                   {"int_valid", "42"},
                   {"int_invalid", "4.2"},
                   {"int_overflow", "99999999999999999999"},
                   {"string_empty", ""},
                   {"string_normal", "hello world"},
                   {"list_empty", "[]"},
                   {"list_single", "[item1]"},
                   {"list_multiple", "[item1][item2][item3]"},
                   {"set_empty", ""},
                   {"set_single", "apple"},
                   {"set_multiple", "apple, banana, cherry"},
                   {"set_with_spaces", " apple ,  banana , cherry "}};
    }

    std::unordered_map<std::string, std::string> testMap;
};

TEST_F(SettingsTest, DefaultConstructorCreatesEmptySettings) {
    Settings settings;
    EXPECT_TRUE(settings.empty());
}

TEST_F(SettingsTest, MapConstructorInitializesSettings) {
    Settings settings(testMap);
    EXPECT_FALSE(settings.empty());
}

TEST_F(SettingsTest, SetAddsNewKeyValuePair) {
    Settings settings;
    settings.set("new_key", "new_value");
    EXPECT_EQ(settings.get_string("new_key"), "new_value");
}

TEST_F(SettingsTest, SetOverwritesExistingKey) {
    Settings settings(testMap);
    settings.set("bool_true", "new_value");
    EXPECT_EQ(settings.get_string("bool_true"), "new_value");
}

TEST_F(SettingsTest, GetBoolReturnsCorrectValues) {
    Settings settings(testMap);

    EXPECT_TRUE(settings.get_bool("bool_true", false));
    EXPECT_FALSE(settings.get_bool("bool_false", true));
    EXPECT_TRUE(settings.get_bool("bool_1", false));
    EXPECT_FALSE(settings.get_bool("bool_0", true));

    // Test default values
    EXPECT_TRUE(settings.get_bool("bool_invalid", true));
    EXPECT_FALSE(settings.get_bool("bool_invalid", false));
    EXPECT_FALSE(settings.get_bool("non_existent", false));
    EXPECT_TRUE(settings.get_bool("non_existent", true));
}

TEST_F(SettingsTest, GetIntReturnsCorrectValues) {
    Settings settings(testMap);

    EXPECT_EQ(settings.get_int("int_valid", 0), 42);

    // Test default values
    EXPECT_EQ(settings.get_int("int_invalid", 10), 10);
    EXPECT_EQ(settings.get_int("non_existent", 99), 99);
}

TEST_F(SettingsTest, GetIntThrowsOnInvalidInput) {
    Settings settings(testMap);

    EXPECT_THROW(settings.get_int("int_overflow", 0), Exception);
}

TEST_F(SettingsTest, GetStringReturnsCorrectValues) {
    Settings settings(testMap);

    EXPECT_EQ(settings.get_string("string_empty"), "");
    EXPECT_EQ(settings.get_string("string_normal"), "hello world");
    EXPECT_EQ(settings.get_string("non_existent"), "");
}

TEST_F(SettingsTest, GetEntryListReturnsCorrectValues) {
    Settings settings(testMap);

    // auto emptyList = settings.get_entry_list("list_empty");
    // EXPECT_TRUE(emptyList.empty());

    // auto singleList = settings.get_entry_list("list_single");
    // ASSERT_EQ(singleList.size(), 1);
    // EXPECT_EQ(singleList[0], "item1");

    auto multiList = settings.get_entry_list("list_multiple");
    ASSERT_EQ(multiList.size(), 1);
    EXPECT_EQ(multiList[0], "item1][item2][item3");

    // auto nonExistent = settings.get_entry_list("non_existent");
    // EXPECT_TRUE(nonExistent.empty());
}

TEST_F(SettingsTest, GetWordSetReturnsCorrectValues) {
    Settings settings(testMap);

    auto emptySet = settings.get_word_set("set_empty");
    EXPECT_TRUE(emptySet.empty());

    auto singleSet = settings.get_word_set("set_single");
    ASSERT_EQ(singleSet.size(), 1);
    EXPECT_NE(singleSet.find("apple"), singleSet.end());

    auto multiSet = settings.get_word_set("set_multiple");
    ASSERT_EQ(multiSet.size(), 3);
    EXPECT_NE(multiSet.find("apple"), multiSet.end());
    EXPECT_NE(multiSet.find("banana"), multiSet.end());
    EXPECT_NE(multiSet.find("cherry"), multiSet.end());

    auto spacedSet = settings.get_word_set("set_with_spaces");
    ASSERT_EQ(spacedSet.size(), 3);
    EXPECT_NE(spacedSet.find("apple"), spacedSet.end());
    EXPECT_NE(spacedSet.find("banana"), spacedSet.end());
    EXPECT_NE(spacedSet.find("cherry"), spacedSet.end());

    auto nonExistent = settings.get_word_set("non_existent");
    EXPECT_TRUE(nonExistent.empty());
}

TEST_F(SettingsTest, ToStringReturnsCorrectFormat) {
    Settings settings;
    settings.set("key1", "value1");
    settings.set("key2", "value2");

    std::string result = settings.to_string();

    // The order is not guaranteed in unordered_map, so we need to check both possibilities
    EXPECT_TRUE(result == "key1=value1, key2=value2" || result == "key2=value2, key1=value1");
}

TEST_F(SettingsTest, CopyConstructorWorks) {
    Settings settings1(testMap);
    Settings settings2(settings1);

    EXPECT_EQ(settings1.get_string("string_normal"), settings2.get_string("string_normal"));
    EXPECT_EQ(settings1.get_int("int_valid", 0), settings2.get_int("int_valid", 0));
}

TEST_F(SettingsTest, MoveConstructorWorks) {
    Settings settings1(testMap);
    Settings settings2(std::move(settings1));

    EXPECT_EQ(settings2.get_string("string_normal"), "hello world");
    EXPECT_TRUE(settings1.empty());
}

TEST_F(SettingsTest, GetEntryListWithBracketsInside) {
    Settings settings;

    settings.set("list_with_brackets_inside", "[item[with]brackets]");
    auto singleWithBrackets = settings.get_entry_list("list_with_brackets_inside");
    ASSERT_EQ(singleWithBrackets.size(), 1);
    EXPECT_EQ(singleWithBrackets[0], "item[with]brackets");

    settings.set("list_multiple_with_brackets",
                 "[item1[with]brackets][item2[also]has[brackets]][item3]");
    auto multiWithBrackets = settings.get_entry_list("list_multiple_with_brackets");
    ASSERT_EQ(multiWithBrackets.size(), 1);
    EXPECT_EQ(multiWithBrackets[0], "item1[with]brackets][item2[also]has[brackets]][item3");

    settings.set("list_nested_brackets", "[[[nested]]][[double]][single]");
    auto nestedBrackets = settings.get_entry_list("list_nested_brackets");
    ASSERT_EQ(nestedBrackets.size(), 1);
    EXPECT_EQ(nestedBrackets[0], "[[nested]]][[double]][single");

    settings.set("list_empty_brackets_inside", "[item[]with][empty][]brackets");
    EXPECT_THROW(settings.get_entry_list("list_empty_brackets_inside"), Exception);
}

TEST_F(SettingsTest, GetEntryListWithCommaSeparators) {
    Settings settings;

    settings.set("list_comma_separated", "[item1],[item2],[item3]");
    auto commaList = settings.get_entry_list("list_comma_separated");
    ASSERT_EQ(commaList.size(), 3);
    EXPECT_EQ(commaList[0], "item1");
    EXPECT_EQ(commaList[1], "item2");
    EXPECT_EQ(commaList[2], "item3");

    settings.set("list_with_empty_items", "[item1],[],[item3]");
    auto listWithEmpty = settings.get_entry_list("list_with_empty_items");
    ASSERT_EQ(listWithEmpty.size(), 2);
    EXPECT_EQ(listWithEmpty[0], "item1");
    EXPECT_EQ(listWithEmpty[1], "item3");

    settings.set("list_comma_separated1", "[item1], [item2], [item3]");
    auto commaList1 = settings.get_entry_list("list_comma_separated1");
    ASSERT_EQ(commaList1.size(), 3);
    EXPECT_EQ(commaList1[0], "item1");
    EXPECT_EQ(commaList1[1], "item2");
    EXPECT_EQ(commaList1[2], "item3");

    settings.set("list_with_empty_items1", "");
    auto listWithEmpty1 = settings.get_entry_list("list_with_empty_items1");
    ASSERT_EQ(listWithEmpty1.size(), 0);
}

TEST_F(SettingsTest, GetEntryListSpecExamples) {
    Settings settings;

    settings.set("ex_empty", "[]");
    auto v0 = settings.get_entry_list("ex_empty");
    EXPECT_TRUE(v0.empty());

    settings.set("ex_nested_balanced", "[[123]]");
    auto v1 = settings.get_entry_list("ex_nested_balanced");
    ASSERT_EQ(v1.size(), 1);
    EXPECT_EQ(v1[0], "[123]");

    settings.set("ex_nested_unbalanced_inside", "[[123[]");
    auto v2 = settings.get_entry_list("ex_nested_unbalanced_inside");
    ASSERT_EQ(v2.size(), 1);
    EXPECT_EQ(v2[0], "[123[");

    settings.set("ex_no_comma_multiple", "[123][123");
    EXPECT_THROW(settings.get_entry_list("ex_no_comma_multiple"), Exception);

    settings.set("ex_comma_missing_closing", "[123],[123");
    EXPECT_THROW(settings.get_entry_list("ex_comma_missing_closing"), Exception);
}

} // namespace doris::segment_v2::inverted_index
