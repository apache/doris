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
// KIND, either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

#include "vec/json/path_in_data.h"

#include <gtest/gtest.h>

namespace doris::vectorized {

class PathInDataTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(PathInDataTest, ConstructorWithPathAndParts) {
    // Test case 1: Basic path with parts
    PathInData::Parts parts;
    parts.emplace_back("a", false, 0);
    parts.emplace_back("b", false, 0);
    parts.emplace_back("c", false, 0);

    PathInData path("a.b.c", parts, false);
    EXPECT_EQ(path.get_path(), "a.b.c");
    EXPECT_EQ(path.get_parts().size(), 3);
    EXPECT_EQ(path.get_parts()[0].key, "a");
    EXPECT_EQ(path.get_parts()[1].key, "b");
    EXPECT_EQ(path.get_parts()[2].key, "c");
    EXPECT_FALSE(path.get_is_typed());

    // Test case 2: Path with typed flag
    PathInData typed_path("a.b.c", parts, true);
    EXPECT_TRUE(typed_path.get_is_typed());
    EXPECT_FALSE(typed_path.is_nested(0));
}

TEST_F(PathInDataTest, ConstructorWithRootAndPaths) {
    // Test case 1: Basic root and paths
    std::string root = "root";
    std::vector<std::string> paths = {"a", "b", "c"};

    PathInData path(root, paths);
    EXPECT_EQ(path.get_path(), "root.a.b.c");
    EXPECT_EQ(path.get_parts().size(), 4);
    EXPECT_EQ(path.get_parts()[0].key, "root");
    EXPECT_EQ(path.get_parts()[1].key, "a");
    EXPECT_EQ(path.get_parts()[2].key, "b");
    EXPECT_EQ(path.get_parts()[3].key, "c");

    // Test case 2: Empty paths
    std::vector<std::string> empty_paths;
    PathInData path2(root, empty_paths);
    EXPECT_EQ(path2.get_path(), "root");
    EXPECT_EQ(path2.get_parts().size(), 1);
    EXPECT_EQ(path2.get_parts()[0].key, "root");
}

TEST_F(PathInDataTest, ConstructorWithPaths) {
    // Test case 1: Basic paths
    std::vector<std::string> paths = {"a", "b", "c"};

    PathInData path(paths);
    EXPECT_EQ(path.get_path(), "a.b.c");
    EXPECT_EQ(path.get_parts().size(), 3);
    EXPECT_EQ(path.get_parts()[0].key, "a");
    EXPECT_EQ(path.get_parts()[1].key, "b");
    EXPECT_EQ(path.get_parts()[2].key, "c");

    // Test case 2: Empty paths
    std::vector<std::string> empty_paths;
    PathInData path2(empty_paths);
    EXPECT_TRUE(path2.get_path().empty());
    EXPECT_TRUE(path2.get_parts().empty());
}

TEST_F(PathInDataTest, ToJsonPath) {
    // Test case 1: Basic path
    std::vector<std::string> paths = {"a", "b", "c"};
    PathInData path(paths);
    EXPECT_EQ(path.to_jsonpath(), "$.a.b.c");

    // Test case 2: Empty path
    std::vector<std::string> empty_paths;
    PathInData path2(empty_paths);
    EXPECT_EQ(path2.to_jsonpath(), "$.");

    // Test case 3: Single element path
    std::vector<std::string> single_path = {"a"};
    PathInData path3(single_path);
    EXPECT_EQ(path3.to_jsonpath(), "$.a");
}

TEST_F(PathInDataTest, CopyPopBack) {
    // Test case 1: Path with multiple elements
    std::vector<std::string> paths = {"a", "b", "c", "d"};
    PathInData path(paths);
    PathInData popped = path.copy_pop_back();
    EXPECT_EQ(popped.get_path(), "a.b.c");
    EXPECT_EQ(popped.get_parts().size(), 3);
    EXPECT_EQ(popped.get_parts()[0].key, "a");
    EXPECT_EQ(popped.get_parts()[1].key, "b");
    EXPECT_EQ(popped.get_parts()[2].key, "c");

    // Test case 2: Path with single element
    std::vector<std::string> single_path = {"a"};
    PathInData path2(single_path);
    PathInData popped2 = path2.copy_pop_back();
    EXPECT_TRUE(popped2.get_path().empty());
    EXPECT_TRUE(popped2.get_parts().empty());

    // Test case 3: Empty path
    std::vector<std::string> empty_paths;
    PathInData path3(empty_paths);
    PathInData popped3 = path3.copy_pop_back();
    EXPECT_TRUE(popped3.get_path().empty());
    EXPECT_TRUE(popped3.get_parts().empty());
}

TEST_F(PathInDataTest, PartOperatorEqual) {
    // Test case 1: Equal parts
    PathInData::Part part1("key", false, 0);
    PathInData::Part part2("key", false, 0);
    EXPECT_TRUE(part1 == part2);

    // Test case 2: Different keys
    PathInData::Part part3("key1", false, 0);
    PathInData::Part part4("key2", false, 0);
    EXPECT_FALSE(part3 == part4);

    // Test case 3: Different is_nested
    PathInData::Part part5("key", true, 0);
    PathInData::Part part6("key", false, 0);
    EXPECT_FALSE(part5 == part6);

    // Test case 4: Different anonymous_array_level
    PathInData::Part part7("key", false, 1);
    PathInData::Part part8("key", false, 2);
    EXPECT_FALSE(part7 == part8);

    // Test case 5: All different
    PathInData::Part part9("key1", true, 1);
    PathInData::Part part10("key2", false, 2);
    EXPECT_FALSE(part9 == part10);

    // Test case 6: Same key but different is_nested and anonymous_array_level
    PathInData::Part part11("key", true, 1);
    PathInData::Part part12("key", false, 2);
    EXPECT_FALSE(part11 == part12);

    // Test case 7: Same key and is_nested but different anonymous_array_level
    PathInData::Part part13("key", true, 1);
    PathInData::Part part14("key", true, 2);
    EXPECT_FALSE(part13 == part14);

    // Test case 8: Same key and anonymous_array_level but different is_nested
    PathInData::Part part15("key", true, 1);
    PathInData::Part part16("key", false, 1);
    EXPECT_FALSE(part15 == part16);
}

} // namespace doris::vectorized