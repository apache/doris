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

#include "util/path_trie.hpp"

#include <gtest/gtest.h>

#include "common/config.h"
#include "util/logging.h"

namespace doris {

class PathTrieTest : public testing::Test {};

TEST_F(PathTrieTest, SplitTest) {
    PathTrie<int> root;
    std::vector<std::string> array;

    array.clear();
    root.split("", &array);
    EXPECT_EQ(0, array.size());

    array.clear();
    root.split("///", &array);
    EXPECT_EQ(0, array.size());

    array.clear();
    root.split("/a/b/c", &array);
    EXPECT_EQ(3, array.size());
    EXPECT_STREQ("a", array[0].c_str());
    EXPECT_STREQ("b", array[1].c_str());
    EXPECT_STREQ("c", array[2].c_str());

    array.clear();
    root.split("/{db}/{table}/{rollup}", &array);
    EXPECT_EQ(3, array.size());
    EXPECT_STREQ("{db}", array[0].c_str());
    EXPECT_STREQ("{table}", array[1].c_str());
    EXPECT_STREQ("{rollup}", array[2].c_str());
}

TEST_F(PathTrieTest, TemplateTest) {
    PathTrie<int> root;
    std::string path = "/{db}/{table}/{rollup}";
    EXPECT_TRUE(root.insert(path, 1));

    std::map<std::string, std::string> params;
    int value;
    EXPECT_TRUE(root.retrieve("/1/2/3", &value));
    EXPECT_EQ(1, value);

    EXPECT_TRUE(root.retrieve("/a/b/c", &value, &params));
    EXPECT_EQ(1, value);
    EXPECT_STREQ("a", params["db"].c_str());
    EXPECT_STREQ("b", params["table"].c_str());
    EXPECT_STREQ("c", params["rollup"].c_str());
}

TEST_F(PathTrieTest, ExactTest) {
    PathTrie<int> root;
    std::string path = "/db/table/rollup";
    EXPECT_TRUE(root.insert(path, 100));

    std::map<std::string, std::string> params;
    int value;
    EXPECT_TRUE(root.retrieve("/db/table/rollup", &value, &params));
    EXPECT_EQ(100, value);
    EXPECT_EQ(0, params.size());

    // No path assert
    EXPECT_FALSE(root.retrieve("/db/table/c", &value, &params));
    EXPECT_FALSE(root.retrieve("/a/b/c", &value, &params));
}

TEST_F(PathTrieTest, MultiInsertTest) {
    PathTrie<int> root;
    std::string path = "/db/table/rollup";
    EXPECT_TRUE(root.insert(path, 100));
    EXPECT_FALSE(root.insert(path, 100));

    path = "/db/table/rollup2";
    EXPECT_TRUE(root.insert(path, 110));

    int value;
    EXPECT_TRUE(root.retrieve("/db/table/rollup", &value));
    EXPECT_EQ(100, value);
    EXPECT_TRUE(root.retrieve("/db/table/rollup2", &value));
    EXPECT_EQ(110, value);

    // Other
    path = "/db/rollup";
    EXPECT_TRUE(root.insert(path, 120));
    EXPECT_TRUE(root.retrieve("/db/rollup", &value));
    EXPECT_EQ(120, value);
}

TEST_F(PathTrieTest, MultiTemplateTest) {
    PathTrie<int> root;
    std::string path = "/db/{table}";
    EXPECT_TRUE(root.insert(path, 100));

    // Duplicate template
    path = "/db/{rollup}/abc";
    EXPECT_FALSE(root.insert(path, 110));

    path = "/db/{table}/abc";
    EXPECT_TRUE(root.insert(path, 110));

    int value;
    std::map<std::string, std::string> params;
    EXPECT_TRUE(root.retrieve("/db/12345", &value, &params));
    EXPECT_EQ(100, value);
    EXPECT_EQ(1, params.size());
    EXPECT_STREQ("12345", params["table"].c_str());
}

TEST_F(PathTrieTest, MultiPlayTest) {
    PathTrie<int> root;
    std::string path = "/db/abc";
    EXPECT_TRUE(root.insert(path, 100));

    // Duplicate template
    path = "/db";
    EXPECT_TRUE(root.insert(path, 110));

    path = "/db/abc/bcd";
    EXPECT_TRUE(root.insert(path, 120));

    int value;
    EXPECT_TRUE(root.retrieve("/db/abc", &value));
    EXPECT_EQ(100, value);
    EXPECT_TRUE(root.retrieve("/db", &value));
    EXPECT_EQ(110, value);
    EXPECT_TRUE(root.retrieve("/db/abc/bcd", &value));
    EXPECT_EQ(120, value);
}

TEST_F(PathTrieTest, EmptyTest) {
    PathTrie<int> root;
    std::string path = "/";
    EXPECT_TRUE(root.insert(path, 100));

    // Duplicate template
    path = "/";
    EXPECT_FALSE(root.insert(path, 110));

    int value;
    EXPECT_TRUE(root.retrieve("/", &value));
    EXPECT_EQ(100, value);

    value = 150;
    EXPECT_TRUE(root.retrieve("", &value));
    EXPECT_EQ(100, value);
}

} // namespace doris
