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
    ASSERT_EQ(0, array.size());

    array.clear();
    root.split("///", &array);
    ASSERT_EQ(0, array.size());

    array.clear();
    root.split("/a/b/c", &array);
    ASSERT_EQ(3, array.size());
    ASSERT_STREQ("a", array[0].c_str());
    ASSERT_STREQ("b", array[1].c_str());
    ASSERT_STREQ("c", array[2].c_str());

    array.clear();
    root.split("/{db}/{table}/{rollup}", &array);
    ASSERT_EQ(3, array.size());
    ASSERT_STREQ("{db}", array[0].c_str());
    ASSERT_STREQ("{table}", array[1].c_str());
    ASSERT_STREQ("{rollup}", array[2].c_str());
}

TEST_F(PathTrieTest, TemplateTest) {
    PathTrie<int> root;
    std::string path = "/{db}/{table}/{rollup}";
    ASSERT_TRUE(root.insert(path, 1));

    std::map<std::string, std::string> params;
    int value;
    ASSERT_TRUE(root.retrieve("/1/2/3", &value));
    ASSERT_EQ(1, value);

    ASSERT_TRUE(root.retrieve("/a/b/c", &value, &params));
    ASSERT_EQ(1, value);
    ASSERT_STREQ("a", params["db"].c_str());
    ASSERT_STREQ("b", params["table"].c_str());
    ASSERT_STREQ("c", params["rollup"].c_str());
}

TEST_F(PathTrieTest, ExactTest) {
    PathTrie<int> root;
    std::string path = "/db/table/rollup";
    ASSERT_TRUE(root.insert(path, 100));

    std::map<std::string, std::string> params;
    int value;
    ASSERT_TRUE(root.retrieve("/db/table/rollup", &value, &params));
    ASSERT_EQ(100, value);
    ASSERT_EQ(0, params.size());

    // No path assert
    ASSERT_FALSE(root.retrieve("/db/table/c", &value, &params));
    ASSERT_FALSE(root.retrieve("/a/b/c", &value, &params));
}

TEST_F(PathTrieTest, MultiInsertTest) {
    PathTrie<int> root;
    std::string path = "/db/table/rollup";
    ASSERT_TRUE(root.insert(path, 100));
    ASSERT_FALSE(root.insert(path, 100));

    path = "/db/table/rollup2";
    ASSERT_TRUE(root.insert(path, 110));

    int value;
    ASSERT_TRUE(root.retrieve("/db/table/rollup", &value));
    ASSERT_EQ(100, value);
    ASSERT_TRUE(root.retrieve("/db/table/rollup2", &value));
    ASSERT_EQ(110, value);

    // Other
    path = "/db/rollup";
    ASSERT_TRUE(root.insert(path, 120));
    ASSERT_TRUE(root.retrieve("/db/rollup", &value));
    ASSERT_EQ(120, value);
}

TEST_F(PathTrieTest, MultiTemplateTest) {
    PathTrie<int> root;
    std::string path = "/db/{table}";
    ASSERT_TRUE(root.insert(path, 100));

    // Duplicate template
    path = "/db/{rollup}/abc";
    ASSERT_FALSE(root.insert(path, 110));

    path = "/db/{table}/abc";
    ASSERT_TRUE(root.insert(path, 110));

    int value;
    std::map<std::string, std::string> params;
    ASSERT_TRUE(root.retrieve("/db/12345", &value, &params));
    ASSERT_EQ(100, value);
    ASSERT_EQ(1, params.size());
    ASSERT_STREQ("12345", params["table"].c_str());
}

TEST_F(PathTrieTest, MultiPlayTest) {
    PathTrie<int> root;
    std::string path = "/db/abc";
    ASSERT_TRUE(root.insert(path, 100));

    // Duplicate template
    path = "/db";
    ASSERT_TRUE(root.insert(path, 110));

    path = "/db/abc/bcd";
    ASSERT_TRUE(root.insert(path, 120));

    int value;
    ASSERT_TRUE(root.retrieve("/db/abc", &value));
    ASSERT_EQ(100, value);
    ASSERT_TRUE(root.retrieve("/db", &value));
    ASSERT_EQ(110, value);
    ASSERT_TRUE(root.retrieve("/db/abc/bcd", &value));
    ASSERT_EQ(120, value);
}

TEST_F(PathTrieTest, EmptyTest) {
    PathTrie<int> root;
    std::string path = "/";
    ASSERT_TRUE(root.insert(path, 100));

    // Duplicate template
    path = "/";
    ASSERT_FALSE(root.insert(path, 110));

    int value;
    ASSERT_TRUE(root.retrieve("/", &value));
    ASSERT_EQ(100, value);

    value = 150;
    ASSERT_TRUE(root.retrieve("", &value));
    ASSERT_EQ(100, value);
}

} // namespace doris

int main(int argc, char* argv[]) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
