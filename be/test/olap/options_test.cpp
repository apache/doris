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

#include "olap/options.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

namespace doris {

void set_up() {
    ASSERT_EQ(system("rm -rf ./test_run && mkdir -p ./test_run"), 0);
    ASSERT_EQ(system("mkdir -p ./test_run/palo && mkdir -p ./test_run/palo.ssd"), 0);
}

void tear_down() {
    ASSERT_EQ(system("rm -rf ./test_run"), 0);
}

class OptionsTest : public testing::Test {
public:
    OptionsTest() {}
    virtual ~OptionsTest() {}
};

TEST_F(OptionsTest, parse_root_path) {
    std::string path_prefix = std::filesystem::absolute("./test_run").string();
    std::string path1 = path_prefix + "/palo";
    std::string path2 = path_prefix + "/palo.ssd";

    std::string root_path;
    StorePath path;

    // /path<.extension>, <capacity>
    {
        root_path = path1;
        ASSERT_EQ(OLAP_SUCCESS, parse_root_path(root_path, &path));
        ASSERT_STREQ(path1.c_str(), path.path.c_str());
        ASSERT_EQ(-1, path.capacity_bytes);
        ASSERT_EQ(TStorageMedium::HDD, path.storage_medium);
    }
    {
        root_path = path2;
        ASSERT_EQ(OLAP_SUCCESS, parse_root_path(root_path, &path));
        ASSERT_STREQ(path2.c_str(), path.path.c_str());
        ASSERT_EQ(-1, path.capacity_bytes);
        ASSERT_EQ(TStorageMedium::SSD, path.storage_medium);
    }
    {
        root_path = path2 + ", 50";
        ASSERT_EQ(OLAP_SUCCESS, parse_root_path(root_path, &path));
        ASSERT_STREQ(path2.c_str(), path.path.c_str());
        ASSERT_EQ(50 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        ASSERT_EQ(TStorageMedium::SSD, path.storage_medium);
    }

    // /path, <property>:<value>,...
    {
        root_path = path1 + ", capacity:50, medium: ssd";
        ASSERT_EQ(OLAP_SUCCESS, parse_root_path(root_path, &path));
        ASSERT_STREQ(path1.c_str(), path.path.c_str());
        ASSERT_EQ(50 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        ASSERT_EQ(TStorageMedium::SSD, path.storage_medium);
    }
    {
        root_path = path1 + ", medium: ssd, capacity:30";
        ASSERT_EQ(OLAP_SUCCESS, parse_root_path(root_path, &path));
        ASSERT_STREQ(path1.c_str(), path.path.c_str());
        ASSERT_EQ(30 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        ASSERT_EQ(TStorageMedium::SSD, path.storage_medium);
    }
    {
        root_path = path1 + " , medium: ssd, 60";
        ASSERT_EQ(OLAP_SUCCESS, parse_root_path(root_path, &path));
        ASSERT_STREQ(path1.c_str(), path.path.c_str());
        ASSERT_EQ(60 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        ASSERT_EQ(TStorageMedium::SSD, path.storage_medium);
    }
    {
        root_path = path1 + ", medium: ssd, 60, medium: hdd, capacity: 10";
        ASSERT_EQ(OLAP_SUCCESS, parse_root_path(root_path, &path));
        ASSERT_STREQ(path1.c_str(), path.path.c_str());
        ASSERT_EQ(10 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        ASSERT_EQ(TStorageMedium::HDD, path.storage_medium);
    }
    {
        root_path = path2 + ", medium: hdd, 60, capacity: 10";
        ASSERT_EQ(OLAP_SUCCESS, parse_root_path(root_path, &path));
        ASSERT_STREQ(path2.c_str(), path.path.c_str());
        ASSERT_EQ(10 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        ASSERT_EQ(TStorageMedium::HDD, path.storage_medium);
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);

    int ret = doris::OLAP_SUCCESS;
    doris::set_up();
    ret = RUN_ALL_TESTS();
    doris::tear_down();

    return ret;
}
