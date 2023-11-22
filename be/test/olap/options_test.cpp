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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <stdlib.h>

#include <filesystem>
#include <string>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "olap/olap_define.h"

namespace doris {

static void set_up() {
    EXPECT_EQ(system("rm -rf ./test_run && mkdir -p ./test_run"), 0);
    EXPECT_EQ(system("mkdir -p ./test_run/palo && mkdir -p ./test_run/palo.ssd"), 0);
}

static void tear_down() {
    EXPECT_EQ(system("rm -rf ./test_run"), 0);
}

class OptionsTest : public testing::Test {
public:
    OptionsTest() {}
    virtual ~OptionsTest() {}
    static void SetUpTestSuite() { set_up(); }

    static void TearDownTestSuite() { tear_down(); }
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
        EXPECT_EQ(Status::OK(), parse_root_path(root_path, &path));
        EXPECT_STREQ(path1.c_str(), path.path.c_str());
        EXPECT_EQ(-1, path.capacity_bytes);
        EXPECT_EQ(TStorageMedium::HDD, path.storage_medium);
    }
    {
        root_path = path2;
        EXPECT_EQ(Status::OK(), parse_root_path(root_path, &path));
        EXPECT_STREQ(path2.c_str(), path.path.c_str());
        EXPECT_EQ(-1, path.capacity_bytes);
        EXPECT_EQ(TStorageMedium::SSD, path.storage_medium);
    }
    {
        root_path = path2 + ", 50";
        EXPECT_EQ(Status::OK(), parse_root_path(root_path, &path));
        EXPECT_STREQ(path2.c_str(), path.path.c_str());
        EXPECT_EQ(50 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        EXPECT_EQ(TStorageMedium::SSD, path.storage_medium);
    }

    // /path, <property>:<value>,...
    {
        root_path = path1 + ", capacity:50, medium: ssd";
        EXPECT_EQ(Status::OK(), parse_root_path(root_path, &path));
        EXPECT_STREQ(path1.c_str(), path.path.c_str());
        EXPECT_EQ(50 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        EXPECT_EQ(TStorageMedium::SSD, path.storage_medium);
    }
    {
        root_path = path1 + ", medium: ssd, capacity:30";
        EXPECT_EQ(Status::OK(), parse_root_path(root_path, &path));
        EXPECT_STREQ(path1.c_str(), path.path.c_str());
        EXPECT_EQ(30 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        EXPECT_EQ(TStorageMedium::SSD, path.storage_medium);
    }
    {
        root_path = path1 + " , medium: ssd, 60";
        EXPECT_EQ(Status::OK(), parse_root_path(root_path, &path));
        EXPECT_STREQ(path1.c_str(), path.path.c_str());
        EXPECT_EQ(60 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        EXPECT_EQ(TStorageMedium::SSD, path.storage_medium);
    }
    {
        root_path = path1 + ", medium: ssd, 60, medium: hdd, capacity: 10";
        EXPECT_EQ(Status::OK(), parse_root_path(root_path, &path));
        EXPECT_STREQ(path1.c_str(), path.path.c_str());
        EXPECT_EQ(10 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        EXPECT_EQ(TStorageMedium::HDD, path.storage_medium);
    }
    {
        root_path = path2 + ", medium: hdd, 60, capacity: 10";
        EXPECT_EQ(Status::OK(), parse_root_path(root_path, &path));
        EXPECT_STREQ(path2.c_str(), path.path.c_str());
        EXPECT_EQ(10 * GB_EXCHANGE_BYTE, path.capacity_bytes);
        EXPECT_EQ(TStorageMedium::HDD, path.storage_medium);
    }
    {
        // test tail `;`
        std::string path = path1 + ";" + path2 + ";";
        std::vector<StorePath> paths;
        EXPECT_EQ(Status::OK(), parse_conf_store_paths(path, &paths));
        EXPECT_EQ(paths.size(), 2);
        EXPECT_STREQ(path1.c_str(), paths[0].path.c_str());
        EXPECT_STREQ(path2.c_str(), paths[1].path.c_str());
    }
}

TEST_F(OptionsTest, parse_conf_store_path) {
    std::string path_prefix = std::filesystem::absolute("./test_run").string();
    std::string path1 = path_prefix + "/palo";
    std::string path2 = path_prefix + "/palo.ssd";

    {
        std::vector<StorePath> paths;
        std::string config_path = path1;
        auto st = parse_conf_store_paths(config_path, &paths);
        EXPECT_EQ(Status::OK(), st);
        EXPECT_EQ(paths.size(), 1);
        EXPECT_STREQ(paths[0].path.c_str(), config_path.c_str());
        EXPECT_EQ(paths[0].capacity_bytes, -1);
        EXPECT_EQ(paths[0].storage_medium, TStorageMedium::HDD);
    }
    {
        std::vector<StorePath> paths;
        std::string config_path = path1 + ";";
        auto st = parse_conf_store_paths(config_path, &paths);
        EXPECT_EQ(Status::OK(), st);
        EXPECT_EQ(paths.size(), 1);
        EXPECT_STREQ(paths[0].path.c_str(), path1.c_str());
        EXPECT_EQ(paths[0].capacity_bytes, -1);
        EXPECT_EQ(paths[0].storage_medium, TStorageMedium::HDD);
    }
    {
        std::vector<StorePath> paths;
        std::string config_path = path1 + ";" + path1;
        auto st = parse_conf_store_paths(config_path, &paths);
        EXPECT_EQ(Status::Error<ErrorCode::INVALID_ARGUMENT>("a duplicated path is found, path={}",
                                                             path1),
                  st);
    }
    {
        std::vector<StorePath> paths;
        std::string config_path = path1 + ";" + path2 + ";";
        auto st = parse_conf_store_paths(config_path, &paths);
        EXPECT_EQ(Status::OK(), st);
        EXPECT_EQ(paths.size(), 2);
        EXPECT_STREQ(paths[0].path.c_str(), path1.c_str());
        EXPECT_EQ(paths[0].capacity_bytes, -1);
        EXPECT_EQ(paths[0].storage_medium, TStorageMedium::HDD);
        EXPECT_STREQ(paths[1].path.c_str(), path2.c_str());
        EXPECT_EQ(paths[1].capacity_bytes, -1);
        EXPECT_EQ(paths[1].storage_medium, TStorageMedium::SSD);
    }
}

TEST_F(OptionsTest, parse_broken_path) {
    {
        std::string broken_paths = "path1";
        std::set<std::string> parsed_paths;
        parse_conf_broken_store_paths(broken_paths, &parsed_paths);
        EXPECT_EQ(parsed_paths.size(), 1);
    }
    {
        std::string broken_paths = "path1;path1;";
        std::set<std::string> parsed_paths;
        parse_conf_broken_store_paths(broken_paths, &parsed_paths);
        EXPECT_EQ(parsed_paths.size(), 1);
        EXPECT_EQ(parsed_paths.count("path1"), 1);
    }
    {
        std::string broken_paths = "path1;path2;";
        std::set<std::string> parsed_paths;
        parse_conf_broken_store_paths(broken_paths, &parsed_paths);
        EXPECT_EQ(parsed_paths.size(), 2);
        EXPECT_EQ(parsed_paths.count("path1"), 1);
        EXPECT_EQ(parsed_paths.count("path2"), 1);
    }
}

} // namespace doris
