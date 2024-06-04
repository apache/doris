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
#include "olap/wal/wal_dirs_info.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <memory>

namespace doris {

class WalDirsInfoTest : public testing::Test {
public:
    WalDirsInfoTest() = default;
    ~WalDirsInfoTest() override = default;
    void SetUp() override {
        // limit 1000 used 100 estimated bytes in wal 200 available 700
        Status st = wal_dirs_info.add(wal_dir_test_1, 0, 0, 0);
        EXPECT_EQ(st, Status::OK());
        // limit 1000 used 200 estimated bytes in wal 300 available 500
        st = wal_dirs_info.add(wal_dir_test_2, 0, 0, 0);
        EXPECT_EQ(st, Status::OK());
        // limit 1000 used 400 estimated bytes in wal 500 available 100
        st = wal_dirs_info.add(wal_dir_test_3, 0, 0, 0);
        EXPECT_EQ(st, Status::OK());
    }
    void TearDown() override {}

    void set_and_check_success(std::string wal_dir, size_t limit, size_t used,
                               size_t estimated_wal_bytes) {
        Status st = wal_dirs_info.update_wal_dir_limit(wal_dir, limit);
        EXPECT_EQ(st, Status::OK());
        std::shared_ptr<WalDirInfo> wal_dir_info;
        st = wal_dirs_info.get_wal_dir_info(wal_dir, wal_dir_info);
        EXPECT_NE(wal_dir_info, nullptr);
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(wal_dir_info->get_limit(), limit);

        st = wal_dirs_info.update_wal_dir_used(wal_dir, used);
        EXPECT_EQ(st, Status::OK());
        st = wal_dirs_info.get_wal_dir_info(wal_dir, wal_dir_info);
        EXPECT_NE(wal_dir_info, nullptr);
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(wal_dir_info->get_used(), used);

        st = wal_dirs_info.update_wal_dir_estimated_wal_bytes(wal_dir, estimated_wal_bytes, 0);
        EXPECT_EQ(st, Status::OK());
        st = wal_dirs_info.get_wal_dir_info(wal_dir, wal_dir_info);
        EXPECT_NE(wal_dir_info, nullptr);
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(wal_dir_info->get_estimated_wal_bytes(), estimated_wal_bytes);

        st = wal_dirs_info.update_wal_dir_estimated_wal_bytes(wal_dir, 0, estimated_wal_bytes);
        EXPECT_EQ(st, Status::OK());
        st = wal_dirs_info.get_wal_dir_info(wal_dir, wal_dir_info);
        EXPECT_NE(wal_dir_info, nullptr);
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(wal_dir_info->get_estimated_wal_bytes(), 0);

        st = wal_dirs_info.update_wal_dir_estimated_wal_bytes(wal_dir, estimated_wal_bytes, 0);
        EXPECT_EQ(st, Status::OK());
        st = wal_dirs_info.get_wal_dir_info(wal_dir, wal_dir_info);
        EXPECT_NE(wal_dir_info, nullptr);
        EXPECT_EQ(st, Status::OK());
        EXPECT_EQ(wal_dir_info->get_estimated_wal_bytes(), estimated_wal_bytes);

        EXPECT_EQ(wal_dir_info->available(), limit - used - estimated_wal_bytes);
    }

    void set_and_check_fail(std::string wal_dir, size_t limit, size_t used,
                            size_t estimated_wal_bytes) {
        Status st = wal_dirs_info.update_wal_dir_limit(wal_dir, limit);
        EXPECT_EQ(st, Status::InternalError(""));
    }

    WalDirsInfo wal_dirs_info;
    std::string wal_dir = std::string(getenv("DORIS_HOME")) + "/wal_test";
    // exist
    std::string wal_dir_test_1 = wal_dir + "/test_1";
    std::string wal_dir_test_2 = wal_dir + "/test_2";
    std::string wal_dir_test_3 = wal_dir + "/test_3";
    // not exist
    std::string wal_dir_test_4 = wal_dir + "/test_4";
};

TEST_F(WalDirsInfoTest, test_wal_set_data) {
    set_and_check_success(wal_dir_test_1, 1000, 100, 200);
    set_and_check_success(wal_dir_test_2, 1000, 200, 300);
    set_and_check_success(wal_dir_test_3, 1000, 400, 500);
    set_and_check_fail(wal_dir_test_4, 0, 0, 0);
}

TEST_F(WalDirsInfoTest, test_wal_dir_select_random_strategy) {
    // available 700
    set_and_check_success(wal_dir_test_1, 1000, 100, 200);
    // available 500
    set_and_check_success(wal_dir_test_2, 1000, 200, 300);
    // available 100
    set_and_check_success(wal_dir_test_3, 1000, 400, 500);
    for (int i = 0; i < 100; i++) {
        std::string wal_dir = wal_dirs_info.get_available_random_wal_dir();
        EXPECT_NE(wal_dir, wal_dir_test_3);
    }
}

TEST_F(WalDirsInfoTest, test_wal_dir_select_max_available_strategy) {
    // available 100
    set_and_check_success(wal_dir_test_1, 1000, 500, 400);
    // available 50
    set_and_check_success(wal_dir_test_2, 1000, 500, 450);
    // available 0
    set_and_check_success(wal_dir_test_3, 1000, 500, 500);

    for (int i = 0; i < 100; i++) {
        std::string wal_dir = wal_dirs_info.get_available_random_wal_dir();
        EXPECT_EQ(wal_dir, wal_dir_test_1);
    }
}
} // namespace doris