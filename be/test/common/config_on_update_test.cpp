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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <map>
#include <string>
#include <thread>
#include <utility>

#include "common/config.h"
#include "common/status.h"

namespace doris {
using namespace config;

// Test variables to track callback invocations
static int64_t g_int64_old_val = 0;
static int64_t g_int64_new_val = 0;
static int g_int64_callback_count = 0;

static std::string g_string_old_val;
static std::string g_string_new_val;
static int g_string_callback_count = 0;

static bool g_bool_old_val = false;
static bool g_bool_new_val = false;
static int g_bool_callback_count = 0;

static std::atomic<bool> g_pause_persistent_update {false};
static std::atomic<bool> g_persistent_update_entered {false};
static std::atomic<bool> g_release_persistent_update {false};
static std::atomic<int32_t> g_concurrent_callback_value {0};
static std::atomic<int> g_concurrent_callback_count {0};

DEFINE_mInt32(cfg_on_update_concurrent, "1");
DEFINE_Validator(cfg_on_update_concurrent, [](int32_t value) {
    if (value == 2 && g_pause_persistent_update.load()) {
        g_persistent_update_entered.store(true);
        while (!g_release_persistent_update.load()) {
            std::this_thread::yield();
        }
    }
    return true;
});
DEFINE_ON_UPDATE(cfg_on_update_concurrent, [](int32_t, int32_t value) {
    g_concurrent_callback_value.store(value);
    g_concurrent_callback_count.fetch_add(1);
});

// Define test configs with DEFINE_ON_UPDATE callbacks
DEFINE_mInt64(cfg_on_update_int64, "100");
DEFINE_ON_UPDATE(cfg_on_update_int64, [](int64_t old_val, int64_t new_val) {
    g_int64_old_val = old_val;
    g_int64_new_val = new_val;
    g_int64_callback_count++;
});

DEFINE_mString(cfg_on_update_string, "default");
DEFINE_ON_UPDATE(cfg_on_update_string, [](std::string old_val, std::string new_val) {
    g_string_old_val = old_val;
    g_string_new_val = new_val;
    g_string_callback_count++;
});

DEFINE_mBool(cfg_on_update_bool, "false");
DEFINE_ON_UPDATE(cfg_on_update_bool, [](bool old_val, bool new_val) {
    g_bool_old_val = old_val;
    g_bool_new_val = new_val;
    g_bool_callback_count++;
});

class ConfigOnUpdateTest : public testing::Test {
protected:
    void SetUp() override {
        // Reset tracking variables before each test
        g_int64_old_val = 0;
        g_int64_new_val = 0;
        g_int64_callback_count = 0;

        g_string_old_val.clear();
        g_string_new_val.clear();
        g_string_callback_count = 0;

        g_bool_old_val = false;
        g_bool_new_val = false;
        g_bool_callback_count = 0;
    }
};

TEST_F(ConfigOnUpdateTest, Int64Callback) {
    // Initial value should be 100
    EXPECT_EQ(cfg_on_update_int64, 100);
    EXPECT_EQ(g_int64_callback_count, 0);

    // Update config to 200
    Status s = config::set_config("cfg_on_update_int64", "200");
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(cfg_on_update_int64, 200);

    // Verify callback was invoked with correct values
    EXPECT_EQ(g_int64_callback_count, 1);
    EXPECT_EQ(g_int64_old_val, 100);
    EXPECT_EQ(g_int64_new_val, 200);

    // Update again
    s = config::set_config("cfg_on_update_int64", "300");
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(cfg_on_update_int64, 300);

    EXPECT_EQ(g_int64_callback_count, 2);
    EXPECT_EQ(g_int64_old_val, 200);
    EXPECT_EQ(g_int64_new_val, 300);
}

TEST_F(ConfigOnUpdateTest, ConcurrentPersistentFailurePreservesSuccessfulUpdate) {
    cfg_on_update_concurrent = 1;
    g_concurrent_callback_value.store(0);
    g_concurrent_callback_count.store(0);
    g_persistent_update_entered.store(false);
    g_release_persistent_update.store(false);
    g_pause_persistent_update.store(true);

    const std::string field = "cfg_on_update_concurrent";
    std::map<std::string, std::string> conf_map {{field, "1"}};
    auto* saved_conf_map = std::exchange(config::full_conf_map, &conf_map);
    std::string saved_conf_dir = std::exchange(config::custom_config_dir, "/dev/null");

    Status persistent_status;
    std::thread persistent_writer(
            [&] { persistent_status = config::set_config(field, "2", true); });
    while (!g_persistent_update_entered.load()) {
        std::this_thread::yield();
    }

    std::promise<void> second_writer_started;
    auto second_writer_started_future = second_writer_started.get_future();
    Status nonpersistent_status;
    std::promise<void> second_writer_finished;
    auto second_writer_finished_future = second_writer_finished.get_future();
    std::thread nonpersistent_writer([&] {
        second_writer_started.set_value();
        nonpersistent_status = config::set_config(field, "3");
        second_writer_finished.set_value();
    });
    second_writer_started_future.wait();
    second_writer_finished_future.wait_for(std::chrono::seconds(1));
    g_release_persistent_update.store(true);

    persistent_writer.join();
    nonpersistent_writer.join();
    g_pause_persistent_update.store(false);
    config::custom_config_dir = std::move(saved_conf_dir);
    config::full_conf_map = saved_conf_map;

    EXPECT_FALSE(persistent_status.ok());
    EXPECT_TRUE(nonpersistent_status.ok());
    EXPECT_EQ(cfg_on_update_concurrent, 3);
    EXPECT_EQ(conf_map.at(field), "3");
    EXPECT_EQ(g_concurrent_callback_value.load(), 3);
    EXPECT_EQ(g_concurrent_callback_count.load(), 1);
}

TEST_F(ConfigOnUpdateTest, StringCallback) {
    // Initial value should be "default"
    EXPECT_EQ(cfg_on_update_string, "default");
    EXPECT_EQ(g_string_callback_count, 0);

    // Update config
    Status s = config::set_config("cfg_on_update_string", "new_value");
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(cfg_on_update_string, "new_value");

    // Verify callback was invoked with correct values
    EXPECT_EQ(g_string_callback_count, 1);
    EXPECT_EQ(g_string_old_val, "default");
    EXPECT_EQ(g_string_new_val, "new_value");
}

TEST_F(ConfigOnUpdateTest, BoolCallback) {
    // Initial value should be false
    EXPECT_EQ(cfg_on_update_bool, false);
    EXPECT_EQ(g_bool_callback_count, 0);

    // Update config to true
    Status s = config::set_config("cfg_on_update_bool", "true");
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(cfg_on_update_bool, true);

    // Verify callback was invoked with correct values
    EXPECT_EQ(g_bool_callback_count, 1);
    EXPECT_EQ(g_bool_old_val, false);
    EXPECT_EQ(g_bool_new_val, true);

    // Update back to false
    s = config::set_config("cfg_on_update_bool", "false");
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(cfg_on_update_bool, false);

    EXPECT_EQ(g_bool_callback_count, 2);
    EXPECT_EQ(g_bool_old_val, true);
    EXPECT_EQ(g_bool_new_val, false);
}

} // namespace doris
