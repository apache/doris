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

#include "olap/memtable_flush_executor.h"

#include <gtest/gtest.h>
#include <sys/file.h>

#include <string>
#include <thread>

#include "common/config.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "io/fs/local_file_system.h"
#include "olap/delta_writer.h"
#include "olap/field.h"
#include "olap/memtable.h"
#include "olap/options.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"

namespace doris {

void set_up() {
    char buffer[1024];
    getcwd(buffer, 1024);
    config::storage_root_path = std::string(buffer) + "/flush_test";
    auto st = io::global_local_filesystem()->delete_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;
    st = io::global_local_filesystem()->create_directory(config::storage_root_path);
    ASSERT_TRUE(st.ok()) << st;

    std::vector<StorePath> paths;
    paths.emplace_back(config::storage_root_path, -1);

    doris::EngineOptions options;
    options.store_paths = paths;
    auto engine = std::make_unique<StorageEngine>(options);
    Status s = engine->open();
    EXPECT_TRUE(s.ok()) << s.to_string();
    ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
}

void tear_down() {
    ExecEnv::GetInstance()->set_storage_engine(nullptr);
    system("rm -rf ./flush_test");
    EXPECT_TRUE(io::global_local_filesystem()
                        ->delete_directory(std::string(getenv("DORIS_HOME")) + "/" + UNUSED_PREFIX)
                        .ok());
}

Schema create_schema() {
    std::vector<TabletColumnPtr> col_schemas;
    col_schemas.emplace_back(
            std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                           FieldType::OLAP_FIELD_TYPE_SMALLINT, true));
    col_schemas.emplace_back(
            std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                           FieldType::OLAP_FIELD_TYPE_INT, true));
    col_schemas.emplace_back(
            std::make_shared<TabletColumn>(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                                           FieldType::OLAP_FIELD_TYPE_BIGINT, true));
    Schema schema(col_schemas, 2);
    return schema;
}

TEST(MemTableFlushExecutorTest, TestDynamicThreadPoolUpdate) {
    // Setup
    set_up();

    auto* flush_executor = ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    ASSERT_NE(flush_executor, nullptr);

    // Store original config values
    int32_t original_flush_thread_num = config::flush_thread_num_per_store;
    int32_t original_high_priority_flush_thread_num =
            config::high_priority_flush_thread_num_per_store;
    int32_t original_max_flush_thread_num = config::max_flush_thread_num_per_cpu;

    // Test 1: Get initial thread pool sizes
    int initial_max_threads = flush_executor->flush_pool()->max_threads();
    int initial_min_threads = flush_executor->flush_pool()->min_threads();
    EXPECT_GT(initial_max_threads, 0);
    EXPECT_GT(initial_min_threads, 0);

    // Test 2: Update flush_thread_num_per_store and verify thread pool updates
    config::flush_thread_num_per_store = 10;
    flush_executor->update_memtable_flush_threads();

    int new_min_threads = flush_executor->flush_pool()->min_threads();
    EXPECT_EQ(new_min_threads, 10);

    // Test 3: Update max_flush_thread_num_per_cpu and verify thread pool updates
    config::max_flush_thread_num_per_cpu = 2;
    flush_executor->update_memtable_flush_threads();

    int num_cpus = std::thread::hardware_concurrency();
    if (num_cpus > 0) {
        int expected_max = std::min(10 * 1, num_cpus * 2); // 1 disk, 10 threads per store
        int actual_max = flush_executor->flush_pool()->max_threads();
        EXPECT_EQ(actual_max, expected_max);
    }

    // Test 4: Update high_priority_flush_thread_num_per_store
    config::high_priority_flush_thread_num_per_store = 8;
    flush_executor->update_memtable_flush_threads();
    // Note: We can't directly access _high_prio_flush_pool, but update should not crash

    // Test 5: Set very small values
    config::flush_thread_num_per_store = 0; // Should be adjusted to 1 by std::max
    flush_executor->update_memtable_flush_threads();
    EXPECT_GE(flush_executor->flush_pool()->min_threads(), 1);

    // Test 6: Set large values
    config::flush_thread_num_per_store = 100;
    flush_executor->update_memtable_flush_threads();
    EXPECT_GE(flush_executor->flush_pool()->min_threads(), 1);

    // Restore original config values
    config::flush_thread_num_per_store = original_flush_thread_num;
    config::high_priority_flush_thread_num_per_store = original_high_priority_flush_thread_num;
    config::max_flush_thread_num_per_cpu = original_max_flush_thread_num;
    flush_executor->update_memtable_flush_threads();

    // Cleanup
    tear_down();
}

TEST(MemTableFlushExecutorTest, TestConfigUpdateTrigger) {
    // Setup
    set_up();

    auto* flush_executor = ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    ASSERT_NE(flush_executor, nullptr);

    // Store original config values
    int32_t original_flush_thread_num = config::flush_thread_num_per_store;

    // Get initial thread pool size
    int initial_min_threads = flush_executor->flush_pool()->min_threads();

    // Test: Simulate config update via set_config
    config::flush_thread_num_per_store = 15;
    config::update_config("flush_thread_num_per_store", "15");

    // Verify thread pool was updated
    int updated_min_threads = flush_executor->flush_pool()->min_threads();
    EXPECT_EQ(updated_min_threads, 15);
    EXPECT_NE(updated_min_threads, initial_min_threads);

    // Restore original config value
    config::flush_thread_num_per_store = original_flush_thread_num;
    flush_executor->update_memtable_flush_threads();

    // Cleanup
    tear_down();
}

TEST(MemTableFlushExecutorTest, TestThreadPoolMinMaxRelationship) {
    // Setup
    set_up();

    auto* flush_executor = ExecEnv::GetInstance()->storage_engine().memtable_flush_executor();
    ASSERT_NE(flush_executor, nullptr);

    // Store original config values
    int32_t original_flush_thread_num = config::flush_thread_num_per_store;
    int32_t original_max_flush_thread_num = config::max_flush_thread_num_per_cpu;

    // Test: Ensure min_threads <= max_threads always
    config::flush_thread_num_per_store = 20;
    config::max_flush_thread_num_per_cpu = 1; // Very restrictive
    flush_executor->update_memtable_flush_threads();

    int min_threads = flush_executor->flush_pool()->min_threads();
    int max_threads = flush_executor->flush_pool()->max_threads();
    EXPECT_LE(min_threads, max_threads);

    // Restore original config values
    config::flush_thread_num_per_store = original_flush_thread_num;
    config::max_flush_thread_num_per_cpu = original_max_flush_thread_num;
    flush_executor->update_memtable_flush_threads();

    // Cleanup
    tear_down();
}

} // namespace doris
