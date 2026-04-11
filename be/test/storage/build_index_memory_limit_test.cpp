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

#include "common/config.h"
#include "runtime/memory/global_memory_arbitrator.h"
#include "storage/storage_engine.h"
#include "util/mem_info.h"

namespace doris {

class BuildIndexMemoryLimitTest : public testing::Test {
public:
    void SetUp() override {
        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        _engine = std::make_unique<StorageEngine>(options);

        _saved_frac = config::build_index_mem_limit_frac;
        _saved_min_bytes = config::build_index_min_memory_per_task_bytes;
        _saved_high_pct = config::build_index_memory_high_watermark_pct;
        _saved_low_pct = config::build_index_memory_low_watermark_pct;
        _saved_soft_mem_limit = MemInfo::_s_soft_mem_limit.load();
        _saved_memory_growth =
                GlobalMemoryArbitrator::refresh_interval_memory_growth.load();

        GlobalMemoryArbitrator::refresh_interval_memory_growth.store(0);
    }

    void TearDown() override {
        config::build_index_mem_limit_frac = _saved_frac;
        config::build_index_min_memory_per_task_bytes = _saved_min_bytes;
        config::build_index_memory_high_watermark_pct = _saved_high_pct;
        config::build_index_memory_low_watermark_pct = _saved_low_pct;
        MemInfo::_s_soft_mem_limit.store(_saved_soft_mem_limit);
        GlobalMemoryArbitrator::refresh_interval_memory_growth.store(
                _saved_memory_growth);
    }

    std::unique_ptr<StorageEngine> _engine;

private:
    double _saved_frac;
    int64_t _saved_min_bytes;
    int32_t _saved_high_pct;
    int32_t _saved_low_pct;
    int64_t _saved_soft_mem_limit;
    int64_t _saved_memory_growth;
};

TEST_F(BuildIndexMemoryLimitTest, TaskCounting) {
    EXPECT_EQ(_engine->running_build_index_tasks(), 1);

    _engine->notify_build_index_task_begin();
    EXPECT_EQ(_engine->running_build_index_tasks(), 1);

    _engine->notify_build_index_task_begin();
    EXPECT_EQ(_engine->running_build_index_tasks(), 2);

    _engine->notify_build_index_task_end();
    EXPECT_EQ(_engine->running_build_index_tasks(), 1);

    _engine->notify_build_index_task_end();
    EXPECT_EQ(_engine->running_build_index_tasks(), 1);
}

TEST_F(BuildIndexMemoryLimitTest, SingleTaskGetsFullBudget) {
    MemInfo::_s_soft_mem_limit.store(100LL * 1024 * 1024 * 1024);
    config::build_index_mem_limit_frac = 0.6;
    config::build_index_min_memory_per_task_bytes = 2LL * 1024 * 1024 * 1024;
    config::build_index_memory_high_watermark_pct = 100;
    config::build_index_memory_low_watermark_pct = 100;

    int64_t limit = _engine->memory_limitation_bytes_for_build_index();
    EXPECT_EQ(limit, 60LL * 1024 * 1024 * 1024);
}

TEST_F(BuildIndexMemoryLimitTest, RemainingMemoryCap) {
    int64_t soft_limit = 100LL * 1024 * 1024 * 1024;
    MemInfo::_s_soft_mem_limit.store(soft_limit);
    config::build_index_mem_limit_frac = 0.6;
    config::build_index_min_memory_per_task_bytes = 1;
    config::build_index_memory_high_watermark_pct = 100;
    config::build_index_memory_low_watermark_pct = 100;

    GlobalMemoryArbitrator::refresh_interval_memory_growth.store(
            soft_limit * 80 / 100);

    int64_t limit = _engine->memory_limitation_bytes_for_build_index();
    int64_t remaining = soft_limit - soft_limit * 80 / 100;
    EXPECT_LE(limit, remaining);
}

TEST_F(BuildIndexMemoryLimitTest, MinBytesFloor) {
    int64_t soft_limit = 100LL * 1024 * 1024 * 1024;
    MemInfo::_s_soft_mem_limit.store(soft_limit);
    config::build_index_mem_limit_frac = 0.6;
    config::build_index_min_memory_per_task_bytes = 2LL * 1024 * 1024 * 1024;
    config::build_index_memory_high_watermark_pct = 100;
    config::build_index_memory_low_watermark_pct = 100;

    GlobalMemoryArbitrator::refresh_interval_memory_growth.store(
            soft_limit * 99 / 100);

    int64_t limit = _engine->memory_limitation_bytes_for_build_index();
    EXPECT_EQ(limit, 2LL * 1024 * 1024 * 1024);
}

TEST_F(BuildIndexMemoryLimitTest, HighWatermarkThrottling) {
    int64_t soft_limit = 100LL * 1024 * 1024 * 1024;
    MemInfo::_s_soft_mem_limit.store(soft_limit);
    config::build_index_mem_limit_frac = 0.6;
    config::build_index_min_memory_per_task_bytes = 1LL * 1024 * 1024 * 1024;
    config::build_index_memory_high_watermark_pct = 85;
    config::build_index_memory_low_watermark_pct = 75;

    GlobalMemoryArbitrator::refresh_interval_memory_growth.store(
            soft_limit * 90 / 100);

    int64_t limit = _engine->memory_limitation_bytes_for_build_index();
    EXPECT_EQ(limit, 1LL * 1024 * 1024 * 1024);
}

TEST_F(BuildIndexMemoryLimitTest, LowWatermarkThrottling) {
    int64_t soft_limit = 100LL * 1024 * 1024 * 1024;
    MemInfo::_s_soft_mem_limit.store(soft_limit);
    config::build_index_mem_limit_frac = 0.6;
    config::build_index_min_memory_per_task_bytes = 1;
    config::build_index_memory_high_watermark_pct = 90;
    config::build_index_memory_low_watermark_pct = 75;

    GlobalMemoryArbitrator::refresh_interval_memory_growth.store(
            soft_limit * 80 / 100);

    int64_t remaining = soft_limit - soft_limit * 80 / 100;
    int64_t limit = _engine->memory_limitation_bytes_for_build_index();
    EXPECT_EQ(limit, remaining / 2);
}

TEST_F(BuildIndexMemoryLimitTest, ConfigIsMutable) {
    MemInfo::_s_soft_mem_limit.store(100LL * 1024 * 1024 * 1024);
    config::build_index_min_memory_per_task_bytes = 2LL * 1024 * 1024 * 1024;
    config::build_index_memory_high_watermark_pct = 100;
    config::build_index_memory_low_watermark_pct = 100;

    config::build_index_mem_limit_frac = 0.3;
    int64_t limit1 = _engine->memory_limitation_bytes_for_build_index();
    EXPECT_EQ(limit1, 30LL * 1024 * 1024 * 1024);

    config::build_index_mem_limit_frac = 0.5;
    int64_t limit2 = _engine->memory_limitation_bytes_for_build_index();
    EXPECT_EQ(limit2, 50LL * 1024 * 1024 * 1024);
}

} // namespace doris
