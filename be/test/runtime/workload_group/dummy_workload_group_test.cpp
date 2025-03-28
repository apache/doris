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

#include <gen_cpp/BackendService_types.h>
#include <gtest/gtest.h>

#include "io/fs/local_file_reader.h"
#include "olap/memtable_flush_executor.h"
#include "olap/storage_engine.h"
#include "pipeline/task_scheduler.h"
#include "runtime/exec_env.h"
#include "runtime/workload_group/workload_group_manager.h"
#include "runtime/workload_group/workload_group_metrics.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "vec/exec/scan/scanner_scheduler.h"

namespace doris {

class DummyWorkloadGroupTest : public testing::Test {
public:
    void SetUp() override {
        ExecEnv::GetInstance()->_without_group_task_scheduler =
                new pipeline::TaskScheduler(1, "", nullptr);
        ExecEnv::GetInstance()->_scanner_scheduler = new vectorized::ScannerScheduler();
        ExecEnv::GetInstance()->_scanner_scheduler->_local_scan_thread_pool =
                std::make_unique<vectorized::SimplifiedScanScheduler>("", nullptr);
        ExecEnv::GetInstance()->_scanner_scheduler->_remote_scan_thread_pool =
                std::make_unique<vectorized::SimplifiedScanScheduler>("", nullptr);

        DataDirInfo data_dir_info;
        data_dir_info.metric_name = "io_metric";
        data_dir_info.path = "local_data_dir";
        io::BeConfDataDirReader::be_config_data_dir_list.push_back(data_dir_info);

        EngineOptions eop;
        ExecEnv::GetInstance()->_storage_engine = std::make_unique<StorageEngine>(eop);
        ExecEnv::GetInstance()->_storage_engine->_memtable_flush_executor =
                std::make_unique<MemTableFlushExecutor>();
        ExecEnv::GetInstance()->_storage_engine->_memtable_flush_executor->init(1);
    }
    void TearDown() override { delete ExecEnv::GetInstance()->_without_group_task_scheduler; }

    DummyWorkloadGroupTest() = default;
    ~DummyWorkloadGroupTest() override = default;
};

TEST_F(DummyWorkloadGroupTest, dummy_wg_basic_test) {
    std::unique_ptr<WorkloadGroupMgr> wgmgr_ptr = std::make_unique<WorkloadGroupMgr>();
    WorkloadGroup* dummy_wg_ptr = wgmgr_ptr->dummy_workload_group().get();
    ASSERT_TRUE(dummy_wg_ptr != nullptr);
    ASSERT_TRUE(dummy_wg_ptr->get_cgroup_cpu_ctl_wptr().lock().get() == nullptr);
    ASSERT_TRUE(dummy_wg_ptr->get_memtable_flush_pool() ==
                ExecEnv::GetInstance()->_storage_engine->_memtable_flush_executor->flush_pool());

    // test metrics update
    dummy_wg_ptr->update_cpu_time(1024);
    dummy_wg_ptr->update_local_scan_io("abc", 2048);
    dummy_wg_ptr->update_remote_scan_io(4096);
    ASSERT_TRUE(dummy_wg_ptr->_wg_metrics->_cpu_time_nanos == 1024);
    ASSERT_TRUE(dummy_wg_ptr->_wg_metrics->workload_group_total_local_scan_bytes->value() == 2048);
    ASSERT_TRUE(dummy_wg_ptr->_wg_metrics->workload_group_remote_scan_bytes->value() == 4096);

    // get_query_scheduler
    doris::pipeline::TaskScheduler* t1 = nullptr;
    vectorized::SimplifiedScanScheduler* t2 = nullptr;
    vectorized::SimplifiedScanScheduler* t3 = nullptr;
    dummy_wg_ptr->get_query_scheduler(&t1, &t2, &t3);
    ASSERT_TRUE(t1 == ExecEnv::GetInstance()->_without_group_task_scheduler);
    ASSERT_TRUE(t2 == ExecEnv::GetInstance()->_scanner_scheduler->_local_scan_thread_pool.get());
    ASSERT_TRUE(t3 == ExecEnv::GetInstance()->_scanner_scheduler->_remote_scan_thread_pool.get());

    // check io throttle
    ASSERT_TRUE(
            dummy_wg_ptr->_scan_io_throttle_map.at("local_data_dir")->_io_bytes_per_second_limit ==
            -1);
    ASSERT_TRUE(dummy_wg_ptr->_remote_scan_io_throttle->_io_bytes_per_second_limit == -1);

    // add query
    TUniqueId q1;
    q1.hi = 1;
    q1.lo = 2;
    ASSERT_TRUE(dummy_wg_ptr->add_query(q1, nullptr) == Status::OK());
    ASSERT_TRUE(dummy_wg_ptr->queries().size() == 1);
}

}; // namespace doris
