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

#include "storage/data_dir_sweep_worker.h"

#include <gtest/gtest.h>

#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

#include "io/fs/local_file_system.h"
#include "storage/data_dir.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "util/uid_util.h"

namespace doris {

class DataDirSweepWorkerTest : public testing::Test {
public:
    void SetUp() override {
        _path = "./ut_dir/data_dir_sweep_worker_test";
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_path).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_path).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_path + "/meta").ok());

        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        _engine = std::make_unique<StorageEngine>(options);
        _data_dir = std::make_unique<DataDir>(*_engine, _path, 1000000000);
        ASSERT_TRUE(_data_dir->init().ok());
    }

    void TearDown() override {
        _data_dir.reset();
        _engine.reset();
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_path).ok());
    }

    TabletSharedPtr create_mock_tablet() {
        auto owner = std::make_shared<int>(1);
        return TabletSharedPtr(owner, reinterpret_cast<Tablet*>(owner.get()));
    }

protected:
    std::string _path;
    std::unique_ptr<StorageEngine> _engine;
    std::unique_ptr<DataDir> _data_dir;
};

TEST_F(DataDirSweepWorkerTest, StopAcceptingDrainsSubmittedJob) {
    DataDirSweepWorker worker(*_engine, _data_dir.get());
    ASSERT_TRUE(worker.start().ok());

    constexpr uint64_t sweep_epoch = 7;
    auto context = std::make_shared<DataDirSweepPhaseContext>(sweep_epoch, 1);
    DataDirSweepJob job;
    job.sweep_epoch = sweep_epoch;
    job.type = DataDirSweepJobType::TRASH_CAPACITY_REFRESH;
    job.data_dir = _data_dir.get();
    job.payload = TrashCapacityRefreshPayload {};
    job.context = context;
    job.result_index = 0;
    ASSERT_TRUE(worker.submit(std::move(job)).ok());

    worker.stop_accepting_jobs();
    DataDirSweepJob rejected_job;
    rejected_job.sweep_epoch = sweep_epoch;
    rejected_job.type = DataDirSweepJobType::TRASH_CAPACITY_REFRESH;
    rejected_job.data_dir = _data_dir.get();
    rejected_job.payload = TrashCapacityRefreshPayload {};
    EXPECT_FALSE(worker.submit(std::move(rejected_job)).ok());

    worker.drain_and_stop();
    worker.join();
    context->completion_latch.wait();

    EXPECT_EQ(context->results[0].sweep_epoch, sweep_epoch);
    EXPECT_EQ(context->results[0].type, DataDirSweepJobType::TRASH_CAPACITY_REFRESH);
    EXPECT_TRUE(context->results[0].status.ok()) << context->results[0].status;
}

TEST_F(DataDirSweepWorkerTest, ShutdownJobContainsExceptionAndReturnsUnresolvedTablets) {
    DataDirSweepWorker worker(*_engine, _data_dir.get());
    ASSERT_TRUE(worker.start().ok());

    std::vector<TabletSharedPtr> tablets {create_mock_tablet(), create_mock_tablet(),
                                          create_mock_tablet()};
    std::vector<Tablet*> tablet_ptrs;
    for (const auto& tablet : tablets) {
        tablet_ptrs.push_back(tablet.get());
    }

    constexpr uint64_t sweep_epoch = 9;
    auto context = std::make_shared<DataDirSweepPhaseContext>(sweep_epoch, 1);
    int calls = 0;
    DataDirSweepJob job;
    job.sweep_epoch = sweep_epoch;
    job.type = DataDirSweepJobType::SHUTDOWN_TABLET_MOVE;
    job.data_dir = _data_dir.get();
    job.payload = ShutdownTabletMovePayload {
            .tablets = std::move(tablets), .move_tablet = [&calls](const TabletSharedPtr&) {
                if (++calls == 2) {
                    throw std::runtime_error("injected move failure");
                }
                return true;
            }};
    job.context = context;
    job.result_index = 0;
    ASSERT_TRUE(worker.submit(std::move(job)).ok());

    worker.drain_and_stop();
    worker.join();
    context->completion_latch.wait();

    const auto& result = context->results[0];
    EXPECT_FALSE(result.status.ok());
    EXPECT_EQ(result.shutdown_resolved, 1);
    EXPECT_EQ(result.shutdown_failed, 2);
    ASSERT_EQ(result.failed_tablets.size(), 2);
    EXPECT_EQ(result.failed_tablets[0].get(), tablet_ptrs[1]);
    EXPECT_EQ(result.failed_tablets[1].get(), tablet_ptrs[2]);
}

} // namespace doris
