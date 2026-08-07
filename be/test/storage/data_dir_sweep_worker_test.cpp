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

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "common/config.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/remote_file_system.h"
#include "storage/data_dir.h"
#include "storage/olap_define.h"
#include "storage/olap_meta.h"
#include "storage/options.h"
#include "storage/storage_engine.h"
#include "storage/storage_policy.h"
#include "util/countdown_latch.h"
#include "util/defer_op.h"
#include "util/uid_util.h"

namespace doris {

namespace {

class GcRemoteFileSystem final : public io::RemoteFileSystem {
public:
    explicit GcRemoteFileSystem(std::string id)
            : RemoteFileSystem(io::Path(), std::move(id), io::FileSystemType::S3) {}

    void set_batch_delete_statuses(std::vector<Status> statuses) {
        _batch_delete_statuses = std::move(statuses);
        _batch_delete_calls = 0;
    }

    size_t batch_delete_calls() const { return _batch_delete_calls; }
    const std::vector<std::string>& operations() const { return _operations; }

protected:
    Status create_file_impl(const io::Path&, io::FileWriterPtr*,
                            const io::FileWriterOptions*) override {
        return Status::NotSupported("not needed by remote GC test");
    }

    Status create_directory_impl(const io::Path&, bool) override {
        return Status::NotSupported("not needed by remote GC test");
    }

    Status delete_file_impl(const io::Path&) override {
        return Status::NotSupported("not needed by remote GC test");
    }

    Status batch_delete_impl(const std::vector<io::Path>&) override {
        _operations.emplace_back("rowset");
        const size_t call_index = _batch_delete_calls++;
        if (call_index < _batch_delete_statuses.size()) {
            return _batch_delete_statuses[call_index];
        }
        return Status::OK();
    }

    Status delete_directory_impl(const io::Path&) override {
        _operations.emplace_back("tablet");
        return Status::OK();
    }

    Status exists_impl(const io::Path&, bool* res) const override {
        *res = false;
        return Status::OK();
    }

    Status file_size_impl(const io::Path&, int64_t* file_size) const override {
        *file_size = 0;
        return Status::OK();
    }

    Status list_impl(const io::Path&, bool, std::vector<io::FileInfo>*, bool* exists) override {
        *exists = false;
        return Status::OK();
    }

    Status rename_impl(const io::Path&, const io::Path&) override {
        return Status::NotSupported("not needed by remote GC test");
    }

    Status upload_impl(const io::Path&, const io::Path&) override {
        return Status::NotSupported("not needed by remote GC test");
    }

    Status batch_upload_impl(const std::vector<io::Path>&, const std::vector<io::Path>&) override {
        return Status::NotSupported("not needed by remote GC test");
    }

    Status download_impl(const io::Path&, const io::Path&) override {
        return Status::NotSupported("not needed by remote GC test");
    }

    Status open_file_internal(const io::Path&, io::FileReaderSPtr*,
                              const io::FileReaderOptions&) override {
        return Status::NotSupported("not needed by remote GC test");
    }

private:
    std::vector<Status> _batch_delete_statuses;
    size_t _batch_delete_calls = 0;
    std::vector<std::string> _operations;
};

} // namespace

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
        _enable_worker_base = config::enable_data_dir_sweep_worker;
    }

    void TearDown() override {
        config::enable_data_dir_sweep_worker = _enable_worker_base;
        _data_dir.reset();
        _engine.reset();
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_path).ok());
        for (const auto& extra_path : _extra_paths) {
            ASSERT_TRUE(io::global_local_filesystem()->delete_directory(extra_path).ok());
        }
    }

    TabletSharedPtr create_mock_tablet() {
        auto owner = std::make_shared<int>(1);
        return TabletSharedPtr(owner, reinterpret_cast<Tablet*>(owner.get()));
    }

    std::unique_ptr<DataDir> create_extra_data_dir(const std::string& suffix) {
        auto path = _path + "_" + suffix;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(path).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(path).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(path + "/meta").ok());
        auto data_dir = std::make_unique<DataDir>(*_engine, path, 1000000000);
        EXPECT_TRUE(data_dir->init().ok());
        _extra_paths.push_back(path);
        return data_dir;
    }

    DataDirSweepJob make_shutdown_job(uint64_t sweep_epoch, DataDir* data_dir,
                                      const std::shared_ptr<DataDirSweepPhaseContext>& context,
                                      size_t result_index, MoveTabletCallback move_tablet) {
        DataDirSweepJob job;
        job.sweep_epoch = sweep_epoch;
        job.type = DataDirSweepJobType::SHUTDOWN_TABLET_MOVE;
        job.data_dir = data_dir;
        job.payload = ShutdownTabletMovePayload {.tablets = {create_mock_tablet()},
                                                 .move_tablet = std::move(move_tablet)};
        job.context = context;
        job.result_index = result_index;
        return job;
    }

protected:
    std::string _path;
    std::unique_ptr<StorageEngine> _engine;
    std::unique_ptr<DataDir> _data_dir;
    std::vector<std::string> _extra_paths;
    bool _enable_worker_base = false;
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
    auto context = std::make_shared<DataDirSweepPhaseContext>(sweep_epoch, 2);
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

    DataDirSweepJob followup_job;
    followup_job.sweep_epoch = sweep_epoch;
    followup_job.type = DataDirSweepJobType::TRASH_CAPACITY_REFRESH;
    followup_job.data_dir = _data_dir.get();
    followup_job.payload = TrashCapacityRefreshPayload {};
    followup_job.context = context;
    followup_job.result_index = 1;
    ASSERT_TRUE(worker.submit(std::move(followup_job)).ok());

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
    EXPECT_TRUE(context->results[1].status.ok()) << context->results[1].status;
    EXPECT_EQ(context->results[1].type, DataDirSweepJobType::TRASH_CAPACITY_REFRESH);
}

TEST_F(DataDirSweepWorkerTest, DifferentDataDirsExecuteJobsConcurrently) {
    auto second_data_dir = create_extra_data_dir("parallel");
    constexpr uint64_t sweep_epoch = 11;
    auto context = std::make_shared<DataDirSweepPhaseContext>(sweep_epoch, 2);
    CountDownLatch jobs_entered(2);
    CountDownLatch release_jobs(1);
    DataDirSweepWorker first_worker(*_engine, _data_dir.get());
    DataDirSweepWorker second_worker(*_engine, second_data_dir.get());
    bool first_worker_running = false;
    bool second_worker_running = false;
    Defer stop_workers {[&] {
        release_jobs.count_down();
        if (first_worker_running) {
            first_worker.drain_and_stop();
            first_worker.join();
        }
        if (second_worker_running) {
            second_worker.drain_and_stop();
            second_worker.join();
        }
    }};
    ASSERT_TRUE(first_worker.start().ok());
    first_worker_running = true;
    ASSERT_TRUE(second_worker.start().ok());
    second_worker_running = true;

    auto move_tablet = [&](const TabletSharedPtr&) {
        jobs_entered.count_down();
        release_jobs.wait();
        return true;
    };

    ASSERT_TRUE(first_worker
                        .submit(make_shutdown_job(sweep_epoch, _data_dir.get(), context, 0,
                                                  move_tablet))
                        .ok());
    ASSERT_TRUE(second_worker
                        .submit(make_shutdown_job(sweep_epoch, second_data_dir.get(), context, 1,
                                                  move_tablet))
                        .ok());

    EXPECT_TRUE(jobs_entered.wait_for(std::chrono::seconds(5)));
    release_jobs.count_down();
    first_worker.drain_and_stop();
    second_worker.drain_and_stop();
    first_worker.join();
    second_worker.join();
    first_worker_running = false;
    second_worker_running = false;
    context->completion_latch.wait();

    EXPECT_EQ(context->results[0].shutdown_resolved, 1);
    EXPECT_EQ(context->results[1].shutdown_resolved, 1);
}

TEST_F(DataDirSweepWorkerTest, SameDataDirExecutesJobsInFifoOrder) {
    constexpr uint64_t sweep_epoch = 13;
    auto context = std::make_shared<DataDirSweepPhaseContext>(sweep_epoch, 2);
    CountDownLatch first_job_entered(1);
    CountDownLatch release_first_job(1);
    CountDownLatch second_job_entered(1);
    DataDirSweepWorker worker(*_engine, _data_dir.get());
    bool worker_running = false;
    Defer stop_worker {[&] {
        release_first_job.count_down();
        if (worker_running) {
            worker.drain_and_stop();
            worker.join();
        }
    }};
    ASSERT_TRUE(worker.start().ok());
    worker_running = true;

    std::vector<int> execution_order;
    std::mutex execution_order_lock;

    auto first_job = make_shutdown_job(sweep_epoch, _data_dir.get(), context, 0,
                                       [&](const TabletSharedPtr&) {
                                           {
                                               std::lock_guard lock(execution_order_lock);
                                               execution_order.push_back(1);
                                           }
                                           first_job_entered.count_down();
                                           release_first_job.wait();
                                           return true;
                                       });
    auto second_job = make_shutdown_job(sweep_epoch, _data_dir.get(), context, 1,
                                        [&](const TabletSharedPtr&) {
                                            {
                                                std::lock_guard lock(execution_order_lock);
                                                execution_order.push_back(2);
                                            }
                                            second_job_entered.count_down();
                                            return true;
                                        });

    ASSERT_TRUE(worker.submit(std::move(first_job)).ok());
    ASSERT_TRUE(worker.submit(std::move(second_job)).ok());
    EXPECT_TRUE(first_job_entered.wait_for(std::chrono::seconds(5)));
    EXPECT_FALSE(second_job_entered.wait_for(std::chrono::milliseconds(100)));
    release_first_job.count_down();
    EXPECT_TRUE(second_job_entered.wait_for(std::chrono::seconds(5)));
    worker.drain_and_stop();
    worker.join();
    worker_running = false;
    context->completion_latch.wait();

    EXPECT_EQ(execution_order, std::vector<int>({1, 2}));
}

TEST_F(DataDirSweepWorkerTest, DispatchReturnsShutdownTabletsWhenSubmitFails) {
    config::enable_data_dir_sweep_worker = true;
    auto worker = std::make_unique<DataDirSweepWorker>(*_engine, _data_dir.get());
    ASSERT_TRUE(worker->start().ok());
    worker->stop_accepting_jobs();
    _engine->_data_dir_sweep_workers.emplace(_data_dir.get(), std::move(worker));
    const int64_t completed_jobs = _data_dir->disks_sweep_worker_completed_jobs->value();
    const int64_t failed_jobs = _data_dir->disks_sweep_worker_failed_jobs->value();
    _data_dir->_record_sweep_job_start(DataDirSweepJobType::REMOTE_GC);

    DataDirSweepJob job;
    job.sweep_epoch = 15;
    job.type = DataDirSweepJobType::SHUTDOWN_TABLET_MOVE;
    job.data_dir = _data_dir.get();
    job.payload =
            ShutdownTabletMovePayload {.tablets = {create_mock_tablet(), create_mock_tablet()},
                                       .move_tablet = [](const TabletSharedPtr&) { return true; }};
    auto results = _engine->_dispatch_data_dir_sweep_jobs(15, {std::move(job)});
    _engine->_stop_data_dir_sweep_workers();

    ASSERT_EQ(results.size(), 1);
    EXPECT_FALSE(results[0].status.ok());
    EXPECT_EQ(results[0].shutdown_resolved, 0);
    EXPECT_EQ(results[0].shutdown_failed, 2);
    EXPECT_EQ(results[0].failed_tablets.size(), 2);
    EXPECT_EQ(_data_dir->disks_sweep_worker_completed_jobs->value(), completed_jobs + 1);
    EXPECT_EQ(_data_dir->disks_sweep_worker_failed_jobs->value(), failed_jobs + 1);
    EXPECT_EQ(_data_dir->disks_sweep_worker_current_job->value(),
              static_cast<int64_t>(DataDirSweepJobType::REMOTE_GC));
    _data_dir->disks_sweep_worker_current_job->set_value(-1);
}

TEST_F(DataDirSweepWorkerTest, DispatchUsesSynchronousFallbackWhenWorkerDisabled) {
    config::enable_data_dir_sweep_worker = false;
    DataDirSweepJob job;
    job.sweep_epoch = 16;
    job.type = DataDirSweepJobType::TRASH_CAPACITY_REFRESH;
    job.data_dir = _data_dir.get();
    job.payload = TrashCapacityRefreshPayload {};

    auto results = _engine->_dispatch_data_dir_sweep_jobs(16, {std::move(job)});

    ASSERT_EQ(results.size(), 1);
    EXPECT_TRUE(results[0].status.ok()) << results[0].status;
    EXPECT_EQ(results[0].sweep_epoch, 16);
    EXPECT_EQ(results[0].type, DataDirSweepJobType::TRASH_CAPACITY_REFRESH);
    EXPECT_TRUE(_engine->_data_dir_sweep_workers.empty());
}

TEST_F(DataDirSweepWorkerTest, RemoteGcRunsRowsetBeforeTablet) {
    constexpr int64_t resource_id = 99001;
    auto remote_fs = std::make_shared<GcRemoteFileSystem>(std::to_string(resource_id));
    put_storage_resource(resource_id, StorageResource {remote_fs}, 1);
    Defer delete_resource {[&] { delete_storage_resource(resource_id); }};

    RemoteRowsetGcPB rowset_gc;
    rowset_gc.set_resource_id(std::to_string(resource_id));
    rowset_gc.set_tablet_id(100);
    rowset_gc.set_num_segments(1);
    ASSERT_TRUE(_data_dir->get_meta()
                        ->put(META_COLUMN_FAMILY_INDEX, REMOTE_ROWSET_GC_PREFIX + "rowset",
                              rowset_gc.SerializeAsString())
                        .ok());
    RemoteTabletGcPB tablet_gc;
    tablet_gc.add_resource_ids(std::to_string(resource_id));
    ASSERT_TRUE(_data_dir->get_meta()
                        ->put(META_COLUMN_FAMILY_INDEX, REMOTE_TABLET_GC_PREFIX + "100",
                              tablet_gc.SerializeAsString())
                        .ok());

    DataDirSweepJob job;
    job.sweep_epoch = 17;
    job.type = DataDirSweepJobType::REMOTE_GC;
    job.data_dir = _data_dir.get();
    job.payload = RemoteGcPayload {};
    auto result = _engine->_execute_data_dir_sweep_job(job);

    EXPECT_TRUE(result.status.ok()) << result.status;
    EXPECT_EQ(result.remote_rowset_gc_scanned, 1);
    EXPECT_EQ(result.remote_rowset_gc_backlog, 0);
    EXPECT_EQ(result.remote_tablet_gc_scanned, 1);
    EXPECT_EQ(result.remote_tablet_gc_backlog, 0);
    EXPECT_EQ(remote_fs->operations(), std::vector<std::string>({"rowset", "tablet"}));
}

TEST_F(DataDirSweepWorkerTest, RemoteGcMissingResourceRetainsBacklogWithoutFailingJob) {
    constexpr uint64_t sweep_epoch = 18;
    const std::string missing_resource_id = "missing-data-dir-sweep-resource-99003";

    RemoteRowsetGcPB rowset_gc;
    rowset_gc.set_resource_id(missing_resource_id);
    rowset_gc.set_tablet_id(101);
    rowset_gc.set_num_segments(1);
    ASSERT_TRUE(_data_dir->get_meta()
                        ->put(META_COLUMN_FAMILY_INDEX, REMOTE_ROWSET_GC_PREFIX + "rowset",
                              rowset_gc.SerializeAsString())
                        .ok());
    RemoteTabletGcPB tablet_gc;
    tablet_gc.add_resource_ids(missing_resource_id);
    ASSERT_TRUE(_data_dir->get_meta()
                        ->put(META_COLUMN_FAMILY_INDEX, REMOTE_TABLET_GC_PREFIX + "101",
                              tablet_gc.SerializeAsString())
                        .ok());

    const int64_t failed_jobs = _data_dir->disks_sweep_worker_failed_jobs->value();
    DataDirSweepJob job;
    job.sweep_epoch = sweep_epoch;
    job.type = DataDirSweepJobType::REMOTE_GC;
    job.data_dir = _data_dir.get();
    job.payload = RemoteGcPayload {};
    auto result = _engine->_execute_data_dir_sweep_job(job);

    EXPECT_TRUE(result.status.ok()) << result.status;
    EXPECT_EQ(result.remote_rowset_gc_scanned, 1);
    EXPECT_EQ(result.remote_rowset_gc_backlog, 1);
    EXPECT_EQ(result.remote_tablet_gc_scanned, 1);
    EXPECT_EQ(result.remote_tablet_gc_backlog, 1);
    EXPECT_EQ(_data_dir->disks_sweep_worker_failed_jobs->value(), failed_jobs);
}

TEST_F(DataDirSweepWorkerTest, RemoteGcMalformedPbStillFailsJob) {
    constexpr uint64_t sweep_epoch = 19;
    const std::string malformed_pb(1, '\xff');
    ASSERT_TRUE(_data_dir->get_meta()
                        ->put(META_COLUMN_FAMILY_INDEX, REMOTE_ROWSET_GC_PREFIX + "rowset",
                              malformed_pb)
                        .ok());
    ASSERT_TRUE(
            _data_dir->get_meta()
                    ->put(META_COLUMN_FAMILY_INDEX, REMOTE_TABLET_GC_PREFIX + "102", malformed_pb)
                    .ok());

    const int64_t failed_jobs = _data_dir->disks_sweep_worker_failed_jobs->value();
    DataDirSweepJob job;
    job.sweep_epoch = sweep_epoch;
    job.type = DataDirSweepJobType::REMOTE_GC;
    job.data_dir = _data_dir.get();
    job.payload = RemoteGcPayload {};
    auto result = _engine->_execute_data_dir_sweep_job(job);

    EXPECT_FALSE(result.status.ok());
    EXPECT_EQ(result.remote_rowset_gc_scanned, 1);
    EXPECT_EQ(result.remote_rowset_gc_backlog, 0);
    EXPECT_EQ(result.remote_tablet_gc_scanned, 1);
    EXPECT_EQ(result.remote_tablet_gc_backlog, 0);
    EXPECT_EQ(_data_dir->disks_sweep_worker_failed_jobs->value(), failed_jobs + 1);
}

TEST_F(DataDirSweepWorkerTest, RemoteRowsetGcRetainsFailedMarkerAndContinues) {
    constexpr int64_t resource_id = 99002;
    auto remote_fs = std::make_shared<GcRemoteFileSystem>(std::to_string(resource_id));
    remote_fs->set_batch_delete_statuses(
            {Status::IOError("injected remote delete failure"), Status::OK()});
    put_storage_resource(resource_id, StorageResource {remote_fs}, 1);
    Defer delete_resource {[&] { delete_storage_resource(resource_id); }};

    for (int i = 0; i < 2; ++i) {
        RemoteRowsetGcPB rowset_gc;
        rowset_gc.set_resource_id(std::to_string(resource_id));
        rowset_gc.set_tablet_id(200 + i);
        rowset_gc.set_num_segments(1);
        ASSERT_TRUE(_data_dir->get_meta()
                            ->put(META_COLUMN_FAMILY_INDEX,
                                  REMOTE_ROWSET_GC_PREFIX + std::to_string(i),
                                  rowset_gc.SerializeAsString())
                            .ok());
    }

    RemoteGcStats first_stats;
    auto first_status = _data_dir->perform_remote_rowset_gc(&first_stats);
    EXPECT_FALSE(first_status.ok());
    EXPECT_EQ(first_stats.scanned, 2);
    EXPECT_EQ(first_stats.backlog, 1);
    EXPECT_EQ(remote_fs->batch_delete_calls(), 2);

    remote_fs->set_batch_delete_statuses({});
    RemoteGcStats retry_stats;
    auto retry_status = _data_dir->perform_remote_rowset_gc(&retry_stats);
    EXPECT_TRUE(retry_status.ok()) << retry_status;
    EXPECT_EQ(retry_stats.scanned, 1);
    EXPECT_EQ(retry_stats.backlog, 0);
}

TEST_F(DataDirSweepWorkerTest, TrashCapacityRefreshCountsNestedFilesAndMissingTrash) {
    const std::string trash_path = _path + "/" + TRASH_PREFIX;
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(trash_path + "/nested").ok());
    io::FileWriterPtr first_writer;
    ASSERT_TRUE(
            io::global_local_filesystem()->create_file(trash_path + "/first", &first_writer).ok());
    ASSERT_TRUE(first_writer->append("abc").ok());
    ASSERT_TRUE(first_writer->close().ok());
    io::FileWriterPtr second_writer;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(trash_path + "/nested/second", &second_writer)
                        .ok());
    ASSERT_TRUE(second_writer->append("12345").ok());
    ASSERT_TRUE(second_writer->close().ok());

    ASSERT_TRUE(_data_dir->update_trash_capacity().ok());
    EXPECT_EQ(_data_dir->get_dir_info().trash_used_capacity, 8);

    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(trash_path).ok());
    ASSERT_TRUE(_data_dir->update_trash_capacity().ok());
    EXPECT_EQ(_data_dir->get_dir_info().trash_used_capacity, 0);
}

} // namespace doris
