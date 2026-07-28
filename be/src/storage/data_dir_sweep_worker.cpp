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

#include <fmt/format.h>

#include <exception>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "storage/data_dir.h"
#include "storage/storage_engine.h"
#include "util/thread.h"

namespace doris {

const char* data_dir_sweep_job_type_name(DataDirSweepJobType type) {
    switch (type) {
    case DataDirSweepJobType::SNAPSHOT_SWEEP:
        return "snapshot_sweep";
    case DataDirSweepJobType::TRASH_SWEEP:
        return "trash_sweep";
    case DataDirSweepJobType::SHUTDOWN_TABLET_MOVE:
        return "shutdown_tablet_move";
    case DataDirSweepJobType::REMOTE_GC:
        return "remote_gc";
    case DataDirSweepJobType::TRASH_CAPACITY_REFRESH:
        return "trash_capacity_refresh";
    }
    return "unknown";
}

DataDirSweepWorker::DataDirSweepWorker(StorageEngine& engine, DataDir* data_dir)
        : _engine(engine), _data_dir(data_dir) {
    DORIS_CHECK(_data_dir != nullptr);
}

DataDirSweepWorker::~DataDirSweepWorker() {
    DORIS_CHECK(_thread == nullptr);
}

Status DataDirSweepWorker::start() {
    DORIS_CHECK(_thread == nullptr);
    RETURN_IF_ERROR(Thread::create(
            "DataDirSweepWorker", fmt::format("sweep_{:x}", _data_dir->path_hash()),
            [this]() { _run(); }, &_thread));
    _data_dir->_set_sweep_worker_running(true);
    return Status::OK();
}

Status DataDirSweepWorker::submit(DataDirSweepJob job) {
    {
        std::lock_guard lock(_lock);
        if (!_accepting_jobs) {
            return Status::Error<ErrorCode::SERVICE_UNAVAILABLE>(
                    "DataDir sweep worker is stopping. path={}", _data_dir->path());
        }
        if (_jobs.size() >= kMaxQueuedJobs) {
            return Status::InternalError(
                    "DataDir sweep worker queue invariant violated. path={}, "
                    "queue_size={}, limit={}",
                    _data_dir->path(), _jobs.size(), kMaxQueuedJobs);
        }
        _jobs.push_back(std::move(job));
        _data_dir->_set_sweep_worker_queue_depth(static_cast<int64_t>(_jobs.size()));
    }
    _cv.notify_one();
    return Status::OK();
}

void DataDirSweepWorker::stop_accepting_jobs() {
    std::lock_guard lock(_lock);
    _accepting_jobs = false;
}

void DataDirSweepWorker::drain_and_stop() {
    {
        std::lock_guard lock(_lock);
        _accepting_jobs = false;
        _stop_requested = true;
    }
    _cv.notify_all();
}

void DataDirSweepWorker::join() {
    DORIS_CHECK(_thread != nullptr);
    _thread->join();
    _thread.reset();
    _data_dir->_set_sweep_worker_running(false);
}

DataDirSweepJobResult DataDirSweepWorker::_execute(DataDirSweepJob& job) {
    auto failed_result = [&job](Status status) {
        DataDirSweepJobResult result;
        result.sweep_epoch = job.sweep_epoch;
        result.type = job.type;
        result.data_dir = job.data_dir;
        result.status = std::move(status);
        if (auto* payload = std::get_if<ShutdownTabletMovePayload>(&job.payload);
            payload != nullptr) {
            result.shutdown_failed = static_cast<int64_t>(payload->tablets.size());
            result.failed_tablets = std::move(payload->tablets);
        }
        return result;
    };

    try {
        return _engine._execute_data_dir_sweep_job(job);
    } catch (const Exception& e) {
        return failed_result(e.to_status());
    } catch (const std::exception& e) {
        return failed_result(Status::InternalError(
                "DataDir sweep job raised an exception. path={}, "
                "job_type={}, error={}",
                _data_dir->path(), data_dir_sweep_job_type_name(job.type), e.what()));
    } catch (...) {
        return failed_result(Status::InternalError(
                "DataDir sweep job raised an unknown exception. path={}, job_type={}",
                _data_dir->path(), data_dir_sweep_job_type_name(job.type)));
    }
}

void DataDirSweepWorker::_run() {
    for (;;) {
        DataDirSweepJob job;
        {
            std::unique_lock lock(_lock);
            _cv.wait(lock, [this]() { return _stop_requested || !_jobs.empty(); });
            if (_jobs.empty()) {
                DORIS_CHECK(_stop_requested);
                return;
            }
            job = std::move(_jobs.front());
            _jobs.pop_front();
            _data_dir->_set_sweep_worker_queue_depth(static_cast<int64_t>(_jobs.size()));
        }

        DORIS_CHECK(job.context != nullptr);
        DORIS_CHECK_EQ(job.context->sweep_epoch, job.sweep_epoch);
        DORIS_CHECK_LT(job.result_index, job.context->results.size());
        CountDownOnScopeExit completion(&job.context->completion_latch);
        job.context->results[job.result_index] = _execute(job);
    }
}

} // namespace doris
