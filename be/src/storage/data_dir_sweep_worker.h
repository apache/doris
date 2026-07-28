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

#pragma once

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <ctime>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <variant>
#include <vector>

#include "common/status.h"
#include "storage/tablet/tablet_fwd.h"
#include "util/countdown_latch.h"

namespace doris {

class DataDir;
class StorageEngine;
class Thread;

enum class DataDirSweepJobType : uint8_t {
    SNAPSHOT_SWEEP = 0,
    TRASH_SWEEP = 1,
    SHUTDOWN_TABLET_MOVE = 2,
    REMOTE_GC = 3,
    TRASH_CAPACITY_REFRESH = 4,
};

struct SnapshotSweepPayload {
    time_t local_now = 0;
    int32_t expire_seconds = 0;
};

struct TrashSweepPayload {
    time_t local_now = 0;
    int32_t expire_seconds = 0;
};

using MoveTabletCallback = std::function<bool(const TabletSharedPtr&)>;

struct ShutdownTabletMovePayload {
    std::vector<TabletSharedPtr> tablets;
    MoveTabletCallback move_tablet;
};

struct RemoteGcPayload {};
struct TrashCapacityRefreshPayload {};

using DataDirSweepJobPayload =
        std::variant<SnapshotSweepPayload, TrashSweepPayload, ShutdownTabletMovePayload,
                     RemoteGcPayload, TrashCapacityRefreshPayload>;

struct DataDirSweepJobResult {
    uint64_t sweep_epoch = 0;
    DataDirSweepJobType type = DataDirSweepJobType::SNAPSHOT_SWEEP;
    DataDir* data_dir = nullptr;
    Status status;
    int64_t elapsed_ms = 0;

    int64_t shutdown_resolved = 0;
    int64_t shutdown_failed = 0;
    std::vector<TabletSharedPtr> failed_tablets;

    int64_t remote_rowset_gc_scanned = 0;
    std::optional<int64_t> remote_rowset_gc_backlog;
    int64_t remote_tablet_gc_scanned = 0;
    std::optional<int64_t> remote_tablet_gc_backlog;
};

struct DataDirSweepPhaseContext {
    DataDirSweepPhaseContext(uint64_t epoch, size_t job_count)
            : sweep_epoch(epoch),
              completion_latch(static_cast<int>(job_count)),
              results(job_count) {}

    uint64_t sweep_epoch;
    CountDownLatch completion_latch;
    std::vector<DataDirSweepJobResult> results;
};

struct DataDirSweepJob {
    uint64_t sweep_epoch = 0;
    DataDirSweepJobType type = DataDirSweepJobType::SNAPSHOT_SWEEP;
    DataDir* data_dir = nullptr;
    DataDirSweepJobPayload payload;
    std::shared_ptr<DataDirSweepPhaseContext> context;
    size_t result_index = 0;
};

class DataDirSweepWorker {
public:
    DataDirSweepWorker(StorageEngine& engine, DataDir* data_dir);
    ~DataDirSweepWorker();

    Status start();
    Status submit(DataDirSweepJob job);
    void stop_accepting_jobs();
    void drain_and_stop();
    void join();

    DataDir* data_dir() const { return _data_dir; }

private:
    void _run();
    DataDirSweepJobResult _execute(DataDirSweepJob& job);

    static constexpr size_t kMaxQueuedJobs = 2;

    StorageEngine& _engine;
    DataDir* _data_dir;

    std::mutex _lock;
    std::condition_variable _cv;
    std::deque<DataDirSweepJob> _jobs;
    bool _accepting_jobs = true;
    bool _stop_requested = false;
    std::shared_ptr<Thread> _thread;
};

const char* data_dir_sweep_job_type_name(DataDirSweepJobType type);

} // namespace doris
