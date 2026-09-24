
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
#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/metrics/metrics.h"
#include "common/status.h"
#include "exec/spill/spill_data_dir.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_remote_upload_budget.h"
#include "util/threadpool.h"

namespace doris {
class RuntimeProfile;
template <typename T>
class AtomicCounter;
using IntAtomicCounter = AtomicCounter<int64_t>;
template <typename T>
class AtomicGauge;
using UIntGauge = AtomicGauge<uint64_t>;
class MetricEntity;
struct MetricPrototype;

class RemoteSpillDataDir;

class SpillFileManager {
public:
    ~SpillFileManager();
    SpillFileManager(
            std::unordered_map<std::string, std::unique_ptr<SpillDataDir>>&& spill_store_map);

    Status init();

    void stop();

    // Create SpillFile and register it
    // @param relative_path  Operator-formatted path under the spill root,
    //                       e.g. "query_id/sort-node_id-task_id-unique_id"
    Status create_spill_file(const std::string& relative_path, SpillFileSPtr& spill_file);

    /// Get a unique ID for constructing spill file paths.
    uint64_t next_id() { return id_++; }

    // Delete SpillFile data synchronously.
    void delete_spill_file(SpillFileSPtr spill_file);

    // Recursively delete a per-query spill directory of a local store during query teardown.
    // Failed deletions are retained by the manager and retried by its GC and shutdown paths.
    // A no-op for the remote store: its spill is deleted per spill file only.
    void delete_query_spill_directory(const std::string& query_id, SpillDataDir* data_dir);

    // Take over a spill file directory whose deletion failed. Its objects are still stored, so
    // `charged_bytes` stay charged to `data_dir` until a retry deletes them; otherwise a
    // deletion outage would free capacity that is still in use.
    void retry_spill_directory_deletion(SpillDataDir* data_dir, std::string dir,
                                        int64_t charged_bytes);

    void gc(int32_t max_work_time_ms);

    void update_spill_write_bytes(int64_t bytes) { _spill_write_bytes_counter->increment(bytes); }

    void update_spill_read_bytes(int64_t bytes) { _spill_read_bytes_counter->increment(bytes); }

    /// Bytes and PutObject/UploadPart requests issued by spill against object storage.
    void update_spill_remote_write(int64_t bytes, int64_t put_requests);

    /// Bytes and GET requests issued by spill against object storage.
    void update_spill_remote_read(int64_t bytes, int64_t get_requests);

    SpillRemoteUploadBudget* remote_upload_budget() { return _remote_upload_budget.get(); }

    /// Number of spill directories whose deletion failed and is being retried.
    size_t pending_delete_dir_count();

    /// Bytes of spill data this process currently holds in object storage (bytes reserved for
    /// parts still being uploaded included); 0 without a remote store. Served to the FEs through
    /// get_be_resource, which every FE polls for SHOW DATA.
    int64_t remote_spill_data_bytes();

private:
    struct PendingSpillDirectory {
        int failed_count {0};
        // A query directory or the directory of one spill file.
        std::string dir;
        SpillDataDir* data_dir {nullptr};
        // Bytes of the objects under `dir` still charged to `data_dir`; released once deleted.
        int64_t charged_bytes {0};
    };

    void _init_metrics();
    Status _init_spill_store_map();
    void _spill_gc_thread_callback();
    Status _try_delete_spill_directory(const PendingSpillDirectory& pending_directory);
    void _retry_pending_spill_directories();
    /// Queue a failed deletion, merged with a pending ancestor or absorbing pending descendants
    /// of the same store so that an outage keeps about one entry per query.
    void _add_pending_directory(PendingSpillDirectory pending_directory);
    std::vector<SpillDataDir*> _get_stores_for_spill(TStorageMedium::type storage_medium);

    std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> _spill_store_map;
    // Views of _spill_store_map by kind: a BE has either local stores or one remote store.
    std::vector<LocalSpillDataDir*> _local_stores;
    RemoteSpillDataDir* _remote_store = nullptr;

    std::shared_ptr<SpillRemoteUploadBudget> _remote_upload_budget;

    CountDownLatch _stop_background_threads_latch;
    std::shared_ptr<Thread> _spill_gc_thread;

    std::mutex _pending_spill_directories_mutex;
    std::vector<PendingSpillDirectory> _pending_spill_directories;

    std::atomic_uint64_t id_ = 0;

    std::shared_ptr<MetricEntity> _entity {nullptr};

    std::unique_ptr<doris::MetricPrototype> _spill_write_bytes_metric {nullptr};
    std::unique_ptr<doris::MetricPrototype> _spill_read_bytes_metric {nullptr};

    IntAtomicCounter* _spill_write_bytes_counter {nullptr};
    IntAtomicCounter* _spill_read_bytes_counter {nullptr};

    std::unique_ptr<doris::MetricPrototype> _spill_remote_write_bytes_metric {nullptr};
    std::unique_ptr<doris::MetricPrototype> _spill_remote_read_bytes_metric {nullptr};
    std::unique_ptr<doris::MetricPrototype> _spill_remote_put_requests_metric {nullptr};
    std::unique_ptr<doris::MetricPrototype> _spill_remote_get_requests_metric {nullptr};
    std::unique_ptr<doris::MetricPrototype> _spill_pending_delete_dir_count_metric {nullptr};
    std::unique_ptr<doris::MetricPrototype> _spill_remote_inflight_upload_bytes_metric {nullptr};

    IntAtomicCounter* _spill_remote_write_bytes_counter {nullptr};
    IntAtomicCounter* _spill_remote_read_bytes_counter {nullptr};
    IntAtomicCounter* _spill_remote_put_requests_counter {nullptr};
    IntAtomicCounter* _spill_remote_get_requests_counter {nullptr};
    IntGauge* _spill_pending_delete_dir_count_gauge {nullptr};
    IntGauge* _spill_remote_inflight_upload_bytes_gauge {nullptr};
};
} // namespace doris
