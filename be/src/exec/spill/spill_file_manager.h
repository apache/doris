
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
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
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

    // Recursively delete a per-query spill directory during query teardown. Failed deletions are
    // retained by the manager and retried by its GC and shutdown paths.
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

    /// True while query directories left behind by the previous process of this BE still wait
    /// to be deleted.
    bool remote_startup_cleanup_pending() const {
        return _remote_startup_cleanup_pending.load(std::memory_order_acquire);
    }

    /// Number of spill directories whose deletion failed and is being retried.
    size_t pending_delete_dir_count();

    /// Record that a query of this process writes under spill/{ip}_{port}/{query_dir} of the remote
    /// store, so that the startup cleanup leaves the directory alone. Called before the first
    /// object of a spill file is written; the query's delete_query_spill_directory() removes
    /// the record.
    void register_remote_query_dir(const std::string& query_dir);

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
    void _retry_pending_query_spill_directories();
    /// Queue a failed deletion, merged with a pending ancestor or absorbing pending descendants
    /// of the same store so that an outage keeps about one entry per query.
    void _add_pending_directory(PendingSpillDirectory pending_directory);
    std::vector<SpillDataDir*> _get_stores_for_spill(TStorageMedium::type storage_medium);
    void _remote_gc();
    /// Rewrite the heartbeat object of the remote store when it is due; GC thread only.
    void _remote_heartbeat();
    /// Delete the query directories under spill/{ip}_{port}/ that the previous process of this BE
    /// left behind; `done` is set when none is left. The directories are taken from one listing,
    /// the first after the store became ready, minus those registered by queries of this
    /// process; one directory is deleted per GC round.
    Status _remote_startup_cleanup(bool* done);

    std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> _spill_store_map;
    // Views of _spill_store_map by kind: a BE has either local stores or one remote store.
    std::vector<LocalSpillDataDir*> _local_stores;
    RemoteSpillDataDir* _remote_store = nullptr;

    std::shared_ptr<SpillRemoteUploadBudget> _remote_upload_budget;
    std::atomic<bool> _remote_startup_cleanup_pending {false};
    int64_t _remote_not_ready_rounds = 0;
    // GC thread only: MonotonicSeconds() at which the heartbeat is next written.
    int64_t _next_remote_heartbeat_s = 0;
    // GC thread only: directories left behind by the previous process, not yet deleted; empty
    // optional until the first listing.
    std::optional<std::vector<std::string>> _remote_residue_dirs;

    std::mutex _remote_query_dirs_mutex;
    // Query directories under spill/{ip}_{port}/ written by queries of this process.
    std::unordered_set<std::string> _remote_query_dirs;

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
    std::unique_ptr<doris::MetricPrototype> _spill_remote_startup_cleanup_pending_metric {nullptr};

    IntAtomicCounter* _spill_remote_write_bytes_counter {nullptr};
    IntAtomicCounter* _spill_remote_read_bytes_counter {nullptr};
    IntAtomicCounter* _spill_remote_put_requests_counter {nullptr};
    IntAtomicCounter* _spill_remote_get_requests_counter {nullptr};
    IntGauge* _spill_pending_delete_dir_count_gauge {nullptr};
    IntGauge* _spill_remote_inflight_upload_bytes_gauge {nullptr};
    IntGauge* _spill_remote_startup_cleanup_pending_gauge {nullptr};
};
} // namespace doris
