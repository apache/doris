
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
#include "util/stopwatch.hpp"
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
class QueryContext;
class ResourceContext;

class RemoteSpillDataDir;
class SpillFileManager;

// Reconcile the byte counter for one remote spill prefix with the objects still listed there.
// Used after uncertain publication or partial deletion, never on the normal write path.
Status reconcile_remote_spill_bytes(SpillDataDir* data_dir, const io::FileSystemSPtr& fs,
                                    const std::string& dir, int64_t* accounted_bytes);

// Adapts one external writer to the same root selection, capacity accounting and query cleanup
// used by Doris spill files.
class ExternalSpillSession {
public:
    ~ExternalSpillSession();

    Status get_paths(std::vector<std::string>* paths);

    Status reserve(const std::string& path, int64_t bytes);

    void update_accounting(const std::string& path, int64_t current_bytes_delta,
                           int64_t write_bytes, int64_t read_bytes);

private:
    friend class SpillFileManager;

    ExternalSpillSession(SpillFileManager* manager, QueryContext* query_context,
                         std::string relative_path);
    bool _contains(const std::string& path) const;

    SpillFileManager* _manager;
    std::weak_ptr<QueryContext> _query_context;
    std::shared_ptr<ResourceContext> _resource_context;
    std::string _query_id;
    std::string _relative_path;
    SpillDataDir* _data_dir = nullptr;
    std::string _path;
    int64_t _accounted_bytes = 0;
    std::mutex _mutex;
};

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

    // Create a lazy managed session for an external spill implementation. A spill root is selected
    // and registered only when the external implementation first requests its path.
    Status create_external_spill_session(const std::string& relative_path,
                                         QueryContext* query_context,
                                         std::unique_ptr<ExternalSpillSession>* spill_session);

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
    void retry_spill_directory_deletion(SpillDataDir* data_dir, io::FileSystemSPtr fs,
                                        std::string dir, int64_t charged_bytes,
                                        int64_t persisted_bytes);

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

    /// Bytes of completed spill objects this process currently holds in object storage;
    /// uncommitted capacity reservations are excluded. Served to the FEs through
    /// get_be_resource, which every FE polls for SHOW DATA.
    int64_t remote_spill_data_bytes();

private:
    friend class ExternalSpillSession;

    struct PendingSpillDirectory {
        int failed_count {0};
        // A query directory or the directory of one spill file.
        std::string dir;
        SpillDataDir* data_dir {nullptr};
        // Pinned for remote spill: a later default-vault rotation must not redirect GC.
        io::FileSystemSPtr fs;
        // Bytes of the objects under `dir` still charged to `data_dir`; released once deleted.
        int64_t charged_bytes {0};
        int64_t persisted_bytes {0};
    };

    struct SpillGcStats {
        bool has_work = false;
        size_t backlog_dirs = 0;
        size_t deleted_dirs = 0;
        size_t deleted_files = 0;
        size_t failed_deletes = 0;
    };

    void _init_metrics();
    Status _init_spill_store_map();
    void _spill_gc_thread_callback();
    Status _try_delete_spill_directory(const PendingSpillDirectory& pending_directory);
    void _retry_pending_spill_directories();
    void _gc_spill_store(LocalSpillDataDir* store_dir, const MonotonicStopWatch& watch,
                         int64_t max_work_time_ns, SpillGcStats* stats);
    /// Queue a failed deletion, merged with a pending ancestor or absorbing pending descendants
    /// of the same store so that an outage keeps about one entry per query.
    void _add_pending_directory(PendingSpillDirectory pending_directory);
    Status _initialize_external_spill_session(ExternalSpillSession* spill_session);
    void _release_external_spill_session(ExternalSpillSession* spill_session);
    std::vector<SpillDataDir*> _get_stores_for_spill(TStorageMedium::type storage_medium);
    std::vector<SpillDataDir*> _get_local_stores_for_spill(TStorageMedium::type storage_medium);
    SpillDataDir* _get_local_store_for_external_spill();

    std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> _spill_store_map;
    // Views of _spill_store_map by kind: a BE has either local stores or one remote store.
    std::vector<LocalSpillDataDir*> _local_stores;
    RemoteSpillDataDir* _remote_store = nullptr;

    std::shared_ptr<SpillRemoteUploadBudget> _remote_upload_budget;

    CountDownLatch _stop_background_threads_latch;
    std::shared_ptr<Thread> _spill_gc_thread;

    // External spill leases defer query-directory deletion until native callbacks finish.
    // Filesystem I/O never holds this mutex.
    std::mutex _pending_spill_directories_mutex;
    std::vector<PendingSpillDirectory> _pending_spill_directories;
    std::unordered_map<std::string, size_t> _external_spill_directory_leases;

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
