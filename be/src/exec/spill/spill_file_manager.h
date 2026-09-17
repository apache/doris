
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
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/metrics/metrics.h"
#include "common/status.h"
#include "exec/spill/spill_data_dir.h"
#include "exec/spill/spill_file.h"
#include "exec/spill/spill_remote_upload_budget.h"
#include "io/fs/file_system.h"
#include "storage/options.h"
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

    void gc(int32_t max_work_time_ms);

    void update_spill_write_bytes(int64_t bytes) { _spill_write_bytes_counter->increment(bytes); }

    void update_spill_read_bytes(int64_t bytes) { _spill_read_bytes_counter->increment(bytes); }

    /// Bytes and PutObject/UploadPart requests issued by spill against object storage.
    void update_spill_remote_write(int64_t bytes, int64_t put_requests);

    /// Bytes and GET requests issued by spill against object storage.
    void update_spill_remote_read(int64_t bytes, int64_t get_requests);

    SpillRemoteUploadBudget* remote_upload_budget() { return _remote_upload_budget.get(); }

    /// True while objects of previous boot generations of this BE still wait to be deleted.
    bool remote_startup_cleanup_pending() const {
        return _remote_startup_cleanup_pending.load(std::memory_order_acquire);
    }

    /// Number of query spill directories whose deletion failed and is being retried.
    size_t pending_delete_dir_count();

    /// Report the spill data currently held in object storage to meta-service now, with bounded
    /// retries. Called on the shutdown path (doris_main after all tasks are done, and stop()) so
    /// that meta-service sees the final size (normally 0) instead of the last periodic value;
    /// safe to call more than once and from any thread.
    void flush_remote_spill_stats();

    using RemoteSpillReportFn = std::function<Status(int64_t backend_id, int64_t boot_id,
                                                     int64_t report_seq, int64_t spill_bytes)>;
    /// Test hooks: replace the meta-service RPC of the spill stats report and the heartbeat
    /// interval, and run one reporting decision (what the GC thread does about once a minute).
    void set_remote_spill_report_fn_for_test(RemoteSpillReportFn fn, int64_t heartbeat_ms);
    void report_remote_spill_stats_for_test(bool final_report);

private:
    struct PendingQuerySpillDirectory {
        int failed_count {0};
        std::string query_dir;
        SpillDataDir* data_dir {nullptr};
    };

    void _init_metrics();
    Status _init_spill_store_map();
    void _spill_gc_thread_callback();
    Status _try_delete_query_spill_directory(const PendingQuerySpillDirectory& pending_directory);
    void _retry_pending_query_spill_directories();
    std::vector<SpillDataDir*> _get_stores_for_spill(TStorageMedium::type storage_medium);
    void _remote_gc(RemoteSpillDataDir* store);
    /// Write the boot marker of the current generation (see get_remote_boot_marker_path).
    Status _remote_write_boot_marker(RemoteSpillDataDir* store);
    /// Delete the data of one other boot generation; `done` is set when none is left. Bounded
    /// work per GC round: one listing of the boots directory and one generation.
    Status _remote_startup_cleanup(RemoteSpillDataDir* store, bool* done);
    /// Send the current spill size to meta-service when it changed since the last successful
    /// report, or when the heartbeat interval elapsed. `final_report` only affects logging.
    void _report_remote_spill_stats(RemoteSpillDataDir* store, bool final_report = false);

    std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> _spill_store_map;
    // Views of _spill_store_map by kind: a BE has either local stores or one remote store.
    std::vector<LocalSpillDataDir*> _local_stores;
    RemoteSpillDataDir* _remote_store = nullptr;

    std::shared_ptr<SpillRemoteUploadBudget> _remote_upload_budget;
    std::atomic<bool> _remote_startup_cleanup_pending {false};
    std::atomic<bool> _remote_boot_marker_pending {false};
    int64_t _remote_boot_marker_rounds = 0;
    int64_t _remote_not_ready_rounds = 0;
    int64_t _remote_report_rounds = 0; // GC thread only
    // Serialises reports between the GC thread and flush_remote_spill_stats(); guards the
    // fields below. -1: nothing reported yet, so the first report always goes out and replaces
    // whatever the previous process of this backend_id left behind.
    std::mutex _remote_report_mutex;
    int64_t _reported_remote_spill_bytes = -1;
    int64_t _remote_last_report_ms = 0;
    int64_t _remote_report_seq = 0;
    int64_t _remote_report_heartbeat_ms = 3600LL * 1000;
    RemoteSpillReportFn _remote_report_fn;

    CountDownLatch _stop_background_threads_latch;
    std::shared_ptr<Thread> _spill_gc_thread;

    std::mutex _pending_query_spill_directories_mutex;
    std::vector<PendingQuerySpillDirectory> _pending_query_spill_directories;

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
