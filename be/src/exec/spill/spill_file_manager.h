
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
#include <string_view>
#include <unordered_map>
#include <vector>

#include "common/metrics/metrics.h"
#include "common/status.h"
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

class SpillFileManager;

/// A spill store. Either one local disk (spill_storage_type=local) or the object storage of a
/// cloud storage vault (spill_storage_type=s3). All file system access of spill goes through
/// fs() so that SpillFile / SpillFileWriter / SpillFileReader share one code path.
class SpillDataDir {
public:
    /// Local disk store.
    SpillDataDir(std::string path, int64_t capacity_bytes,
                 TStorageMedium::type storage_medium = TStorageMedium::HDD);

    /// Remote (object storage) store. The file system and the object key root are resolved
    /// lazily by ensure_ready(): the storage vault and the backend id may not be available
    /// when BE starts (both come from meta-service / FE heartbeat).
    /// @param vault_id  storage vault id, empty means the default vault of the instance.
    /// @param boot_id   boot generation of this BE process, see get_spill_data_path().
    struct Remote {};
    SpillDataDir(Remote, std::string vault_id, int64_t boot_id);

    Status init();

    bool is_remote() const { return _is_remote; }

    /// Local stores are always ready. Remote stores become ready after ensure_ready() or
    /// init_remote_fs() succeeded.
    bool ready() const { return _ready.load(std::memory_order_acquire); }

    /// Remote only: resolve the storage vault file system and bind it. Idempotent and thread
    /// safe. Returns an error while the backend id or the storage vault is not available yet.
    Status ensure_ready();

    /// Remote only: bind a file system directly. Used by ensure_ready() and by tests.
    void init_remote_fs(io::FileSystemSPtr fs, int64_t backend_id);

    /// File system used for all spill IO of this store. Remote stores return nullptr until ready.
    io::FileSystemSPtr fs() const;

    const std::string& path() const { return _path; }

    /// Root of spill data of this store, optionally for one query:
    ///   local:  {path}/spill[/query_id]
    ///   remote: spill/{backend_id}/data/{boot_id}[/query_id]   (relative to the vault prefix)
    /// Every object written by this process lives under the current boot_id, so objects under
    /// another boot_id always belong to a dead process and can be deleted at startup without
    /// racing with running queries.
    std::string get_spill_data_path(const std::string& query_id = "") const;

    std::string get_spill_data_gc_path(const std::string& sub_dir_name = "") const;

    /// Remote only: key prefix shared by all boot generations of this BE, spill/{backend_id}.
    /// backend_id is FE-assigned and unique per BE (cloud_unique_id is not: every BE added by
    /// one ADD BACKEND statement shares it), so no two live processes ever share this prefix.
    const std::string& get_remote_be_root() const { return _remote_be_root; }
    /// Remote only: spill/{backend_id}/data/{boot_id} — the data of one boot generation.
    std::string get_remote_boot_data_path(std::string_view boot_id) const {
        return fmt::format("{}/data/{}", _remote_be_root, boot_id);
    }
    /// Remote only: spill/{backend_id}/boots/{boot_id} — an empty marker object written once
    /// the store is ready and refreshed daily. Startup cleanup lists only this small directory
    /// to discover other boot generations instead of listing every spill object of the BE.
    /// The meta-service recycler deletes markers like any other spill object once they are
    /// older than spill_objects_expire_time_second (> 1 day by contract); the daily refresh keeps
    /// the marker of a live process alive, and a generation whose marker is gone (crash before
    /// the first GC round, TTL misconfigured) is only reclaimed by that recycler.
    std::string get_remote_boot_marker_path(std::string_view boot_id) const {
        return fmt::format("{}/boots/{}", _remote_be_root, boot_id);
    }
    std::string get_remote_boots_path() const { return fmt::format("{}/boots", _remote_be_root); }
    int64_t backend_id() const { return _backend_id; }

    int64_t boot_id() const { return _boot_id; }

    const std::string& vault_id() const { return _vault_id; }

    TStorageMedium::type storage_medium() const { return _storage_medium; }

    // check if the capacity reach the limit after adding the incoming data
    // return true if limit reached, otherwise, return false.
    bool reach_capacity_limit(int64_t incoming_data_size);

    /// Atomically check the capacity limit and account `bytes` as used. Returns
    /// DISK_REACH_CAPACITY_LIMIT without accounting when the limit would be exceeded.
    /// With force=true the bytes are accounted without checking (used for part footers so
    /// that an open part can always be closed).
    Status try_reserve(int64_t bytes, bool force = false);

    /// Give back bytes accounted by try_reserve().
    void release(int64_t bytes) { update_spill_data_usage(-bytes); }

    Status update_capacity();

    void update_spill_data_usage(int64_t incoming_data_size) {
        std::lock_guard<std::mutex> l(_mutex);
        _spill_data_bytes += incoming_data_size;
        spill_disk_data_size->set_value(_spill_data_bytes);
    }

    int64_t get_spill_data_bytes() {
        std::lock_guard<std::mutex> l(_mutex);
        return _spill_data_bytes;
    }

    int64_t get_spill_data_limit() {
        std::lock_guard<std::mutex> l(_mutex);
        return _spill_data_limit_bytes;
    }

    std::string debug_string();

private:
    bool _reach_disk_capacity_limit(int64_t incoming_data_size);
    double _get_disk_usage(int64_t incoming_data_size) const {
        return _disk_capacity_bytes == 0
                       ? 0
                       : (double)(_disk_capacity_bytes - _available_bytes + incoming_data_size) /
                                 (double)_disk_capacity_bytes;
    }

    bool _reach_limit_unlocked(int64_t incoming_data_size);

    friend class SpillFileManager;
    std::string _path;

    // ── Remote store state ──
    bool _is_remote = false;
    std::string _vault_id;
    int64_t _boot_id = 0;
    int64_t _backend_id = 0;
    std::mutex _init_mutex;
    std::atomic<bool> _ready {false};
    io::FileSystemSPtr _fs;
    std::string _remote_be_root;
    // Root of spill data: local "{path}/spill", remote "spill/{backend_id}/data/{boot_id}".
    std::string _spill_root;

    // protect _disk_capacity_bytes, _available_bytes, _spill_data_limit_bytes, _spill_data_bytes
    std::mutex _mutex;
    // the actual capacity of the disk of this data dir
    size_t _disk_capacity_bytes;
    int64_t _spill_data_limit_bytes = 0;
    // the actual available capacity of the disk of this data dir
    size_t _available_bytes = 0;
    int64_t _spill_data_bytes = 0;
    TStorageMedium::type _storage_medium;

    std::shared_ptr<MetricEntity> spill_data_dir_metric_entity;
    IntGauge* spill_disk_capacity = nullptr;
    IntGauge* spill_disk_limit = nullptr;
    IntGauge* spill_disk_avail_capacity = nullptr;
    IntGauge* spill_disk_data_size = nullptr;
    // for test
    IntGauge* spill_disk_has_spill_data = nullptr;
    IntGauge* spill_disk_has_spill_gc_data = nullptr;
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

    /// Send the since-boot object storage traffic to meta-service now, with bounded retries.
    /// Called on the shutdown path (doris_main after all tasks are done, and stop()) so that the
    /// last reporting interval is not lost; safe to call more than once and from any thread.
    void flush_remote_spill_stats();

    /// Object storage traffic of spill since this process started; the source of the numbers
    /// reported to meta-service (SHOW DATA) and exposed as metrics.
    int64_t remote_write_bytes_since_boot() const {
        return _remote_write_bytes_since_boot.load(std::memory_order_relaxed);
    }
    int64_t remote_put_requests_since_boot() const {
        return _remote_put_requests_since_boot.load(std::memory_order_relaxed);
    }

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
    void _remote_gc(SpillDataDir* store);
    /// Write the boot marker of the current generation (see get_remote_boot_marker_path).
    Status _remote_write_boot_marker(SpillDataDir* store);
    /// Delete the data of one other boot generation; `done` is set when none is left. Bounded
    /// work per GC round: one listing of the boots directory and one generation.
    Status _remote_startup_cleanup(SpillDataDir* store, bool* done);
    /// Send the since-boot totals to meta-service when they changed since the last report.
    /// `final_report` skips the cadence check.
    void _report_remote_spill_stats(SpillDataDir* store, bool final_report = false);

    std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> _spill_store_map;

    std::shared_ptr<SpillRemoteUploadBudget> _remote_upload_budget;
    std::atomic<bool> _remote_startup_cleanup_pending {false};
    std::atomic<bool> _remote_boot_marker_pending {false};
    int64_t _remote_boot_marker_rounds = 0;
    int64_t _remote_not_ready_rounds = 0;
    std::atomic<int64_t> _remote_write_bytes_since_boot {0};
    std::atomic<int64_t> _remote_put_requests_since_boot {0};
    // Serialises reports between the GC thread and flush_remote_spill_stats(); guards the
    // three fields below.
    std::mutex _remote_report_mutex;
    int64_t _reported_remote_write_bytes = 0;
    int64_t _reported_remote_put_requests = 0;
    int64_t _remote_report_rounds = 0;

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
