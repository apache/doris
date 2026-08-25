
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
#include "exec/spill/spill_file.h"
#include "storage/options.h"
#include "util/threadpool.h"

namespace doris {
#include "common/compile_check_begin.h"
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

class SpillFileManager;
class SpillDataDir {
public:
    SpillDataDir(std::string path, int64_t capacity_bytes,
                 TStorageMedium::type storage_medium = TStorageMedium::HDD);

    Status init();

    const std::string& path() const { return _path; }

    std::string get_spill_data_path(const std::string& query_id = "") const;

    std::string get_spill_data_gc_path(const std::string& sub_dir_name = "") const;

    TStorageMedium::type storage_medium() const { return _storage_medium; }

    // check if the capacity reach the limit after adding the incoming data
    // return true if limit reached, otherwise, return false.
    bool reach_capacity_limit(int64_t incoming_data_size);

    // Atomically check the capacity limit and reserve spill usage for an external writer.
    bool try_reserve_spill_data(int64_t incoming_data_size);

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

    friend class SpillFileManager;
    std::string _path;

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

// Owns one external writer's Doris spill directories, capacity reservation and I/O accounting.
class ExternalSpillSession {
public:
    ~ExternalSpillSession();

    Status get_paths(std::vector<std::string>* paths);

    Status reserve(const std::string& path, int64_t bytes);

    void update_accounting(const std::string& path, int64_t current_bytes_delta,
                           int64_t write_bytes, int64_t read_bytes);

    // Account files written directly below an IOManager temp directory by Paimon components which
    // do not use BufferFileWriter (for example lookup stores and RocksDB global indexes).
    Status reconcile_direct_file_usage(bool allow_release);

private:
    friend class SpillFileManager;

    struct ManagedPath {
        SpillDataDir* data_dir;
        std::string path;
        int64_t buffer_accounted_bytes = 0;
        std::unordered_map<std::string, int64_t> buffer_accounted_bytes_by_path;
        int64_t direct_file_accounted_bytes = 0;
    };

    ExternalSpillSession(SpillFileManager* manager, QueryContext* query_context,
                         std::string relative_path);
    ManagedPath* _find_managed_path(const std::string& path);

    SpillFileManager* _manager;
    std::weak_ptr<QueryContext> _query_context;
    std::shared_ptr<ResourceContext> _resource_context;
    std::string _query_id;
    std::string _relative_path;
    std::vector<ManagedPath> _managed_paths;
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

    // Recursively delete a per-query spill directory during query teardown. Failed deletions are
    // retained by the manager and retried by its GC and shutdown paths.
    void delete_query_spill_directory(const std::string& query_id, SpillDataDir* data_dir);

    void gc(int32_t max_work_time_ms);

    void update_spill_write_bytes(int64_t bytes) { _spill_write_bytes_counter->increment(bytes); }

    void update_spill_read_bytes(int64_t bytes) { _spill_read_bytes_counter->increment(bytes); }

private:
    friend class ExternalSpillSession;

    struct PendingQuerySpillDirectory {
        int failed_count {0};
        std::string query_dir;
    };

    struct PendingExternalSpillAccounting {
        SpillDataDir* data_dir = nullptr;
        int64_t bytes = 0;
    };

    void _init_metrics();
    Status _init_spill_store_map();
    void _spill_gc_thread_callback();
    Status _try_delete_query_spill_directory(const PendingQuerySpillDirectory& pending_directory);
    void _retry_pending_query_spill_directories();
    Status _initialize_external_spill_session(ExternalSpillSession* spill_session);
    void _transfer_external_spill_accounting(const std::string& query_dir, SpillDataDir* data_dir,
                                             int64_t bytes);
    void _release_pending_external_spill_accounting(const std::string& query_dir);
    void _release_external_spill_session(const ExternalSpillSession* spill_session);
    bool _has_external_spill_lease(const std::string& query_dir);
    std::vector<SpillDataDir*> _get_stores_for_spill(TStorageMedium::type storage_medium);
    std::vector<SpillDataDir*> _get_stores_for_spill();
    SpillDataDir* _get_store_for_spill();

    std::unordered_map<std::string, std::unique_ptr<SpillDataDir>> _spill_store_map;

    CountDownLatch _stop_background_threads_latch;
    std::shared_ptr<Thread> _spill_gc_thread;

    std::mutex _pending_query_spill_directories_mutex;
    std::vector<PendingQuerySpillDirectory> _pending_query_spill_directories;

    std::mutex _pending_external_spill_accounting_mutex;
    std::unordered_map<std::string, PendingExternalSpillAccounting>
            _pending_external_spill_accounting;

    std::mutex _external_spill_leases_mutex;
    std::unordered_map<std::string, size_t> _external_spill_leases;

    std::atomic_uint64_t id_ = 0;

    std::shared_ptr<MetricEntity> _entity {nullptr};

    std::unique_ptr<doris::MetricPrototype> _spill_write_bytes_metric {nullptr};
    std::unique_ptr<doris::MetricPrototype> _spill_read_bytes_metric {nullptr};

    IntAtomicCounter* _spill_write_bytes_counter {nullptr};
    IntAtomicCounter* _spill_read_bytes_counter {nullptr};
};
} // namespace doris
#include "common/compile_check_end.h"
