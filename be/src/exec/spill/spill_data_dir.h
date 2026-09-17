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

#include <gen_cpp/Types_types.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

#include "common/status.h"
#include "io/fs/file_system.h"

namespace doris {

template <typename T>
class AtomicGauge;
using IntGauge = AtomicGauge<int64_t>;
class MetricEntity;

/// A spill store: one local disk (LocalSpillDataDir) or the object storage of a cloud storage
/// vault (RemoteSpillDataDir). All file system access of spill goes through fs() so that
/// SpillFile / SpillFileWriter / SpillFileReader share one code path. The base class owns
/// what both kinds share: the spill root, the byte accounting against a limit, and the metrics.
class SpillDataDir {
public:
    virtual ~SpillDataDir();

    virtual Status init() = 0;

    virtual bool is_remote() const { return false; }

    /// Whether the store can be used. Local stores are always ready; a remote store becomes
    /// ready once its storage vault is bound.
    virtual bool ready() const { return true; }

    /// File system used for all spill IO of this store. Remote stores return nullptr until ready.
    virtual io::FileSystemSPtr fs() const = 0;

    /// Refresh capacity and the byte limit from the disk / the configs.
    virtual Status update_capacity() = 0;

    /// Disk usage ratio after adding `incoming_data_size`, used to spread spill over local
    /// disks; object storage reports 0.
    virtual double get_disk_usage(int64_t incoming_data_size) const { return 0.0; }

    const std::string& path() const { return _path; }

    /// Root of spill data of this store, optionally for one query:
    ///   local:  {path}/spill[/query_id]
    ///   remote: spill/{instance_id}/{backend_id}/data/{boot_id}[/query_id]   (relative to the
    ///           vault prefix)
    std::string get_spill_data_path(const std::string& query_id = "") const;

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

    void update_spill_data_usage(int64_t incoming_data_size);

    int64_t get_spill_data_bytes();

    int64_t get_spill_data_limit();

    std::string debug_string();

protected:
    /// @param path         identifies the store (a directory for local disks, "s3:{vault}" for
    ///                     object storage); used in logs, metrics and error messages.
    /// @param spill_root   initial spill root; a remote store fills it in when it is bound.
    /// @param metric_path  value of the "path" label of the metric entity.
    SpillDataDir(std::string path, std::string spill_root, const std::string& metric_path,
                 int64_t capacity_bytes, TStorageMedium::type storage_medium);

    /// Called with _mutex held.
    virtual bool _reach_limit_unlocked(int64_t incoming_data_size) = 0;

    std::string _path;
    // Root of spill data: local "{path}/spill", remote
    // "spill/{instance_id}/{backend_id}/data/{boot_id}".
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

/// Spill store on one local disk (spill_storage_type=local).
class LocalSpillDataDir final : public SpillDataDir {
public:
    LocalSpillDataDir(std::string path, int64_t capacity_bytes,
                      TStorageMedium::type storage_medium = TStorageMedium::HDD);

    Status init() override;

    io::FileSystemSPtr fs() const override;

    Status update_capacity() override;

    double get_disk_usage(int64_t incoming_data_size) const override {
        return _disk_capacity_bytes == 0
                       ? 0
                       : (double)(_disk_capacity_bytes - _available_bytes + incoming_data_size) /
                                 (double)_disk_capacity_bytes;
    }

    /// {path}/spill_gc[/sub_dir]: spill directories moved here are deleted by the GC thread.
    std::string get_spill_data_gc_path(const std::string& sub_dir_name = "") const;

protected:
    bool _reach_limit_unlocked(int64_t incoming_data_size) override;

private:
    bool _reach_disk_capacity_limit(int64_t incoming_data_size);
};

} // namespace doris
