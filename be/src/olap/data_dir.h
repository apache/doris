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
#include <stddef.h>

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"
#include "util/metrics.h"

namespace doris {

class Tablet;
class TabletManager;
class TxnManager;
class OlapMeta;
class RowsetIdGenerator;
class StorageEngine;

const char* const kTestFilePath = ".testfile";

// A DataDir used to manage data in same path.
// Now, After DataDir was created, it will never be deleted for easy implementation.
class DataDir {
public:
    DataDir(StorageEngine& engine, const std::string& path, int64_t capacity_bytes = -1,
            TStorageMedium::type storage_medium = TStorageMedium::HDD);
    ~DataDir();

    Status init(bool init_meta = true);
    void stop_bg_worker();

    const std::string& path() const { return _path; }
    size_t path_hash() const { return _path_hash; }

    bool is_used() const { return _is_used; }
    int32_t cluster_id() const { return _cluster_id; }
    bool cluster_id_incomplete() const { return _cluster_id_incomplete; }

    DataDirInfo get_dir_info() {
        DataDirInfo info;
        info.path = _path;
        info.path_hash = _path_hash;
        info.disk_capacity = _disk_capacity_bytes;
        info.available = _available_bytes;
        info.trash_used_capacity = _trash_used_bytes;
        info.is_used = _is_used;
        info.storage_medium = _storage_medium;
        return info;
    }

    // save a cluster_id file under data path to prevent
    // invalid be config for example two be use the same
    // data path
    Status set_cluster_id(int32_t cluster_id);
    void health_check();

    uint64_t get_shard() {
        return _current_shard.fetch_add(1, std::memory_order_relaxed) % MAX_SHARD_NUM;
    }

    OlapMeta* get_meta() { return _meta; }

    bool is_ssd_disk() const { return _storage_medium == TStorageMedium::SSD; }

    TStorageMedium::type storage_medium() const { return _storage_medium; }

    void register_tablet(Tablet* tablet);
    void deregister_tablet(Tablet* tablet);
    void clear_tablets(std::vector<TabletInfo>* tablet_infos);

    std::string get_absolute_shard_path(int64_t shard_id);
    std::string get_absolute_tablet_path(int64_t shard_id, int64_t tablet_id, int32_t schema_hash);

    void find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths);

    static std::string get_root_path_from_schema_hash_path_in_trash(
            const std::string& schema_hash_dir_in_trash);

    // load data from meta and data files
    Status load();

    void perform_path_gc();

    void perform_remote_rowset_gc();

    void perform_remote_tablet_gc();

    // check if the capacity reach the limit after adding the incoming data
    // return true if limit reached, otherwise, return false.
    // TODO(cmy): for now we can not precisely calculate the capacity Doris used,
    // so in order to avoid running out of disk capacity, we currently use the actual
    // disk available capacity and total capacity to do the calculation.
    // So that the capacity Doris actually used may exceeds the user specified capacity.
    bool reach_capacity_limit(int64_t incoming_data_size);

    Status update_capacity();

    void update_trash_capacity();

    void update_local_data_size(int64_t size);

    void update_remote_data_size(int64_t size);

    size_t tablet_size() const;

    void disks_compaction_score_increment(int64_t delta);

    void disks_compaction_num_increment(int64_t delta);

    double get_usage(int64_t incoming_data_size) const {
        return _disk_capacity_bytes == 0
                       ? 0
                       : (_disk_capacity_bytes - _available_bytes + incoming_data_size) /
                                 (double)_disk_capacity_bytes;
    }

    // Move tablet to trash.
    Status move_to_trash(const std::string& tablet_path);

    static Status delete_tablet_parent_path_if_empty(const std::string& tablet_path);

private:
    Status _init_cluster_id();
    Status _init_capacity_and_create_shards();
    Status _init_meta();

    Status _check_disk();
    Status _read_and_write_test_file();
    // Check whether has old format (hdr_ start) in olap. When doris updating to current version,
    // it may lead to data missing. When conf::storage_strict_check_incompatible_old_format is true,
    // process will log fatal.
    Status _check_incompatible_old_format_tablet();

    int _path_gc_step {0};

    void _perform_tablet_gc(const std::string& tablet_schema_hash_path, int16_t shard_name);

    void _perform_rowset_gc(const std::string& tablet_schema_hash_path);

private:
    std::atomic<bool> _stop_bg_worker = false;

    StorageEngine& _engine;
    std::string _path;
    size_t _path_hash;

    // the actual available capacity of the disk of this data dir
    size_t _available_bytes;
    // the actual capacity of the disk of this data dir
    size_t _disk_capacity_bytes;
    size_t _trash_used_bytes;
    TStorageMedium::type _storage_medium;
    bool _is_used;

    int32_t _cluster_id;
    bool _cluster_id_incomplete = false;
    // This flag will be set true if this store was not in root path when reloading
    bool _to_be_deleted;

    static constexpr uint64_t MAX_SHARD_NUM = 1024;
    std::atomic<uint64_t> _current_shard {0};
    // used to protect and _tablet_set
    mutable std::mutex _mutex;
    std::set<TabletInfo> _tablet_set;

    OlapMeta* _meta = nullptr;

    std::shared_ptr<MetricEntity> _data_dir_metric_entity;
    IntGauge* disks_total_capacity = nullptr;
    IntGauge* disks_avail_capacity = nullptr;
    IntGauge* disks_local_used_capacity = nullptr;
    IntGauge* disks_remote_used_capacity = nullptr;
    IntGauge* disks_trash_used_capacity = nullptr;
    IntGauge* disks_state = nullptr;
    IntGauge* disks_compaction_score = nullptr;
    IntGauge* disks_compaction_num = nullptr;
};

} // namespace doris
