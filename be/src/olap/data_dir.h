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
#include "io/fs/file_system.h"
#include "olap/olap_common.h"
#include "util/metrics.h"

namespace doris {

class Tablet;
class TabletManager;
class TxnManager;
class OlapMeta;
class RowsetIdGenerator;

// A DataDir used to manage data in same path.
// Now, After DataDir was created, it will never be deleted for easy implementation.
class DataDir {
public:
    DataDir(const std::string& path, int64_t capacity_bytes = -1,
            TStorageMedium::type storage_medium = TStorageMedium::HDD,
            TabletManager* tablet_manager = nullptr, TxnManager* txn_manager = nullptr);
    ~DataDir();

    Status init();
    void stop_bg_worker();

    const std::string& path() const { return _path; }
    size_t path_hash() const { return _path_hash; }

    const io::FileSystemSPtr& fs() const { return _fs; }

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

    Status get_shard(uint64_t* shard);

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

    void add_pending_ids(const std::string& id);

    void remove_pending_ids(const std::string& id);

    // this function scans the paths in data dir to collect the paths to check
    // this is a producer function. After scan, it will notify the perform_path_gc function to gc
    Status perform_path_scan();

    void perform_path_gc_by_rowsetid();

    void perform_path_gc_by_tablet();

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

    size_t disk_capacity() const;

    size_t disk_available() const;

    size_t tablet_num() const;

    void disks_compaction_score_increment(int64_t delta);

    void disks_compaction_num_increment(int64_t delta);

    // Move tablet to trash.
    Status move_to_trash(const std::string& tablet_path);

private:
    Status _init_cluster_id();
    Status _init_capacity_and_create_shards();
    Status _init_meta();

    Status _check_disk();
    Status _read_and_write_test_file();
    Status read_cluster_id(const std::string& cluster_id_path, int32_t* cluster_id);
    Status _write_cluster_id_to_path(const std::string& path, int32_t cluster_id);
    // Check whether has old format (hdr_ start) in olap. When doris updating to current version,
    // it may lead to data missing. When conf::storage_strict_check_incompatible_old_format is true,
    // process will log fatal.
    Status _check_incompatible_old_format_tablet();

    void _process_garbage_path(const std::string& path);

    void _remove_check_paths(const std::set<std::string>& paths);

    bool _check_pending_ids(const std::string& id);

private:
    std::atomic<bool> _stop_bg_worker = false;

    std::string _path;
    size_t _path_hash;

    io::FileSystemSPtr _fs;
    // the actual available capacity of the disk of this data dir
    size_t _available_bytes;
    // the actual capacity of the disk of this data dir
    size_t _disk_capacity_bytes;
    size_t _trash_used_bytes;
    TStorageMedium::type _storage_medium;
    bool _is_used;

    TabletManager* _tablet_manager;
    TxnManager* _txn_manager;
    int32_t _cluster_id;
    bool _cluster_id_incomplete = false;
    // This flag will be set true if this store was not in root path when reloading
    bool _to_be_deleted;

    // used to protect _current_shard and _tablet_set
    mutable std::mutex _mutex;
    uint64_t _current_shard;
    std::set<TabletInfo> _tablet_set;

    static const uint32_t MAX_SHARD_NUM = 1024;

    OlapMeta* _meta = nullptr;
    RowsetIdGenerator* _id_generator = nullptr;

    std::mutex _check_path_mutex;
    std::condition_variable _check_path_cv;
    std::set<std::string> _all_check_paths;
    std::set<std::string> _all_tablet_schemahash_paths;

    mutable std::shared_mutex _pending_path_mutex;
    std::set<std::string> _pending_path_ids;

    std::shared_ptr<MetricEntity> _data_dir_metric_entity;
    IntGauge* disks_total_capacity;
    IntGauge* disks_avail_capacity;
    IntGauge* disks_local_used_capacity;
    IntGauge* disks_remote_used_capacity;
    IntGauge* disks_trash_used_capacity;
    IntGauge* disks_state;
    IntGauge* disks_compaction_score;
    IntGauge* disks_compaction_num;
};

} // namespace doris
