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

#include <butil/macros.h>
#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/Types_types.h>
#include <stddef.h>
#include <stdint.h>

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"

namespace doris {

class DataDir;
class CumulativeCompactionPolicy;
class MemTracker;
class TCreateTabletReq;
class TTablet;
class TTabletInfo;

// TabletManager provides get, add, delete tablet method for storage engine
// NOTE: If you want to add a method that needs to hold meta-lock before you can call it,
// please uniformly name the method in "xxx_unlocked()" mode
class TabletManager {
public:
    TabletManager(StorageEngine& engine, int32_t tablet_map_lock_shard_size);
    ~TabletManager();

    bool check_tablet_id_exist(TTabletId tablet_id);

    // The param stores holds all candidate data_dirs for this tablet.
    // NOTE: If the request is from a schema-changing tablet, The directory selected by the
    // new tablet should be the same as the directory of origin tablet. Because the
    // linked-schema-change type requires Linux hard-link, which does not support cross disk.
    // TODO(lingbin): Other schema-change type do not need to be on the same disk. Because
    // there may be insufficient space on the current disk, which will lead the schema-change
    // task to be fail, even if there is enough space on other disks
    Status create_tablet(const TCreateTabletReq& request, std::vector<DataDir*> stores,
                         RuntimeProfile* profile);

    // Drop a tablet by description.
    // If `is_drop_table_or_partition` is true, we need to remove all remote rowsets in this tablet.
    Status drop_tablet(TTabletId tablet_id, TReplicaId replica_id, bool is_drop_table_or_partition);

    // Find two tablets.
    // One with the highest score to execute single compaction,
    // the other with the highest score to execute cumu or base compaction.
    // Single compaction needs to be completed successfully after the peer completes it.
    // We need to generate two types of tasks separately to avoid continuously generating
    // single compaction tasks for the tablet.
    std::vector<TabletSharedPtr> find_best_tablets_to_compaction(
            CompactionType compaction_type, DataDir* data_dir,
            const std::unordered_set<TabletSharedPtr>& tablet_submitted_compaction, uint32_t* score,
            const std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>&
                    all_cumulative_compaction_policies);

    TabletSharedPtr get_tablet(TTabletId tablet_id, bool include_deleted = false,
                               std::string* err = nullptr);

    TabletSharedPtr get_tablet(TTabletId tablet_id, TabletUid tablet_uid,
                               bool include_deleted = false, std::string* err = nullptr);

    std::vector<TabletSharedPtr> get_all_tablet(
            std::function<bool(Tablet*)>&& filter = filter_used_tablets);

    // Handler not hold the shard lock.
    void for_each_tablet(std::function<void(const TabletSharedPtr&)>&& handler,
                         std::function<bool(Tablet*)>&& filter = filter_used_tablets);

    static bool filter_all_tablets(Tablet* tablet) { return true; }
    static bool filter_used_tablets(Tablet* tablet) { return tablet->is_used(); }

    uint64_t get_rowset_nums();
    uint64_t get_segment_nums();

    // Extract tablet_id and schema_hash from given path.
    //
    // The normal path pattern is like "/data/{shard_id}/{tablet_id}/{schema_hash}/xxx.data".
    // Besides that, this also support empty tablet path, which path looks like
    // "/data/{shard_id}/{tablet_id}"
    //
    // Return true when the path matches the path pattern, and tablet_id and schema_hash is
    // saved in input params. When input path is an empty tablet directory, schema_hash will
    // be set to 0. Return false if the path don't match valid pattern.
    static bool get_tablet_id_and_schema_hash_from_path(const std::string& path,
                                                        TTabletId* tablet_id,
                                                        TSchemaHash* schema_hash);

    static bool get_rowset_id_from_path(const std::string& path, RowsetId* rowset_id);

    void get_tablet_stat(TTabletStatResult* result);

    // parse tablet header msg to generate tablet object
    // - restore: whether the request is from restore tablet action,
    //   where we should change tablet status from shutdown back to running
    Status load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id, TSchemaHash schema_hash,
                                 std::string_view header, bool update_meta, bool force = false,
                                 bool restore = false, bool check_path = true);

    Status load_tablet_from_dir(DataDir* data_dir, TTabletId tablet_id, SchemaHash schema_hash,
                                const std::string& schema_hash_path, bool force = false,
                                bool restore = false);

    // 获取所有tables的名字
    //
    // Return OK, if run ok
    //        Status::Error<INVALID_ARGUMENT>(), if tables is null
    Status report_tablet_info(TTabletInfo* tablet_info);

    void build_all_report_tablets_info(std::map<TTabletId, TTablet>* tablets_info);

    Status start_trash_sweep();

    void try_delete_unused_tablet_path(DataDir* data_dir, TTabletId tablet_id,
                                       SchemaHash schema_hash, const std::string& schema_hash_path,
                                       int16_t shard_id);

    void update_root_path_info(std::map<std::string, DataDirInfo>* path_map,
                               size_t* tablet_counter);

    void get_partition_related_tablets(int64_t partition_id, std::set<TabletInfo>* tablet_infos);

    void get_partitions_visible_version(std::map<int64_t, int64_t>* partitions_version);

    void update_partitions_visible_version(const std::map<int64_t, int64_t>& partitions_version);

    void do_tablet_meta_checkpoint(DataDir* data_dir);

    void obtain_specific_quantity_tablets(std::vector<TabletInfo>& tablets_info, int64_t num);

    // return `true` if register success
    Status register_transition_tablet(int64_t tablet_id, std::string reason);
    void unregister_transition_tablet(int64_t tablet_id, std::string reason);

    void get_tablets_distribution_on_different_disks(
            std::map<int64_t, std::map<DataDir*, int64_t>>& tablets_num_on_disk,
            std::map<int64_t, std::map<DataDir*, std::vector<TabletSize>>>& tablets_info_on_disk);
    void get_cooldown_tablets(std::vector<TabletSharedPtr>* tables,
                              std::vector<RowsetSharedPtr>* rowsets,
                              std::function<bool(const TabletSharedPtr&)> skip_tablet);

    void get_all_tablets_storage_format(TCheckStorageFormatResult* result);

    std::set<int64_t> check_all_tablet_segment(bool repair);

    bool update_tablet_partition_id(::doris::TPartitionId partition_id,
                                    ::doris::TTabletId tablet_id);

private:
    // Add a tablet pointer to StorageEngine
    // If force, drop the existing tablet add this new one
    //
    // Return OK, if run ok
    //        OLAP_ERR_TABLE_INSERT_DUPLICATION_ERROR, if find duplication
    //        Status::Error<UNINITIALIZED>(), if not inited
    Status _add_tablet_unlocked(TTabletId tablet_id, const TabletSharedPtr& tablet,
                                bool update_meta, bool force, RuntimeProfile* profile);

    Status _add_tablet_to_map_unlocked(TTabletId tablet_id, const TabletSharedPtr& tablet,
                                       bool update_meta, bool keep_files, bool drop_old,
                                       RuntimeProfile* profile);

    bool _check_tablet_id_exist_unlocked(TTabletId tablet_id);

    Status _drop_tablet_unlocked(TTabletId tablet_id, TReplicaId replica_id, bool keep_files,
                                 bool is_drop_table_or_partition);

    TabletSharedPtr _get_tablet_unlocked(TTabletId tablet_id);
    TabletSharedPtr _get_tablet_unlocked(TTabletId tablet_id, bool include_deleted,
                                         std::string* err);

    TabletSharedPtr _internal_create_tablet_unlocked(const TCreateTabletReq& request,
                                                     const bool is_schema_change,
                                                     const Tablet* base_tablet,
                                                     const std::vector<DataDir*>& data_dirs,
                                                     RuntimeProfile* profile);
    TabletSharedPtr _create_tablet_meta_and_dir_unlocked(const TCreateTabletReq& request,
                                                         const bool is_schema_change,
                                                         const Tablet* base_tablet,
                                                         const std::vector<DataDir*>& data_dirs,
                                                         RuntimeProfile* profile);
    Status _create_tablet_meta_unlocked(const TCreateTabletReq& request, DataDir* store,
                                        const bool is_schema_change_tablet,
                                        const Tablet* base_tablet,
                                        TabletMetaSharedPtr* tablet_meta);

    void _add_tablet_to_partition(const TabletSharedPtr& tablet);

    void _remove_tablet_from_partition(const TabletSharedPtr& tablet);

    std::shared_mutex& _get_tablets_shard_lock(TTabletId tabletId);

    bool _move_tablet_to_trash(const TabletSharedPtr& tablet);

private:
    DISALLOW_COPY_AND_ASSIGN(TabletManager);

    using tablet_map_t = std::unordered_map<int64_t, TabletSharedPtr>;

    struct tablets_shard {
        tablets_shard() = default;
        tablets_shard(tablets_shard&& shard) {
            tablet_map = std::move(shard.tablet_map);
            tablets_under_transition = std::move(shard.tablets_under_transition);
        }
        mutable std::shared_mutex lock;
        tablet_map_t tablet_map;
        std::mutex lock_for_transition;
        // tablet do clone, path gc, move to trash, disk migrate will record in tablets_under_transition
        // tablet <reason, thread_id, lock_times>
        std::map<int64_t, std::tuple<std::string, std::thread::id, int64_t>>
                tablets_under_transition;
    };

    struct Partition {
        std::set<TabletInfo> tablets;
        std::shared_ptr<VersionWithTime> visible_version {new VersionWithTime};
    };

    StorageEngine& _engine;

    // TODO: memory size of TabletSchema cannot be accurately tracked.
    std::shared_ptr<MemTracker> _tablet_meta_mem_tracker;

    const int32_t _tablets_shards_size;
    const int32_t _tablets_shards_mask;
    std::vector<tablets_shard> _tablets_shards;

    // Protect _partitions, should not be obtained before _tablet_map_lock to avoid dead lock
    std::shared_mutex _partitions_lock;
    // partition_id => partition
    std::map<int64_t, Partition> _partitions;

    // Protect _shutdown_tablets, should not be obtained before _tablet_map_lock to avoid dead lock
    std::shared_mutex _shutdown_tablets_lock;
    // the delete tablets. notice only allow function `start_trash_sweep` can erase tablets in _shutdown_tablets
    std::list<TabletSharedPtr> _shutdown_tablets;
    std::mutex _gc_tablets_lock;

    std::mutex _tablet_stat_cache_mutex;
    std::shared_ptr<std::vector<TTabletStat>> _tablet_stat_list_cache =
            std::make_shared<std::vector<TTabletStat>>();

    tablet_map_t& _get_tablet_map(TTabletId tablet_id);

    tablets_shard& _get_tablets_shard(TTabletId tabletId);

    std::mutex _two_tablet_mtx;
};

} // namespace doris
