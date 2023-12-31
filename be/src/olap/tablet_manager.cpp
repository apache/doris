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

#include "olap/tablet_manager.h"

#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <re2/re2.h>
#include <unistd.h>

#include <algorithm>
#include <list>
#include <mutex>
#include <ostream>

#include "bvar/bvar.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "gutil/integral_types.h"
#include "gutil/strings/strcat.h"
#include "gutil/strings/substitute.h"
#include "io/fs/local_file_system.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/pb_helper.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/tablet_schema.h"
#include "olap/txn_manager.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/histogram.h"
#include "util/metrics.h"
#include "util/path_util.h"
#include "util/scoped_cleanup.h"
#include "util/stopwatch.hpp"
#include "util/time.h"
#include "util/trace.h"
#include "util/uid_util.h"

namespace doris {
class CumulativeCompactionPolicy;
} // namespace doris

using std::map;
using std::set;
using std::string;
using std::vector;

namespace doris {
using namespace ErrorCode;

DEFINE_GAUGE_METRIC_PROTOTYPE_5ARG(tablet_meta_mem_consumption, MetricUnit::BYTES, "",
                                   mem_consumption, Labels({{"type", "tablet_meta"}}));

bvar::Adder<int64_t> g_tablet_meta_schema_columns_count("tablet_meta_schema_columns_count");

TabletManager::TabletManager(int32_t tablet_map_lock_shard_size)
        : _tablet_meta_mem_tracker(std::make_shared<MemTracker>(
                  "TabletMeta", ExecEnv::GetInstance()->experimental_mem_tracker())),
          _tablets_shards_size(tablet_map_lock_shard_size),
          _tablets_shards_mask(tablet_map_lock_shard_size - 1) {
    CHECK_GT(_tablets_shards_size, 0);
    CHECK_EQ(_tablets_shards_size & _tablets_shards_mask, 0);
    _tablets_shards.resize(_tablets_shards_size);
    REGISTER_HOOK_METRIC(tablet_meta_mem_consumption,
                         [this]() { return _tablet_meta_mem_tracker->consumption(); });
}

TabletManager::~TabletManager() {
    DEREGISTER_HOOK_METRIC(tablet_meta_mem_consumption);
}

Status TabletManager::_add_tablet_unlocked(TTabletId tablet_id, const TabletSharedPtr& tablet,
                                           bool update_meta, bool force, RuntimeProfile* profile) {
    if (profile->get_counter("AddTablet") == nullptr) {
        ADD_TIMER(profile, "AddTablet");
    }
    Status res = Status::OK();
    VLOG_NOTICE << "begin to add tablet to TabletManager. "
                << "tablet_id=" << tablet_id << ", force=" << force;

    TabletSharedPtr existed_tablet = nullptr;
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    const auto& iter = tablet_map.find(tablet_id);
    if (iter != tablet_map.end()) {
        existed_tablet = iter->second;
    }

    if (existed_tablet == nullptr) {
        return _add_tablet_to_map_unlocked(tablet_id, tablet, update_meta, false /*keep_files*/,
                                           false /*drop_old*/, profile);
    }
    // During restore process, the tablet is exist and snapshot loader will replace the tablet's rowsets
    // and then reload the tablet, the tablet's path will the same
    if (!force) {
        if (existed_tablet->tablet_path() == tablet->tablet_path()) {
            return Status::Error<ENGINE_INSERT_EXISTS_TABLE>(
                    "add the same tablet twice! tablet_id={}, tablet_path={}", tablet_id,
                    tablet->tablet_path());
        }
        if (existed_tablet->data_dir() == tablet->data_dir()) {
            return Status::Error<ENGINE_INSERT_EXISTS_TABLE>(
                    "add tablet with same data dir twice! tablet_id={}", tablet_id);
        }
    }

    MonotonicStopWatch watch;
    watch.start();

    // During storage migration, the tablet is moved to another disk, have to check
    // if the new tablet's rowset version is larger than the old one to prevent losting data during
    // migration
    int64_t old_time, new_time;
    int32_t old_version, new_version;
    {
        std::shared_lock rdlock(existed_tablet->get_header_lock());
        const RowsetSharedPtr old_rowset = existed_tablet->rowset_with_max_version();
        const RowsetSharedPtr new_rowset = tablet->rowset_with_max_version();
        // If new tablet is empty, it is a newly created schema change tablet.
        // the old tablet is dropped before add tablet. it should not exist old tablet
        if (new_rowset == nullptr) {
            // it seems useless to call unlock and return here.
            // it could prevent error when log level is changed in the future.
            return Status::Error<ENGINE_INSERT_EXISTS_TABLE>(
                    "new tablet is empty and old tablet exists. it should not happen. tablet_id={}",
                    tablet_id);
        }
        old_time = old_rowset == nullptr ? -1 : old_rowset->creation_time();
        new_time = new_rowset->creation_time();
        old_version = old_rowset == nullptr ? -1 : old_rowset->end_version();
        new_version = new_rowset->end_version();
    }
    COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "GetExistTabletVersion", "AddTablet"),
                   static_cast<int64_t>(watch.reset()));

    // In restore process, we replace all origin files in tablet dir with
    // the downloaded snapshot files. Then we try to reload tablet header.
    // force == true means we forcibly replace the Tablet in tablet_map
    // with the new one. But if we do so, the files in the tablet dir will be
    // dropped when the origin Tablet deconstruct.
    // So we set keep_files == true to not delete files when the
    // origin Tablet deconstruct.
    // During restore process, snapshot loader
    // replaced the old tablet's rowset with new rowsets, but the tablet path is reused, if drop files
    // here, the new rowset's file will also be dropped, so use keep files here
    bool keep_files = force;
    if (force ||
        (new_version > old_version || (new_version == old_version && new_time >= old_time))) {
        // check if new tablet's meta is in store and add new tablet's meta to meta store
        res = _add_tablet_to_map_unlocked(tablet_id, tablet, update_meta, keep_files,
                                          true /*drop_old*/, profile);
    } else {
        RETURN_IF_ERROR(tablet->set_tablet_state(TABLET_SHUTDOWN));
        tablet->save_meta();
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "SaveMeta", "AddTablet"),
                       static_cast<int64_t>(watch.reset()));
        {
            std::lock_guard<std::shared_mutex> shutdown_tablets_wrlock(_shutdown_tablets_lock);
            _shutdown_tablets.push_back(tablet);
        }

        res = Status::Error<ENGINE_INSERT_OLD_TABLET>(
                "set tablet to shutdown state. tablet_id={}, tablet_path={}", tablet->tablet_id(),
                tablet->tablet_path());
    }
    LOG(WARNING) << "add duplicated tablet. force=" << force << ", res=" << res
                 << ", tablet_id=" << tablet_id << ", old_version=" << old_version
                 << ", new_version=" << new_version << ", old_time=" << old_time
                 << ", new_time=" << new_time
                 << ", old_tablet_path=" << existed_tablet->tablet_path()
                 << ", new_tablet_path=" << tablet->tablet_path();

    return res;
}

Status TabletManager::_add_tablet_to_map_unlocked(TTabletId tablet_id,
                                                  const TabletSharedPtr& tablet, bool update_meta,
                                                  bool keep_files, bool drop_old,
                                                  RuntimeProfile* profile) {
    // check if new tablet's meta is in store and add new tablet's meta to meta store
    Status res = Status::OK();
    MonotonicStopWatch watch;
    watch.start();
    if (update_meta) {
        // call tablet save meta in order to valid the meta
        tablet->save_meta();
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "SaveMeta", "AddTablet"),
                       static_cast<int64_t>(watch.reset()));
    }
    if (drop_old) {
        // If the new tablet is fresher than the existing one, then replace
        // the existing tablet with the new one.
        // Use default replica_id to ignore whether replica_id is match when drop tablet.
        Status status = _drop_tablet_unlocked(tablet_id, /* replica_id */ 0, keep_files, false);
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "DropOldTablet", "AddTablet"),
                       static_cast<int64_t>(watch.reset()));
        RETURN_NOT_OK_STATUS_WITH_WARN(
                status, strings::Substitute("failed to drop old tablet when add new tablet. "
                                            "tablet_id=$0",
                                            tablet_id));
    }
    // Register tablet into DataDir, so that we can manage tablet from
    // the perspective of root path.
    // Example: unregister all tables when a bad disk found.
    tablet->register_tablet_into_dir();
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map[tablet_id] = tablet;
    _add_tablet_to_partition(tablet);
    // TODO: remove multiply 2 of tablet meta mem size
    // Because table schema will copy in tablet, there will be double mem cost
    // so here multiply 2
    _tablet_meta_mem_tracker->consume(tablet->tablet_meta()->mem_size() * 2);
    g_tablet_meta_schema_columns_count << tablet->tablet_meta()->tablet_columns_num();
    COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "RegisterTabletInfo", "AddTablet"),
                   static_cast<int64_t>(watch.reset()));

    VLOG_NOTICE << "add tablet to map successfully."
                << " tablet_id=" << tablet_id;

    return res;
}

bool TabletManager::check_tablet_id_exist(TTabletId tablet_id) {
    std::shared_lock rdlock(_get_tablets_shard_lock(tablet_id));
    return _check_tablet_id_exist_unlocked(tablet_id);
}

bool TabletManager::_check_tablet_id_exist_unlocked(TTabletId tablet_id) {
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    return tablet_map.find(tablet_id) != tablet_map.end();
}

Status TabletManager::create_tablet(const TCreateTabletReq& request, std::vector<DataDir*> stores,
                                    RuntimeProfile* profile) {
    DorisMetrics::instance()->create_tablet_requests_total->increment(1);

    int64_t tablet_id = request.tablet_id;
    LOG(INFO) << "begin to create tablet. tablet_id=" << tablet_id
              << ", table_id=" << request.table_id << ", partition_id=" << request.partition_id
              << ", replica_id=" << request.replica_id;

    // when we create rollup tablet A(assume on shard-1) from tablet B(assume on shard-2)
    // we need use write lock on shard-1 and then use read lock on shard-2
    // if there have create rollup tablet C(assume on shard-2) from tablet D(assume on shard-1) at the same time, we will meet deadlock
    std::unique_lock two_tablet_lock(_two_tablet_mtx, std::defer_lock);
    bool is_schema_change = request.__isset.base_tablet_id && request.base_tablet_id > 0;
    bool need_two_lock = is_schema_change && ((_tablets_shards_mask & request.base_tablet_id) !=
                                              (_tablets_shards_mask & tablet_id));
    if (need_two_lock) {
        SCOPED_TIMER(ADD_TIMER(profile, "GetTwoTableLock"));
        two_tablet_lock.lock();
    }

    MonotonicStopWatch shard_lock_watch;
    shard_lock_watch.start();
    std::lock_guard wrlock(_get_tablets_shard_lock(tablet_id));
    shard_lock_watch.stop();
    COUNTER_UPDATE(ADD_TIMER(profile, "GetShardLock"),
                   static_cast<int64_t>(shard_lock_watch.elapsed_time()));
    // Make create_tablet operation to be idempotent:
    // 1. Return true if tablet with same tablet_id and schema_hash exist;
    //           false if tablet with same tablet_id but different schema_hash exist.
    // 2. When this is an alter task, if the tablet(both tablet_id and schema_hash are
    // same) already exist, then just return true(an duplicate request). But if
    // tablet_id exist but with different schema_hash, return an error(report task will
    // eventually trigger its deletion).
    {
        SCOPED_TIMER(ADD_TIMER(profile, "GetTabletUnlocked"));
        if (_get_tablet_unlocked(tablet_id) != nullptr) {
            LOG(INFO) << "success to create tablet. tablet already exist. tablet_id=" << tablet_id;
            return Status::OK();
        }
    }

    TabletSharedPtr base_tablet = nullptr;
    // If the CreateTabletReq has base_tablet_id then it is a alter-tablet request
    if (is_schema_change) {
        // if base_tablet_id's lock diffrent with new_tablet_id, we need lock it.
        if (need_two_lock) {
            SCOPED_TIMER(ADD_TIMER(profile, "GetBaseTablet"));
            base_tablet = get_tablet(request.base_tablet_id);
            two_tablet_lock.unlock();
        } else {
            SCOPED_TIMER(ADD_TIMER(profile, "GetBaseTabletUnlocked"));
            base_tablet = _get_tablet_unlocked(request.base_tablet_id);
        }
        if (base_tablet == nullptr) {
            DorisMetrics::instance()->create_tablet_requests_failed->increment(1);
            return Status::Error<TABLE_CREATE_META_ERROR>(
                    "fail to create tablet(change schema), base tablet does not exist. "
                    "new_tablet_id={}, base_tablet_id={}",
                    tablet_id, request.base_tablet_id);
        }
        // If we are doing schema-change, we should use the same data dir
        // TODO(lingbin): A litter trick here, the directory should be determined before
        // entering this method
        if (request.storage_medium == base_tablet->data_dir()->storage_medium()) {
            stores.clear();
            stores.push_back(base_tablet->data_dir());
        }
    }

    // set alter type to schema-change. it is useless
    TabletSharedPtr tablet = _internal_create_tablet_unlocked(request, is_schema_change,
                                                              base_tablet.get(), stores, profile);
    if (tablet == nullptr) {
        DorisMetrics::instance()->create_tablet_requests_failed->increment(1);
        return Status::Error<CE_CMD_PARAMS_ERROR>("fail to create tablet. tablet_id={}",
                                                  request.tablet_id);
    }

    LOG(INFO) << "success to create tablet. tablet_id=" << tablet_id;
    return Status::OK();
}

TabletSharedPtr TabletManager::_internal_create_tablet_unlocked(
        const TCreateTabletReq& request, const bool is_schema_change, const Tablet* base_tablet,
        const std::vector<DataDir*>& data_dirs, RuntimeProfile* profile) {
    // If in schema-change state, base_tablet must also be provided.
    // i.e., is_schema_change and base_tablet are either assigned or not assigned
    DCHECK((is_schema_change && base_tablet) || (!is_schema_change && !base_tablet));

    // NOTE: The existence of tablet_id and schema_hash has already been checked,
    // no need check again here.

    const std::string parent_timer_name = "InternalCreateTablet";
    SCOPED_TIMER(ADD_TIMER(profile, parent_timer_name));

    MonotonicStopWatch watch;
    watch.start();
    auto create_meta_timer = ADD_CHILD_TIMER(profile, "CreateMeta", parent_timer_name);
    auto tablet = _create_tablet_meta_and_dir_unlocked(request, is_schema_change, base_tablet,
                                                       data_dirs, profile);
    COUNTER_UPDATE(create_meta_timer, static_cast<int64_t>(watch.reset()));
    if (tablet == nullptr) {
        return nullptr;
    }

    int64_t new_tablet_id = request.tablet_id;
    int32_t new_schema_hash = request.tablet_schema.schema_hash;

    // should remove the tablet's pending_id no matter create-tablet success or not
    DataDir* data_dir = tablet->data_dir();

    // TODO(yiguolei)
    // the following code is very difficult to understand because it mixed alter tablet v2
    // and alter tablet v1 should remove alter tablet v1 code after v0.12
    Status res = Status::OK();
    bool is_tablet_added = false;
    do {
        res = tablet->init();
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "TabletInit", parent_timer_name),
                       static_cast<int64_t>(watch.reset()));
        if (!res.ok()) {
            LOG(WARNING) << "tablet init failed. tablet:" << tablet->tablet_id();
            break;
        }

        // Create init version if this is not a restore mode replica and request.version is set
        // bool in_restore_mode = request.__isset.in_restore_mode && request.in_restore_mode;
        // if (!in_restore_mode && request.__isset.version) {
        // create initial rowset before add it to storage engine could omit many locks
        res = tablet->create_initial_rowset(request.version);
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "InitRowset", parent_timer_name),
                       static_cast<int64_t>(watch.reset()));
        if (!res.ok()) {
            LOG(WARNING) << "fail to create initial version for tablet. res=" << res;
            break;
        }

        if (is_schema_change) {
            // if this is a new alter tablet, has to set its state to not ready
            // because schema change handler depends on it to check whether history data
            // convert finished
            static_cast<void>(tablet->set_tablet_state(TabletState::TABLET_NOTREADY));
        }
        // Add tablet to StorageEngine will make it visible to user
        // Will persist tablet meta
        auto add_tablet_timer = ADD_CHILD_TIMER(profile, "AddTablet", parent_timer_name);
        res = _add_tablet_unlocked(new_tablet_id, tablet, /*update_meta*/ true, false, profile);
        COUNTER_UPDATE(add_tablet_timer, static_cast<int64_t>(watch.reset()));
        if (!res.ok()) {
            LOG(WARNING) << "fail to add tablet to StorageEngine. res=" << res;
            break;
        }
        is_tablet_added = true;

        // TODO(lingbin): The following logic seems useless, can be removed?
        // Because if _add_tablet_unlocked() return OK, we must can get it from map.
        TabletSharedPtr tablet_ptr = _get_tablet_unlocked(new_tablet_id);
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "GetTablet", parent_timer_name),
                       static_cast<int64_t>(watch.reset()));
        if (tablet_ptr == nullptr) {
            res = Status::Error<TABLE_NOT_FOUND>("fail to get tablet. res={}", res);
            break;
        }
    } while (false);

    if (res.ok()) {
        return tablet;
    }
    // something is wrong, we need clear environment
    if (is_tablet_added) {
        Status status = _drop_tablet_unlocked(new_tablet_id, request.replica_id, false, false);
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "DropTablet", parent_timer_name),
                       static_cast<int64_t>(watch.reset()));
        if (!status.ok()) {
            LOG(WARNING) << "fail to drop tablet when create tablet failed. res=" << res;
        }
    } else {
        tablet->delete_all_files();
        static_cast<void>(TabletMetaManager::remove(data_dir, new_tablet_id, new_schema_hash));
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "RemoveTabletFiles", parent_timer_name),
                       static_cast<int64_t>(watch.reset()));
    }
    return nullptr;
}

static string _gen_tablet_dir(const string& dir, int16_t shard_id, int64_t tablet_id) {
    string path = dir;
    path = path_util::join_path_segments(path, DATA_PREFIX);
    path = path_util::join_path_segments(path, std::to_string(shard_id));
    path = path_util::join_path_segments(path, std::to_string(tablet_id));
    return path;
}

TabletSharedPtr TabletManager::_create_tablet_meta_and_dir_unlocked(
        const TCreateTabletReq& request, const bool is_schema_change, const Tablet* base_tablet,
        const std::vector<DataDir*>& data_dirs, RuntimeProfile* profile) {
    string pending_id = StrCat(TABLET_ID_PREFIX, request.tablet_id);
    // Many attempts are made here in the hope that even if a disk fails, it can still continue.
    std::string parent_timer_name = "CreateMeta";
    MonotonicStopWatch watch;
    watch.start();
    for (auto& data_dir : data_dirs) {
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "RemovePendingIds", parent_timer_name),
                       static_cast<int64_t>(watch.reset()));

        TabletMetaSharedPtr tablet_meta;
        // if create meta failed, do not need to clean dir, because it is only in memory
        Status res = _create_tablet_meta_unlocked(request, data_dir, is_schema_change, base_tablet,
                                                  &tablet_meta);
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "CreateMetaUnlock", parent_timer_name),
                       static_cast<int64_t>(watch.reset()));
        if (!res.ok()) {
            LOG(WARNING) << "fail to create tablet meta. res=" << res
                         << ", root=" << data_dir->path();
            continue;
        }

        string tablet_dir =
                _gen_tablet_dir(data_dir->path(), tablet_meta->shard_id(), request.tablet_id);
        string schema_hash_dir = path_util::join_path_segments(
                tablet_dir, std::to_string(request.tablet_schema.schema_hash));

        // Because the tablet is removed asynchronously, so that the dir may still exist when BE
        // receive create-tablet request again, For example retried schema-change request
        bool exists = true;
        res = io::global_local_filesystem()->exists(schema_hash_dir, &exists);
        if (!res.ok()) {
            continue;
        }
        if (exists) {
            LOG(WARNING) << "skip this dir because tablet path exist, path=" << schema_hash_dir;
            continue;
        } else {
            Status st = io::global_local_filesystem()->create_directory(schema_hash_dir);
            if (!st.ok()) {
                continue;
            }
        }

        if (tablet_meta->partition_id() <= 0) {
            LOG(WARNING) << "invalid partition id " << tablet_meta->partition_id() << ", tablet "
                         << tablet_meta->tablet_id();
        }
        TabletSharedPtr new_tablet = std::make_shared<Tablet>(std::move(tablet_meta), data_dir);
        COUNTER_UPDATE(ADD_CHILD_TIMER(profile, "CreateTabletFromMeta", parent_timer_name),
                       static_cast<int64_t>(watch.reset()));
        return new_tablet;
    }
    return nullptr;
}

Status TabletManager::drop_tablet(TTabletId tablet_id, TReplicaId replica_id,
                                  bool is_drop_table_or_partition) {
    auto& shard = _get_tablets_shard(tablet_id);
    std::lock_guard wrlock(shard.lock);
    if (shard.tablets_under_clone.count(tablet_id) > 0) {
        return Status::Aborted("tablet {} is under clone, skip drop task", tablet_id);
    }
    return _drop_tablet_unlocked(tablet_id, replica_id, false, is_drop_table_or_partition);
}

// Drop specified tablet.
Status TabletManager::_drop_tablet_unlocked(TTabletId tablet_id, TReplicaId replica_id,
                                            bool keep_files, bool is_drop_table_or_partition) {
    LOG(INFO) << "begin drop tablet. tablet_id=" << tablet_id << ", replica_id=" << replica_id
              << ", is_drop_table_or_partition=" << is_drop_table_or_partition;
    DorisMetrics::instance()->drop_tablet_requests_total->increment(1);

    // Fetch tablet which need to be dropped
    TabletSharedPtr to_drop_tablet = _get_tablet_unlocked(tablet_id);
    if (to_drop_tablet == nullptr) {
        LOG(WARNING) << "fail to drop tablet because it does not exist. "
                     << "tablet_id=" << tablet_id;
        return Status::OK();
    }
    // We should compare replica id to avoid dropping new cloned tablet.
    // Iff request replica id is 0, FE may be an older release, then we drop this tablet as before.
    if (to_drop_tablet->replica_id() != replica_id && replica_id != 0) {
        return Status::Aborted("replica_id not match({} vs {})", to_drop_tablet->replica_id(),
                               replica_id);
    }
    _remove_tablet_from_partition(to_drop_tablet);
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map.erase(tablet_id);
    {
        std::shared_lock rlock(to_drop_tablet->get_header_lock());
        static auto recycle_segment_cache = [](const auto& rowset_map) {
            for (auto& [_, rowset] : rowset_map) {
                // If the tablet was deleted, it need to remove all rowsets fds directly
                SegmentLoader::instance()->erase_segments(rowset->rowset_id(),
                                                          rowset->num_segments());
            }
        };
        recycle_segment_cache(to_drop_tablet->rowset_map());
        recycle_segment_cache(to_drop_tablet->stale_rowset_map());
    }
    if (!keep_files) {
        // drop tablet will update tablet meta, should lock
        std::lock_guard<std::shared_mutex> wrlock(to_drop_tablet->get_header_lock());
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        LOG(INFO) << "set tablet to shutdown state and remove it from memory. "
                  << "tablet_id=" << tablet_id << ", tablet_path=" << to_drop_tablet->tablet_path();
        // NOTE: has to update tablet here, but must not update tablet meta directly.
        // because other thread may hold the tablet object, they may save meta too.
        // If update meta directly here, other thread may override the meta
        // and the tablet will be loaded at restart time.
        // To avoid this exception, we first set the state of the tablet to `SHUTDOWN`.
        RETURN_IF_ERROR(to_drop_tablet->set_tablet_state(TABLET_SHUTDOWN));
        // We must record unused remote rowsets path info to OlapMeta before tablet state is marked as TABLET_SHUTDOWN in OlapMeta,
        // otherwise if BE shutdown after saving tablet state, these remote rowsets path info will lost.
        if (is_drop_table_or_partition) {
            RETURN_IF_ERROR(to_drop_tablet->remove_all_remote_rowsets());
        }
        to_drop_tablet->save_meta();
        {
            std::lock_guard<std::shared_mutex> wrdlock(_shutdown_tablets_lock);
            _shutdown_tablets.push_back(to_drop_tablet);
        }
    }

    to_drop_tablet->deregister_tablet_from_dir();
    _tablet_meta_mem_tracker->release(to_drop_tablet->tablet_meta()->mem_size() * 2);
    g_tablet_meta_schema_columns_count << -to_drop_tablet->tablet_meta()->tablet_columns_num();
    return Status::OK();
}

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, bool include_deleted, string* err) {
    std::shared_lock rdlock(_get_tablets_shard_lock(tablet_id));
    return _get_tablet_unlocked(tablet_id, include_deleted, err);
}

std::vector<TabletSharedPtr> TabletManager::get_all_tablet(std::function<bool(Tablet*)>&& filter) {
    std::vector<TabletSharedPtr> res;
    for_each_tablet([&](const TabletSharedPtr& tablet) { res.emplace_back(tablet); },
                    std::move(filter));
    return res;
}

void TabletManager::for_each_tablet(std::function<void(const TabletSharedPtr&)>&& handler,
                                    std::function<bool(Tablet*)>&& filter) {
    std::vector<TabletSharedPtr> tablets;
    for (const auto& tablets_shard : _tablets_shards) {
        tablets.clear();
        {
            std::shared_lock rdlock(tablets_shard.lock);
            for (const auto& [id, tablet] : tablets_shard.tablet_map) {
                if (filter(tablet.get())) {
                    tablets.emplace_back(tablet);
                }
            }
        }
        for (const auto& tablet : tablets) {
            handler(tablet);
        }
    }
}

TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id, bool include_deleted,
                                                    string* err) {
    TabletSharedPtr tablet;
    tablet = _get_tablet_unlocked(tablet_id);
    if (tablet == nullptr && include_deleted) {
        std::shared_lock rdlock(_shutdown_tablets_lock);
        for (auto& deleted_tablet : _shutdown_tablets) {
            CHECK(deleted_tablet != nullptr) << "deleted tablet is nullptr";
            if (deleted_tablet->tablet_id() == tablet_id) {
                tablet = deleted_tablet;
                break;
            }
        }
    }

    if (tablet == nullptr) {
        if (err != nullptr) {
            *err = "tablet does not exist. " + BackendOptions::get_localhost();
        }
        return nullptr;
    }
#ifndef BE_TEST
    if (!tablet->is_used()) {
        LOG(WARNING) << "tablet cannot be used. tablet=" << tablet_id;
        if (err != nullptr) {
            *err = "tablet cannot be used. " + BackendOptions::get_localhost();
        }
        return nullptr;
    }
#endif

    return tablet;
}

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, TabletUid tablet_uid,
                                          bool include_deleted, string* err) {
    std::shared_lock rdlock(_get_tablets_shard_lock(tablet_id));
    TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, include_deleted, err);
    if (tablet != nullptr && tablet->tablet_uid() == tablet_uid) {
        return tablet;
    }
    return nullptr;
}

uint64_t TabletManager::get_rowset_nums() {
    uint64_t rowset_nums = 0;
    for_each_tablet([&](const TabletSharedPtr& tablet) { rowset_nums += tablet->version_count(); },
                    filter_all_tablets);
    return rowset_nums;
}

uint64_t TabletManager::get_segment_nums() {
    uint64_t segment_nums = 0;
    for_each_tablet([&](const TabletSharedPtr& tablet) { segment_nums += tablet->segment_count(); },
                    filter_all_tablets);
    return segment_nums;
}

bool TabletManager::get_tablet_id_and_schema_hash_from_path(const string& path,
                                                            TTabletId* tablet_id,
                                                            TSchemaHash* schema_hash) {
    // the path like: /data/14/10080/964828783/
    static re2::RE2 normal_re("/data/\\d+/(\\d+)/(\\d+)($|/)");
    // match tablet schema hash data path, for example, the path is /data/1/16791/29998
    // 1 is shard id , 16791 is tablet id, 29998 is schema hash
    if (RE2::PartialMatch(path, normal_re, tablet_id, schema_hash)) {
        return true;
    }

    // If we can't match normal path pattern, this may be a path which is a empty tablet
    // directory. Use this pattern to match empty tablet directory. In this case schema_hash
    // will be set to zero.
    static re2::RE2 empty_tablet_re("/data/\\d+/(\\d+)($|/$)");
    if (!RE2::PartialMatch(path, empty_tablet_re, tablet_id)) {
        return false;
    }
    *schema_hash = 0;
    return true;
}

bool TabletManager::get_rowset_id_from_path(const string& path, RowsetId* rowset_id) {
    // the path like: /data/14/10080/964828783/02000000000000969144d8725cb62765f9af6cd3125d5a91_0.dat
    static re2::RE2 re("/data/\\d+/\\d+/\\d+/([A-Fa-f0-9]+)_.*");
    string id_str;
    bool ret = RE2::PartialMatch(path, re, &id_str);
    if (ret) {
        rowset_id->init(id_str);
        return true;
    }
    return false;
}

void TabletManager::get_tablet_stat(TTabletStatResult* result) {
    std::shared_ptr<std::vector<TTabletStat>> local_cache;
    {
        std::lock_guard<std::mutex> guard(_tablet_stat_cache_mutex);
        local_cache = _tablet_stat_list_cache;
    }
    result->__set_tablet_stat_list(*local_cache);
}

TabletSharedPtr TabletManager::find_best_tablet_to_compaction(
        CompactionType compaction_type, DataDir* data_dir,
        const std::unordered_set<TTabletId>& tablet_submitted_compaction, uint32_t* score,
        const std::unordered_map<std::string_view, std::shared_ptr<CumulativeCompactionPolicy>>&
                all_cumulative_compaction_policies) {
    int64_t now_ms = UnixMillis();
    const string& compaction_type_str =
            compaction_type == CompactionType::BASE_COMPACTION ? "base" : "cumulative";
    uint32_t highest_score = 0;
    uint32_t compaction_score = 0;
    TabletSharedPtr best_tablet;
    auto handler = [&](const TabletSharedPtr& tablet_ptr) {
        if (config::enable_skip_tablet_compaction &&
            tablet_ptr->should_skip_compaction(compaction_type, UnixSeconds())) {
            return;
        }
        if (!tablet_ptr->can_do_compaction(data_dir->path_hash(), compaction_type)) {
            return;
        }

        auto search = tablet_submitted_compaction.find(tablet_ptr->tablet_id());
        if (search != tablet_submitted_compaction.end()) {
            return;
        }

        int64_t last_failure_ms = tablet_ptr->last_cumu_compaction_failure_time();
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            last_failure_ms = tablet_ptr->last_base_compaction_failure_time();
        }
        if (now_ms - last_failure_ms <= 5000) {
            VLOG_DEBUG << "Too often to check compaction, skip it. "
                       << "compaction_type=" << compaction_type_str
                       << ", last_failure_time_ms=" << last_failure_ms
                       << ", tablet_id=" << tablet_ptr->tablet_id();
            return;
        }

        if (compaction_type == CompactionType::BASE_COMPACTION) {
            std::unique_lock<std::mutex> lock(tablet_ptr->get_base_compaction_lock(),
                                              std::try_to_lock);
            if (!lock.owns_lock()) {
                LOG(INFO) << "can not get base lock: " << tablet_ptr->tablet_id();
                return;
            }
        } else {
            std::unique_lock<std::mutex> lock(tablet_ptr->get_cumulative_compaction_lock(),
                                              std::try_to_lock);
            if (!lock.owns_lock()) {
                LOG(INFO) << "can not get cumu lock: " << tablet_ptr->tablet_id();
                return;
            }
        }
        auto cumulative_compaction_policy = all_cumulative_compaction_policies.at(
                tablet_ptr->tablet_meta()->compaction_policy());
        uint32_t current_compaction_score =
                tablet_ptr->calc_compaction_score(compaction_type, cumulative_compaction_policy);
        if (current_compaction_score < 5) {
            tablet_ptr->set_skip_compaction(true, compaction_type, UnixSeconds());
        }
        if (current_compaction_score > highest_score) {
            highest_score = current_compaction_score;
            compaction_score = current_compaction_score;
            best_tablet = tablet_ptr;
        }
    };

    for_each_tablet(handler, filter_all_tablets);
    if (best_tablet != nullptr) {
        VLOG_CRITICAL << "Found the best tablet for compaction. "
                      << "compaction_type=" << compaction_type_str
                      << ", tablet_id=" << best_tablet->tablet_id() << ", path=" << data_dir->path()
                      << ", compaction_score=" << compaction_score
                      << ", highest_score=" << highest_score;
        *score = compaction_score;
    }
    return best_tablet;
}

Status TabletManager::load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id,
                                            TSchemaHash schema_hash, const string& meta_binary,
                                            bool update_meta, bool force, bool restore,
                                            bool check_path) {
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    Status status = tablet_meta->deserialize(meta_binary);
    if (!status.ok()) {
        return Status::Error<HEADER_PB_PARSE_FAILED>(
                "fail to load tablet because can not parse meta_binary string. tablet_id={}, "
                "schema_hash={}, path={}, status={}",
                tablet_id, schema_hash, data_dir->path(), status);
    }
    tablet_meta->init_rs_metas_fs(data_dir->fs());

    // check if tablet meta is valid
    if (tablet_meta->tablet_id() != tablet_id || tablet_meta->schema_hash() != schema_hash) {
        return Status::Error<HEADER_PB_PARSE_FAILED>(
                "fail to load tablet because meet invalid tablet meta. trying to load "
                "tablet(tablet_id={}, schema_hash={}), but meet tablet={}, path={}",
                tablet_id, schema_hash, tablet_meta->tablet_id(), data_dir->path());
    }
    if (tablet_meta->tablet_uid().hi == 0 && tablet_meta->tablet_uid().lo == 0) {
        return Status::Error<HEADER_PB_PARSE_FAILED>(
                "fail to load tablet because its uid == 0. tablet={}, path={}",
                tablet_meta->tablet_id(), data_dir->path());
    }

    if (restore) {
        // we're restoring tablet from trash, tablet state should be changed from shutdown back to running
        tablet_meta->set_tablet_state(TABLET_RUNNING);
    }

    if (tablet_meta->partition_id() <= 0) {
        LOG(WARNING) << "invalid partition id " << tablet_meta->partition_id() << ", tablet "
                     << tablet_meta->tablet_id();
    }
    TabletSharedPtr tablet = std::make_shared<Tablet>(std::move(tablet_meta), data_dir);

    // NOTE: method load_tablet_from_meta could be called by two cases as below
    // case 1: BE start;
    // case 2: Clone Task/Restore
    // For case 1 doesn't need path check because BE is just starting and not ready,
    // just check tablet meta status to judge whether tablet is delete is enough.
    // For case 2, If a tablet has just been copied to local BE,
    // it may be cleared by gc-thread(see perform_path_gc_by_tablet) because the tablet meta may not be loaded to memory.
    // So clone task should check path and then failed and retry in this case.
    if (check_path) {
        bool exists = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(tablet->tablet_path(), &exists));
        if (!exists) {
            return Status::Error<TABLE_ALREADY_DELETED_ERROR>(
                    "tablet path not exists, create tablet failed, path={}", tablet->tablet_path());
        }
    }

    if (tablet->tablet_meta()->tablet_state() == TABLET_SHUTDOWN) {
        {
            std::lock_guard<std::shared_mutex> shutdown_tablets_wrlock(_shutdown_tablets_lock);
            _shutdown_tablets.push_back(tablet);
        }
        return Status::Error<TABLE_ALREADY_DELETED_ERROR>(
                "fail to load tablet because it is to be deleted. tablet_id={}, schema_hash={}, "
                "path={}",
                tablet_id, schema_hash, data_dir->path());
    }
    // NOTE: We do not check tablet's initial version here, because if BE restarts when
    // one tablet is doing schema-change, we may meet empty tablet.
    if (tablet->max_version().first == -1 && tablet->tablet_state() == TABLET_RUNNING) {
        // tablet state is invalid, drop tablet
        return Status::Error<TABLE_INDEX_VALIDATE_ERROR>(
                "fail to load tablet. it is in running state but without delta. tablet={}, path={}",
                tablet->tablet_id(), data_dir->path());
    }

    RETURN_NOT_OK_STATUS_WITH_WARN(
            tablet->init(),
            strings::Substitute("tablet init failed. tablet=$0", tablet->tablet_id()));

    RuntimeProfile profile("CreateTablet");
    std::lock_guard<std::shared_mutex> wrlock(_get_tablets_shard_lock(tablet_id));
    RETURN_NOT_OK_STATUS_WITH_WARN(
            _add_tablet_unlocked(tablet_id, tablet, update_meta, force, &profile),
            strings::Substitute("fail to add tablet. tablet=$0", tablet->tablet_id()));

    return Status::OK();
}

Status TabletManager::load_tablet_from_dir(DataDir* store, TTabletId tablet_id,
                                           SchemaHash schema_hash, const string& schema_hash_path,
                                           bool force, bool restore) {
    LOG(INFO) << "begin to load tablet from dir. "
              << " tablet_id=" << tablet_id << " schema_hash=" << schema_hash
              << " path = " << schema_hash_path << " force = " << force << " restore = " << restore;
    // not add lock here, because load_tablet_from_meta already add lock
    std::string header_path = TabletMeta::construct_header_file_path(schema_hash_path, tablet_id);
    // should change shard id before load tablet
    std::string shard_path =
            path_util::dir_name(path_util::dir_name(path_util::dir_name(header_path)));
    std::string shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    int32_t shard = stol(shard_str);

    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(header_path, &exists));
    if (!exists) {
        return Status::Error<FILE_NOT_EXIST>("fail to find header file. [header_path={}]",
                                             header_path);
    }

    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    if (!tablet_meta->create_from_file(header_path).ok()) {
        return Status::Error<ENGINE_LOAD_INDEX_TABLE_ERROR>(
                "fail to load tablet_meta. file_path={}", header_path);
    }
    TabletUid tablet_uid = TabletUid::gen_uid();

    // remove rowset binlog metas
    auto binlog_metas_file = fmt::format("{}/rowset_binlog_metas.pb", schema_hash_path);
    bool binlog_metas_file_exists = false;
    auto file_exists_status =
            io::global_local_filesystem()->exists(binlog_metas_file, &binlog_metas_file_exists);
    if (!file_exists_status.ok()) {
        return file_exists_status;
    }
    bool contain_binlog = false;
    RowsetBinlogMetasPB rowset_binlog_metas_pb;
    if (binlog_metas_file_exists) {
        auto binlog_meta_filesize = std::filesystem::file_size(binlog_metas_file);
        if (binlog_meta_filesize > 0) {
            contain_binlog = true;
            RETURN_IF_ERROR(read_pb(binlog_metas_file, &rowset_binlog_metas_pb));
        }
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(binlog_metas_file));
    }
    if (contain_binlog) {
        auto binlog_dir = fmt::format("{}/_binlog", schema_hash_path);
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(binlog_dir));

        std::vector<io::FileInfo> files;
        RETURN_IF_ERROR(
                io::global_local_filesystem()->list(schema_hash_path, true, &files, &exists));
        for (auto& file : files) {
            auto& filename = file.file_name;
            if (!filename.ends_with(".binlog")) {
                continue;
            }

            // change clone_file suffix .binlog to .dat
            std::string new_filename = filename;
            new_filename.replace(filename.size() - 7, 7, ".dat");
            auto from = fmt::format("{}/{}", schema_hash_path, filename);
            auto to = fmt::format("{}/_binlog/{}", schema_hash_path, new_filename);
            RETURN_IF_ERROR(io::global_local_filesystem()->rename(from, to));
        }

        auto meta = store->get_meta();
        // if ingest binlog metas error, it will be gc in gc_unused_binlog_metas
        RETURN_IF_ERROR(
                RowsetMetaManager::ingest_binlog_metas(meta, tablet_uid, &rowset_binlog_metas_pb));
    }

    // has to change shard id here, because meta file maybe copied from other source
    // its shard is different from local shard
    tablet_meta->set_shard_id(shard);
    // load dir is called by clone, restore, storage migration
    // should change tablet uid when tablet object changed
    tablet_meta->set_tablet_uid(std::move(tablet_uid));
    std::string meta_binary;
    RETURN_IF_ERROR(tablet_meta->serialize(&meta_binary));
    RETURN_NOT_OK_STATUS_WITH_WARN(
            load_tablet_from_meta(store, tablet_id, schema_hash, meta_binary, true, force, restore,
                                  true),
            strings::Substitute("fail to load tablet. header_path=$0", header_path));

    return Status::OK();
}

Status TabletManager::report_tablet_info(TTabletInfo* tablet_info) {
    LOG(INFO) << "begin to process report tablet info."
              << "tablet_id=" << tablet_info->tablet_id;

    Status res = Status::OK();

    TabletSharedPtr tablet = get_tablet(tablet_info->tablet_id);
    if (tablet == nullptr) {
        return Status::Error<TABLE_NOT_FOUND>("can't find tablet={}", tablet_info->tablet_id);
    }

    tablet->build_tablet_report_info(tablet_info);
    VLOG_TRACE << "success to process report tablet info.";
    return res;
}

Status TabletManager::build_all_report_tablets_info(std::map<TTabletId, TTablet>* tablets_info) {
    DCHECK(tablets_info != nullptr);
    VLOG_NOTICE << "begin to build all report tablets info";

    // build the expired txn map first, outside the tablet map lock
    std::map<TabletInfo, std::vector<int64_t>> expire_txn_map;
    StorageEngine::instance()->txn_manager()->build_expire_txn_map(&expire_txn_map);
    LOG(INFO) << "find expired transactions for " << expire_txn_map.size() << " tablets";

    HistogramStat tablet_version_num_hist;
    auto local_cache = std::make_shared<std::vector<TTabletStat>>();
    auto handler = [&](const TabletSharedPtr& tablet) {
        auto& t_tablet = (*tablets_info)[tablet->tablet_id()];
        TTabletInfo& tablet_info = t_tablet.tablet_infos.emplace_back();
        tablet->build_tablet_report_info(&tablet_info, true, true);
        // find expired transaction corresponding to this tablet
        TabletInfo tinfo(tablet->tablet_id(), tablet->tablet_uid());
        auto find = expire_txn_map.find(tinfo);
        if (find != expire_txn_map.end()) {
            tablet_info.__set_transaction_ids(find->second);
            expire_txn_map.erase(find);
        }
        tablet_version_num_hist.add(tablet->version_count());
        auto& t_tablet_stat = local_cache->emplace_back();
        t_tablet_stat.__set_tablet_id(tablet_info.tablet_id);
        t_tablet_stat.__set_data_size(tablet_info.data_size);
        t_tablet_stat.__set_remote_data_size(tablet_info.remote_data_size);
        t_tablet_stat.__set_row_num(tablet_info.row_count);
        t_tablet_stat.__set_version_count(tablet_info.version_count);
    };
    for_each_tablet(handler, filter_all_tablets);

    {
        std::lock_guard<std::mutex> guard(_tablet_stat_cache_mutex);
        _tablet_stat_list_cache.swap(local_cache);
    }
    DorisMetrics::instance()->tablet_version_num_distribution->set_histogram(
            tablet_version_num_hist);
    LOG(INFO) << "success to build all report tablets info. tablet_count=" << tablets_info->size();
    return Status::OK();
}

Status TabletManager::start_trash_sweep() {
    std::unique_lock<std::mutex> lock(_gc_tablets_lock, std::defer_lock);
    if (!lock.try_lock()) {
        return Status::OK();
    }

    for_each_tablet([](const TabletSharedPtr& tablet) { tablet->delete_expired_stale_rowset(); },
                    filter_all_tablets);

    std::list<TabletSharedPtr>::iterator last_it;
    {
        std::shared_lock rdlock(_shutdown_tablets_lock);
        last_it = _shutdown_tablets.begin();
        if (last_it == _shutdown_tablets.end()) {
            return Status::OK();
        }
    }

    auto get_batch_tablets = [this, &last_it](int limit) {
        std::vector<TabletSharedPtr> batch_tablets;
        std::lock_guard<std::shared_mutex> wrdlock(_shutdown_tablets_lock);
        while (last_it != _shutdown_tablets.end() && batch_tablets.size() < limit) {
            // it means current tablet is referenced by other thread
            if (last_it->use_count() > 1) {
                last_it++;
            } else {
                batch_tablets.push_back(*last_it);
                last_it = _shutdown_tablets.erase(last_it);
            }
        }

        return batch_tablets;
    };

    std::list<TabletSharedPtr> failed_tablets;
    // return true if need continue delete
    auto delete_one_batch = [this, get_batch_tablets, &failed_tablets]() -> bool {
        int limit = 200;
        for (;;) {
            auto batch_tablets = get_batch_tablets(limit);
            for (const auto& tablet : batch_tablets) {
                if (_move_tablet_to_trash(tablet)) {
                    limit--;
                } else {
                    failed_tablets.push_back(tablet);
                }
            }
            if (limit <= 0) {
                return true;
            }
            if (batch_tablets.empty()) {
                return false;
            }
        }

        return false;
    };

    while (delete_one_batch()) {
#ifndef BE_TEST
        sleep(1);
#endif
    }

    if (!failed_tablets.empty()) {
        std::lock_guard<std::shared_mutex> wrlock(_shutdown_tablets_lock);
        _shutdown_tablets.splice(_shutdown_tablets.end(), failed_tablets);
    }

    return Status::OK();
}

bool TabletManager::_move_tablet_to_trash(const TabletSharedPtr& tablet) {
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    int64_t get_meta_ts = MonotonicMicros();
    Status check_st = TabletMetaManager::get_meta(tablet->data_dir(), tablet->tablet_id(),
                                                  tablet->schema_hash(), tablet_meta);
    if (check_st.ok()) {
        if (tablet_meta->tablet_state() != TABLET_SHUTDOWN ||
            tablet_meta->tablet_uid() != tablet->tablet_uid()) {
            LOG(WARNING) << "tablet's state changed to normal, skip remove dirs"
                         << " tablet id = " << tablet_meta->tablet_id()
                         << " schema hash = " << tablet_meta->schema_hash()
                         << " old tablet_uid=" << tablet->tablet_uid()
                         << " cur tablet_uid=" << tablet_meta->tablet_uid();
            return true;
        }
        // move data to trash
        const auto& tablet_path = tablet->tablet_path();
        bool exists = false;
        Status exists_st = io::global_local_filesystem()->exists(tablet_path, &exists);
        if (!exists_st) {
            return false;
        }
        if (exists) {
            // take snapshot of tablet meta
            auto meta_file_path = fmt::format("{}/{}.hdr", tablet_path, tablet->tablet_id());
            int64_t save_meta_ts = MonotonicMicros();
            auto save_st = tablet->tablet_meta()->save(meta_file_path);
            if (!save_st.ok()) {
                LOG(WARNING) << "failed to save meta, tablet_id=" << tablet_meta->tablet_id()
                             << ", tablet_uid=" << tablet_meta->tablet_uid()
                             << ", error=" << save_st;
                return false;
            }
            int64_t now = MonotonicMicros();
            LOG(INFO) << "start to move tablet to trash. " << tablet_path
                      << ". rocksdb get meta cost " << (save_meta_ts - get_meta_ts)
                      << " us, rocksdb save meta cost " << (now - save_meta_ts) << " us";
            Status rm_st = tablet->data_dir()->move_to_trash(tablet_path);
            if (!rm_st.ok()) {
                LOG(WARNING) << "fail to move dir to trash. " << tablet_path;
                return false;
            }
        }
        // remove tablet meta
        auto remove_st = TabletMetaManager::remove(tablet->data_dir(), tablet->tablet_id(),
                                                   tablet->schema_hash());
        if (!remove_st.ok()) {
            LOG(WARNING) << "failed to remove meta, tablet_id=" << tablet_meta->tablet_id()
                         << ", tablet_uid=" << tablet_meta->tablet_uid() << ", error=" << remove_st;
            return false;
        }
        LOG(INFO) << "successfully move tablet to trash. "
                  << "tablet_id=" << tablet->tablet_id()
                  << ", schema_hash=" << tablet->schema_hash() << ", tablet_path=" << tablet_path;
        return true;
    } else {
        // if could not find tablet info in meta store, then check if dir existed
        const auto& tablet_path = tablet->tablet_path();
        bool exists = false;
        Status exists_st = io::global_local_filesystem()->exists(tablet_path, &exists);
        if (!exists_st) {
            return false;
        }
        if (exists) {
            LOG(WARNING) << "errors while load meta from store, skip this tablet. "
                         << "tablet_id=" << tablet->tablet_id()
                         << ", schema_hash=" << tablet->schema_hash();
            return false;
        } else {
            LOG(INFO) << "could not find tablet dir, skip it and remove it from gc-queue. "
                      << "tablet_id=" << tablet->tablet_id()
                      << ", schema_hash=" << tablet->schema_hash()
                      << ", tablet_path=" << tablet_path;
            return true;
        }
    }
}

bool TabletManager::register_clone_tablet(int64_t tablet_id) {
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    std::lock_guard<std::shared_mutex> wrlock(shard.lock);
    return shard.tablets_under_clone.insert(tablet_id).second;
}

void TabletManager::unregister_clone_tablet(int64_t tablet_id) {
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    std::lock_guard<std::shared_mutex> wrlock(shard.lock);
    shard.tablets_under_clone.erase(tablet_id);
}

void TabletManager::try_delete_unused_tablet_path(DataDir* data_dir, TTabletId tablet_id,
                                                  SchemaHash schema_hash,
                                                  const string& schema_hash_path) {
    // acquire the read lock, so that there is no creating tablet or load tablet from meta tasks
    // create tablet and load tablet task should check whether the dir exists
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    std::shared_lock rdlock(shard.lock);

    // check if meta already exists
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    Status check_st = TabletMetaManager::get_meta(data_dir, tablet_id, schema_hash, tablet_meta);
    if (check_st.ok()) {
        LOG(INFO) << "tablet meta exists in meta store, skip delete the path " << schema_hash_path;
        return;
    }

    if (shard.tablets_under_clone.count(tablet_id) > 0) {
        LOG(INFO) << "tablet is under clone, skip delete the path " << schema_hash_path;
        return;
    }

    // TODO(ygl): may do other checks in the future
    bool exists = false;
    Status exists_st = io::global_local_filesystem()->exists(schema_hash_path, &exists);
    if (exists_st && exists) {
        LOG(INFO) << "start to move tablet to trash. tablet_path = " << schema_hash_path;
        Status rm_st = data_dir->move_to_trash(schema_hash_path);
        if (!rm_st.ok()) {
            LOG(WARNING) << "fail to move dir to trash. dir=" << schema_hash_path;
        } else {
            LOG(INFO) << "move path " << schema_hash_path << " to trash successfully";
        }
    }
}

void TabletManager::update_root_path_info(std::map<string, DataDirInfo>* path_map,
                                          size_t* tablet_count) {
    DCHECK(tablet_count);
    *tablet_count = 0;
    auto filter = [path_map, tablet_count](Tablet* t) -> bool {
        ++(*tablet_count);
        auto iter = path_map->find(t->data_dir()->path());
        return iter != path_map->end() && iter->second.is_used;
    };

    auto handler = [&](const TabletSharedPtr& tablet) {
        auto& data_dir_info = (*path_map)[tablet->data_dir()->path()];
        data_dir_info.local_used_capacity += tablet->tablet_local_size();
        data_dir_info.remote_used_capacity += tablet->tablet_remote_size();
    };

    for_each_tablet(handler, filter);
}

void TabletManager::get_partition_related_tablets(int64_t partition_id,
                                                  std::set<TabletInfo>* tablet_infos) {
    std::shared_lock rdlock(_partition_tablet_map_lock);
    if (_partition_tablet_map.find(partition_id) != _partition_tablet_map.end()) {
        *tablet_infos = _partition_tablet_map[partition_id];
    }
}

void TabletManager::do_tablet_meta_checkpoint(DataDir* data_dir) {
    auto filter = [data_dir](Tablet* tablet) -> bool {
        return tablet->tablet_state() == TABLET_RUNNING &&
               tablet->data_dir()->path_hash() == data_dir->path_hash() && tablet->is_used() &&
               tablet->init_succeeded();
    };

    std::vector<TabletSharedPtr> related_tablets = get_all_tablet(filter);
    int counter = 0;
    MonotonicStopWatch watch;
    watch.start();
    for (TabletSharedPtr tablet : related_tablets) {
        if (tablet->do_tablet_meta_checkpoint()) {
            ++counter;
        }
    }
    int64_t cost = watch.elapsed_time() / 1000 / 1000;
    LOG(INFO) << "finish to do meta checkpoint on dir: " << data_dir->path()
              << ", number: " << counter << ", cost(ms): " << cost;
}

Status TabletManager::_create_tablet_meta_unlocked(const TCreateTabletReq& request, DataDir* store,
                                                   const bool is_schema_change,
                                                   const Tablet* base_tablet,
                                                   TabletMetaSharedPtr* tablet_meta) {
    uint32_t next_unique_id = 0;
    std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id;
    if (!is_schema_change) {
        for (uint32_t col_idx = 0; col_idx < request.tablet_schema.columns.size(); ++col_idx) {
            col_idx_to_unique_id[col_idx] = col_idx;
        }
        next_unique_id = request.tablet_schema.columns.size();
    } else {
        next_unique_id = base_tablet->next_unique_id();
        auto& new_columns = request.tablet_schema.columns;
        for (uint32_t new_col_idx = 0; new_col_idx < new_columns.size(); ++new_col_idx) {
            const TColumn& column = new_columns[new_col_idx];
            // For schema change, compare old_tablet and new_tablet:
            // 1. if column exist in both new_tablet and old_tablet, choose the column's
            //    unique_id in old_tablet to be the column's ordinal number in new_tablet
            // 2. if column exists only in new_tablet, assign next_unique_id of old_tablet
            //    to the new column
            int32_t old_col_idx = base_tablet->tablet_schema()->field_index(column.column_name);
            if (old_col_idx != -1) {
                uint32_t old_unique_id =
                        base_tablet->tablet_schema()->column(old_col_idx).unique_id();
                col_idx_to_unique_id[new_col_idx] = old_unique_id;
            } else {
                // Not exist in old tablet, it is a new added column
                col_idx_to_unique_id[new_col_idx] = next_unique_id++;
            }
        }
    }
    VLOG_NOTICE << "creating tablet meta. next_unique_id=" << next_unique_id;

    // We generate a new tablet_uid for this new tablet.
    uint64_t shard_id = store->get_shard();
    *tablet_meta = TabletMeta::create(request, TabletUid::gen_uid(), shard_id, next_unique_id,
                                      col_idx_to_unique_id);
    if (request.__isset.storage_format) {
        if (request.storage_format == TStorageFormat::DEFAULT) {
            (*tablet_meta)
                    ->set_preferred_rowset_type(StorageEngine::instance()->default_rowset_type());
        } else if (request.storage_format == TStorageFormat::V1) {
            (*tablet_meta)->set_preferred_rowset_type(ALPHA_ROWSET);
        } else if (request.storage_format == TStorageFormat::V2) {
            (*tablet_meta)->set_preferred_rowset_type(BETA_ROWSET);
        } else {
            return Status::Error<CE_CMD_PARAMS_ERROR>("invalid TStorageFormat: {}",
                                                      request.storage_format);
        }
    }
    return Status::OK();
}

TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id) {
    VLOG_NOTICE << "begin to get tablet. tablet_id=" << tablet_id;
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    const auto& iter = tablet_map.find(tablet_id);
    if (iter != tablet_map.end()) {
        return iter->second;
    }
    return nullptr;
}

void TabletManager::_add_tablet_to_partition(const TabletSharedPtr& tablet) {
    std::lock_guard<std::shared_mutex> wrlock(_partition_tablet_map_lock);
    _partition_tablet_map[tablet->partition_id()].insert(tablet->get_tablet_info());
}

void TabletManager::_remove_tablet_from_partition(const TabletSharedPtr& tablet) {
    std::lock_guard<std::shared_mutex> wrlock(_partition_tablet_map_lock);
    _partition_tablet_map[tablet->partition_id()].erase(tablet->get_tablet_info());
    if (_partition_tablet_map[tablet->partition_id()].empty()) {
        _partition_tablet_map.erase(tablet->partition_id());
    }
}

void TabletManager::obtain_specific_quantity_tablets(vector<TabletInfo>& tablets_info,
                                                     int64_t num) {
    for (const auto& tablets_shard : _tablets_shards) {
        std::shared_lock rdlock(tablets_shard.lock);
        for (const auto& item : tablets_shard.tablet_map) {
            TabletSharedPtr tablet = item.second;
            if (tablets_info.size() >= num) {
                return;
            }
            if (tablet == nullptr) {
                continue;
            }
            tablets_info.push_back(tablet->get_tablet_info());
        }
    }
}

std::shared_mutex& TabletManager::_get_tablets_shard_lock(TTabletId tabletId) {
    return _get_tablets_shard(tabletId).lock;
}

TabletManager::tablet_map_t& TabletManager::_get_tablet_map(TTabletId tabletId) {
    return _get_tablets_shard(tabletId).tablet_map;
}

TabletManager::tablets_shard& TabletManager::_get_tablets_shard(TTabletId tabletId) {
    return _tablets_shards[tabletId & _tablets_shards_mask];
}

void TabletManager::get_tablets_distribution_on_different_disks(
        std::map<int64_t, std::map<DataDir*, int64_t>>& tablets_num_on_disk,
        std::map<int64_t, std::map<DataDir*, std::vector<TabletSize>>>& tablets_info_on_disk) {
    std::vector<DataDir*> data_dirs = StorageEngine::instance()->get_stores();
    std::map<int64_t, std::set<TabletInfo>> partition_tablet_map;
    {
        // When drop tablet, '_partition_tablet_map_lock' is locked in 'tablet_shard_lock'.
        // To avoid locking 'tablet_shard_lock' in '_partition_tablet_map_lock', we lock and
        // copy _partition_tablet_map here.
        std::shared_lock rdlock(_partition_tablet_map_lock);
        partition_tablet_map = _partition_tablet_map;
    }
    std::map<int64_t, std::set<TabletInfo>>::iterator partition_iter = partition_tablet_map.begin();
    for (; partition_iter != partition_tablet_map.end(); ++partition_iter) {
        std::map<DataDir*, int64_t> tablets_num;
        std::map<DataDir*, std::vector<TabletSize>> tablets_info;
        for (int i = 0; i < data_dirs.size(); i++) {
            tablets_num[data_dirs[i]] = 0;
        }
        int64_t partition_id = partition_iter->first;
        std::set<TabletInfo>::iterator tablet_info_iter = (partition_iter->second).begin();
        for (; tablet_info_iter != (partition_iter->second).end(); ++tablet_info_iter) {
            // get_tablet() will hold 'tablet_shard_lock'
            TabletSharedPtr tablet = get_tablet(tablet_info_iter->tablet_id);
            if (tablet == nullptr) {
                continue;
            }
            DataDir* data_dir = tablet->data_dir();
            size_t tablet_footprint = tablet->tablet_footprint();
            tablets_num[data_dir]++;
            TabletSize tablet_size(tablet_info_iter->tablet_id, tablet_footprint);
            tablets_info[data_dir].push_back(tablet_size);
        }
        tablets_num_on_disk[partition_id] = tablets_num;
        tablets_info_on_disk[partition_id] = tablets_info;
    }
}

struct SortCtx {
    SortCtx(TabletSharedPtr tablet, RowsetSharedPtr rowset, int64_t cooldown_timestamp,
            int64_t file_size)
            : tablet(tablet), cooldown_timestamp(cooldown_timestamp), file_size(file_size) {}
    TabletSharedPtr tablet;
    RowsetSharedPtr rowset;
    // to ensure the tablet with -1 would always be greater than other
    uint64_t cooldown_timestamp;
    int64_t file_size;
    bool operator<(const SortCtx& other) const {
        if (this->cooldown_timestamp == other.cooldown_timestamp) {
            return this->file_size > other.file_size;
        }
        return this->cooldown_timestamp < other.cooldown_timestamp;
    }
};

void TabletManager::get_cooldown_tablets(std::vector<TabletSharedPtr>* tablets,
                                         std::vector<RowsetSharedPtr>* rowsets,
                                         std::function<bool(const TabletSharedPtr&)> skip_tablet) {
    std::vector<SortCtx> sort_ctx_vec;
    std::vector<std::weak_ptr<Tablet>> candidates;
    for_each_tablet([&](const TabletSharedPtr& tablet) { candidates.emplace_back(tablet); },
                    filter_all_tablets);
    auto get_cooldown_tablet = [&sort_ctx_vec, &skip_tablet](std::weak_ptr<Tablet>& t) {
        const TabletSharedPtr& tablet = t.lock();
        RowsetSharedPtr rowset = nullptr;
        if (UNLIKELY(nullptr == tablet)) {
            return;
        }
        std::shared_lock rdlock(tablet->get_header_lock());
        int64_t cooldown_timestamp = -1;
        size_t file_size = -1;
        if (!skip_tablet(tablet) &&
            (rowset = tablet->need_cooldown(&cooldown_timestamp, &file_size))) {
            sort_ctx_vec.emplace_back(tablet, rowset, cooldown_timestamp, file_size);
        }
    };
    std::for_each(candidates.begin(), candidates.end(), get_cooldown_tablet);

    std::sort(sort_ctx_vec.begin(), sort_ctx_vec.end());

    for (SortCtx& ctx : sort_ctx_vec) {
        VLOG_DEBUG << "get cooldown tablet: " << ctx.tablet->tablet_id();
        tablets->push_back(std::move(ctx.tablet));
        rowsets->push_back(std::move(ctx.rowset));
    }
}

void TabletManager::get_all_tablets_storage_format(TCheckStorageFormatResult* result) {
    DCHECK(result != nullptr);
    auto handler = [result](const TabletSharedPtr& tablet) {
        if (tablet->all_beta()) {
            result->v2_tablets.push_back(tablet->tablet_id());
        } else {
            result->v1_tablets.push_back(tablet->tablet_id());
        }
    };

    for_each_tablet(handler, filter_all_tablets);
    result->__isset.v1_tablets = true;
    result->__isset.v2_tablets = true;
}

std::set<int64_t> TabletManager::check_all_tablet_segment(bool repair) {
    std::set<int64_t> bad_tablets;
    std::map<int64_t, std::vector<int64_t>> repair_shard_bad_tablets;
    auto handler = [&](const TabletSharedPtr& tablet) {
        if (!tablet->check_all_rowset_segment()) {
            int64_t tablet_id = tablet->tablet_id();
            bad_tablets.insert(tablet_id);
            if (repair) {
                repair_shard_bad_tablets[tablet_id & _tablets_shards_mask].push_back(tablet_id);
            }
        }
    };
    for_each_tablet(handler, filter_all_tablets);

    for (const auto& [shard_index, shard_tablets] : repair_shard_bad_tablets) {
        auto& tablets_shard = _tablets_shards[shard_index];
        auto& tablet_map = tablets_shard.tablet_map;
        std::lock_guard<std::shared_mutex> wrlock(tablets_shard.lock);
        for (auto tablet_id : shard_tablets) {
            auto it = tablet_map.find(tablet_id);
            if (it == tablet_map.end()) {
                bad_tablets.erase(tablet_id);
                LOG(WARNING) << "Bad tablet has be removed. tablet_id=" << tablet_id;
            } else {
                const auto& tablet = it->second;
                static_cast<void>(tablet->set_tablet_state(TABLET_SHUTDOWN));
                tablet->save_meta();
                {
                    std::lock_guard<std::shared_mutex> shutdown_tablets_wrlock(
                            _shutdown_tablets_lock);
                    _shutdown_tablets.push_back(tablet);
                }
                LOG(WARNING) << "There are some segments lost, set tablet to shutdown state."
                             << "tablet_id=" << tablet->tablet_id()
                             << ", tablet_path=" << tablet->tablet_path();
            }
        }
    }

    return bad_tablets;
}

} // end namespace doris
