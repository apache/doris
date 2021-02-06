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

#include <rapidjson/document.h>
#include <re2/re2.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <cstdio>
#include <cstdlib>

#include "env/env.h"
#include "gutil/strings/strcat.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/schema_change.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "util/doris_metrics.h"
#include "util/file_utils.h"
#include "util/path_util.h"
#include "util/pretty_printer.h"
#include "util/scoped_cleanup.h"
#include "util/time.h"
#include "util/trace.h"

using std::list;
using std::map;
using std::set;
using std::string;
using std::vector;
using strings::Substitute;

namespace doris {

static bool _cmp_tablet_by_create_time(const TabletSharedPtr& a, const TabletSharedPtr& b) {
    return a->creation_time() < b->creation_time();
}

TabletManager::TabletManager(int32_t tablet_map_lock_shard_size)
    : _tablets_shards_size(tablet_map_lock_shard_size),
      _tablets_shards_mask(tablet_map_lock_shard_size - 1),
      _last_update_stat_ms(0) {
    CHECK_GT(_tablets_shards_size, 0);
    CHECK_EQ(_tablets_shards_size & _tablets_shards_mask, 0);
    _tablets_shards.resize(_tablets_shards_size);
    for (auto& tablets_shard : _tablets_shards) {
        tablets_shard.lock = std::unique_ptr<RWMutex>(new RWMutex());
    }
}

OLAPStatus TabletManager::_add_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash,
                                               const TabletSharedPtr& tablet, bool update_meta,
                                               bool force) {
    OLAPStatus res = OLAP_SUCCESS;
    VLOG_NOTICE << "begin to add tablet to TabletManager. "
            << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash << ", force=" << force;

    TabletSharedPtr existed_tablet = nullptr;
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    for (TabletSharedPtr item : tablet_map[tablet_id].table_arr) {
        if (item->equal(tablet_id, schema_hash)) {
            existed_tablet = item;
            break;
        }
    }

    if (existed_tablet == nullptr) {
        return _add_tablet_to_map_unlocked(tablet_id, schema_hash, tablet, update_meta,
                                           false /*keep_files*/, false /*drop_old*/);
    }

    if (!force) {
        if (existed_tablet->tablet_path() == tablet->tablet_path()) {
            LOG(WARNING) << "add the same tablet twice! tablet_id=" << tablet_id
                         << ", schema_hash=" << schema_hash
                         << ", tablet_path=" << tablet->tablet_path();
            return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
        }
        if (existed_tablet->data_dir() == tablet->data_dir()) {
            LOG(WARNING) << "add tablet with same data dir twice! tablet_id=" << tablet_id
                         << ", schema_hash=" << schema_hash;
            return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
        }
    }

    existed_tablet->obtain_header_rdlock();
    const RowsetSharedPtr old_rowset = existed_tablet->rowset_with_max_version();
    const RowsetSharedPtr new_rowset = tablet->rowset_with_max_version();

    // If new tablet is empty, it is a newly created schema change tablet.
    // the old tablet is dropped before add tablet. it should not exist old tablet
    if (new_rowset == nullptr) {
        existed_tablet->release_header_lock();
        // it seems useless to call unlock and return here.
        // it could prevent error when log level is changed in the future.
        LOG(FATAL) << "new tablet is empty and old tablet exists. it should not happen."
                   << " tablet_id=" << tablet_id << " schema_hash=" << schema_hash;
        return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
    }
    int64_t old_time = old_rowset == nullptr ? -1 : old_rowset->creation_time();
    int64_t new_time = new_rowset->creation_time();
    int32_t old_version = old_rowset == nullptr ? -1 : old_rowset->end_version();
    int32_t new_version = new_rowset->end_version();
    existed_tablet->release_header_lock();

    // In restore process, we replace all origin files in tablet dir with
    // the downloaded snapshot files. Then we try to reload tablet header.
    // force == true means we forcibly replace the Tablet in tablet_map
    // with the new one. But if we do so, the files in the tablet dir will be
    // dropped when the origin Tablet deconstruct.
    // So we set keep_files == true to not delete files when the
    // origin Tablet deconstruct.
    bool keep_files = force ? true : false;
    if (force ||
        (new_version > old_version || (new_version == old_version && new_time > old_time))) {
        // check if new tablet's meta is in store and add new tablet's meta to meta store
        res = _add_tablet_to_map_unlocked(tablet_id, schema_hash, tablet, update_meta, keep_files,
                                          true /*drop_old*/);
    } else {
        res = OLAP_ERR_ENGINE_INSERT_OLD_TABLET;
    }
    LOG(WARNING) << "add duplicated tablet. force=" << force << ", res=" << res
                 << ", tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
                 << ", old_version=" << old_version << ", new_version=" << new_version
                 << ", old_time=" << old_time << ", new_time=" << new_time
                 << ", old_tablet_path=" << existed_tablet->tablet_path()
                 << ", new_tablet_path=" << tablet->tablet_path();

    return res;
}

OLAPStatus TabletManager::_add_tablet_to_map_unlocked(TTabletId tablet_id, SchemaHash schema_hash,
                                                      const TabletSharedPtr& tablet,
                                                      bool update_meta, bool keep_files,
                                                      bool drop_old) {
    // check if new tablet's meta is in store and add new tablet's meta to meta store
    OLAPStatus res = OLAP_SUCCESS;
    if (update_meta) {
        // call tablet save meta in order to valid the meta
        tablet->save_meta();
    }
    if (drop_old) {
        // If the new tablet is fresher than the existing one, then replace
        // the existing tablet with the new one.
        RETURN_NOT_OK_LOG(_drop_tablet_unlocked(tablet_id, schema_hash, keep_files),
                          strings::Substitute("failed to drop old tablet when add new tablet. "
                                              "tablet_id=$0, schema_hash=$1",
                                              tablet_id, schema_hash));
    }
    // Register tablet into DataDir, so that we can manage tablet from
    // the perspective of root path.
    // Example: unregister all tables when a bad disk found.
    tablet->register_tablet_into_dir();
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map[tablet_id].table_arr.push_back(tablet);
    tablet_map[tablet_id].table_arr.sort(_cmp_tablet_by_create_time);
    _add_tablet_to_partition(*tablet);

    VLOG_NOTICE << "add tablet to map successfully."
            << " tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
    return res;
}

bool TabletManager::check_tablet_id_exist(TTabletId tablet_id) {
    ReadLock rlock(_get_tablets_shard_lock(tablet_id));
    return _check_tablet_id_exist_unlocked(tablet_id);
}

bool TabletManager::_check_tablet_id_exist_unlocked(TTabletId tablet_id) {
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map_t::iterator it = tablet_map.find(tablet_id);
    return it != tablet_map.end() && !it->second.table_arr.empty();
}

OLAPStatus TabletManager::create_tablet(const TCreateTabletReq& request,
                                        std::vector<DataDir*> stores) {
    DorisMetrics::instance()->create_tablet_requests_total->increment(1);

    int64_t tablet_id = request.tablet_id;
    int32_t schema_hash = request.tablet_schema.schema_hash;
    LOG(INFO) << "begin to create tablet. tablet_id=" << tablet_id
              << ", schema_hash=" << schema_hash;

    WriteLock wlock(_get_tablets_shard_lock(tablet_id));
    TRACE("got tablets shard lock");
    // Make create_tablet operation to be idempotent:
    // 1. Return true if tablet with same tablet_id and schema_hash exist;
    //           false if tablet with same tablet_id but different schema_hash exist.
    // 2. When this is an alter task, if the tablet(both tablet_id and schema_hash are
    // same) already exist, then just return true(an duplicate request). But if
    // tablet_id exist but with different schema_hash, return an error(report task will
    // eventually trigger its deletion).
    if (_check_tablet_id_exist_unlocked(tablet_id)) {
        TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, schema_hash);
        if (tablet != nullptr) {
            LOG(INFO) << "success to create tablet. tablet already exist. tablet_id=" << tablet_id;
            return OLAP_SUCCESS;
        } else {
            LOG(WARNING) << "fail to create tablet. tablet exist but with different schema_hash. "
                         << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
            DorisMetrics::instance()->create_tablet_requests_failed->increment(1);
            return OLAP_ERR_CE_TABLET_ID_EXIST;
        }
    }

    TabletSharedPtr base_tablet = nullptr;
    bool is_schema_change = false;
    // If the CreateTabletReq has base_tablet_id then it is a alter-tablet request
    if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
        is_schema_change = true;
        base_tablet = _get_tablet_unlocked(request.base_tablet_id, request.base_schema_hash);
        if (base_tablet == nullptr) {
            LOG(WARNING) << "fail to create tablet(change schema), base tablet does not exist. "
                         << "new_tablet_id=" << tablet_id << ", new_schema_hash=" << schema_hash
                         << ", base_tablet_id=" << request.base_tablet_id
                         << ", base_schema_hash=" << request.base_schema_hash;
            DorisMetrics::instance()->create_tablet_requests_failed->increment(1);
            return OLAP_ERR_TABLE_CREATE_META_ERROR;
        }
        // If we are doing schema-change, we should use the same data dir
        // TODO(lingbin): A litter trick here, the directory should be determined before
        // entering this method
        stores.clear();
        stores.push_back(base_tablet->data_dir());
    }
    TRACE("got base tablet");

    // set alter type to schema-change. it is useless
    TabletSharedPtr tablet = _internal_create_tablet_unlocked(
            AlterTabletType::SCHEMA_CHANGE, request, is_schema_change, base_tablet.get(), stores);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to create tablet. tablet_id=" << request.tablet_id;
        DorisMetrics::instance()->create_tablet_requests_failed->increment(1);
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }
    TRACE("succeed to create tablet");

    LOG(INFO) << "success to create tablet. tablet_id=" << tablet_id
              << ", schema_hash=" << schema_hash;
    return OLAP_SUCCESS;
}

TabletSharedPtr TabletManager::_internal_create_tablet_unlocked(
        const AlterTabletType alter_type, const TCreateTabletReq& request,
        const bool is_schema_change, const Tablet* base_tablet,
        const std::vector<DataDir*>& data_dirs) {
    // If in schema-change state, base_tablet must also be provided.
    // i.e., is_schema_change and base_tablet are either assigned or not assigned
    DCHECK((is_schema_change && base_tablet) || (!is_schema_change && !base_tablet));

    // NOTE: The existence of tablet_id and schema_hash has already been checked,
    // no need check again here.

    auto tablet =
            _create_tablet_meta_and_dir_unlocked(request, is_schema_change, base_tablet, data_dirs);
    if (tablet == nullptr) {
        return nullptr;
    }
    TRACE("create tablet meta");

    int64_t new_tablet_id = request.tablet_id;
    int32_t new_schema_hash = request.tablet_schema.schema_hash;

    // should remove the tablet's pending_id no matter create-tablet success or not
    DataDir* data_dir = tablet->data_dir();
    SCOPED_CLEANUP({ data_dir->remove_pending_ids(StrCat(TABLET_ID_PREFIX, new_tablet_id)); });

    // TODO(yiguolei)
    // the following code is very difficult to understand because it mixed alter tablet v2
    // and alter tablet v1 should remove alter tablet v1 code after v0.12
    OLAPStatus res = OLAP_SUCCESS;
    bool is_tablet_added = false;
    do {
        res = tablet->init();
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "tablet init failed. tablet:" << tablet->full_name();
            break;
        }
        // TODO(lingbin): is it needed? because all type of create_tablet will be true.
        // 1. !is_schema_change: not in schema-change state;
        // 2. request.base_tablet_id > 0: in schema-change state;
        if (!is_schema_change || (request.__isset.base_tablet_id && request.base_tablet_id > 0)) {
            // Create init version if this is not a restore mode replica and request.version is set
            // bool in_restore_mode = request.__isset.in_restore_mode && request.in_restore_mode;
            // if (!in_restore_mode && request.__isset.version) {
            // create initial rowset before add it to storage engine could omit many locks
            res = _create_initial_rowset_unlocked(request, tablet.get());
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to create initial version for tablet. res=" << res;
                break;
            }
            TRACE("create initial rowset");
        }
        if (is_schema_change) {
            if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
                LOG(INFO) << "request for alter-tablet v2, do not add alter task to tablet";
                // if this is a new alter tablet, has to set its state to not ready
                // because schema change handler depends on it to check whether history data
                // convert finished
                tablet->set_tablet_state(TabletState::TABLET_NOTREADY);
            } else {
                // add alter task to new tablet if it is a new tablet during schema change
                tablet->add_alter_task(base_tablet->tablet_id(), base_tablet->schema_hash(),
                                       std::vector<Version>(), alter_type);
            }
            // 有可能出现以下2种特殊情况：
            // 1. 因为操作系统时间跳变，导致新生成的表的creation_time小于旧表的creation_time时间
            // 2. 因为olap engine代码中统一以秒为单位，所以如果2个操作(比如create一个表,
            //    然后立即alter该表)之间的时间间隔小于1s，则alter得到的新表和旧表的creation_time会相同
            //
            // 当出现以上2种情况时，为了能够区分alter得到的新表和旧表，这里把新表的creation_time设置为
            // 旧表的creation_time加1
            if (tablet->creation_time() <= base_tablet->creation_time()) {
                LOG(WARNING) << "new tablet's create time is less than or equal to old tablet"
                             << "new_tablet_create_time=" << tablet->creation_time()
                             << ", base_tablet_create_time=" << base_tablet->creation_time();
                int64_t new_creation_time = base_tablet->creation_time() + 1;
                tablet->set_creation_time(new_creation_time);
            }
            TRACE("update schema change info");
        }
        // Add tablet to StorageEngine will make it visible to user
        res = _add_tablet_unlocked(new_tablet_id, new_schema_hash, tablet, true, false);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to add tablet to StorageEngine. res=" << res;
            break;
        }
        is_tablet_added = true;

        // TODO(lingbin): The following logic seems useless, can be removed?
        // Because if _add_tablet_unlocked() return OK, we must can get it from map.
        TabletSharedPtr tablet_ptr = _get_tablet_unlocked(new_tablet_id, new_schema_hash);
        if (tablet_ptr == nullptr) {
            res = OLAP_ERR_TABLE_NOT_FOUND;
            LOG(WARNING) << "fail to get tablet. res=" << res;
            break;
        }
        TRACE("add tablet to StorageEngine");
    } while (0);

    if (res == OLAP_SUCCESS) {
        return tablet;
    }
    // something is wrong, we need clear environment
    if (is_tablet_added) {
        OLAPStatus status = _drop_tablet_unlocked(new_tablet_id, new_schema_hash, false);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to drop tablet when create tablet failed. res=" << res;
        }
    } else {
        tablet->delete_all_files();
        TabletMetaManager::remove(data_dir, new_tablet_id, new_schema_hash);
    }
    TRACE("revert changes on error");
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
        const std::vector<DataDir*>& data_dirs) {
    string pending_id = StrCat(TABLET_ID_PREFIX, request.tablet_id);
    // Many attempts are made here in the hope that even if a disk fails, it can still continue.
    DataDir* last_dir = nullptr;
    for (auto& data_dir : data_dirs) {
        if (last_dir != nullptr) {
            // If last_dir != null, it means the last attempt to create a tablet failed
            last_dir->remove_pending_ids(pending_id);
        }
        last_dir = data_dir;

        TabletMetaSharedPtr tablet_meta;
        // if create meta failed, do not need to clean dir, because it is only in memory
        OLAPStatus res = _create_tablet_meta_unlocked(request, data_dir, is_schema_change,
                                                      base_tablet, &tablet_meta);
        if (res != OLAP_SUCCESS) {
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
        if (FileUtils::check_exist(schema_hash_dir)) {
            LOG(WARNING) << "skip this dir because tablet path exist, path=" << schema_hash_dir;
            continue;
        } else {
            data_dir->add_pending_ids(pending_id);
            Status st = FileUtils::create_dir(schema_hash_dir);
            if (!st.ok()) {
                LOG(WARNING) << "create dir fail. path=" << schema_hash_dir
                             << " error=" << st.to_string();
                continue;
            }
        }

        TabletSharedPtr new_tablet = Tablet::create_tablet_from_meta(tablet_meta, data_dir);
        DCHECK(new_tablet != nullptr);
        return new_tablet;
    }
    return nullptr;
}

OLAPStatus TabletManager::drop_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                                      bool keep_files) {
    WriteLock wlock(_get_tablets_shard_lock(tablet_id));
    return _drop_tablet_unlocked(tablet_id, schema_hash, keep_files);
}

// Drop specified tablet, the main logical is as follows:
// 1. tablet not in schema change:
//      drop specified tablet directly;
// 2. tablet in schema change:
//      a. schema change not finished && the dropping tablet is a base-tablet:
//          base-tablet cannot be dropped;
//      b. other cases:
//          drop specified tablet directly and clear schema change info.
OLAPStatus TabletManager::_drop_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash,
                                                bool keep_files) {
    LOG(INFO) << "begin drop tablet. tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
    DorisMetrics::instance()->drop_tablet_requests_total->increment(1);

    // Fetch tablet which need to be dropped
    TabletSharedPtr to_drop_tablet = _get_tablet_unlocked(tablet_id, schema_hash);
    if (to_drop_tablet == nullptr) {
        LOG(WARNING) << "fail to drop tablet because it does not exist. "
                     << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
        return OLAP_SUCCESS;
    }

    // Try to get schema change info, we can drop tablet directly if it is not
    // in schema-change state.
    AlterTabletTaskSharedPtr alter_task = to_drop_tablet->alter_task();
    if (alter_task == nullptr) {
        return _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    }

    AlterTabletState alter_state = alter_task->alter_state();
    TTabletId related_tablet_id = alter_task->related_tablet_id();
    TSchemaHash related_schema_hash = alter_task->related_schema_hash();
    ;

    TabletSharedPtr related_tablet = _get_tablet_unlocked(related_tablet_id, related_schema_hash);
    if (related_tablet == nullptr) {
        // TODO(lingbin): in what case, can this happen?
        LOG(WARNING) << "drop tablet directly when related tablet not found. "
                     << " tablet_id=" << related_tablet_id
                     << " schema_hash=" << related_schema_hash;
        return _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    }

    // Check whether the tablet we want to delete is in schema-change state
    bool is_schema_change_finished = (alter_state == ALTER_FINISHED || alter_state == ALTER_FAILED);

    // Check whether the tablet we want to delete is base-tablet
    bool is_dropping_base_tablet = false;
    if (to_drop_tablet->creation_time() < related_tablet->creation_time()) {
        is_dropping_base_tablet = true;
    }

    if (is_dropping_base_tablet && !is_schema_change_finished) {
        LOG(WARNING) << "fail to drop tablet. it is in schema-change state. tablet="
                     << to_drop_tablet->full_name();
        return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
    }

    // When the code gets here, there are two possibilities:
    // 1. The tablet currently being deleted is a base-tablet, and the corresponding
    //    schema-change process has finished;
    // 2. The tablet we are currently trying to drop is not base-tablet(i.e. a tablet
    //    generated from its base-tablet due to schema-change). For example, the current
    //    request is triggered by cancel alter). In this scenario, the corresponding
    //    schema-change task may still in process.

    // Drop specified tablet and clear schema-change info
    // NOTE: must first break the hard-link and then drop the tablet.
    // Otherwise, if first drop tablet, then break link. If BE restarts during execution,
    // after BE restarts, the tablet is no longer in metadata, but because the hard-link
    // is still there, the corresponding file may never be deleted from disk.
    related_tablet->obtain_header_wrlock();
    // should check the related tablet_id in alter task is current tablet to be dropped
    // For example: A related to B, BUT B related to C.
    // If drop A, should not clear B's alter task
    OLAPStatus res = OLAP_SUCCESS;
    AlterTabletTaskSharedPtr related_alter_task = related_tablet->alter_task();
    if (related_alter_task != nullptr && related_alter_task->related_tablet_id() == tablet_id &&
        related_alter_task->related_schema_hash() == schema_hash) {
        related_tablet->delete_alter_task();
        related_tablet->save_meta();
    }
    related_tablet->release_header_lock();
    res = _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to drop tablet which in schema change. tablet="
                     << to_drop_tablet->full_name();
        return res;
    }

    LOG(INFO) << "finish to drop tablet. res=" << res;
    return res;
}

OLAPStatus TabletManager::drop_tablets_on_error_root_path(
        const std::vector<TabletInfo>& tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    for (int32 i = 0; i < _tablets_shards_size; i++) {
        WriteLock wlock(_tablets_shards[i].lock.get());
        for (const TabletInfo& tablet_info : tablet_info_vec) {
            TTabletId tablet_id = tablet_info.tablet_id;
            if ((tablet_id & _tablets_shards_mask) != i) {
                continue;
            }
            TSchemaHash schema_hash = tablet_info.schema_hash;
            VLOG_NOTICE << "drop_tablet begin. tablet_id=" << tablet_id
                    << ", schema_hash=" << schema_hash;
            TabletSharedPtr dropped_tablet = _get_tablet_unlocked(tablet_id, schema_hash);
            if (dropped_tablet == nullptr) {
                LOG(WARNING) << "dropping tablet not exist. "
                             << " tablet=" << tablet_id << " schema_hash=" << schema_hash;
                continue;
            } else {
                tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
                for (list<TabletSharedPtr>::iterator it = tablet_map[tablet_id].table_arr.begin();
                     it != tablet_map[tablet_id].table_arr.end();) {
                    if ((*it)->equal(tablet_id, schema_hash)) {
                        // We should first remove tablet from partition_map to avoid iterator
                        // becoming invalid.
                        _remove_tablet_from_partition(*(*it));
                        it = tablet_map[tablet_id].table_arr.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
        }
    }
    return res;
}

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                                          bool include_deleted, string* err) {
    ReadLock rlock(_get_tablets_shard_lock(tablet_id));
    return _get_tablet_unlocked(tablet_id, schema_hash, include_deleted, err);
}

TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash,
                                                    bool include_deleted, string* err) {
    TabletSharedPtr tablet;
    tablet = _get_tablet_unlocked(tablet_id, schema_hash);
    if (tablet == nullptr && include_deleted) {
        ReadLock rlock(&_shutdown_tablets_lock);
        for (auto& deleted_tablet : _shutdown_tablets) {
            CHECK(deleted_tablet != nullptr) << "deleted tablet is nullptr";
            if (deleted_tablet->tablet_id() == tablet_id &&
                deleted_tablet->schema_hash() == schema_hash) {
                tablet = deleted_tablet;
                break;
            }
        }
    }

    if (tablet == nullptr) {
        if (err != nullptr) {
            *err = "tablet does not exist";
        }
        return nullptr;
    }

    if (!tablet->is_used()) {
        LOG(WARNING) << "tablet cannot be used. tablet=" << tablet_id;
        if (err != nullptr) {
            *err = "tablet cannot be used";
        }
        return nullptr;
    }

    return tablet;
}

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                                          TabletUid tablet_uid, bool include_deleted, string* err) {
    ReadLock rlock(_get_tablets_shard_lock(tablet_id));
    TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, schema_hash, include_deleted, err);
    if (tablet != nullptr && tablet->tablet_uid() == tablet_uid) {
        return tablet;
    }
    return nullptr;
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
    int64_t curr_ms = UnixMillis();
    // Update cache if it is too old
    {
        int interval_sec = config::tablet_stat_cache_update_interval_second;
        std::lock_guard<std::mutex> l(_tablet_stat_mutex);
        if (curr_ms - _last_update_stat_ms > interval_sec * 1000) {
            VLOG_NOTICE << "update tablet stat.";
            _build_tablet_stat();
            _last_update_stat_ms = UnixMillis();
        }
    }

    result->__set_tablets_stats(_tablet_stat_cache);
}

TabletSharedPtr TabletManager::find_best_tablet_to_compaction(
        CompactionType compaction_type, DataDir* data_dir,
        std::vector<TTabletId>& tablet_submitted_compaction) {
    int64_t now_ms = UnixMillis();
    const string& compaction_type_str =
            compaction_type == CompactionType::BASE_COMPACTION ? "base" : "cumulative";
    double highest_score = 0.0;
    uint32_t compaction_score = 0;
    double tablet_scan_frequency = 0.0;
    TabletSharedPtr best_tablet;
    for (const auto& tablets_shard : _tablets_shards) {
        ReadLock rlock(tablets_shard.lock.get());
        for (const auto& tablet_map : tablets_shard.tablet_map) {
            for (const TabletSharedPtr& tablet_ptr : tablet_map.second.table_arr) {
                std::vector<TTabletId>::iterator it_tablet =
                        find(tablet_submitted_compaction.begin(), tablet_submitted_compaction.end(),
                             tablet_ptr->tablet_id());
                if (it_tablet != tablet_submitted_compaction.end()) {
                    continue;
                }
                AlterTabletTaskSharedPtr cur_alter_task = tablet_ptr->alter_task();
                if (cur_alter_task != nullptr && cur_alter_task->alter_state() != ALTER_FINISHED &&
                    cur_alter_task->alter_state() != ALTER_FAILED) {
                    TabletSharedPtr related_tablet =
                            _get_tablet_unlocked(cur_alter_task->related_tablet_id(),
                                                 cur_alter_task->related_schema_hash());
                    if (related_tablet != nullptr &&
                        tablet_ptr->creation_time() > related_tablet->creation_time()) {
                        // Current tablet is newly created during schema-change or rollup, skip it
                        continue;
                    }
                }
                // A not-ready tablet maybe a newly created tablet under schema-change, skip it
                if (tablet_ptr->tablet_state() == TABLET_NOTREADY) {
                    continue;
                }

                if (tablet_ptr->data_dir()->path_hash() != data_dir->path_hash() ||
                    !tablet_ptr->is_used() || !tablet_ptr->init_succeeded() ||
                    !tablet_ptr->can_do_compaction()) {
                    continue;
                }

                int64_t last_failure_ms = tablet_ptr->last_cumu_compaction_failure_time();
                if (compaction_type == CompactionType::BASE_COMPACTION) {
                    last_failure_ms = tablet_ptr->last_base_compaction_failure_time();
                }
                if (now_ms - last_failure_ms <=
                    config::min_compaction_failure_interval_sec * 1000) {
                    VLOG_CRITICAL << "Too often to check compaction, skip it. "
                            << "compaction_type=" << compaction_type_str
                            << ", last_failure_time_ms=" << last_failure_ms
                            << ", tablet_id=" << tablet_ptr->tablet_id();
                    continue;
                }

                if (compaction_type == CompactionType::BASE_COMPACTION) {
                    MutexLock lock(tablet_ptr->get_base_lock(), TRY_LOCK);
                    if (!lock.own_lock()) {
                        continue;
                    }
                } else {
                    MutexLock lock(tablet_ptr->get_cumulative_lock(), TRY_LOCK);
                    if (!lock.own_lock()) {
                        continue;
                    }
                }

                uint32_t current_compaction_score =
                        tablet_ptr->calc_compaction_score(compaction_type);

                double scan_frequency = 0.0;
                if (config::compaction_tablet_scan_frequency_factor != 0) {
                    scan_frequency = tablet_ptr->calculate_scan_frequency();
                }

                double tablet_score =
                        config::compaction_tablet_scan_frequency_factor * scan_frequency +
                        config::compaction_tablet_compaction_score_factor *
                                current_compaction_score;
                if (tablet_score > highest_score) {
                    highest_score = tablet_score;
                    compaction_score = current_compaction_score;
                    tablet_scan_frequency = scan_frequency;
                    best_tablet = tablet_ptr;
                }
            }
        }
    }

    if (best_tablet != nullptr) {
        VLOG_CRITICAL << "Found the best tablet for compaction. "
                << "compaction_type=" << compaction_type_str
                << ", tablet_id=" << best_tablet->tablet_id() << ", path=" << data_dir->path()
                << ", compaction_score=" << compaction_score
                << ", tablet_scan_frequency=" << tablet_scan_frequency
                << ", highest_score=" << highest_score;
        // TODO(lingbin): Remove 'max' from metric name, it would be misunderstood as the
        // biggest in history(like peak), but it is really just the value at current moment.
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            DorisMetrics::instance()->tablet_base_max_compaction_score->set_value(compaction_score);
        } else {
            DorisMetrics::instance()->tablet_cumulative_max_compaction_score->set_value(
                    compaction_score);
        }
    }
    return best_tablet;
}

OLAPStatus TabletManager::load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id,
                                                TSchemaHash schema_hash, const string& meta_binary,
                                                bool update_meta, bool force, bool restore, bool check_path) {
    WriteLock wlock(_get_tablets_shard_lock(tablet_id));
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    OLAPStatus status = tablet_meta->deserialize(meta_binary);
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to load tablet because can not parse meta_binary string. "
                     << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
                     << ", path=" << data_dir->path();
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }

    // check if tablet meta is valid
    if (tablet_meta->tablet_id() != tablet_id || tablet_meta->schema_hash() != schema_hash) {
        LOG(WARNING) << "fail to load tablet because meet invalid tablet meta. "
                     << "trying to load tablet(tablet_id=" << tablet_id
                     << ", schema_hash=" << schema_hash << ")"
                     << ", but meet tablet=" << tablet_meta->full_name()
                     << ", path=" << data_dir->path();
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }
    if (tablet_meta->tablet_uid().hi == 0 && tablet_meta->tablet_uid().lo == 0) {
        LOG(WARNING) << "fail to load tablet because its uid == 0. "
                     << "tablet=" << tablet_meta->full_name() << ", path=" << data_dir->path();
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }

    if (restore) {
        // we're restoring tablet from trash, tablet state should be changed from shutdown back to running
        tablet_meta->set_tablet_state(TABLET_RUNNING);
    }

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, data_dir);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to load tablet. tablet_id=" << tablet_id
                     << ", schema_hash:" << schema_hash;
        return OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR;
    }

    // NOTE: method load_tablet_from_meta could be called by two cases as below
    // case 1: BE start;
    // case 2: Clone Task/Restore
    // For case 1 doesn't need path check because BE is just starting and not ready,
    // just check tablet meta status to judge whether tablet is delete is enough.
    // For case 2, If a tablet has just been copied to local BE,
    // it may be cleared by gc-thread(see perform_path_gc_by_tablet) because the tablet meta may not be loaded to memory.
    // So clone task should check path and then failed and retry in this case.
    if (check_path && !Env::Default()->path_exists(tablet->tablet_path()).ok()) {
        LOG(WARNING) << "tablet path not exists, create tablet failed, path="
                     << tablet->tablet_path();
        return OLAP_ERR_TABLE_ALREADY_DELETED_ERROR;
    }

    if (tablet_meta->tablet_state() == TABLET_SHUTDOWN) {
        LOG(INFO) << "fail to load tablet because it is to be deleted. tablet_id=" << tablet_id
                  << " schema_hash=" << schema_hash << ", path=" << data_dir->path();
        {
            WriteLock shutdown_tablets_wlock(&_shutdown_tablets_lock);
            _shutdown_tablets.push_back(tablet);
        }
        return OLAP_ERR_TABLE_ALREADY_DELETED_ERROR;
    }
    // NOTE: We do not check tablet's initial version here, because if BE restarts when
    // one tablet is doing schema-change, we may meet empty tablet.
    if (tablet->max_version().first == -1 && tablet->tablet_state() == TABLET_RUNNING) {
        LOG(WARNING) << "fail to load tablet. it is in running state but without delta. "
                     << "tablet=" << tablet->full_name() << ", path=" << data_dir->path();
        // tablet state is invalid, drop tablet
        return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
    }

    RETURN_NOT_OK_LOG(tablet->init(),
                      strings::Substitute("tablet init failed. tablet=$0", tablet->full_name()));
    RETURN_NOT_OK_LOG(_add_tablet_unlocked(tablet_id, schema_hash, tablet, update_meta, force),
                      strings::Substitute("fail to add tablet. tablet=$0", tablet->full_name()));

    return OLAP_SUCCESS;
}

OLAPStatus TabletManager::load_tablet_from_dir(DataDir* store, TTabletId tablet_id,
                                               SchemaHash schema_hash,
                                               const string& schema_hash_path, bool force,
                                               bool restore) {
    LOG(INFO) << "begin to load tablet from dir. "
              << " tablet_id=" << tablet_id << " schema_hash=" << schema_hash
              << " path = " << schema_hash_path << " force = " << force << " restore = " << restore;
    // not add lock here, because load_tablet_from_meta already add lock
    string header_path = TabletMeta::construct_header_file_path(schema_hash_path, tablet_id);
    // should change shard id before load tablet
    string shard_path = path_util::dir_name(path_util::dir_name(path_util::dir_name(header_path)));
    string shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    int32_t shard = stol(shard_str);
    // load dir is called by clone, restore, storage migration
    // should change tablet uid when tablet object changed
    RETURN_NOT_OK_LOG(
            TabletMeta::reset_tablet_uid(header_path),
            strings::Substitute("failed to set tablet uid when copied meta file. header_path=%0",
                                header_path));
    ;

    if (!Env::Default()->path_exists(header_path).ok()) {
        LOG(WARNING) << "fail to find header file. [header_path=" << header_path << "]";
        return OLAP_ERR_FILE_NOT_EXIST;
    }

    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    if (tablet_meta->create_from_file(header_path) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to load tablet_meta. file_path=" << header_path;
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }
    // has to change shard id here, because meta file maybe copied from other source
    // its shard is different from local shard
    tablet_meta->set_shard_id(shard);
    string meta_binary;
    tablet_meta->serialize(&meta_binary);
    RETURN_NOT_OK_LOG(
            load_tablet_from_meta(store, tablet_id, schema_hash, meta_binary, true, force, restore, true),
            strings::Substitute("fail to load tablet. header_path=$0", header_path));

    return OLAP_SUCCESS;
}

void TabletManager::release_schema_change_lock(TTabletId tablet_id) {
    VLOG_NOTICE << "release_schema_change_lock begin. tablet_id=" << tablet_id;
    ReadLock rlock(_get_tablets_shard_lock(tablet_id));

    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map_t::iterator it = tablet_map.find(tablet_id);
    if (it == tablet_map.end()) {
        LOG(WARNING) << "tablet does not exists. tablet=" << tablet_id;
    } else {
        it->second.schema_change_lock.unlock();
    }
    VLOG_NOTICE << "release_schema_change_lock end. tablet_id=" << tablet_id;
}

OLAPStatus TabletManager::report_tablet_info(TTabletInfo* tablet_info) {
    DorisMetrics::instance()->report_tablet_requests_total->increment(1);
    LOG(INFO) << "begin to process report tablet info."
              << "tablet_id=" << tablet_info->tablet_id
              << ", schema_hash=" << tablet_info->schema_hash;

    OLAPStatus res = OLAP_SUCCESS;

    TabletSharedPtr tablet = get_tablet(tablet_info->tablet_id, tablet_info->schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "can't find tablet. "
                     << " tablet=" << tablet_info->tablet_id
                     << " schema_hash=" << tablet_info->schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    tablet->build_tablet_report_info(tablet_info);
    VLOG_TRACE << "success to process report tablet info.";
    return res;
}

OLAPStatus TabletManager::report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info) {
    DCHECK(tablets_info != nullptr);
    LOG(INFO) << "begin to report all tablets info";

    // build the expired txn map first, outside the tablet map lock
    std::map<TabletInfo, std::vector<int64_t>> expire_txn_map;
    StorageEngine::instance()->txn_manager()->build_expire_txn_map(&expire_txn_map);
    LOG(INFO) << "find expired transactions for " << expire_txn_map.size() << " tablets";

    DorisMetrics::instance()->report_all_tablets_requests_total->increment(1);

    for (const auto& tablets_shard : _tablets_shards) {
        ReadLock rlock(tablets_shard.lock.get());
        for (const auto& item : tablets_shard.tablet_map) {
            if (item.second.table_arr.empty()) {
                continue;
            }

            uint64_t tablet_id = item.first;
            TTablet t_tablet;
            for (TabletSharedPtr tablet_ptr : item.second.table_arr) {
                TTabletInfo tablet_info;
                tablet_ptr->build_tablet_report_info(&tablet_info);

                // find expired transaction corresponding to this tablet
                TabletInfo tinfo(tablet_id, tablet_ptr->schema_hash(), tablet_ptr->tablet_uid());
                auto find = expire_txn_map.find(tinfo);
                if (find != expire_txn_map.end()) {
                    tablet_info.__set_transaction_ids(find->second);
                    expire_txn_map.erase(find);
                }
                t_tablet.tablet_infos.push_back(tablet_info);
            }

            if (!t_tablet.tablet_infos.empty()) {
                tablets_info->emplace(tablet_id, t_tablet);
            }
        }
    }
    LOG(INFO) << "success to report all tablets info. tablet_count=" << tablets_info->size();
    return OLAP_SUCCESS;
}

OLAPStatus TabletManager::start_trash_sweep() {
    {
        std::vector<int64_t> tablets_to_clean;
        std::vector<TabletSharedPtr>
                all_tablets; // we use this vector to save all tablet ptr for saving lock time.
        for (auto& tablets_shard : _tablets_shards) {
            tablet_map_t& tablet_map = tablets_shard.tablet_map;
            {
                ReadLock rlock(tablets_shard.lock.get());
                for (auto& item : tablet_map) {
                    // try to clean empty item
                    if (item.second.table_arr.empty()) {
                        tablets_to_clean.push_back(item.first);
                    }
                    for (TabletSharedPtr tablet : item.second.table_arr) {
                        all_tablets.push_back(tablet);
                    }
                }
            }

            for (const auto& tablet : all_tablets) {
                tablet->delete_expired_inc_rowsets();
                tablet->delete_expired_stale_rowset();
            }
            all_tablets.clear();

            if (!tablets_to_clean.empty()) {
                WriteLock wlock(tablets_shard.lock.get());
                // clean empty tablet id item
                for (const auto& tablet_id_to_clean : tablets_to_clean) {
                    auto& item = tablet_map[tablet_id_to_clean];
                    if (item.table_arr.empty()) {
                        // try to get schema change lock if could get schema change lock, then nobody
                        // own the lock could remove the item
                        // it will core if schema change thread may hold the lock and this thread will deconstruct lock
                        if (item.schema_change_lock.trylock() == OLAP_SUCCESS) {
                            item.schema_change_lock.unlock();
                            tablet_map.erase(tablet_id_to_clean);
                        }
                    }
                }
                tablets_to_clean.clear(); // We should clear the vector before next loop
            }
        }
    }

    int32_t clean_num = 0;
    do {
#ifndef BE_TEST
        sleep(1);
#endif
        clean_num = 0;
        // should get write lock here, because it will remove tablet from shut_down_tablets
        // and get tablet will access shut_down_tablets
        WriteLock wlock(&_shutdown_tablets_lock);
        auto it = _shutdown_tablets.begin();
        while (it != _shutdown_tablets.end()) {
            // check if the meta has the tablet info and its state is shutdown
            if (it->use_count() > 1) {
                // it means current tablet is referenced by other thread
                ++it;
                continue;
            }
            TabletMetaSharedPtr tablet_meta(new TabletMeta());
            OLAPStatus check_st = TabletMetaManager::get_meta((*it)->data_dir(), (*it)->tablet_id(),
                                                              (*it)->schema_hash(), tablet_meta);
            if (check_st == OLAP_SUCCESS) {
                if (tablet_meta->tablet_state() != TABLET_SHUTDOWN ||
                    tablet_meta->tablet_uid() != (*it)->tablet_uid()) {
                    LOG(WARNING) << "tablet's state changed to normal, skip remove dirs"
                                 << " tablet id = " << tablet_meta->tablet_id()
                                 << " schema hash = " << tablet_meta->schema_hash()
                                 << " old tablet_uid=" << (*it)->tablet_uid()
                                 << " cur tablet_uid=" << tablet_meta->tablet_uid();
                    // remove it from list
                    it = _shutdown_tablets.erase(it);
                    continue;
                }
                // move data to trash
                string tablet_path = (*it)->tablet_path();
                if (Env::Default()->path_exists(tablet_path).ok()) {
                    // take snapshot of tablet meta
                    string meta_file_path = path_util::join_path_segments(
                            (*it)->tablet_path(), std::to_string((*it)->tablet_id()) + ".hdr");
                    (*it)->tablet_meta()->save(meta_file_path);
                    LOG(INFO) << "start to move tablet to trash. tablet_path = " << tablet_path;
                    OLAPStatus rm_st = move_to_trash(tablet_path, tablet_path);
                    if (rm_st != OLAP_SUCCESS) {
                        LOG(WARNING) << "fail to move dir to trash. dir=" << tablet_path;
                        ++it;
                        continue;
                    }
                }
                // remove tablet meta
                TabletMetaManager::remove((*it)->data_dir(), (*it)->tablet_id(),
                                          (*it)->schema_hash());
                LOG(INFO) << "successfully move tablet to trash. "
                          << "tablet_id=" << (*it)->tablet_id()
                          << ", schema_hash=" << (*it)->schema_hash()
                          << ", tablet_path=" << tablet_path;
                it = _shutdown_tablets.erase(it);
                ++clean_num;
            } else {
                // if could not find tablet info in meta store, then check if dir existed
                string tablet_path = (*it)->tablet_path();
                if (Env::Default()->path_exists(tablet_path).ok()) {
                    LOG(WARNING) << "errors while load meta from store, skip this tablet. "
                                 << "tablet_id=" << (*it)->tablet_id()
                                 << ", schema_hash=" << (*it)->schema_hash();
                    ++it;
                } else {
                    LOG(INFO) << "could not find tablet dir, skip it and remove it from gc-queue. "
                              << "tablet_id=" << (*it)->tablet_id()
                              << ", schema_hash=" << (*it)->schema_hash()
                              << ", tablet_path=" << tablet_path;
                    it = _shutdown_tablets.erase(it);
                }
            }

            // yield to avoid holding _tablet_map_lock for too long
            if (clean_num >= 200) {
                break;
            }
        }
    } while (clean_num >= 200);
    return OLAP_SUCCESS;
} // start_trash_sweep

void TabletManager::register_clone_tablet(int64_t tablet_id) {
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    WriteLock wlock(shard.lock.get());
    shard.tablets_under_clone.insert(tablet_id);
}

void TabletManager::unregister_clone_tablet(int64_t tablet_id) {
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    WriteLock wlock(shard.lock.get());
    shard.tablets_under_clone.erase(tablet_id);
}

void TabletManager::try_delete_unused_tablet_path(DataDir* data_dir, TTabletId tablet_id,
                                                  SchemaHash schema_hash,
                                                  const string& schema_hash_path) {
    // acquire the read lock, so that there is no creating tablet or load tablet from meta tasks
    // create tablet and load tablet task should check whether the dir exists
    tablets_shard& shard = _get_tablets_shard(tablet_id);
    ReadLock rlock(shard.lock.get());

    // check if meta already exists
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    OLAPStatus check_st =
            TabletMetaManager::get_meta(data_dir, tablet_id, schema_hash, tablet_meta);
    if (check_st == OLAP_SUCCESS) {
        LOG(INFO) << "tablet meta exist is meta store, skip delete the path " << schema_hash_path;
        return;
    }

    if (shard.tablets_under_clone.count(tablet_id) > 0) {
        LOG(INFO) << "tablet is under clone, skip delete the path " << schema_hash_path;
        return;
    }

    // TODO(ygl): may do other checks in the future
    if (Env::Default()->path_exists(schema_hash_path).ok()) {
        LOG(INFO) << "start to move tablet to trash. tablet_path = " << schema_hash_path;
        OLAPStatus rm_st = move_to_trash(schema_hash_path, schema_hash_path);
        if (rm_st != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to move dir to trash. dir=" << schema_hash_path;
        } else {
            LOG(INFO) << "move path " << schema_hash_path << " to trash successfully";
        }
    }
    return;
}

bool TabletManager::try_schema_change_lock(TTabletId tablet_id) {
    bool res = false;
    VLOG_NOTICE << "try_schema_change_lock begin. tablet_id=" << tablet_id;
    ReadLock rlock(_get_tablets_shard_lock(tablet_id));
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map_t::iterator it = tablet_map.find(tablet_id);
    if (it == tablet_map.end()) {
        LOG(WARNING) << "tablet does not exists. tablet_id=" << tablet_id;
    } else {
        res = (it->second.schema_change_lock.trylock() == OLAP_SUCCESS);
    }
    VLOG_NOTICE << "try_schema_change_lock end. tablet_id=" << tablet_id;
    return res;
}

void TabletManager::update_root_path_info(std::map<string, DataDirInfo>* path_map,
                                          size_t* tablet_count) {
    DCHECK(tablet_count != 0);
    *tablet_count = 0;
    for (const auto& tablets_shard : _tablets_shards) {
        ReadLock rlock(tablets_shard.lock.get());
        for (const auto& entry : tablets_shard.tablet_map) {
            const TableInstances& instance = entry.second;
            for (auto& tablet : instance.table_arr) {
                ++(*tablet_count);
                int64_t data_size = tablet->tablet_footprint();
                auto iter = path_map->find(tablet->data_dir()->path());
                if (iter == path_map->end()) {
                    continue;
                }
                if (iter->second.is_used) {
                    iter->second.data_used_capacity += data_size;
                }
            }
        }
    }
}

void TabletManager::get_partition_related_tablets(int64_t partition_id,
                                                  std::set<TabletInfo>* tablet_infos) {
    ReadLock rlock(&_partition_tablet_map_lock);
    if (_partition_tablet_map.find(partition_id) != _partition_tablet_map.end()) {
        *tablet_infos = _partition_tablet_map[partition_id];
    }
}

void TabletManager::do_tablet_meta_checkpoint(DataDir* data_dir) {
    std::vector<TabletSharedPtr> related_tablets;
    {
        for (const auto& tablets_shard : _tablets_shards) {
            ReadLock rlock(tablets_shard.lock.get());
            for (const auto& tablet_map : tablets_shard.tablet_map) {
                for (const TabletSharedPtr& tablet_ptr : tablet_map.second.table_arr) {
                    if (tablet_ptr->tablet_state() != TABLET_RUNNING) {
                        continue;
                    }

                    if (tablet_ptr->data_dir()->path_hash() != data_dir->path_hash() ||
                        !tablet_ptr->is_used() || !tablet_ptr->init_succeeded()) {
                        continue;
                    }
                    related_tablets.push_back(tablet_ptr);
                }
            }
        }
    }
    for (TabletSharedPtr tablet : related_tablets) {
        tablet->do_tablet_meta_checkpoint();
    }
    return;
}

void TabletManager::_build_tablet_stat() {
    _tablet_stat_cache.clear();
    for (const auto& tablets_shard : _tablets_shards) {
        ReadLock rlock(tablets_shard.lock.get());
        for (const auto& item : tablets_shard.tablet_map) {
            if (item.second.table_arr.empty()) {
                continue;
            }

            TTabletStat stat;
            stat.tablet_id = item.first;
            for (TabletSharedPtr tablet : item.second.table_arr) {
                // TODO(lingbin): if it is nullptr, why is it not deleted?
                if (tablet == nullptr) {
                    continue;
                }
                stat.__set_data_size(tablet->tablet_footprint());
                stat.__set_row_num(tablet->num_rows());
                VLOG_NOTICE << "building tablet stat. tablet_id=" << item.first
                        << ", data_size=" << tablet->tablet_footprint()
                        << ", row_num=" << tablet->num_rows();
                break;
            }

            _tablet_stat_cache.emplace(item.first, stat);
        }
    }
}

OLAPStatus TabletManager::_create_initial_rowset_unlocked(const TCreateTabletReq& request,
                                                          Tablet* tablet) {
    OLAPStatus res = OLAP_SUCCESS;
    if (request.version < 1) {
        LOG(WARNING) << "init version of tablet should at least 1. req.ver=" << request.version;
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    } else {
        Version version(0, request.version);
        VLOG_NOTICE << "begin to create init version. version=" << version;
        RowsetSharedPtr new_rowset;
        do {
            RowsetWriterContext context;
            context.rowset_id = StorageEngine::instance()->next_rowset_id();
            context.tablet_uid = tablet->tablet_uid();
            context.tablet_id = tablet->tablet_id();
            context.partition_id = tablet->partition_id();
            context.tablet_schema_hash = tablet->schema_hash();
            if (!request.__isset.storage_format ||
                request.storage_format == TStorageFormat::DEFAULT) {
                context.rowset_type = StorageEngine::instance()->default_rowset_type();
            } else if (request.storage_format == TStorageFormat::V1) {
                context.rowset_type = RowsetTypePB::ALPHA_ROWSET;
            } else if (request.storage_format == TStorageFormat::V2) {
                context.rowset_type = RowsetTypePB::BETA_ROWSET;
            } else {
                LOG(ERROR) << "invalid TStorageFormat: " << request.storage_format;
                DCHECK(false);
                context.rowset_type = StorageEngine::instance()->default_rowset_type();
            }
            context.rowset_path_prefix = tablet->tablet_path();
            context.tablet_schema = &(tablet->tablet_schema());
            context.rowset_state = VISIBLE;
            context.version = version;
            context.version_hash = request.version_hash;
            // there is no data in init rowset, so overlapping info is unknown.
            context.segments_overlap = OVERLAP_UNKNOWN;

            std::unique_ptr<RowsetWriter> builder;
            res = RowsetFactory::create_rowset_writer(context, &builder);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to init rowset writer for tablet " << tablet->full_name();
                break;
            }
            res = builder->flush();
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to flush rowset writer for tablet " << tablet->full_name();
                break;
            }

            new_rowset = builder->build();
            res = tablet->add_rowset(new_rowset, false);
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to add rowset for tablet " << tablet->full_name();
                break;
            }
        } while (0);

        // Unregister index and delete files(index and data) if failed
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to create initial rowset. res=" << res << " version=" << version;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            return res;
        }
    }
    tablet->set_cumulative_layer_point(request.version + 1);
    // NOTE: should not save tablet meta here, because it will be saved if add to map successfully

    return res;
}

OLAPStatus TabletManager::_create_tablet_meta_unlocked(const TCreateTabletReq& request,
                                                       DataDir* store, const bool is_schema_change,
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
            int32_t old_col_idx = base_tablet->field_index(column.column_name);
            if (old_col_idx != -1) {
                uint32_t old_unique_id =
                        base_tablet->tablet_schema().column(old_col_idx).unique_id();
                col_idx_to_unique_id[new_col_idx] = old_unique_id;
            } else {
                // Not exist in old tablet, it is a new added column
                col_idx_to_unique_id[new_col_idx] = next_unique_id++;
            }
        }
    }
    LOG(INFO) << "creating tablet meta. next_unique_id=" << next_unique_id;

    // We generate a new tablet_uid for this new tablet.
    uint64_t shard_id = 0;
    RETURN_NOT_OK_LOG(store->get_shard(&shard_id), "fail to get root path shard");
    OLAPStatus res = TabletMeta::create(request, TabletUid::gen_uid(), shard_id, next_unique_id,
                                        col_idx_to_unique_id, tablet_meta);

    // TODO(lingbin): when beta-rowset is default, should remove it
    if (request.__isset.storage_format && request.storage_format == TStorageFormat::V2) {
        (*tablet_meta)->set_preferred_rowset_type(BETA_ROWSET);
    } else {
        (*tablet_meta)->set_preferred_rowset_type(ALPHA_ROWSET);
    }
    return res;
}

OLAPStatus TabletManager::_drop_tablet_directly_unlocked(TTabletId tablet_id,
                                                         SchemaHash schema_hash, bool keep_files) {
    TabletSharedPtr dropped_tablet = _get_tablet_unlocked(tablet_id, schema_hash);
    if (dropped_tablet == nullptr) {
        LOG(WARNING) << "fail to drop tablet because it does not exist. "
                     << " tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    list<TabletSharedPtr>& candidate_tablets = tablet_map[tablet_id].table_arr;
    list<TabletSharedPtr>::iterator it = candidate_tablets.begin();
    while (it != candidate_tablets.end()) {
        if (!(*it)->equal(tablet_id, schema_hash)) {
            ++it;
            continue;
        }

        TabletSharedPtr tablet = *it;
        _remove_tablet_from_partition(*(*it));
        it = candidate_tablets.erase(it);
        if (!keep_files) {
            // drop tablet will update tablet meta, should lock
            WriteLock wrlock(tablet->get_header_lock_ptr());
            LOG(INFO) << "set tablet to shutdown state and remove it from memory. "
                      << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
                      << ", tablet_path=" << dropped_tablet->tablet_path();
            // NOTE: has to update tablet here, but must not update tablet meta directly.
            // because other thread may hold the tablet object, they may save meta too.
            // If update meta directly here, other thread may override the meta
            // and the tablet will be loaded at restart time.
            // To avoid this exception, we first set the state of the tablet to `SHUTDOWN`.
            tablet->set_tablet_state(TABLET_SHUTDOWN);
            tablet->save_meta();
            {
                WriteLock wlock(&_shutdown_tablets_lock);
                _shutdown_tablets.push_back(tablet);
            }
        }
    }

    dropped_tablet->deregister_tablet_from_dir();
    return OLAP_SUCCESS;
}

TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash) {
    VLOG_NOTICE << "begin to get tablet. tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map_t::iterator it = tablet_map.find(tablet_id);
    if (it != tablet_map.end()) {
        for (TabletSharedPtr tablet : it->second.table_arr) {
            CHECK(tablet != nullptr) << "tablet is nullptr. tablet_id=" << tablet_id;
            if (tablet->equal(tablet_id, schema_hash)) {
                VLOG_NOTICE << "get tablet success. tablet_id=" << tablet_id
                        << ", schema_hash=" << schema_hash;
                return tablet;
            }
        }
    }

    VLOG_NOTICE << "fail to get tablet. tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
    // Return nullptr tablet if fail
    TabletSharedPtr tablet;
    return tablet;
}

void TabletManager::_add_tablet_to_partition(const Tablet& tablet) {
    WriteLock wlock(&_partition_tablet_map_lock);
    _partition_tablet_map[tablet.partition_id()].insert(tablet.get_tablet_info());
}

void TabletManager::_remove_tablet_from_partition(const Tablet& tablet) {
    WriteLock wlock(&_partition_tablet_map_lock);
    _partition_tablet_map[tablet.partition_id()].erase(tablet.get_tablet_info());
    if (_partition_tablet_map[tablet.partition_id()].empty()) {
        _partition_tablet_map.erase(tablet.partition_id());
    }
}

void TabletManager::obtain_specific_quantity_tablets(vector<TabletInfo> &tablets_info,
                                                     int64_t num) {
    for (const auto& tablets_shard : _tablets_shards) {
        ReadLock rlock(tablets_shard.lock.get());
        for (const auto& item : tablets_shard.tablet_map) {
            for (TabletSharedPtr tablet : item.second.table_arr) {
                if (tablets_info.size() >= num) {
                    return;
                }
                if (tablet == nullptr) {
                    continue;
                }
                TabletInfo tablet_info(tablet->get_tablet_info().tablet_id,
                                       tablet->get_tablet_info().schema_hash,
                                       tablet->get_tablet_info().tablet_uid);
                tablets_info.emplace_back(tablet_info);
            }
        }
    }
}

RWMutex* TabletManager::_get_tablets_shard_lock(TTabletId tabletId) {
    return _get_tablets_shard(tabletId).lock.get();
}

TabletManager::tablet_map_t& TabletManager::_get_tablet_map(TTabletId tabletId) {
    return _get_tablets_shard(tabletId).tablet_map;
}

TabletManager::tablets_shard& TabletManager::_get_tablets_shard(TTabletId tabletId) {
    return _tablets_shards[tabletId & _tablets_shards_mask];
}

void TabletManager::get_tablets_distribution_on_different_disks(
                    std::map<int64_t, std::map<DataDir*, int64_t>> &tablets_num_on_disk,
                    std::map<int64_t, std::map<DataDir*, std::vector<TabletSize>>> &tablets_info_on_disk) {
    std::vector<DataDir*> data_dirs = StorageEngine::instance()->get_stores();
    ReadLock rlock(&_partition_tablet_map_lock);
    std::map<int64_t, std::set<TabletInfo>>::iterator partition_iter = _partition_tablet_map.begin();
    for (; partition_iter != _partition_tablet_map.end(); partition_iter++) {
        std::map<DataDir*, int64_t> tablets_num;
        std::map<DataDir*, std::vector<TabletSize>> tablets_info;
        for(int i = 0; i < data_dirs.size(); i++) {
            tablets_num[data_dirs[i]] = 0;
        }
        int64_t partition_id = partition_iter->first;
        std::set<TabletInfo>::iterator tablet_info_iter = (partition_iter->second).begin();
        for(; tablet_info_iter != (partition_iter->second).end(); tablet_info_iter++) {
            TabletSharedPtr tablet = get_tablet(tablet_info_iter->tablet_id, tablet_info_iter->schema_hash);
            DataDir* data_dir = tablet->data_dir();
            size_t tablet_footprint = tablet->tablet_footprint();
            tablets_num[data_dir]++;
            TabletSize tablet_size(tablet_info_iter->tablet_id, tablet_info_iter->schema_hash, tablet_footprint);
            tablets_info[data_dir].push_back(tablet_size);
        }
        tablets_num_on_disk[partition_id] = tablets_num;
        tablets_info_on_disk[partition_id] = tablets_info;
    }
}

} // end namespace doris
