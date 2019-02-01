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

#include <signal.h>

#include <algorithm>
#include <cstdio>
#include <new>
#include <queue>
#include <set>
#include <random>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <rapidjson/document.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "agent/file_downloader.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/lru_cache.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/schema_change.h"
#include "olap/data_dir.h"
#include "olap/utils.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_id_generator.h"
#include "util/time.h"
#include "util/doris_metrics.h"
#include "util/pretty_printer.h"

using apache::thrift::ThriftDebugString;
using boost::filesystem::canonical;
using boost::filesystem::directory_iterator;
using boost::filesystem::path;
using boost::filesystem::recursive_directory_iterator;
using std::back_inserter;
using std::copy;
using std::inserter;
using std::list;
using std::map;
using std::nothrow;
using std::pair;
using std::priority_queue;
using std::set;
using std::set_difference;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {

TabletManager* TabletManager::_s_instance = nullptr;
std::mutex TabletManager::_mlock;

TabletManager* TabletManager::instance() {
    if (_s_instance == nullptr) {
        std::lock_guard<std::mutex> lock(_mlock);
        if (_s_instance == nullptr) {
            _s_instance = new TabletManager();
        }
    }
    return _s_instance;
}

bool _sort_tablet_by_creation_time(const TabletSharedPtr& a, const TabletSharedPtr& b) {
    return a->creation_time() < b->creation_time();
}

TabletManager::TabletManager()
    : _global_tablet_id(0),
      _tablet_stat_cache_update_time_ms(0),
      _available_storage_medium_type_count(0) { }

OLAPStatus TabletManager::add_tablet(TTabletId tablet_id, SchemaHash schema_hash,
                                 const TabletSharedPtr& tablet, bool force) {
    OLAPStatus res = OLAP_SUCCESS;
    VLOG(3) << "begin to add tablet to TabletManager. "
            << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
            << ", force=" << force;
    _tablet_map_lock.wrlock();

    tablet->set_id(_global_tablet_id++);

    TabletSharedPtr table_item;
    for (TabletSharedPtr item : _tablet_map[tablet_id].table_arr) {
        if (item->equal(tablet_id, schema_hash)) {
            table_item = item;
            break;
        }
    }

    if (table_item.get() == NULL) {
        _tablet_map[tablet_id].table_arr.push_back(tablet);
        _tablet_map[tablet_id].table_arr.sort(_sort_tablet_by_creation_time);
        _tablet_map_lock.unlock();
        return res;
    }
    _tablet_map_lock.unlock();

    if (!force) {
        if (table_item->tablet_path() == tablet->tablet_path()) {
            LOG(WARNING) << "add the same tablet twice! tablet_id="
                << tablet_id << " schema_hash=" << tablet_id;
            return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
        }
    }

    table_item->obtain_header_rdlock();
    int64_t old_time = table_item->rowset_with_max_version()->creation_time();
    int64_t new_time = tablet->rowset_with_max_version()->creation_time();
    int32_t old_version = table_item->rowset_with_max_version()->end_version();
    int32_t new_version = tablet->rowset_with_max_version()->end_version();
    table_item->release_header_lock();

    /*
     * In restore process, we replace all origin files in tablet dir with
     * the downloaded snapshot files. Than we try to reload tablet header.
     * force == true means we forcibly replace the Tablet in _tablet_map
     * with the new one. But if we do so, the files in the tablet dir will be
     * dropped when the origin Tablet deconstruct.
     * So we set keep_files == true to not delete files when the
     * origin Tablet deconstruct.
     */
    bool keep_files = force ? true : false;
    if (force || (new_version > old_version
            || (new_version == old_version && new_time > old_time))) {
        drop_tablet(tablet_id, schema_hash, keep_files);
        _tablet_map_lock.wrlock();
        _tablet_map[tablet_id].table_arr.push_back(tablet);
        _tablet_map[tablet_id].table_arr.sort(_sort_tablet_by_creation_time);
        _tablet_map_lock.unlock();
    } else {
        tablet->mark_dropped();
        res = OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
    }
    LOG(WARNING) << "add duplicated tablet. force=" << force << ", res=" << res
            << ", tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
            << ", old_version=" << old_version << ", new_version=" << new_version
            << ", old_time=" << old_time << ", new_time=" << new_time
            << ", old_tablet_path=" << table_item->tablet_path()
            << ", new_tablet_path=" << tablet->tablet_path();

    return res;
} // add_tablet

void TabletManager::cancel_unfinished_schema_change() {
    // Schema Change在引擎退出时schemachange信息还保存在在Header里，
    // 引擎重启后，需清除schemachange信息，上层会重做
    uint64_t canceled_num = 0;
    LOG(INFO) << "begin to cancel unfinished schema change.";

    TTabletId tablet_id;
    TSchemaHash schema_hash;
    vector<Version> schema_change_versions;

    for (const auto& tablet_instance : _tablet_map) {
        for (TabletSharedPtr tablet : tablet_instance.second.table_arr) {
            if (tablet.get() == NULL) {
                OLAP_LOG_WARNING("get empty TabletSharedPtr. [tablet_id=%ld]", tablet_instance.first);
                continue;
            }

            const AlterTabletTask& alter_task = tablet->alter_task();
            AlterTabletState alter_state = alter_task.alter_state();
            tablet_id = alter_task.related_tablet_id();
            schema_hash = alter_task.related_schema_hash();;
            if (alter_state == AlterTabletState::ALTER_NONE) {
                continue;
            }

            TabletSharedPtr new_tablet = get_tablet(tablet_id, schema_hash, false);
            if (new_tablet.get() == NULL) {
                OLAP_LOG_WARNING("the tablet referenced by schema change cannot be found. "
                                 "schema change cancelled. [tablet='%s']",
                                 tablet->full_name().c_str());
                continue;
            }

            // DORIS-3741. Upon restart, it should not clear schema change request.
            new_tablet->set_alter_state(AlterTabletState::ALTER_FAILED);
            tablet->set_alter_state(AlterTabletState::ALTER_FAILED);
            VLOG(3) << "cancel unfinished schema change. tablet=" << tablet->full_name();
            ++canceled_num;
        }
    }

    LOG(INFO) << "finish to cancel unfinished schema change! canceled_num=" << canceled_num;
}

bool TabletManager::check_tablet_id_exist(TTabletId tablet_id) {
    bool is_exist = false;
    _tablet_map_lock.rdlock();

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end() && it->second.table_arr.size() != 0) {
        is_exist = true;
    }

    _tablet_map_lock.unlock();
    return is_exist;
} // check_tablet_id_exist

void TabletManager::clear() {
    _tablet_map.clear();
} // clear

OLAPStatus TabletManager::create_inital_rowset(TTabletId tablet_id, SchemaHash schema_hash,
                                           Version version, VersionHash version_hash) {
    VLOG(3) << "begin to create init version. "
            << "begin=" << version.first << ", end=" << version.second;
    TabletSharedPtr tablet;
    RowsetSharedPtr new_rowset;
    OLAPStatus res = OLAP_SUCCESS;
    do {
        if (version.first > version.second) {
            OLAP_LOG_WARNING("begin should not larger than end. [begin=%d end=%d]",
                             version.first, version.second);
            res = OLAP_ERR_INPUT_PARAMETER_ERROR;
            break;
        }

        // Get tablet and generate new index
        tablet = get_tablet(tablet_id, schema_hash);
        if (tablet.get() == NULL) {
            OLAP_LOG_WARNING("fail to find tablet. [tablet=%ld]", tablet_id);
            res = OLAP_ERR_TABLE_NOT_FOUND;
            break;
        }
        RowsetId rowset_id = 0;
        RowsetIdGenerator::instance()->get_next_id(tablet->data_dir(), &rowset_id);
        RowsetWriterContextBuilder context_builder;
        context_builder.set_rowset_id(rowset_id)
                       .set_tablet_id(tablet->tablet_id())
                       .set_partition_id(tablet->partition_id())
                       .set_tablet_schema_hash(tablet->schema_hash())
                       .set_rowset_type(ALPHA_ROWSET)
                       .set_rowset_path_prefix(tablet->tablet_path())
                       .set_tablet_schema(&(tablet->tablet_schema()))
                       .set_data_dir(tablet->data_dir())
                       .set_rowset_state(VISIBLE)
                       .set_version(version)
                       .set_version_hash(version_hash);
        RowsetWriterContext context = context_builder.build();
        RowsetWriter* builder = new (std::nothrow)AlphaRowsetWriter(); 
        if (builder == nullptr) {
            LOG(WARNING) << "fail to new rowset.";
            return OLAP_ERR_MALLOC_ERROR;
        }
        builder->init(context);
        if (OLAP_SUCCESS != builder->flush()) {
            LOG(WARNING) << "fail to finalize writer. tablet=" << tablet->full_name();
            break;
        }

        new_rowset = builder->build();
        res = tablet->add_rowset(new_rowset);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to add rowset to tablet. "
                         << "tablet=" << tablet->full_name();
            break;
        }
    } while (0);

    // Unregister index and delete files(index and data) if failed
    if (res != OLAP_SUCCESS && tablet != nullptr) {
        StorageEngine::instance()->add_unused_rowset(new_rowset);
    }

    VLOG(3) << "create init version end. res=" << res;
    return res;
} // create_inital_rowset

OLAPStatus TabletManager::create_tablet(const TCreateTabletReq& request, 
    std::vector<DataDir*> stores) {
    OLAPStatus res = OLAP_SUCCESS;
    bool is_tablet_added = false;

    LOG(INFO) << "begin to process create tablet. tablet=" << request.tablet_id
              << ", schema_hash=" << request.tablet_schema.schema_hash;

    DorisMetrics::create_tablet_requests_total.increment(1);

    // 1. Make sure create_tablet operation is idempotent:
    //    return success if tablet with same tablet_id and schema_hash exist,
    //           false if tablet with same tablet_id but different schema_hash exist
    if (check_tablet_id_exist(request.tablet_id)) {
        TabletSharedPtr tablet = get_tablet(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (tablet.get() != NULL) {
            LOG(INFO) << "create tablet success for tablet already exist.";
            return OLAP_SUCCESS;
        } else {
            OLAP_LOG_WARNING("tablet with different schema hash already exists.");
            return OLAP_ERR_CE_TABLET_ID_EXIST;
        }
    }

    // 2. Lock to ensure that all create_tablet operation execute in serial
    static Mutex create_tablet_lock;
    MutexLock auto_lock(&create_tablet_lock);

    TabletSharedPtr tablet;
    do {
        // 3. Create tablet with only header, no deltas
        tablet = create_tablet(request, false, NULL, stores);
        if (tablet == NULL) {
            res = OLAP_ERR_CE_CMD_PARAMS_ERROR;
            OLAP_LOG_WARNING("fail to create tablet. [res=%d]", res);
            break;
        }

        // 4. Add tablet to StorageEngine will make it visiable to user
        res = add_tablet(
                request.tablet_id, request.tablet_schema.schema_hash, tablet, false);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to add tablet to StorageEngine. [res=%d]", res);
            break;
        }
        is_tablet_added = true;

        TabletSharedPtr tablet_ptr = get_tablet(
                request.tablet_id, request.tablet_schema.schema_hash);
        if (tablet_ptr.get() == NULL) {
            res = OLAP_ERR_TABLE_NOT_FOUND;
            OLAP_LOG_WARNING("fail to get tablet. [res=%d]", res);
            break;
        }

        // 5. Register tablet into StorageEngine, so that we can manage tablet from
        // the perspective of root path.
        // Example: unregister all tables when a bad disk found.
        res = tablet_ptr->register_tablet_into_dir();
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to register tablet into StorageEngine. [res=%d, root_path=%s]",
                    res, tablet_ptr->storage_root_path_name().c_str());
            break;
        }

        // 6. Create init version if this is not a restore mode replica and request.version is set
        // bool in_restore_mode = request.__isset.in_restore_mode && request.in_restore_mode;
        // if (!in_restore_mode && request.__isset.version) {
        res = _create_inital_rowset(tablet_ptr, request);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create initial version for tablet. [res=%d]", res);
        }
        // }
    } while (0);

    // 7. clear environment
    if (res != OLAP_SUCCESS) {
        DorisMetrics::create_tablet_requests_failed.increment(1);
        if (is_tablet_added) {
            OLAPStatus status = drop_tablet(
                    request.tablet_id, request.tablet_schema.schema_hash);
            if (status !=  OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to drop tablet when create tablet failed. [res=%d]", res);
            }
        } else if (NULL != tablet) {
            tablet->delete_all_files();
        }
    }

    LOG(INFO) << "finish to process create tablet. res=" << res;
    return res;
} // create_tablet

TabletSharedPtr TabletManager::create_tablet(
        const TCreateTabletReq& request, const bool is_schema_change_tablet,
        const TabletSharedPtr ref_tablet, std::vector<DataDir*> stores) {

    TabletSharedPtr tablet;
    // Try to create tablet on each of all_available_root_path, util success
    for (auto& store : stores) {
        TabletMeta* tablet_meta = nullptr;
        OLAPStatus res = _create_tablet_meta(request, store, is_schema_change_tablet, ref_tablet, &tablet_meta);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to create tablet meta. res=" << res << ", root=" << store->path();
            break;
        }

        tablet = Tablet::create_from_tablet_meta(tablet_meta, store);
        if (tablet == nullptr) {
            LOG(WARNING) << "fail to load tablet from tablet_meta. root_path:" << store->path();
            break;
        }

        // save tablet_meta finally
        res = TabletMetaManager::save(store, request.tablet_id, request.tablet_schema.schema_hash, tablet_meta);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to save tablet meta. res=" << res << ", root=" << store->path();
            break;
        }
        break;
    }

    return tablet;
} // create_tablet

// Drop tablet specified, the main logical is as follows:
// 1. tablet not in schema change:
//      drop specified tablet directly;
// 2. tablet in schema change:
//      a. schema change not finished && dropped tablet is base :
//          base tablet cannot be dropped;
//      b. other cases:
//          drop specified tablet and clear schema change info.
OLAPStatus TabletManager::drop_tablet(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    LOG(INFO) << "begin to process drop tablet."
        << "tablet=" << tablet_id << ", schema_hash=" << schema_hash;
    DorisMetrics::drop_tablet_requests_total.increment(1);

    OLAPStatus res = OLAP_SUCCESS;

    // Get tablet which need to be droped
    _tablet_map_lock.rdlock();
    TabletSharedPtr dropped_tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
    _tablet_map_lock.unlock();
    if (dropped_tablet.get() == NULL) {
        OLAP_LOG_WARNING("fail to drop not existed tablet. [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // Try to get schema change info
    dropped_tablet->obtain_header_rdlock();
    const AlterTabletTask& alter_task = dropped_tablet->alter_task();
    AlterTabletState alter_state = alter_task.alter_state();
    TTabletId related_tablet_id = alter_task.related_tablet_id();
    TSchemaHash related_schema_hash = alter_task.related_schema_hash();;
    dropped_tablet->release_header_lock();

    // Drop tablet directly when not in schema change
    if (alter_state == AlterTabletState::ALTER_NONE) {
        return _drop_tablet_directly(tablet_id, schema_hash, keep_files);
    }

    // Check tablet is in schema change or not, is base tablet or not
    bool is_schema_change_finished = true;
    if (alter_state != AlterTabletState::ALTER_NONE) {
        is_schema_change_finished = false;
    }

    bool is_drop_base_tablet = false;
    _tablet_map_lock.rdlock();
    TabletSharedPtr related_tablet = _get_tablet_with_no_lock(
            related_tablet_id, related_schema_hash);
    _tablet_map_lock.unlock();
    if (related_tablet.get() == NULL) {
        OLAP_LOG_WARNING("drop tablet directly when related tablet not found. "
                         "[tablet_id=%ld schema_hash=%d]",
                         related_tablet_id, related_schema_hash);
        return _drop_tablet_directly(tablet_id, schema_hash, keep_files);
    }

    if (dropped_tablet->creation_time() < related_tablet->creation_time()) {
        is_drop_base_tablet = true;
    }

    if (is_drop_base_tablet && !is_schema_change_finished) {
        OLAP_LOG_WARNING("base tablet in schema change cannot be droped. [tablet=%s]",
                         dropped_tablet->full_name().c_str());
        return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
    }

    // Drop specified tablet and clear schema change info
    _tablet_map_lock.wrlock();
    related_tablet->obtain_header_wrlock();
    related_tablet->delete_alter_task();
    res = related_tablet->save_tablet_meta();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "fail to save tablet header. res=" << res
                   << ", tablet=" << related_tablet->full_name();
    }

    res = _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    related_tablet->release_header_lock();
    _tablet_map_lock.unlock();
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to drop tablet which in schema change. [tablet=%s]",
                         dropped_tablet->full_name().c_str());
        return res;
    }

    LOG(INFO) << "finish to drop tablet. res=" << res;
    return res;
} // drop_tablet

OLAPStatus TabletManager::drop_tablets_on_error_root_path(
        const vector<TabletInfo>& tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;

    _tablet_map_lock.wrlock();

    for (const TabletInfo& tablet_info : tablet_info_vec) {
        TTabletId tablet_id = tablet_info.tablet_id;
        TSchemaHash schema_hash = tablet_info.schema_hash;
        VLOG(3) << "drop_tablet begin. tablet_id=" << tablet_id
                << ", schema_hash=" << schema_hash;
        TabletSharedPtr dropped_tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
        if (dropped_tablet.get() == NULL) {
            OLAP_LOG_WARNING("dropping tablet not exist. [tablet=%ld schema_hash=%d]",
                             tablet_id, schema_hash);
            continue;
        } else {
            for (list<TabletSharedPtr>::iterator it = _tablet_map[tablet_id].table_arr.begin();
                    it != _tablet_map[tablet_id].table_arr.end();) {
                if ((*it)->equal(tablet_id, schema_hash)) {
                    it = _tablet_map[tablet_id].table_arr.erase(it);
                } else {
                    ++it;
                }
            }

            if (_tablet_map[tablet_id].table_arr.empty()) {
                _tablet_map.erase(tablet_id);
            }
        }
    }

    _tablet_map_lock.unlock();

    return res;
} // drop_tablets_on_error_root_path

TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, SchemaHash schema_hash, bool load_tablet) {
    _tablet_map_lock.rdlock();
    TabletSharedPtr tablet;
    tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
    _tablet_map_lock.unlock();

    if (tablet.get() != NULL) {
        if (!tablet->is_used()) {
            OLAP_LOG_WARNING("tablet cannot be used. [tablet=%ld]", tablet_id);
            tablet.reset();
        } else if (load_tablet && !tablet->is_loaded()) {
            if (tablet->load() != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to load tablet. [tablet=%ld]", tablet_id);
                tablet.reset();
            }
        }
    }

    return tablet;
} // get_tablet

void TabletManager::get_tablet_stat(TTabletStatResult& result) {
    VLOG(3) << "begin to get all tablet stat.";

    // get current time
    int64_t current_time = UnixMillis();
    
    _tablet_map_lock.wrlock();
    // update cache if too old
    if (current_time - _tablet_stat_cache_update_time_ms > 
        config::tablet_stat_cache_update_interval_second * 1000) {
        VLOG(3) << "update tablet stat.";
        _build_tablet_stat();
    }

    result.__set_tablets_stats(_tablet_stat_cache);

    _tablet_map_lock.unlock();
} // get_tablet_stat

TabletSharedPtr TabletManager::find_best_tablet_to_compaction(CompactionType compaction_type) {
    ReadLock tablet_map_rdlock(&_tablet_map_lock);
    uint32_t highest_score = 0;
    TabletSharedPtr best_tablet;
    for (tablet_map_t::value_type& table_ins : _tablet_map){
        for (TabletSharedPtr& table_ptr : table_ins.second.table_arr) {
            if (!table_ptr->is_loaded() || !table_ptr->can_do_compaction()) {
                continue;
            }

            ReadLock rdlock(table_ptr->get_header_lock_ptr());
            uint32_t table_score = 0;
            if (compaction_type == CompactionType::BASE_COMPACTION) {
                table_score = table_ptr->calc_base_compaction_score();
            } else if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                table_score = table_ptr->calc_cumulative_compaction_score();
            }
            if (table_score > highest_score) {
                highest_score = table_score;
                best_tablet = table_ptr;
            }
        }
    }
    return best_tablet;
}

OLAPStatus TabletManager::load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id,
        TSchemaHash schema_hash, const std::string& meta_binary) {
    std::unique_ptr<TabletMeta> tablet_meta(new TabletMeta());
    bool parsed = tablet_meta->deserialize(meta_binary);
    if (!parsed) {
        LOG(WARNING) << "parse meta_binary string failed for tablet_id:" << tablet_id << " schema_hash:" << schema_hash;
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }

    // init must be called
    TabletSharedPtr tablet =
        Tablet::create_from_tablet_meta(tablet_meta.release(), data_dir);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to new tablet. tablet_id=" << tablet_id << ", schema_hash:" << schema_hash;
        return OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR;
    }

    if (tablet->rowset_with_max_version() == nullptr && !tablet->is_schema_changing()) {
        LOG(WARNING) << "tablet not in schema change state without delta is invalid."
                     << "tablet=" << tablet->full_name();
        // tablet state is invalid, drop tablet
        tablet->mark_dropped();
        return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
    }

    OLAPStatus res = add_tablet(tablet_id, schema_hash, tablet, false);
    if (res != OLAP_SUCCESS) {
        // insert existed tablet return OLAP_SUCCESS
        if (res == OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE) {
            LOG(WARNING) << "add duplicate tablet. tablet=" << tablet->full_name();
        }

        LOG(WARNING) << "failed to add tablet. tablet=" << tablet->full_name();
        return res;
    }
    res = tablet->register_tablet_into_dir();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to register tablet into root path. root_path=" << tablet->storage_root_path_name();
        if (drop_tablet(tablet_id, schema_hash, false) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to drop tablet when create tablet failed. "
                <<"tablet=" << tablet_id << " schema_hash=" << schema_hash;
        }
        return res;
    }

    return OLAP_SUCCESS;
} // load_tablet_from_meta

OLAPStatus TabletManager::load_one_tablet(
        DataDir* store, TTabletId tablet_id, SchemaHash schema_hash,
        const string& schema_hash_path, bool force) {
    stringstream header_name_stream;
    header_name_stream << schema_hash_path << "/" << tablet_id << ".hdr";
    string header_path = header_name_stream.str();
    path boost_schema_hash_path(schema_hash_path);

    if (access(header_path.c_str(), F_OK) != 0) {
        LOG(WARNING) << "fail to find header file. [header_path=" << header_path << "]";
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_FILE_NOT_EXIST;
    }

    auto tablet = Tablet::create_from_tablet_meta_file(header_path, store);
    if (tablet == NULL) {
        LOG(WARNING) << "fail to load tablet. [header_path=" << header_path << "]";
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    if (tablet->rowset_with_max_version() == NULL && !tablet->is_schema_changing()) {
        OLAP_LOG_WARNING("tablet not in schema change state without delta is invalid. "
                         "[header_path=%s]",
                         header_path.c_str());
        move_to_trash(boost_schema_hash_path, boost_schema_hash_path);
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    // 这里不需要SAFE_DELETE(tablet),因为tablet指针已经在add_table中托管到smart pointer中
    OLAPStatus res = OLAP_SUCCESS;
    string table_name = tablet->full_name();
    res = add_tablet(tablet_id, schema_hash, tablet, force);
    if (res != OLAP_SUCCESS) {
        // 插入已经存在的table时返回成功
        if (res == OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE) {
            return OLAP_SUCCESS;
        }

        LOG(WARNING) << "failed to add tablet. [tablet=" << table_name << "]";
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    if (tablet->register_tablet_into_dir() != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to register tablet into root path. [root_path=%s]",
                         schema_hash_path.c_str());

        if (drop_tablet(tablet_id, schema_hash) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to drop tablet when create tablet failed. "
                             "[tablet=%ld schema_hash=%d]",
                             tablet_id, schema_hash);
        }

        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }

    VLOG(3) << "succeed to add tablet. tablet=" << tablet->full_name()
            << ", path=" << schema_hash_path;
    return OLAP_SUCCESS;
} // load_one_tablet

void TabletManager::release_schema_change_lock(TTabletId tablet_id) {
    VLOG(3) << "release_schema_change_lock begin. tablet_id=" << tablet_id;
    _tablet_map_lock.rdlock();

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it == _tablet_map.end()) {
        OLAP_LOG_WARNING("tablet does not exists. [tablet=%ld]", tablet_id);
    } else {
        it->second.schema_change_lock.unlock();
    }

    _tablet_map_lock.unlock();
    VLOG(3) << "release_schema_change_lock end. tablet_id=" << tablet_id;
} // release_schema_change_lock

OLAPStatus TabletManager::report_tablet_info(TTabletInfo* tablet_info) {
    DorisMetrics::report_tablet_requests_total.increment(1);
    LOG(INFO) << "begin to process report tablet info."
              << "tablet_id=" << tablet_info->tablet_id
              << ", schema_hash=" << tablet_info->schema_hash;

    OLAPStatus res = OLAP_SUCCESS;

    TabletSharedPtr tablet = get_tablet(
            tablet_info->tablet_id, tablet_info->schema_hash);
    if (tablet.get() == NULL) {
        OLAP_LOG_WARNING("can't find tablet. [tablet=%ld schema_hash=%d]",
                         tablet_info->tablet_id, tablet_info->schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    _build_tablet_info(tablet, tablet_info);
    LOG(INFO) << "success to process report tablet info.";
    return res;
} // report_tablet_info

OLAPStatus TabletManager::report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info) {
    LOG(INFO) << "begin to process report all tablets info.";
    DorisMetrics::report_all_tablets_requests_total.increment(1);

    if (tablets_info == NULL) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    _tablet_map_lock.rdlock();
    for (const auto& item : _tablet_map) {
        if (item.second.table_arr.size() == 0) {
            continue;
        }

        TTablet tablet;
        for (TabletSharedPtr tablet_ptr : item.second.table_arr) {
            if (tablet_ptr == NULL) {
                continue;
            }

            TTabletInfo tablet_info;
            _build_tablet_info(tablet_ptr, &tablet_info);

            // report expire transaction
            vector<int64_t> transaction_ids;
            // TODO(ygl): tablet manager and txn manager may be dead lock
            TxnManager::instance()->get_expire_txns(tablet_ptr->tablet_id(), &transaction_ids);
            tablet_info.__set_transaction_ids(transaction_ids);

            if (_available_storage_medium_type_count > 1) {
                tablet_info.__set_storage_medium(tablet_ptr->data_dir()->storage_medium());
            }

            tablet_info.__set_version_count(tablet_ptr->version_count());
            tablet_info.__set_path_hash(tablet_ptr->data_dir()->path_hash());

            tablet.tablet_infos.push_back(tablet_info);
        }

        if (tablet.tablet_infos.size() != 0) {
            tablets_info->insert(pair<TTabletId, TTablet>(tablet.tablet_infos[0].tablet_id, tablet));
        }
    }
    _tablet_map_lock.unlock();

    LOG(INFO) << "success to process report all tablets info. tablet_num=" << tablets_info->size();
    return OLAP_SUCCESS;
} // report_all_tablets_info

AlterTabletState TabletManager::show_alter_tablet_state(
        TTabletId tablet_id,
        TSchemaHash schema_hash) {
    LOG(INFO) << "begin to process show alter tablet state."
              << "tablet_id" << tablet_id
              << ", schema_hash" << schema_hash;
    AlterTabletState state = AlterTabletState::ALTER_NONE;

    TabletSharedPtr tablet = get_tablet(tablet_id, schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to get tablet. tablet=" << tablet_id
                     << ", schema_hash=" << schema_hash;
    } else {
        state = tablet->alter_state();
    }

    return state;
} // show_alter_tablet_state

OLAPStatus TabletManager::start_trash_sweep() {
    _tablet_map_lock.rdlock();
    for (const auto& item : _tablet_map) {
        for (TabletSharedPtr tablet : item.second.table_arr) {
            if (tablet == nullptr) {
                continue;
            }
            tablet->delete_expired_incremental_rowset();
        }
    }
    _tablet_map_lock.unlock();
    return OLAP_SUCCESS;
} // start_trash_sweep

bool TabletManager::try_schema_change_lock(TTabletId tablet_id) {
    bool res = false;
    VLOG(3) << "try_schema_change_lock begin. table_id=" << tablet_id;
    _tablet_map_lock.rdlock();

    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it == _tablet_map.end()) {
        OLAP_LOG_WARNING("tablet does not exists. [tablet=%ld]", tablet_id);
    } else {
        res = (it->second.schema_change_lock.trylock() == OLAP_SUCCESS);
    }

    _tablet_map_lock.unlock();
    VLOG(3) << "try_schema_change_lock end. table_id=" <<  tablet_id;
    return res;
} // try_schema_change_lock

void TabletManager::update_root_path_info(std::map<std::string, DataDirInfo>* path_map, 
    int* tablet_counter) {
    _tablet_map_lock.rdlock();
    for (auto& entry : _tablet_map) {
        TableInstances& instance = entry.second;
        for (auto& tablet : instance.table_arr) {
            (*tablet_counter) ++ ;
            int64_t data_size = tablet->get_data_size();
            auto find = path_map->find(tablet->storage_root_path_name()); 
            if (find == path_map->end()) {
                continue;
            }
            if (find->second.is_used) {
                find->second.data_used_capacity += data_size;
            }
        } 
    }
    _tablet_map_lock.unlock();
} // update_root_path_info

void TabletManager::update_storage_medium_type_count(uint32_t storage_medium_type_count) {
    _available_storage_medium_type_count = storage_medium_type_count;
}

void TabletManager::_build_tablet_info(TabletSharedPtr tablet, TTabletInfo* tablet_info) {
    tablet->get_tablet_info(tablet_info);
}

void TabletManager::_build_tablet_stat() {
    _tablet_stat_cache.clear();
    for (const auto& item : _tablet_map) {
        if (item.second.table_arr.size() == 0) {
            continue;
        }

        TTabletStat stat;
        stat.tablet_id = item.first;
        for (TabletSharedPtr tablet : item.second.table_arr) {
            if (tablet.get() == NULL) {
                continue;
            }
                
            // we only get base tablet's stat
            stat.__set_data_size(tablet->get_data_size());
            stat.__set_row_num(tablet->num_rows());
            VLOG(3) << "tablet_id=" << item.first 
                    << ", data_size=" << tablet->get_data_size()
                    << ", row_num:" << tablet->num_rows();
            break;
        }

        _tablet_stat_cache.emplace(item.first, stat);
    }

    _tablet_stat_cache_update_time_ms = UnixMillis();
}

OLAPStatus TabletManager::_create_inital_rowset(
        TabletSharedPtr tablet, const TCreateTabletReq& request) {
    OLAPStatus res = OLAP_SUCCESS;

    if (request.version < 1) {
        OLAP_LOG_WARNING("init version of tablet should at least 1.");
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    } else {
        Version init_base_version(0, request.version);
        res = create_inital_rowset(
                request.tablet_id, request.tablet_schema.schema_hash,
                init_base_version, request.version_hash);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to create init base version. [res=%d version=%ld]",
                    res, request.version);
            return res;
        }
    }

    tablet->obtain_header_wrlock();
    tablet->set_cumulative_layer_point(request.version + 1);
    res = tablet->save_tablet_meta();
    tablet->release_header_lock();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to save header. [tablet=" << tablet->full_name() << "]";
    }

    return res;
}

OLAPStatus TabletManager::_create_tablet_meta(
        const TCreateTabletReq& request,
        DataDir* store,
        const bool is_schema_change_tablet,
        const TabletSharedPtr ref_tablet,
        TabletMeta** tablet_meta) {
    uint64_t shard_id = 0;
    OLAPStatus res = store->get_shard(&shard_id);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to get root path shard. res=" << res;
        return res;
    }
    stringstream schema_hash_dir_stream;
    schema_hash_dir_stream << store->path()
                      << DATA_PREFIX
                      << "/" << shard_id
                      << "/" << request.tablet_id
                      << "/" << request.tablet_schema.schema_hash;
    string schema_hash_dir = schema_hash_dir_stream.str();
    if (check_dir_existed(schema_hash_dir)) {
        LOG(WARNING) << "failed to create the dir that existed. path=" << schema_hash_dir;
        return OLAP_ERR_CANNOT_CREATE_DIR;
    }
    res = create_dirs(schema_hash_dir);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "create dir fail. [res=" << res << " path:" << schema_hash_dir;
        return res;
    }

    uint32_t next_unique_id = 0;
    uint32_t col_ordinal = 0; 
    std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
    if (!is_schema_change_tablet) {
        for (TColumn column : request.tablet_schema.columns) {
            col_ordinal_to_unique_id[col_ordinal] = col_ordinal;
            col_ordinal++;
        }
        next_unique_id = col_ordinal;
    } else {
        uint32_t next_unique_id = ref_tablet->next_unique_id();
        uint32_t col_ordinal = 0; 
        for (TColumn column : request.tablet_schema.columns) {
            /*
             * for schema change, compare old_tablet and new_tablet
             * 1. if column in both new_tablet and old_tablet,
             * assign unique_id of old_tablet to the column of new_tablet
             * 2. if column exists only in new_tablet, assign next_unique_id of old_tablet
             * to the new column
             *
            */
            size_t num_columns = ref_tablet->num_columns();
            for (size_t field = 0 ; field < num_columns; ++field) {
                if (ref_tablet->tablet_schema().column(field).name() == column.column_name) {
                    uint32_t unique_id = ref_tablet->tablet_schema().column(field).unique_id();
                    col_ordinal_to_unique_id[col_ordinal] = unique_id;
                    break;
                }
                if (field == num_columns) {
                    col_ordinal_to_unique_id[col_ordinal] = next_unique_id;
                    next_unique_id++;
                }
            }
            col_ordinal++;
        }
    }

    TabletMeta::create(request.table_id, request.partition_id,
                       request.tablet_id, request.tablet_schema.schema_hash,
                       shard_id, request.tablet_schema,
                       next_unique_id, col_ordinal_to_unique_id,
                       tablet_meta);

    return OLAP_SUCCESS;
}

OLAPStatus TabletManager::_drop_tablet_directly(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    _tablet_map_lock.wrlock();
    OLAPStatus res = _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    _tablet_map_lock.unlock();
    return res;
} // _drop_tablet_directly

OLAPStatus TabletManager::_drop_tablet_directly_unlocked(
        TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    OLAPStatus res = OLAP_SUCCESS;

    TabletSharedPtr dropped_tablet = _get_tablet_with_no_lock(tablet_id, schema_hash);
    if (dropped_tablet.get() == NULL) {
        OLAP_LOG_WARNING("fail to drop not existed tablet. [tablet_id=%ld schema_hash=%d]",
                         tablet_id, schema_hash);
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    for (list<TabletSharedPtr>::iterator it = _tablet_map[tablet_id].table_arr.begin();
            it != _tablet_map[tablet_id].table_arr.end();) {
        if ((*it)->equal(tablet_id, schema_hash)) {
            if (!keep_files) {
                (*it)->mark_dropped();
            }
            it = _tablet_map[tablet_id].table_arr.erase(it);
        } else {
            ++it;
        }
    }

    if (_tablet_map[tablet_id].table_arr.empty()) {
        _tablet_map.erase(tablet_id);
    }

    res = dropped_tablet->data_dir()->deregister_tablet(dropped_tablet.get());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to unregister from root path. [res=%d tablet=%ld]",
                         res, tablet_id);
    }

    return res;
} // _drop_tablet_directly_unlocked

TabletSharedPtr TabletManager::_get_tablet_with_no_lock(TTabletId tablet_id, SchemaHash schema_hash) {
    VLOG(3) << "begin to get tablet. tablet_id=" << tablet_id;
    tablet_map_t::iterator it = _tablet_map.find(tablet_id);
    if (it != _tablet_map.end()) {
        for (TabletSharedPtr tablet : it->second.table_arr) {
            if (tablet->equal(tablet_id, schema_hash)) {
                VLOG(3) << "get tablet success. tablet_id=" << tablet_id;
                return tablet;
            }
        }
    }

    VLOG(3) << "fail to get tablet. tablet_id=" << tablet_id;
    // Return empty tablet if fail
    TabletSharedPtr tablet;
    return tablet;
} // _get_tablet_with_no_lock

} // doris
