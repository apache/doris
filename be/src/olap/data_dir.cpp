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

#include "olap/data_dir.h"

#include <fmt/core.h>
#include <fmt/format.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>

#include <atomic>
#include <cstdio>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <cstddef>
#include <filesystem>
#include <memory>
#include <new>
#include <roaring/roaring.hh>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "olap/delete_handler.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta_manager.h"
#include "olap/txn_manager.h"
#include "olap/utils.h" // for check_dir_existed
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/string_util.h"
#include "util/uid_util.h"

namespace doris {
using namespace ErrorCode;

namespace {

Status read_cluster_id(const std::string& cluster_id_path, int32_t* cluster_id) {
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(cluster_id_path, &exists));
    *cluster_id = -1;
    if (exists) {
        io::FileReaderSPtr reader;
        RETURN_IF_ERROR(io::global_local_filesystem()->open_file(cluster_id_path, &reader));
        size_t fsize = reader->size();
        if (fsize > 0) {
            std::string content;
            content.resize(fsize, '\0');
            size_t bytes_read = 0;
            RETURN_IF_ERROR(reader->read_at(0, {content.data(), fsize}, &bytes_read));
            DCHECK_EQ(fsize, bytes_read);
            *cluster_id = std::stoi(content);
        }
    }
    return Status::OK();
}

Status _write_cluster_id_to_path(const std::string& path, int32_t cluster_id) {
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(path, &exists));
    if (!exists) {
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(path, &file_writer));
        RETURN_IF_ERROR(file_writer->append(std::to_string(cluster_id)));
        RETURN_IF_ERROR(file_writer->close());
    }
    return Status::OK();
}

} // namespace

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_total_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_avail_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_local_used_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_remote_used_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_trash_used_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_state, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_compaction_score, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_compaction_num, MetricUnit::NOUNIT);

DataDir::DataDir(StorageEngine& engine, const std::string& path, int64_t capacity_bytes,
                 TStorageMedium::type storage_medium)
        : _engine(engine),
          _path(path),
          _available_bytes(0),
          _disk_capacity_bytes(0),
          _trash_used_bytes(0),
          _storage_medium(storage_medium),
          _is_used(false),
          _cluster_id(-1),
          _to_be_deleted(false) {
    _data_dir_metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("data_dir.") + path, {{"path", path}});
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_total_capacity);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_avail_capacity);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_local_used_capacity);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_remote_used_capacity);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_trash_used_capacity);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_state);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_compaction_score);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_compaction_num);
}

DataDir::~DataDir() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_data_dir_metric_entity);
    delete _meta;
}

Status DataDir::init(bool init_meta) {
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(_path, &exists));
    if (!exists) {
        RETURN_NOT_OK_STATUS_WITH_WARN(Status::IOError("opendir failed, path={}", _path),
                                       "check file exist failed");
    }

    RETURN_NOT_OK_STATUS_WITH_WARN(update_capacity(), "update_capacity failed");
    RETURN_NOT_OK_STATUS_WITH_WARN(_init_cluster_id(), "_init_cluster_id failed");
    RETURN_NOT_OK_STATUS_WITH_WARN(_init_capacity_and_create_shards(),
                                   "_init_capacity_and_create_shards failed");
    if (init_meta) {
        RETURN_NOT_OK_STATUS_WITH_WARN(_init_meta(), "_init_meta failed");
    }

    _is_used = true;
    return Status::OK();
}

void DataDir::stop_bg_worker() {
    _stop_bg_worker = true;
}

Status DataDir::_init_cluster_id() {
    auto cluster_id_path = fmt::format("{}/{}", _path, CLUSTER_ID_PREFIX);
    RETURN_IF_ERROR(read_cluster_id(cluster_id_path, &_cluster_id));
    if (_cluster_id == -1) {
        _cluster_id_incomplete = true;
    }
    return Status::OK();
}

Status DataDir::_init_capacity_and_create_shards() {
    RETURN_IF_ERROR(io::global_local_filesystem()->get_space_info(_path, &_disk_capacity_bytes,
                                                                  &_available_bytes));
    auto data_path = fmt::format("{}/{}", _path, DATA_PREFIX);
    bool exists = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(data_path, &exists));
    if (!exists) {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(data_path));
    }
    for (int i = 0; i < MAX_SHARD_NUM; ++i) {
        auto shard_path = fmt::format("{}/{}", data_path, i);
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(shard_path, &exists));
        if (!exists) {
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(shard_path));
        }
    }

    return Status::OK();
}

Status DataDir::_init_meta() {
    // init path hash
    _path_hash = hash_of_path(BackendOptions::get_localhost(), _path);
    LOG(INFO) << "path: " << _path << ", hash: " << _path_hash;

    // init meta
    _meta = new (std::nothrow) OlapMeta(_path);
    if (_meta == nullptr) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::MemoryAllocFailed("allocate memory for OlapMeta failed"),
                "new OlapMeta failed");
    }
    Status res = _meta->init();
    if (!res.ok()) {
        RETURN_NOT_OK_STATUS_WITH_WARN(Status::IOError("open rocksdb failed, path={}", _path),
                                       "init OlapMeta failed");
    }
    return Status::OK();
}

Status DataDir::set_cluster_id(int32_t cluster_id) {
    if (_cluster_id != -1 && _cluster_id != cluster_id) {
        LOG(ERROR) << "going to set cluster id to already assigned store, cluster_id="
                   << _cluster_id << ", new_cluster_id=" << cluster_id;
        return Status::InternalError("going to set cluster id to already assigned store");
    }
    if (!_cluster_id_incomplete) {
        return Status::OK();
    }
    auto cluster_id_path = fmt::format("{}/{}", _path, CLUSTER_ID_PREFIX);
    return _write_cluster_id_to_path(cluster_id_path, cluster_id);
}

void DataDir::health_check() {
    // check disk
    if (_is_used) {
        Status res = _read_and_write_test_file();
        if (!res && res.is<IO_ERROR>()) {
            LOG(WARNING) << "store read/write test file occur IO Error. path=" << _path
                         << ", err: " << res;
            _engine.add_broken_path(_path);
            _is_used = !res.is<IO_ERROR>();
        }
    }
    disks_state->set_value(_is_used ? 1 : 0);
}

Status DataDir::_read_and_write_test_file() {
    auto test_file = fmt::format("{}/{}", _path, kTestFilePath);
    return read_write_test_file(test_file);
}

void DataDir::register_tablet(Tablet* tablet) {
    TabletInfo tablet_info(tablet->tablet_id(), tablet->tablet_uid());

    std::lock_guard<std::mutex> l(_mutex);
    _tablet_set.emplace(std::move(tablet_info));
}

void DataDir::deregister_tablet(Tablet* tablet) {
    TabletInfo tablet_info(tablet->tablet_id(), tablet->tablet_uid());

    std::lock_guard<std::mutex> l(_mutex);
    _tablet_set.erase(tablet_info);
}

void DataDir::clear_tablets(std::vector<TabletInfo>* tablet_infos) {
    std::lock_guard<std::mutex> l(_mutex);

    tablet_infos->insert(tablet_infos->end(), _tablet_set.begin(), _tablet_set.end());
    _tablet_set.clear();
}

std::string DataDir::get_absolute_shard_path(int64_t shard_id) {
    return fmt::format("{}/{}/{}", _path, DATA_PREFIX, shard_id);
}

std::string DataDir::get_absolute_tablet_path(int64_t shard_id, int64_t tablet_id,
                                              int32_t schema_hash) {
    return fmt::format("{}/{}/{}", get_absolute_shard_path(shard_id), tablet_id, schema_hash);
}

void DataDir::find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths) {
    // path: /root_path/trash/time_label/tablet_id/schema_hash
    auto trash_path = fmt::format("{}/{}", _path, TRASH_PREFIX);
    bool exists = true;
    std::vector<io::FileInfo> sub_dirs;
    Status st = io::global_local_filesystem()->list(trash_path, false, &sub_dirs, &exists);
    if (!st) {
        return;
    }

    for (auto& sub_dir : sub_dirs) {
        // sub dir is time_label
        if (sub_dir.is_file) {
            continue;
        }
        auto sub_path = fmt::format("{}/{}", trash_path, sub_dir.file_name);
        auto tablet_path = fmt::format("{}/{}", sub_path, tablet_id);
        st = io::global_local_filesystem()->exists(tablet_path, &exists);
        if (st && exists) {
            paths->emplace_back(std::move(tablet_path));
        }
    }
}

std::string DataDir::get_root_path_from_schema_hash_path_in_trash(
        const std::string& schema_hash_dir_in_trash) {
    return io::Path(schema_hash_dir_in_trash)
            .parent_path()
            .parent_path()
            .parent_path()
            .parent_path()
            .string();
}

Status DataDir::_check_incompatible_old_format_tablet() {
    auto check_incompatible_old_func = [](int64_t tablet_id, int32_t schema_hash,
                                          std::string_view value) -> bool {
        // if strict check incompatible old format, then log fatal
        if (config::storage_strict_check_incompatible_old_format) {
            LOG(FATAL)
                    << "There are incompatible old format metas, current version does not support "
                    << "and it may lead to data missing!!! "
                    << "tablet_id = " << tablet_id << " schema_hash = " << schema_hash;
        } else {
            LOG(WARNING)
                    << "There are incompatible old format metas, current version does not support "
                    << "and it may lead to data missing!!! "
                    << "tablet_id = " << tablet_id << " schema_hash = " << schema_hash;
        }
        return false;
    };

    // seek old header prefix. when check_incompatible_old_func is called, it has old format in olap_meta
    Status check_incompatible_old_status = TabletMetaManager::traverse_headers(
            _meta, check_incompatible_old_func, OLD_HEADER_PREFIX);
    if (!check_incompatible_old_status) {
        LOG(WARNING) << "check incompatible old format meta fails, it may lead to data missing!!! "
                     << _path;
    } else {
        LOG(INFO) << "successfully check incompatible old format meta " << _path;
    }
    return check_incompatible_old_status;
}

// TODO(ygl): deal with rowsets and tablets when load failed
Status DataDir::load() {
    LOG(INFO) << "start to load tablets from " << _path;

    // load rowset meta from meta env and create rowset
    // COMMITTED: add to txn manager
    // VISIBLE: add to tablet
    // if one rowset load failed, then the total data dir will not be loaded

    // necessarily check incompatible old format. when there are old metas, it may load to data missing
    RETURN_IF_ERROR(_check_incompatible_old_format_tablet());

    std::vector<RowsetMetaSharedPtr> dir_rowset_metas;
    LOG(INFO) << "begin loading rowset from meta";
    auto load_rowset_func = [&dir_rowset_metas, this](TabletUid tablet_uid, RowsetId rowset_id,
                                                      std::string_view meta_str) -> bool {
        RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
        bool parsed = rowset_meta->init(meta_str);
        if (!parsed) {
            LOG(WARNING) << "parse rowset meta string failed for rowset_id:" << rowset_id;
            // return false will break meta iterator, return true to skip this error
            return true;
        }

        if (rowset_meta->has_delete_predicate()) {
            // copy the delete sub pred v1 to check then
            auto orig_delete_sub_pred = rowset_meta->delete_predicate().sub_predicates();
            auto* delete_pred = rowset_meta->mutable_delete_pred_pb();

            if ((!delete_pred->sub_predicates().empty() &&
                 delete_pred->sub_predicates_v2().empty()) ||
                (!delete_pred->in_predicates().empty() &&
                 delete_pred->in_predicates()[0].has_column_unique_id())) {
                // convert pred and write only when delete sub pred v2 is not set or there is in list pred to be set column uid
                RETURN_IF_ERROR(DeleteHandler::convert_to_sub_pred_v2(
                        delete_pred, rowset_meta->tablet_schema()));
                LOG(INFO) << fmt::format(
                        "convert rowset with old delete pred: rowset_id={}, tablet_id={}",
                        rowset_id.to_string(), tablet_uid.to_string());
                CHECK_EQ(orig_delete_sub_pred.size(), delete_pred->sub_predicates().size())
                        << "inconsistent sub predicate v1 after conversion";
                for (size_t i = 0; i < orig_delete_sub_pred.size(); ++i) {
                    CHECK_STREQ(orig_delete_sub_pred.Get(i).c_str(),
                                delete_pred->sub_predicates().Get(i).c_str())
                            << "inconsistent sub predicate v1 after conversion";
                }
                std::string result;
                rowset_meta->serialize(&result);
                std::string key =
                        ROWSET_PREFIX + tablet_uid.to_string() + "_" + rowset_id.to_string();
                RETURN_IF_ERROR(_meta->put(META_COLUMN_FAMILY_INDEX, key, result));
            }
        }

        if (rowset_meta->partition_id() == 0) {
            LOG(WARNING) << "rs tablet=" << rowset_meta->tablet_id() << " rowset_id=" << rowset_id
                         << " load from meta but partition id eq 0";
        }

        dir_rowset_metas.push_back(rowset_meta);
        return true;
    };
    Status load_rowset_status = RowsetMetaManager::traverse_rowset_metas(_meta, load_rowset_func);

    if (!load_rowset_status) {
        LOG(WARNING) << "errors when load rowset meta from meta env, skip this data dir:" << _path;
    } else {
        LOG(INFO) << "load rowset from meta finished, data dir: " << _path;
    }

    // load tablet
    // create tablet from tablet meta and add it to tablet mgr
    LOG(INFO) << "begin loading tablet from meta";
    std::set<int64_t> tablet_ids;
    std::set<int64_t> failed_tablet_ids;
    auto load_tablet_func = [this, &tablet_ids, &failed_tablet_ids](
                                    int64_t tablet_id, int32_t schema_hash,
                                    std::string_view value) -> bool {
        Status status = _engine.tablet_manager()->load_tablet_from_meta(
                this, tablet_id, schema_hash, value, false, false, false, false);
        if (!status.ok() && !status.is<TABLE_ALREADY_DELETED_ERROR>() &&
            !status.is<ENGINE_INSERT_OLD_TABLET>()) {
            // load_tablet_from_meta() may return Status::Error<TABLE_ALREADY_DELETED_ERROR>()
            // which means the tablet status is DELETED
            // This may happen when the tablet was just deleted before the BE restarted,
            // but it has not been cleared from rocksdb. At this time, restarting the BE
            // will read the tablet in the DELETE state from rocksdb. These tablets have been
            // added to the garbage collection queue and will be automatically deleted afterwards.
            // Therefore, we believe that this situation is not a failure.

            // Besides, load_tablet_from_meta() may return Status::Error<ENGINE_INSERT_OLD_TABLET>()
            // when BE is restarting and the older tablet have been added to the
            // garbage collection queue but not deleted yet.
            // In this case, since the data_dirs are parallel loaded, a later loaded tablet
            // may be older than previously loaded one, which should not be acknowledged as a
            // failure.
            LOG(WARNING) << "load tablet from header failed. status:" << status
                         << ", tablet=" << tablet_id << "." << schema_hash;
            failed_tablet_ids.insert(tablet_id);
        } else {
            tablet_ids.insert(tablet_id);
        }
        return true;
    };
    Status load_tablet_status = TabletMetaManager::traverse_headers(_meta, load_tablet_func);
    if (!failed_tablet_ids.empty()) {
        LOG(WARNING) << "load tablets from header failed"
                     << ", loaded tablet: " << tablet_ids.size()
                     << ", error tablet: " << failed_tablet_ids.size() << ", path: " << _path;
        if (!config::ignore_load_tablet_failure) {
            LOG(FATAL) << "load tablets encounter failure. stop BE process. path: " << _path;
        }
    }
    if (!load_tablet_status) {
        LOG(WARNING) << "there is failure when loading tablet headers"
                     << ", loaded tablet: " << tablet_ids.size()
                     << ", error tablet: " << failed_tablet_ids.size() << ", path: " << _path;
    } else {
        LOG(INFO) << "load tablet from meta finished"
                  << ", loaded tablet: " << tablet_ids.size()
                  << ", error tablet: " << failed_tablet_ids.size() << ", path: " << _path;
    }

    for (int64_t tablet_id : tablet_ids) {
        TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(tablet_id);
        if (tablet && tablet->set_tablet_schema_into_rowset_meta()) {
            RETURN_IF_ERROR(TabletMetaManager::save(this, tablet->tablet_id(),
                                                    tablet->schema_hash(), tablet->tablet_meta()));
        }
    }

    auto load_pending_publish_info_func =
            [&engine = _engine](int64_t tablet_id, int64_t publish_version, std::string_view info) {
                PendingPublishInfoPB pending_publish_info_pb;
                bool parsed = pending_publish_info_pb.ParseFromArray(info.data(), info.size());
                if (!parsed) {
                    LOG(WARNING) << "parse pending publish info failed, tablet_id: " << tablet_id
                                 << " publish_version: " << publish_version;
                }
                engine.add_async_publish_task(pending_publish_info_pb.partition_id(), tablet_id,
                                              publish_version,
                                              pending_publish_info_pb.transaction_id(), true);
                return true;
            };
    RETURN_IF_ERROR(
            TabletMetaManager::traverse_pending_publish(_meta, load_pending_publish_info_func));

    int64_t rowset_partition_id_eq_0_num = 0;
    for (auto rowset_meta : dir_rowset_metas) {
        if (rowset_meta->partition_id() == 0) {
            ++rowset_partition_id_eq_0_num;
        }
    }
    if (rowset_partition_id_eq_0_num > config::ignore_invalid_partition_id_rowset_num) {
        LOG(FATAL) << fmt::format(
                "roswet partition id eq 0 is {} bigger than config {}, be exit, plz check be.INFO",
                rowset_partition_id_eq_0_num, config::ignore_invalid_partition_id_rowset_num);
        exit(-1);
    }

    // traverse rowset
    // 1. add committed rowset to txn map
    // 2. add visible rowset to tablet
    // ignore any errors when load tablet or rowset, because fe will repair them after report
    int64_t invalid_rowset_counter = 0;
    for (auto&& rowset_meta : dir_rowset_metas) {
        TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(rowset_meta->tablet_id());
        // tablet maybe dropped, but not drop related rowset meta
        if (tablet == nullptr) {
            VLOG_NOTICE << "could not find tablet id: " << rowset_meta->tablet_id()
                        << ", schema hash: " << rowset_meta->tablet_schema_hash()
                        << ", for rowset: " << rowset_meta->rowset_id() << ", skip this rowset";
            ++invalid_rowset_counter;
            continue;
        }

        if (rowset_meta->partition_id() == 0) {
            LOG(WARNING) << "skip tablet_id=" << tablet->tablet_id()
                         << " rowset: " << rowset_meta->rowset_id()
                         << " txn: " << rowset_meta->txn_id();
            continue;
        }

        RowsetSharedPtr rowset;
        Status create_status = tablet->create_rowset(rowset_meta, &rowset);
        if (!create_status) {
            LOG(WARNING) << "could not create rowset from rowsetmeta: "
                         << " rowset_id: " << rowset_meta->rowset_id()
                         << " rowset_type: " << rowset_meta->rowset_type()
                         << " rowset_state: " << rowset_meta->rowset_state();
            continue;
        }
        if (rowset_meta->rowset_state() == RowsetStatePB::COMMITTED &&
            rowset_meta->tablet_uid() == tablet->tablet_uid()) {
            if (!rowset_meta->tablet_schema()) {
                rowset_meta->set_tablet_schema(tablet->tablet_schema());
                RETURN_IF_ERROR(RowsetMetaManager::save(_meta, rowset_meta->tablet_uid(),
                                                        rowset_meta->rowset_id(),
                                                        rowset_meta->get_rowset_pb(), false));
            }
            Status commit_txn_status = _engine.txn_manager()->commit_txn(
                    _meta, rowset_meta->partition_id(), rowset_meta->txn_id(),
                    rowset_meta->tablet_id(), rowset_meta->tablet_uid(), rowset_meta->load_id(),
                    rowset, _engine.pending_local_rowsets().add(rowset_meta->rowset_id()), true);
            if (commit_txn_status || commit_txn_status.is<PUSH_TRANSACTION_ALREADY_EXIST>()) {
                LOG(INFO) << "successfully to add committed rowset: " << rowset_meta->rowset_id()
                          << " to tablet: " << rowset_meta->tablet_id()
                          << " schema hash: " << rowset_meta->tablet_schema_hash()
                          << " for txn: " << rowset_meta->txn_id();

            } else if (commit_txn_status.is<ErrorCode::INTERNAL_ERROR>()) {
                LOG(WARNING) << "failed to add committed rowset: " << rowset_meta->rowset_id()
                             << " to tablet: " << rowset_meta->tablet_id()
                             << " for txn: " << rowset_meta->txn_id()
                             << " error: " << commit_txn_status;
                return commit_txn_status;
            } else {
                LOG(WARNING) << "failed to add committed rowset: " << rowset_meta->rowset_id()
                             << " to tablet: " << rowset_meta->tablet_id()
                             << " for txn: " << rowset_meta->txn_id()
                             << " error: " << commit_txn_status;
            }
        } else if (rowset_meta->rowset_state() == RowsetStatePB::VISIBLE &&
                   rowset_meta->tablet_uid() == tablet->tablet_uid()) {
            if (!rowset_meta->tablet_schema()) {
                rowset_meta->set_tablet_schema(tablet->tablet_schema());
                RETURN_IF_ERROR(RowsetMetaManager::save(_meta, rowset_meta->tablet_uid(),
                                                        rowset_meta->rowset_id(),
                                                        rowset_meta->get_rowset_pb(), false));
            }
            Status publish_status = tablet->add_rowset(rowset);
            if (!publish_status && !publish_status.is<PUSH_VERSION_ALREADY_EXIST>()) {
                LOG(WARNING) << "add visible rowset to tablet failed rowset_id:"
                             << rowset->rowset_id() << " tablet id: " << rowset_meta->tablet_id()
                             << " txn id:" << rowset_meta->txn_id()
                             << " start_version: " << rowset_meta->version().first
                             << " end_version: " << rowset_meta->version().second;
            }
        } else {
            LOG(WARNING) << "find invalid rowset: " << rowset_meta->rowset_id()
                         << " with tablet id: " << rowset_meta->tablet_id()
                         << " tablet uid: " << rowset_meta->tablet_uid()
                         << " schema hash: " << rowset_meta->tablet_schema_hash()
                         << " txn: " << rowset_meta->txn_id()
                         << " current valid tablet uid: " << tablet->tablet_uid();
            ++invalid_rowset_counter;
        }
    }

    auto load_delete_bitmap_func = [this](int64_t tablet_id, int64_t version,
                                          std::string_view val) {
        TabletSharedPtr tablet = _engine.tablet_manager()->get_tablet(tablet_id);
        if (!tablet) {
            return true;
        }
        const std::vector<RowsetMetaSharedPtr>& all_rowsets = tablet->tablet_meta()->all_rs_metas();
        RowsetIdUnorderedSet rowset_ids;
        for (auto& rowset_meta : all_rowsets) {
            rowset_ids.insert(rowset_meta->rowset_id());
        }

        DeleteBitmapPB delete_bitmap_pb;
        delete_bitmap_pb.ParseFromArray(val.data(), val.size());
        int rst_ids_size = delete_bitmap_pb.rowset_ids_size();
        int seg_ids_size = delete_bitmap_pb.segment_ids_size();
        int seg_maps_size = delete_bitmap_pb.segment_delete_bitmaps_size();
        CHECK(rst_ids_size == seg_ids_size && seg_ids_size == seg_maps_size);

        for (size_t i = 0; i < rst_ids_size; ++i) {
            RowsetId rst_id;
            rst_id.init(delete_bitmap_pb.rowset_ids(i));
            // only process the rowset in _rs_metas
            if (rowset_ids.find(rst_id) == rowset_ids.end()) {
                continue;
            }
            auto seg_id = delete_bitmap_pb.segment_ids(i);
            auto iter = tablet->tablet_meta()->delete_bitmap().delete_bitmap.find(
                    {rst_id, seg_id, version});
            // This version of delete bitmap already exists
            if (iter != tablet->tablet_meta()->delete_bitmap().delete_bitmap.end()) {
                continue;
            }
            auto bitmap = delete_bitmap_pb.segment_delete_bitmaps(i).data();
            tablet->tablet_meta()->delete_bitmap().delete_bitmap[{rst_id, seg_id, version}] =
                    roaring::Roaring::read(bitmap);
        }
        return true;
    };
    RETURN_IF_ERROR(TabletMetaManager::traverse_delete_bitmap(_meta, load_delete_bitmap_func));

    // At startup, we only count these invalid rowset, but do not actually delete it.
    // The actual delete operation is in StorageEngine::_clean_unused_rowset_metas,
    // which is cleaned up uniformly by the background cleanup thread.
    LOG(INFO) << "finish to load tablets from " << _path
              << ", total rowset meta: " << dir_rowset_metas.size()
              << ", invalid rowset num: " << invalid_rowset_counter;

    return Status::OK();
}

// gc unused local tablet dir
void DataDir::_perform_tablet_gc(const std::string& tablet_schema_hash_path, int16_t shard_id) {
    if (_stop_bg_worker) {
        return;
    }

    TTabletId tablet_id = -1;
    TSchemaHash schema_hash = -1;
    bool is_valid = TabletManager::get_tablet_id_and_schema_hash_from_path(
            tablet_schema_hash_path, &tablet_id, &schema_hash);
    if (!is_valid || tablet_id < 1 || schema_hash < 1) [[unlikely]] {
        LOG(WARNING) << "[path gc] unknown path: " << tablet_schema_hash_path;
        return;
    }

    auto tablet = _engine.tablet_manager()->get_tablet(tablet_id);
    if (!tablet || tablet->data_dir() != this) {
        if (tablet) {
            LOG(INFO) << "The tablet in path " << tablet_schema_hash_path
                      << " is not same with the running one: " << tablet->tablet_path()
                      << ", might be the old tablet after migration, try to move it to trash";
        }
        _engine.tablet_manager()->try_delete_unused_tablet_path(this, tablet_id, schema_hash,
                                                                tablet_schema_hash_path, shard_id);
        return;
    }

    _perform_rowset_gc(tablet_schema_hash_path);
}

// gc unused local rowsets under tablet dir
void DataDir::_perform_rowset_gc(const std::string& tablet_schema_hash_path) {
    if (_stop_bg_worker) {
        return;
    }

    TTabletId tablet_id = -1;
    TSchemaHash schema_hash = -1;
    bool is_valid = doris::TabletManager::get_tablet_id_and_schema_hash_from_path(
            tablet_schema_hash_path, &tablet_id, &schema_hash);
    if (!is_valid || tablet_id < 1 || schema_hash < 1) [[unlikely]] {
        LOG(WARNING) << "[path gc] unknown path: " << tablet_schema_hash_path;
        return;
    }

    auto tablet = _engine.tablet_manager()->get_tablet(tablet_id);
    if (!tablet) {
        // Could not found the tablet, maybe it's a dropped tablet, will be reclaimed
        // in the next time `_perform_path_gc_by_tablet`
        return;
    }

    if (tablet->data_dir() != this) {
        // Current running tablet is not in same data_dir, maybe it's a tablet after migration,
        // will be reclaimed in the next time `_perform_path_gc_by_tablet`
        return;
    }

    bool exists;
    std::vector<io::FileInfo> files;
    auto st = io::global_local_filesystem()->list(tablet_schema_hash_path, true, &files, &exists);
    if (!st.ok()) [[unlikely]] {
        LOG(WARNING) << "[path gc] fail to list tablet path " << tablet_schema_hash_path << " : "
                     << st;
        return;
    }

    // Rowset files excluding pending rowsets
    std::vector<std::pair<RowsetId, std::string /* filename */>> rowsets_not_pending;
    for (auto&& file : files) {
        auto rowset_id = extract_rowset_id(file.file_name);
        if (rowset_id.hi == 0) {
            continue; // Not a rowset
        }

        if (_engine.pending_local_rowsets().contains(rowset_id)) {
            continue; // Pending rowset file
        }

        rowsets_not_pending.emplace_back(rowset_id, std::move(file.file_name));
    }

    RowsetIdUnorderedSet rowsets_in_version_map;
    tablet->traverse_rowsets(
            [&rowsets_in_version_map](auto& rs) { rowsets_in_version_map.insert(rs->rowset_id()); },
            true);

    auto reclaim_rowset_file = [](const std::string& path) {
        auto st = io::global_local_filesystem()->delete_file(path);
        if (!st.ok()) [[unlikely]] {
            LOG(WARNING) << "[path gc] failed to delete garbage rowset file: " << st;
            return;
        }
        LOG(INFO) << "[path gc] delete garbage path: " << path; // Audit log
    };

    auto should_reclaim = [&, this](const RowsetId& rowset_id) {
        return !rowsets_in_version_map.contains(rowset_id) &&
               !_engine.check_rowset_id_in_unused_rowsets(rowset_id) &&
               RowsetMetaManager::exists(get_meta(), tablet->tablet_uid(), rowset_id)
                       .is<META_KEY_NOT_FOUND>();
    };

    // rowset_id -> is_garbage
    std::unordered_map<RowsetId, bool> checked_rowsets;
    for (auto&& [rowset_id, filename] : rowsets_not_pending) {
        if (_stop_bg_worker) {
            return;
        }

        if (auto it = checked_rowsets.find(rowset_id); it != checked_rowsets.end()) {
            if (it->second) { // Is checked garbage rowset
                reclaim_rowset_file(tablet_schema_hash_path + '/' + filename);
            }
            continue;
        }

        if (should_reclaim(rowset_id)) {
            if (config::path_gc_check_step > 0 &&
                ++_path_gc_step % config::path_gc_check_step == 0) {
                std::this_thread::sleep_for(
                        std::chrono::milliseconds(config::path_gc_check_step_interval_ms));
            }
            reclaim_rowset_file(tablet_schema_hash_path + '/' + filename);
            checked_rowsets.emplace(rowset_id, true);
        } else {
            checked_rowsets.emplace(rowset_id, false);
        }
    }
}

void DataDir::perform_path_gc() {
    if (_stop_bg_worker) {
        return;
    }

    LOG(INFO) << "start to gc data dir " << _path;
    auto data_path = fmt::format("{}/{}", _path, DATA_PREFIX);
    std::vector<io::FileInfo> shards;
    bool exists = true;
    const auto& fs = io::global_local_filesystem();
    auto st = fs->list(data_path, false, &shards, &exists);
    if (!st.ok()) [[unlikely]] {
        LOG(WARNING) << "failed to scan data dir: " << st;
        return;
    }

    for (const auto& shard : shards) {
        if (_stop_bg_worker) {
            break;
        }

        if (shard.is_file) {
            continue;
        }

        auto shard_path = fmt::format("{}/{}", data_path, shard.file_name);
        std::vector<io::FileInfo> tablet_ids;
        st = io::global_local_filesystem()->list(shard_path, false, &tablet_ids, &exists);
        if (!st.ok()) [[unlikely]] {
            LOG(WARNING) << "fail to walk dir, shard_path=" << shard_path << " : " << st;
            continue;
        }

        for (const auto& tablet_id : tablet_ids) {
            if (_stop_bg_worker) {
                break;
            }

            if (tablet_id.is_file) {
                continue;
            }

            auto tablet_id_path = fmt::format("{}/{}", shard_path, tablet_id.file_name);
            std::vector<io::FileInfo> schema_hashes;
            st = fs->list(tablet_id_path, false, &schema_hashes, &exists);
            if (!st.ok()) [[unlikely]] {
                LOG(WARNING) << "fail to walk dir, tablet_id_path=" << tablet_id_path << " : "
                             << st;
                continue;
            }

            for (auto&& schema_hash : schema_hashes) {
                if (schema_hash.is_file) {
                    continue;
                }

                if (config::path_gc_check_step > 0 &&
                    ++_path_gc_step % config::path_gc_check_step == 0) {
                    std::this_thread::sleep_for(
                            std::chrono::milliseconds(config::path_gc_check_step_interval_ms));
                }
                int16_t shard_id = -1;
                try {
                    shard_id = std::stoi(shard.file_name);
                } catch (const std::exception&) {
                    LOG(WARNING) << "failed to stoi shard_id, shard name=" << shard.file_name;
                    continue;
                }
                _perform_tablet_gc(tablet_id_path + '/' + schema_hash.file_name, shard_id);
            }
        }
    }

    LOG(INFO) << "gc data dir path: " << _path << " finished";
}

Status DataDir::update_capacity() {
    RETURN_IF_ERROR(io::global_local_filesystem()->get_space_info(_path, &_disk_capacity_bytes,
                                                                  &_available_bytes));
    disks_total_capacity->set_value(_disk_capacity_bytes);
    disks_avail_capacity->set_value(_available_bytes);
    LOG(INFO) << "path: " << _path << " total capacity: " << _disk_capacity_bytes
              << ", available capacity: " << _available_bytes << ", usage: " << get_usage(0)
              << ", in_use: " << is_used();

    return Status::OK();
}

void DataDir::update_trash_capacity() {
    auto trash_path = fmt::format("{}/{}", _path, TRASH_PREFIX);
    try {
        _trash_used_bytes = _engine.get_file_or_directory_size(trash_path);
    } catch (const std::filesystem::filesystem_error& e) {
        LOG(WARNING) << "update trash capacity failed, path: " << _path << ", err: " << e.what();
        return;
    }
    disks_trash_used_capacity->set_value(_trash_used_bytes);
    LOG(INFO) << "path: " << _path << " trash capacity: " << _trash_used_bytes;
}

void DataDir::update_local_data_size(int64_t size) {
    disks_local_used_capacity->set_value(size);
}

void DataDir::update_remote_data_size(int64_t size) {
    disks_remote_used_capacity->set_value(size);
}

size_t DataDir::tablet_size() const {
    std::lock_guard<std::mutex> l(_mutex);
    return _tablet_set.size();
}

bool DataDir::reach_capacity_limit(int64_t incoming_data_size) {
    double used_pct = get_usage(incoming_data_size);
    int64_t left_bytes = _available_bytes - incoming_data_size;
    if (used_pct >= config::storage_flood_stage_usage_percent / 100.0 &&
        left_bytes <= config::storage_flood_stage_left_capacity_bytes) {
        LOG(WARNING) << "reach capacity limit. used pct: " << used_pct
                     << ", left bytes: " << left_bytes << ", path: " << _path;
        return true;
    }
    return false;
}

void DataDir::disks_compaction_score_increment(int64_t delta) {
    disks_compaction_score->increment(delta);
}

void DataDir::disks_compaction_num_increment(int64_t delta) {
    disks_compaction_num->increment(delta);
}

Status DataDir::move_to_trash(const std::string& tablet_path) {
    Status res = Status::OK();

    // 1. get timestamp string
    string time_str;
    if ((res = gen_timestamp_string(&time_str)) != Status::OK()) {
        LOG(WARNING) << "failed to generate time_string when move file to trash.err code=" << res;
        return res;
    }

    // 2. generate new file path
    // a global counter to avoid file name duplication.
    static std::atomic<uint64_t> delete_counter(0);
    auto trash_root_path =
            fmt::format("{}/{}/{}.{}", _path, TRASH_PREFIX, time_str, delete_counter++);
    auto fs_tablet_path = io::Path(tablet_path);
    auto trash_tablet_path = trash_root_path /
                             fs_tablet_path.parent_path().filename() /* tablet_id */ /
                             fs_tablet_path.filename() /* schema_hash */;

    // 3. create target dir, or the rename() function will fail.
    auto trash_tablet_parent = trash_tablet_path.parent_path();
    // create dir if not exists
    bool exists = true;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(trash_tablet_parent, &exists));
    if (!exists) {
        RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(trash_tablet_parent));
    }

    // 4. move tablet to trash
    VLOG_NOTICE << "move file to trash. " << tablet_path << " -> " << trash_tablet_path;
    if (rename(tablet_path.c_str(), trash_tablet_path.c_str()) < 0) {
        return Status::Error<OS_ERROR>("move file to trash failed. file={}, target={}, err={}",
                                       tablet_path, trash_tablet_path.native(), Errno::str());
    }

    // 5. check parent dir of source file, delete it when empty
    RETURN_IF_ERROR(delete_tablet_parent_path_if_empty(tablet_path));

    return Status::OK();
}

Status DataDir::delete_tablet_parent_path_if_empty(const std::string& tablet_path) {
    auto fs_tablet_path = io::Path(tablet_path);
    std::string source_parent_dir = fs_tablet_path.parent_path(); // tablet_id level
    std::vector<io::FileInfo> sub_files;
    bool exists = true;
    RETURN_IF_ERROR(
            io::global_local_filesystem()->list(source_parent_dir, false, &sub_files, &exists));
    if (sub_files.empty()) {
        LOG(INFO) << "remove empty dir " << source_parent_dir;
        // no need to exam return status
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(source_parent_dir));
    }
    return Status::OK();
}

void DataDir::perform_remote_rowset_gc() {
    std::vector<std::pair<std::string, std::string>> gc_kvs;
    auto traverse_remote_rowset_func = [&gc_kvs](std::string_view key,
                                                 std::string_view value) -> bool {
        gc_kvs.emplace_back(key, value);
        return true;
    };
    static_cast<void>(_meta->iterate(META_COLUMN_FAMILY_INDEX, REMOTE_ROWSET_GC_PREFIX,
                                     traverse_remote_rowset_func));
    std::vector<std::string> deleted_keys;
    for (auto& [key, val] : gc_kvs) {
        auto rowset_id = key.substr(REMOTE_ROWSET_GC_PREFIX.size());
        RemoteRowsetGcPB gc_pb;
        if (!gc_pb.ParseFromString(val)) {
            LOG(WARNING) << "malformed RemoteRowsetGcPB. rowset_id=" << rowset_id;
            deleted_keys.push_back(std::move(key));
            continue;
        }

        auto storage_resource = get_storage_resource(gc_pb.resource_id());
        if (!storage_resource) {
            LOG(WARNING) << "Cannot get file system: " << gc_pb.resource_id();
            continue;
        }

        std::vector<io::Path> seg_paths;
        seg_paths.reserve(gc_pb.num_segments());
        for (int i = 0; i < gc_pb.num_segments(); ++i) {
            seg_paths.emplace_back(
                    storage_resource->first.remote_segment_path(gc_pb.tablet_id(), rowset_id, i));
        }

        auto& fs = storage_resource->first.fs;
        LOG(INFO) << "delete remote rowset. root_path=" << fs->root_path()
                  << ", rowset_id=" << rowset_id;
        auto st = fs->batch_delete(seg_paths);
        if (st.ok()) {
            deleted_keys.push_back(std::move(key));
        } else {
            LOG(WARNING) << "failed to delete remote rowset. err=" << st;
        }
    }
    for (const auto& key : deleted_keys) {
        static_cast<void>(_meta->remove(META_COLUMN_FAMILY_INDEX, key));
    }
}

void DataDir::perform_remote_tablet_gc() {
    std::vector<std::pair<std::string, std::string>> tablet_gc_kvs;
    auto traverse_remote_tablet_func = [&tablet_gc_kvs](std::string_view key,
                                                        std::string_view value) -> bool {
        tablet_gc_kvs.emplace_back(key, value);
        return true;
    };
    static_cast<void>(_meta->iterate(META_COLUMN_FAMILY_INDEX, REMOTE_TABLET_GC_PREFIX,
                                     traverse_remote_tablet_func));
    std::vector<std::string> deleted_keys;
    for (auto& [key, val] : tablet_gc_kvs) {
        auto tablet_id = key.substr(REMOTE_TABLET_GC_PREFIX.size());
        RemoteTabletGcPB gc_pb;
        if (!gc_pb.ParseFromString(val)) {
            LOG(WARNING) << "malformed RemoteTabletGcPB. tablet_id=" << tablet_id;
            deleted_keys.push_back(std::move(key));
            continue;
        }
        bool success = true;
        for (auto& resource_id : gc_pb.resource_ids()) {
            auto fs = get_filesystem(resource_id);
            if (!fs) {
                LOG(WARNING) << "could not get file system. resource_id=" << resource_id;
                success = false;
                continue;
            }
            LOG(INFO) << "delete remote rowsets of tablet. root_path=" << fs->root_path()
                      << ", tablet_id=" << tablet_id;
            auto st = fs->delete_directory(DATA_PREFIX + '/' + tablet_id);
            if (!st.ok()) {
                LOG(WARNING) << "failed to delete all remote rowset in tablet. err=" << st;
                success = false;
            }
        }
        if (success) {
            deleted_keys.push_back(std::move(key));
        }
    }
    for (const auto& key : deleted_keys) {
        static_cast<void>(_meta->remove(META_COLUMN_FAMILY_INDEX, key));
    }
}

} // namespace doris
