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

#include <ctype.h>
#include <mntent.h>
#include <stdio.h>
#include <sys/file.h>
#include <sys/statfs.h>
#include <utime.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/filesystem.hpp>
#include <boost/interprocess/sync/file_lock.hpp>
#include <fstream>
#include <set>
#include <sstream>

#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "olap/file_helper.h"
#include "olap/olap_define.h"
#include "olap/olap_snapshot_converter.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h" // for check_dir_existed
#include "service/backend_options.h"
#include "util/errno.h"
#include "util/file_utils.h"
#include "util/monotime.h"
#include "util/string_util.h"

using strings::Substitute;

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_total_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_avail_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_data_used_capacity, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_state, MetricUnit::BYTES);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_compaction_score, MetricUnit::NOUNIT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(disks_compaction_num, MetricUnit::NOUNIT);

static const char* const kMtabPath = "/etc/mtab";
static const char* const kTestFilePath = "/.testfile";

DataDir::DataDir(const std::string& path, int64_t capacity_bytes,
                 TStorageMedium::type storage_medium, TabletManager* tablet_manager,
                 TxnManager* txn_manager)
        : _path(path),
          _capacity_bytes(capacity_bytes),
          _available_bytes(0),
          _disk_capacity_bytes(0),
          _storage_medium(storage_medium),
          _is_used(false),
          _tablet_manager(tablet_manager),
          _txn_manager(txn_manager),
          _cluster_id(-1),
          _to_be_deleted(false),
          _current_shard(0),
          _meta(nullptr) {
    _data_dir_metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("data_dir.") + path, {{"path", path}});
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_total_capacity);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_avail_capacity);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_data_used_capacity);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_state);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_compaction_score);
    INT_GAUGE_METRIC_REGISTER(_data_dir_metric_entity, disks_compaction_num);
}

DataDir::~DataDir() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_data_dir_metric_entity);
    delete _id_generator;
    delete _meta;
}

Status DataDir::init() {
    if (!FileUtils::check_exist(_path)) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(strings::Substitute("opendir failed, path=$0", _path)),
                "check file exist failed");
    }

    RETURN_NOT_OK_STATUS_WITH_WARN(update_capacity(), "update_capacity failed");
    RETURN_NOT_OK_STATUS_WITH_WARN(_init_cluster_id(), "_init_cluster_id failed");
    RETURN_NOT_OK_STATUS_WITH_WARN(_init_capacity(), "_init_capacity failed");
    RETURN_NOT_OK_STATUS_WITH_WARN(_init_file_system(), "_init_file_system failed");
    RETURN_NOT_OK_STATUS_WITH_WARN(_init_meta(), "_init_meta failed");

    _is_used = true;
    return Status::OK();
}

void DataDir::stop_bg_worker() {
    std::unique_lock<std::mutex> lck(_check_path_mutex);
    _stop_bg_worker = true;
    _check_path_cv.notify_one();
}

Status DataDir::_init_cluster_id() {
    std::string cluster_id_path = _path + CLUSTER_ID_PREFIX;
    if (access(cluster_id_path.c_str(), F_OK) != 0) {
        int fd = open(cluster_id_path.c_str(), O_RDWR | O_CREAT,
                      S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (fd < 0 || close(fd) < 0) {
            RETURN_NOT_OK_STATUS_WITH_WARN(Status::IOError(strings::Substitute(
                                                   "failed to create cluster id file $0, err=$1",
                                                   cluster_id_path, errno_to_string(errno))),
                                           "create file failed");
        }
    }

    // obtain lock of all cluster id paths
    FILE* fp = NULL;
    fp = fopen(cluster_id_path.c_str(), "r+b");
    if (fp == NULL) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(
                        strings::Substitute("failed to open cluster id file $0", cluster_id_path)),
                "open file failed");
    }

    int lock_res = flock(fp->_fileno, LOCK_EX | LOCK_NB);
    if (lock_res < 0) {
        fclose(fp);
        fp = NULL;
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(
                        strings::Substitute("failed to flock cluster id file $0", cluster_id_path)),
                "flock file failed");
    }

    // obtain cluster id of all root paths
    auto st = _read_cluster_id(cluster_id_path, &_cluster_id);
    fclose(fp);
    return st;
}

Status DataDir::_read_cluster_id(const std::string& cluster_id_path, int32_t* cluster_id) {
    int32_t tmp_cluster_id = -1;

    std::fstream fs(cluster_id_path.c_str(), std::fstream::in);
    if (!fs.is_open()) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(
                        strings::Substitute("failed to open cluster id file $0", cluster_id_path)),
                "open file failed");
    }

    fs >> tmp_cluster_id;
    fs.close();

    if (tmp_cluster_id == -1 && (fs.rdstate() & std::fstream::eofbit) != 0) {
        *cluster_id = -1;
    } else if (tmp_cluster_id >= 0 && (fs.rdstate() & std::fstream::eofbit) != 0) {
        *cluster_id = tmp_cluster_id;
    } else {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::Corruption(strings::Substitute(
                        "cluster id file $0 is corrupt. [id=$1 eofbit=$2 failbit=$3 badbit=$4]",
                        cluster_id_path, tmp_cluster_id, fs.rdstate() & std::fstream::eofbit,
                        fs.rdstate() & std::fstream::failbit, fs.rdstate() & std::fstream::badbit)),
                "file content is error");
    }
    return Status::OK();
}

Status DataDir::_init_capacity() {
    boost::filesystem::path boost_path = _path;
    int64_t disk_capacity = boost::filesystem::space(boost_path).capacity;
    if (_capacity_bytes == -1) {
        _capacity_bytes = disk_capacity;
    } else if (_capacity_bytes > disk_capacity) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::InvalidArgument(strings::Substitute(
                        "root path $0's capacity $1 should not larger than disk capacity $2", _path,
                        _capacity_bytes, disk_capacity)),
                "init capacity failed");
    }

    std::string data_path = _path + DATA_PREFIX;
    if (!FileUtils::check_exist(data_path) && !FileUtils::create_dir(data_path).ok()) {
        RETURN_NOT_OK_STATUS_WITH_WARN(Status::IOError(strings::Substitute(
                                               "failed to create data root path $0", data_path)),
                                       "check_exist failed");
    }

    return Status::OK();
}

Status DataDir::_init_file_system() {
    struct stat s;
    if (stat(_path.c_str(), &s) != 0) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(strings::Substitute("stat file $0 failed, err=$1", _path,
                                                    errno_to_string(errno))),
                "stat file failed");
    }

    dev_t mount_device;
    if ((s.st_mode & S_IFMT) == S_IFBLK) {
        mount_device = s.st_rdev;
    } else {
        mount_device = s.st_dev;
    }

    FILE* mount_tablet = nullptr;
    if ((mount_tablet = setmntent(kMtabPath, "r")) == NULL) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(strings::Substitute("setmntent file $0 failed, err=$1", _path,
                                                    errno_to_string(errno))),
                "setmntent file failed");
    }

    bool is_find = false;
    struct mntent* mount_entry = NULL;
    struct mntent ent;
    char buf[1024];
    while ((mount_entry = getmntent_r(mount_tablet, &ent, buf, sizeof(buf))) != NULL) {
        if (strcmp(_path.c_str(), mount_entry->mnt_dir) == 0 ||
            strcmp(_path.c_str(), mount_entry->mnt_fsname) == 0) {
            is_find = true;
            break;
        }

        if (stat(mount_entry->mnt_fsname, &s) == 0 && s.st_rdev == mount_device) {
            is_find = true;
            break;
        }

        if (stat(mount_entry->mnt_dir, &s) == 0 && s.st_dev == mount_device) {
            is_find = true;
            break;
        }
    }

    endmntent(mount_tablet);

    if (!is_find) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(strings::Substitute("file system $0 not found", _path)),
                "find file system failed");
    }

    _file_system = mount_entry->mnt_fsname;

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
    OLAPStatus res = _meta->init();
    if (res != OLAP_SUCCESS) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(strings::Substitute("open rocksdb failed, path=$0", _path)),
                "init OlapMeta failed");
    }
    return Status::OK();
}

Status DataDir::set_cluster_id(int32_t cluster_id) {
    if (_cluster_id != -1) {
        if (_cluster_id == cluster_id) {
            return Status::OK();
        }
        LOG(ERROR) << "going to set cluster id to already assigned store, cluster_id="
                   << _cluster_id << ", new_cluster_id=" << cluster_id;
        return Status::InternalError("going to set cluster id to already assigned store");
    }
    return _write_cluster_id_to_path(_cluster_id_path(), cluster_id);
}

Status DataDir::_write_cluster_id_to_path(const std::string& path, int32_t cluster_id) {
    std::fstream fs(path.c_str(), std::fstream::out);
    if (!fs.is_open()) {
        LOG(WARNING) << "fail to open cluster id path. path=" << path;
        return Status::InternalError("IO Error");
    }
    fs << cluster_id;
    fs.close();
    return Status::OK();
}

void DataDir::health_check() {
    // check disk
    if (_is_used) {
        OLAPStatus res = OLAP_SUCCESS;
        if ((res = _read_and_write_test_file()) != OLAP_SUCCESS) {
            LOG(WARNING) << "store read/write test file occur IO Error. path=" << _path;
            if (is_io_error(res)) {
                _is_used = false;
            }
        }
    }
    disks_state->set_value(_is_used ? 1 : 0);
}

OLAPStatus DataDir::_read_and_write_test_file() {
    std::string test_file = _path + kTestFilePath;
    return read_write_test_file(test_file);
    ;
}

OLAPStatus DataDir::get_shard(uint64_t* shard) {
    std::stringstream shard_path_stream;
    uint32_t next_shard = 0;
    {
        std::lock_guard<std::mutex> l(_mutex);
        next_shard = _current_shard;
        _current_shard = (_current_shard + 1) % MAX_SHARD_NUM;
    }
    shard_path_stream << _path << DATA_PREFIX << "/" << next_shard;
    std::string shard_path = shard_path_stream.str();
    if (!FileUtils::check_exist(shard_path)) {
        RETURN_WITH_WARN_IF_ERROR(FileUtils::create_dir(shard_path), OLAP_ERR_CANNOT_CREATE_DIR,
                                  "fail to create path. path=" + shard_path);
    }

    *shard = next_shard;
    return OLAP_SUCCESS;
}

void DataDir::register_tablet(Tablet* tablet) {
    TabletInfo tablet_info(tablet->tablet_id(), tablet->schema_hash(), tablet->tablet_uid());

    std::lock_guard<std::mutex> l(_mutex);
    _tablet_set.emplace(std::move(tablet_info));
}

void DataDir::deregister_tablet(Tablet* tablet) {
    TabletInfo tablet_info(tablet->tablet_id(), tablet->schema_hash(), tablet->tablet_uid());

    std::lock_guard<std::mutex> l(_mutex);
    _tablet_set.erase(tablet_info);
}

void DataDir::clear_tablets(std::vector<TabletInfo>* tablet_infos) {
    std::lock_guard<std::mutex> l(_mutex);

    tablet_infos->insert(tablet_infos->end(), _tablet_set.begin(), _tablet_set.end());
    _tablet_set.clear();
}

std::string DataDir::get_absolute_shard_path(int64_t shard_id) {
    return strings::Substitute("$0$1/$2", _path, DATA_PREFIX, shard_id);
}

std::string DataDir::get_absolute_tablet_path(int64_t shard_id, int64_t tablet_id,
                                              int32_t schema_hash) {
    return strings::Substitute("$0/$1/$2", get_absolute_shard_path(shard_id), tablet_id,
                               schema_hash);
}

void DataDir::find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths) {
    // path: /root_path/trash/time_label/tablet_id/schema_hash
    std::string trash_path = _path + TRASH_PREFIX;
    std::vector<std::string> sub_dirs;
    FileUtils::list_files(Env::Default(), trash_path, &sub_dirs);
    for (auto& sub_dir : sub_dirs) {
        // sub dir is time_label
        std::string sub_path = trash_path + "/" + sub_dir;
        if (!FileUtils::is_dir(sub_path)) {
            continue;
        }
        std::string tablet_path = sub_path + "/" + std::to_string(tablet_id);
        bool exist = FileUtils::check_exist(tablet_path);
        if (exist) {
            paths->emplace_back(std::move(tablet_path));
        }
    }
}

std::string DataDir::get_root_path_from_schema_hash_path_in_trash(
        const std::string& schema_hash_dir_in_trash) {
    boost::filesystem::path schema_hash_path_in_trash(schema_hash_dir_in_trash);
    return schema_hash_path_in_trash.parent_path()
            .parent_path()
            .parent_path()
            .parent_path()
            .string();
}

OLAPStatus DataDir::_clean_unfinished_converting_data() {
    auto clean_unifinished_tablet_meta_func = [this](int64_t tablet_id, int32_t schema_hash,
                                                     const std::string& value) -> bool {
        TabletMetaManager::remove(this, tablet_id, schema_hash, HEADER_PREFIX);
        LOG(INFO) << "successfully clean temp tablet meta for tablet=" << tablet_id << "."
                  << schema_hash << "from data dir: " << _path;
        return true;
    };
    OLAPStatus clean_unfinished_meta_status = TabletMetaManager::traverse_headers(
            _meta, clean_unifinished_tablet_meta_func, HEADER_PREFIX);
    if (clean_unfinished_meta_status != OLAP_SUCCESS) {
        // If failed to clean meta just skip the error, there will be useless metas in rocksdb column family
        LOG(WARNING) << "there is failure when clean temp tablet meta from data dir=" << _path;
    } else {
        LOG(INFO) << "successfully clean temp tablet meta from data dir=" << _path;
    }
    auto clean_unifinished_rowset_meta_func = [this](TabletUid tablet_uid, RowsetId rowset_id,
                                                     const std::string& value) -> bool {
        RowsetMetaManager::remove(_meta, tablet_uid, rowset_id);
        LOG(INFO) << "successfully clean temp rowset meta for rowset_id=" << rowset_id
                  << " from data dir=" << _path;
        return true;
    };
    OLAPStatus clean_unfinished_rowset_meta_status =
            RowsetMetaManager::traverse_rowset_metas(_meta, clean_unifinished_rowset_meta_func);
    if (clean_unfinished_rowset_meta_status != OLAP_SUCCESS) {
        // If failed to clean meta just skip the error, there will be useless metas in rocksdb column family
        LOG(FATAL) << "fail to clean temp rowset meta from data dir=" << _path;
    } else {
        LOG(INFO) << "success to clean temp rowset meta from data dir=" << _path;
    }
    return OLAP_SUCCESS;
}

bool DataDir::convert_old_data_success() {
    return _convert_old_data_success;
}

OLAPStatus DataDir::set_convert_finished() {
    OLAPStatus res = _meta->set_tablet_convert_finished();
    if (res != OLAP_SUCCESS) {
        LOG(FATAL) << "save convert flag failed after convert old tablet. dir=" << _path;
        return res;
    }
    return OLAP_SUCCESS;
}

OLAPStatus DataDir::_check_incompatible_old_format_tablet() {
    auto check_incompatible_old_func = [this](int64_t tablet_id, int32_t schema_hash,
                                              const std::string& value) -> bool {
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
    OLAPStatus check_incompatible_old_status = TabletMetaManager::traverse_headers(
            _meta, check_incompatible_old_func, OLD_HEADER_PREFIX);
    if (check_incompatible_old_status != OLAP_SUCCESS) {
        LOG(WARNING) << "check incompatible old format meta fails, it may lead to data missing!!! "
                     << _path;
    } else {
        LOG(INFO) << "successfully check incompatible old format meta " << _path;
    }
    return check_incompatible_old_status;
}

// TODO(ygl): deal with rowsets and tablets when load failed
OLAPStatus DataDir::load() {
    LOG(INFO) << "start to load tablets from " << _path;
    // load rowset meta from meta env and create rowset
    // COMMITTED: add to txn manager
    // VISIBLE: add to tablet
    // if one rowset load failed, then the total data dir will not be loaded

    // necessarily check incompatible old format. when there are old metas, it may load to data missing
    _check_incompatible_old_format_tablet();

    std::vector<RowsetMetaSharedPtr> dir_rowset_metas;
    LOG(INFO) << "begin loading rowset from meta";
    auto load_rowset_func = [&dir_rowset_metas](TabletUid tablet_uid, RowsetId rowset_id,
                                                const std::string& meta_str) -> bool {
        RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
        bool parsed = rowset_meta->init(meta_str);
        if (!parsed) {
            LOG(WARNING) << "parse rowset meta string failed for rowset_id:" << rowset_id;
            // return false will break meta iterator, return true to skip this error
            return true;
        }
        dir_rowset_metas.push_back(rowset_meta);
        return true;
    };
    OLAPStatus load_rowset_status =
            RowsetMetaManager::traverse_rowset_metas(_meta, load_rowset_func);

    if (load_rowset_status != OLAP_SUCCESS) {
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
                                    const std::string& value) -> bool {
        OLAPStatus status = _tablet_manager->load_tablet_from_meta(this, tablet_id, schema_hash,
                                                                   value, false, false, false, false);
        if (status != OLAP_SUCCESS && status != OLAP_ERR_TABLE_ALREADY_DELETED_ERROR
            && status != OLAP_ERR_ENGINE_INSERT_OLD_TABLET) {
            // load_tablet_from_meta() may return OLAP_ERR_TABLE_ALREADY_DELETED_ERROR
            // which means the tablet status is DELETED
            // This may happen when the tablet was just deleted before the BE restarted,
            // but it has not been cleared from rocksdb. At this time, restarting the BE
            // will read the tablet in the DELETE state from rocksdb. These tablets have been
            // added to the garbage collection queue and will be automatically deleted afterwards.
            // Therefore, we believe that this situation is not a failure.

            // Besides, load_tablet_from_meta() may return OLAP_ERR_ENGINE_INSERT_OLD_TABLET
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
    OLAPStatus load_tablet_status = TabletMetaManager::traverse_headers(_meta, load_tablet_func);
    if (failed_tablet_ids.size() != 0) {
        LOG(WARNING) << "load tablets from header failed"
                     << ", loaded tablet: " << tablet_ids.size()
                     << ", error tablet: " << failed_tablet_ids.size() << ", path: " << _path;
        if (!config::ignore_load_tablet_failure) {
            LOG(FATAL) << "load tablets encounter failure. stop BE process. path: " << _path;
        }
    }
    if (load_tablet_status != OLAP_SUCCESS) {
        LOG(WARNING) << "there is failure when loading tablet headers"
                     << ", loaded tablet: " << tablet_ids.size()
                     << ", error tablet: " << failed_tablet_ids.size() << ", path: " << _path;
    } else {
        LOG(INFO) << "load tablet from meta finished"
                  << ", loaded tablet: " << tablet_ids.size()
                  << ", error tablet: " << failed_tablet_ids.size() << ", path: " << _path;
    }

    // traverse rowset
    // 1. add committed rowset to txn map
    // 2. add visible rowset to tablet
    // ignore any errors when load tablet or rowset, because fe will repair them after report
    for (auto rowset_meta : dir_rowset_metas) {
        TabletSharedPtr tablet = _tablet_manager->get_tablet(rowset_meta->tablet_id(),
                                                             rowset_meta->tablet_schema_hash());
        // tablet maybe dropped, but not drop related rowset meta
        if (tablet == nullptr) {
            LOG(WARNING) << "could not find tablet id: " << rowset_meta->tablet_id()
                         << ", schema hash: " << rowset_meta->tablet_schema_hash()
                         << ", for rowset: " << rowset_meta->rowset_id() << ", skip this rowset";
            continue;
        }
        RowsetSharedPtr rowset;
        OLAPStatus create_status = RowsetFactory::create_rowset(
                &tablet->tablet_schema(), tablet->tablet_path(), rowset_meta, &rowset);
        if (create_status != OLAP_SUCCESS) {
            LOG(WARNING) << "could not create rowset from rowsetmeta: "
                         << " rowset_id: " << rowset_meta->rowset_id()
                         << " rowset_type: " << rowset_meta->rowset_type()
                         << " rowset_state: " << rowset_meta->rowset_state();
            continue;
        }
        if (rowset_meta->rowset_state() == RowsetStatePB::COMMITTED &&
            rowset_meta->tablet_uid() == tablet->tablet_uid()) {
            OLAPStatus commit_txn_status = _txn_manager->commit_txn(
                    _meta, rowset_meta->partition_id(), rowset_meta->txn_id(),
                    rowset_meta->tablet_id(), rowset_meta->tablet_schema_hash(),
                    rowset_meta->tablet_uid(), rowset_meta->load_id(), rowset, true);
            if (commit_txn_status != OLAP_SUCCESS &&
                commit_txn_status != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
                LOG(WARNING) << "failed to add committed rowset: " << rowset_meta->rowset_id()
                             << " to tablet: " << rowset_meta->tablet_id()
                             << " for txn: " << rowset_meta->txn_id();
            } else {
                LOG(INFO) << "successfully to add committed rowset: " << rowset_meta->rowset_id()
                          << " to tablet: " << rowset_meta->tablet_id()
                          << " schema hash: " << rowset_meta->tablet_schema_hash()
                          << " for txn: " << rowset_meta->txn_id();
            }
        } else if (rowset_meta->rowset_state() == RowsetStatePB::VISIBLE &&
                   rowset_meta->tablet_uid() == tablet->tablet_uid()) {
            OLAPStatus publish_status = tablet->add_rowset(rowset, false);
            if (publish_status != OLAP_SUCCESS &&
                publish_status != OLAP_ERR_PUSH_VERSION_ALREADY_EXIST) {
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
        }
    }
    return OLAP_SUCCESS;
}

void DataDir::add_pending_ids(const std::string& id) {
    WriteLock wr_lock(&_pending_path_mutex);
    _pending_path_ids.insert(id);
}

void DataDir::remove_pending_ids(const std::string& id) {
    WriteLock wr_lock(&_pending_path_mutex);
    _pending_path_ids.erase(id);
}

// gc unused tablet schemahash dir
void DataDir::perform_path_gc_by_tablet() {
    std::unique_lock<std::mutex> lck(_check_path_mutex);
    _check_path_cv.wait(lck, [this] { return _stop_bg_worker || !_all_tablet_schemahash_paths.empty(); });
    if (_stop_bg_worker) {
        return;
    }
    LOG(INFO) << "start to path gc by tablet schemahash.";
    int counter = 0;
    for (const auto& path : _all_tablet_schemahash_paths) {
        ++counter;
        if (config::path_gc_check_step > 0 && counter % config::path_gc_check_step == 0) {
            SleepFor(MonoDelta::FromMilliseconds(config::path_gc_check_step_interval_ms));
        }
        TTabletId tablet_id = -1;
        TSchemaHash schema_hash = -1;
        bool is_valid = _tablet_manager->get_tablet_id_and_schema_hash_from_path(path, &tablet_id,
                                                                                 &schema_hash);
        if (!is_valid) {
            LOG(WARNING) << "unknown path:" << path;
            continue;
        }
        // should not happen, because already check it is a valid tablet schema hash path in previous step
        // so that log fatal here
        if (tablet_id < 1 || schema_hash < 1) {
            LOG(WARNING) << "invalid tablet id " << tablet_id << " or schema hash " << schema_hash
                         << ", path=" << path;
            continue;
        }
        TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_id, schema_hash);
        if (tablet != nullptr) {
            // could find the tablet, then skip check it
            continue;
        }
        boost::filesystem::path tablet_path(path);
        boost::filesystem::path data_dir_path =
                tablet_path.parent_path().parent_path().parent_path().parent_path();
        std::string data_dir_string = data_dir_path.string();
        DataDir* data_dir = StorageEngine::instance()->get_store(data_dir_string);
        if (data_dir == nullptr) {
            LOG(WARNING) << "could not find data dir for tablet path " << path;
            continue;
        }
        _tablet_manager->try_delete_unused_tablet_path(data_dir, tablet_id, schema_hash, path);
    }
    _all_tablet_schemahash_paths.clear();
    LOG(INFO) << "finished one time path gc by tablet.";
}

void DataDir::perform_path_gc_by_rowsetid() {
    // init the set of valid path
    // validate the path in data dir
    std::unique_lock<std::mutex> lck(_check_path_mutex);
    _check_path_cv.wait(lck, [this] { return _stop_bg_worker || !_all_check_paths.empty(); });
    if (_stop_bg_worker) {
        return;
    }
    LOG(INFO) << "start to path gc by rowsetid.";
    int counter = 0;
    for (const auto& path : _all_check_paths) {
        ++counter;
        if (config::path_gc_check_step > 0 && counter % config::path_gc_check_step == 0) {
            SleepFor(MonoDelta::FromMilliseconds(config::path_gc_check_step_interval_ms));
        }
        TTabletId tablet_id = -1;
        TSchemaHash schema_hash = -1;
        bool is_valid = _tablet_manager->get_tablet_id_and_schema_hash_from_path(path, &tablet_id,
                                                                                 &schema_hash);
        if (!is_valid) {
            LOG(WARNING) << "unknown path:" << path;
            continue;
        }
        if (tablet_id > 0 && schema_hash > 0) {
            // tablet schema hash path or rowset file path
            // gc thread should get tablet include deleted tablet
            // or it will delete rowset file before tablet is garbage collected
            RowsetId rowset_id;
            bool is_rowset_file = TabletManager::get_rowset_id_from_path(path, &rowset_id);
            if (is_rowset_file) {
                TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_id, schema_hash);
                if (tablet != nullptr) {
                    if (!tablet->check_rowset_id(rowset_id) &&
                        !StorageEngine::instance()->check_rowset_id_in_unused_rowsets(rowset_id)) {
                        _process_garbage_path(path);
                    }
                }
            }
        }
    }
    _all_check_paths.clear();
    LOG(INFO) << "finished one time path gc by rowsetid.";
}

// path producer
void DataDir::perform_path_scan() {
    std::unique_lock<std::mutex> lck(_check_path_mutex);
    if (!_all_check_paths.empty()) {
        LOG(INFO) << "_all_check_paths is not empty when path scan.";
        return;
    }
    LOG(INFO) << "start to scan data dir path:" << _path;
    std::set<std::string> shards;
    std::string data_path = _path + DATA_PREFIX;

    Status ret = FileUtils::list_dirs_files(data_path, &shards, nullptr, Env::Default());
    if (!ret.ok()) {
        LOG(WARNING) << "fail to walk dir. path=[" + data_path << "] error[" << ret.to_string()
                     << "]";
        return;
    }

    for (const auto& shard : shards) {
        std::string shard_path = data_path + "/" + shard;
        std::set<std::string> tablet_ids;
        ret = FileUtils::list_dirs_files(shard_path, &tablet_ids, nullptr, Env::Default());
        if (!ret.ok()) {
            LOG(WARNING) << "fail to walk dir. [path=" << shard_path << "] error["
                         << ret.to_string() << "]";
            continue;
        }
        for (const auto& tablet_id : tablet_ids) {
            std::string tablet_id_path = shard_path + "/" + tablet_id;
            std::set<std::string> schema_hashes;
            ret = FileUtils::list_dirs_files(tablet_id_path, &schema_hashes, nullptr,
                                             Env::Default());
            if (!ret.ok()) {
                LOG(WARNING) << "fail to walk dir. [path=" << tablet_id_path << "]"
                             << " error[" << ret.to_string() << "]";
                continue;
            }
            for (const auto& schema_hash : schema_hashes) {
                std::string tablet_schema_hash_path = tablet_id_path + "/" + schema_hash;
                _all_tablet_schemahash_paths.insert(tablet_schema_hash_path);

                std::set<std::string> rowset_files;
                ret = FileUtils::list_dirs_files(tablet_schema_hash_path, nullptr,
                                                 &rowset_files, Env::Default());
                if (!ret.ok()) {
                    LOG(WARNING) << "fail to walk dir. [path=" << tablet_schema_hash_path
                                 << "] error[" << ret.to_string() << "]";
                    continue;
                }
                for (const auto& rowset_file : rowset_files) {
                    std::string rowset_file_path = tablet_schema_hash_path + "/" + rowset_file;
                    _all_check_paths.insert(rowset_file_path);
                }
            }
        }
    }
    LOG(INFO) << "scan data dir path: " << _path
              << " finished. path size: " << _all_check_paths.size() + _all_tablet_schemahash_paths.size();
    _check_path_cv.notify_one();
}

void DataDir::_process_garbage_path(const std::string& path) {
    if (FileUtils::check_exist(path)) {
        LOG(INFO) << "collect garbage dir path: " << path;
        WARN_IF_ERROR(FileUtils::remove_all(path), "remove garbage dir failed. path: " + path);
    }
}

bool DataDir::_check_pending_ids(const std::string& id) {
    ReadLock rd_lock(&_pending_path_mutex);
    return _pending_path_ids.find(id) != _pending_path_ids.end();
}

Status DataDir::update_capacity() {
    try {
        boost::filesystem::path path_name(_path);
        boost::filesystem::space_info path_info = boost::filesystem::space(path_name);
        _available_bytes = path_info.available;
        if (_disk_capacity_bytes == 0) {
            // disk capacity only need to be set once
            _disk_capacity_bytes = path_info.capacity;
        }
    } catch (boost::filesystem::filesystem_error& e) {
        RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::IOError(strings::Substitute(
                        "get path $0 available capacity failed, error=$1", _path, e.what())),
                "boost::filesystem::space failed");
    }

    disks_total_capacity->set_value(_disk_capacity_bytes);
    disks_avail_capacity->set_value(_available_bytes);
    LOG(INFO) << "path: " << _path << " total capacity: " << _disk_capacity_bytes
              << ", available capacity: " << _available_bytes;

    return Status::OK();
}

void DataDir::update_user_data_size(int64_t size) {
    disks_data_used_capacity->set_value(size);
}

size_t DataDir::tablet_size() const {
    std::lock_guard<std::mutex> l(_mutex);
    return _tablet_set.size();
}

bool DataDir::reach_capacity_limit(int64_t incoming_data_size) {
    double used_pct = (_disk_capacity_bytes - _available_bytes + incoming_data_size) /
                      (double)_disk_capacity_bytes;
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
} // namespace doris
