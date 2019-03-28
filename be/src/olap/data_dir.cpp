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
#include <mntent.h>
#include <stdio.h>
#include <sys/file.h>
#include <sys/statfs.h>
#include <sys/statfs.h>
#include <utime.h>

#include <fstream>
#include <sstream>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/filesystem.hpp>
#include <boost/interprocess/sync/file_lock.hpp>

#include "olap/file_helper.h"
#include "olap/olap_define.h"
#include "olap/olap_snapshot_converter.h"
#include "olap/utils.h" // for check_dir_existed
#include "service/backend_options.h"
#include "util/file_utils.h"
#include "util/string_util.h"
#include "olap/tablet_meta_manager.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/rowset_factory.h"

namespace doris {

static const char* const kMtabPath = "/etc/mtab";
static const char* const kTestFilePath = "/.testfile";

DataDir::DataDir(const std::string& path, int64_t capacity_bytes)
        : _path(path),
        _cluster_id(-1),
        _capacity_bytes(capacity_bytes),
        _available_bytes(0),
        _used_bytes(0),
        _current_shard(0),
        _is_used(false),
        _to_be_deleted(false),
        _test_file_read_buf(nullptr),
        _test_file_write_buf(nullptr),
        _meta((nullptr)) {
}

DataDir::~DataDir() {
    free(_test_file_read_buf);
    free(_test_file_write_buf);
    delete _id_generator;
    delete _meta;
}

Status DataDir::init() {
    _rand_seed = static_cast<uint32_t>(time(NULL));
    if (posix_memalign((void**)&_test_file_write_buf,
                       DIRECT_IO_ALIGNMENT,
                       TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "fail to allocate memory. size=" <<  TEST_FILE_BUF_SIZE;
        return Status("No memory");
    }
    if (posix_memalign((void**)&_test_file_read_buf,
                       DIRECT_IO_ALIGNMENT,
                       TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "fail to allocate memory. size=" <<  TEST_FILE_BUF_SIZE;
        return Status("No memory");
    }
    RETURN_IF_ERROR(_check_path_exist());
    std::string align_tag_path = _path + ALIGN_TAG_PREFIX;
    if (access(align_tag_path.c_str(), F_OK) == 0) {
        LOG(WARNING) << "align tag was found, path=" << _path;
        return Status("invalid root path: ");
    }

    RETURN_IF_ERROR(_init_cluster_id());
    RETURN_IF_ERROR(_init_extension_and_capacity());
    RETURN_IF_ERROR(_init_file_system());
    RETURN_IF_ERROR(_init_meta());

    _id_generator = new RowsetIdGenerator(_meta);
    auto res = _id_generator->init();
    if (res != OLAP_SUCCESS) {
        return Status("Id generator initialized failed.");
    }

    _is_used = true;
    return Status::OK;
}

Status DataDir::_check_path_exist() {
    DIR* dirp = opendir(_path.c_str());
    if (dirp == nullptr) {
        char buf[64];
        LOG(WARNING) << "opendir failed, path=" << _path
            << ", errno=" << errno << ", errmsg=" << strerror_r(errno, buf, 64);
        return Status("opendir failed");
    }
    struct dirent dirent;
    struct dirent* result = nullptr;
    if (readdir_r(dirp, &dirent, &result) != 0) {
        char buf[64];
        LOG(WARNING) << "readdir failed, path=" << _path
            << ", errno=" << errno << ", errmsg=" << strerror_r(errno, buf, 64);
        closedir(dirp);
        return Status("readdir failed");
    }
    return Status::OK;
}

Status DataDir::_init_cluster_id() {
    std::string cluster_id_path = _path + CLUSTER_ID_PREFIX;
    if (access(cluster_id_path.c_str(), F_OK) != 0) {
        int fd = open(cluster_id_path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (fd < 0 || close(fd) < 0) {
            char errmsg[64];
            LOG(WARNING) << "fail to create file. [path='" << cluster_id_path
                << "' err='" << strerror_r(errno, errmsg, 64) << "']";
            return Status("invalid store path: create cluster id failed");
        }
    }

    // obtain lock of all cluster id paths
    FILE* fp = NULL;
    fp = fopen(cluster_id_path.c_str(), "r+b");
    if (fp == NULL) {
        LOG(WARNING) << "fail to open cluster id path. path=" << cluster_id_path;
        return Status("invalid store path: open cluster id failed");
    }

    int lock_res = flock(fp->_fileno, LOCK_EX | LOCK_NB);
    if (lock_res < 0) {
        LOG(WARNING) << "fail to lock file descriptor. path=" << cluster_id_path;
        fclose(fp);
        fp = NULL;
        return Status("invalid store path: flock cluster id failed");
    }

    // obtain cluster id of all root paths
    auto st = _read_cluster_id(cluster_id_path, &_cluster_id);
    fclose(fp);
    return st;
}

Status DataDir::_read_cluster_id(const std::string& path, int32_t* cluster_id) {
    int32_t tmp_cluster_id = -1;

    std::fstream fs(path.c_str(), std::fstream::in);
    if (!fs.is_open()) {
        LOG(WARNING) << "fail to open cluster id path. [path='" << path << "']";
        return Status("open file failed");
    }

    fs >> tmp_cluster_id;
    fs.close();

    if (tmp_cluster_id == -1 && (fs.rdstate() & std::fstream::eofbit) != 0) {
        *cluster_id = -1;
    } else if (tmp_cluster_id >= 0 && (fs.rdstate() & std::fstream::eofbit) != 0) {
        *cluster_id = tmp_cluster_id;
    } else {
        OLAP_LOG_WARNING("fail to read cluster id from file. "
                         "[id=%d eofbit=%d failbit=%d badbit=%d]",
                         tmp_cluster_id,
                         fs.rdstate() & std::fstream::eofbit,
                         fs.rdstate() & std::fstream::failbit,
                         fs.rdstate() & std::fstream::badbit);
        return Status("cluster id file corrupt");
    }
    return Status::OK;
}

Status DataDir::_init_extension_and_capacity() {
    boost::filesystem::path boost_path = _path;
    std::string extension = boost::filesystem::canonical(boost_path).extension().string();
    if (extension != "") {
        if (boost::iequals(extension, ".ssd")) {
            _storage_medium = TStorageMedium::SSD;
        } else if (boost::iequals(extension, ".hdd")) {
            _storage_medium = TStorageMedium::HDD;
        } else {
            LOG(WARNING) << "store path has wrong extension. path=" << _path;
            return Status("invalid sotre path: invalid extension");
        }
    } else {
        _storage_medium = TStorageMedium::HDD;
    }

    int64_t disk_capacity = boost::filesystem::space(boost_path).capacity;
    if (_capacity_bytes == -1) {
        _capacity_bytes = disk_capacity;
    } else if (_capacity_bytes > disk_capacity) {
        LOG(WARNING) << "root path capacity should not larger than disk capacity. "
            << "path=" << _path
            << ", capacity_bytes=" << _capacity_bytes
            << ", disk_capacity=" << disk_capacity;
        return Status("invalid store path: invalid capacity");
    }

    std::string data_path = _path + DATA_PREFIX;
    if (!check_dir_existed(data_path) && create_dir(data_path) != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to create data root path. path=" << data_path;
        return Status("invalid store path: failed to create data directory");
    }

    return Status::OK;
}

Status DataDir::_init_file_system() {
    struct stat s;
    if (stat(_path.c_str(), &s) != 0) {
        char errmsg[64];
        LOG(WARNING) << "stat failed, path=" << _path
            << ", errno=" << errno << ", errmsg=" << strerror_r(errno, errmsg, 64);
        return Status("invalid store path: stat failed");
    }

    dev_t mount_device;
    if ((s.st_mode & S_IFMT) == S_IFBLK) {
        mount_device = s.st_rdev;
    } else {
        mount_device = s.st_dev;
    }

    FILE* mount_tablet = nullptr;
    if ((mount_tablet = setmntent(kMtabPath, "r")) == NULL) {
        char errmsg[64];
        LOG(WARNING) << "setmntent failed, path=" << kMtabPath
            << ", errno=" << errno << ", errmsg=" << strerror_r(errno, errmsg, 64);
        return Status("invalid store path: setmntent failed");
    }

    bool is_find = false;
    struct mntent* mount_entry = NULL;
    while ((mount_entry = getmntent(mount_tablet)) != NULL) {
        if (strcmp(_path.c_str(), mount_entry->mnt_dir) == 0
                || strcmp(_path.c_str(), mount_entry->mnt_fsname) == 0) {
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
        LOG(WARNING) << "fail to find file system, path=" << _path;
        return Status("invalid store path: find file system failed");
    }

    _file_system = mount_entry->mnt_fsname;

    return Status::OK;
}

Status DataDir::_init_meta() {
    // init path hash
    _path_hash = hash_of_path(BackendOptions::get_localhost(), _path);
    LOG(INFO) << "get hash of path: " << _path
              << ": " << _path_hash;

    // init meta
    _meta = new(std::nothrow) OlapMeta(_path);
    if (_meta == nullptr) {
        LOG(WARNING) << "new olap meta failed";
        return Status("new olap meta failed");
    }
    OLAPStatus res = _meta->init();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "init meta failed";
        return Status("init meta failed");
    }
    return Status::OK;
}

Status DataDir::set_cluster_id(int32_t cluster_id) {
    if (_cluster_id != -1) {
        if (_cluster_id == cluster_id) {
            return Status::OK;
        }
        LOG(ERROR) << "going to set cluster id to already assigned store, cluster_id="
            << _cluster_id << ", new_cluster_id=" << cluster_id;
        return Status("going to set cluster id to already assigned store");
    }
    return _write_cluster_id_to_path(_cluster_id_path(), cluster_id);
}

Status DataDir::_write_cluster_id_to_path(const std::string& path, int32_t cluster_id) {
    std::fstream fs(path.c_str(), std::fstream::out);
    if (!fs.is_open()) {
        LOG(WARNING) << "fail to open cluster id path. path=" << path;
        return Status("IO Error");
    }
    fs << cluster_id;
    fs.close();
    return Status::OK;
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
}

OLAPStatus DataDir::_read_and_write_test_file() {
    std::string test_file = _path + kTestFilePath;

    if (access(test_file.c_str(), F_OK) == 0) {
        if (remove(test_file.c_str()) != 0) {
            char errmsg[64];
            LOG(WARNING) << "fail to delete test file. "
                << "path=" << test_file
                << ", errno=" << errno << ", err=" << strerror_r(errno, errmsg, 64);
            return OLAP_ERR_IO_ERROR;
        }
    } else {
        if (errno != ENOENT) {
            char errmsg[64];
            LOG(WARNING) << "fail to access test file. "
                << "path=" << test_file
                << ", errno=" << errno << ", err=" << strerror_r(errno, errmsg, 64);
            return OLAP_ERR_IO_ERROR;
        }
    }

    OLAPStatus res = OLAP_SUCCESS;
    FileHandler file_handler;
    if ((res = file_handler.open_with_mode(test_file.c_str(),
                                           O_RDWR | O_CREAT | O_DIRECT,
                                           S_IRUSR | S_IWUSR)) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to create test file. path=" << test_file;
        return res;
    }

    for (size_t i = 0; i < TEST_FILE_BUF_SIZE; ++i) {
        int32_t tmp_value = rand_r(&_rand_seed);
        _test_file_write_buf[i] = static_cast<char>(tmp_value);
    }

    if ((res = file_handler.pwrite(_test_file_write_buf, TEST_FILE_BUF_SIZE, SEEK_SET)) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to write test file. [file_name=" << test_file << "]";
        return res;
    }

    if ((res = file_handler.pread(_test_file_read_buf, TEST_FILE_BUF_SIZE, SEEK_SET)) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to read test file. [file_name=" << test_file << "]";
        return res;
    }

    if (memcmp(_test_file_write_buf, _test_file_read_buf, TEST_FILE_BUF_SIZE) != 0) {
        OLAP_LOG_WARNING("the test file write_buf and read_buf not equal.");
        return OLAP_ERR_TEST_FILE_ERROR;
    }

    if ((res = file_handler.close()) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to close test file. [file_name=" << test_file << "]";
        return res;
    }

    if (remove(test_file.c_str()) != 0) {
        char errmsg[64];
        VLOG(3) << "fail to delete test file. [err='" << strerror_r(errno, errmsg, 64)
                << "' path='" << test_file << "']";
        return OLAP_ERR_IO_ERROR;
    }

    return res;
}

OLAPStatus DataDir::get_shard(uint64_t* shard) {
    OLAPStatus res = OLAP_SUCCESS;
    std::lock_guard<std::mutex> l(_mutex);

    std::stringstream shard_path_stream;
    uint32_t next_shard = _current_shard;
    _current_shard = (_current_shard + 1) % MAX_SHARD_NUM;
    shard_path_stream << _path << DATA_PREFIX << "/" << next_shard;
    std::string shard_path = shard_path_stream.str();
    if (!check_dir_existed(shard_path)) {
        res = create_dir(shard_path);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to create path. [path='" << shard_path << "']";
            return res;
        }
    }

    *shard = next_shard;
    return OLAP_SUCCESS;
}

OLAPStatus DataDir::register_tablet(Tablet* tablet) {
    std::lock_guard<std::mutex> l(_mutex);

    TabletInfo tablet_info(tablet->tablet_id(), tablet->schema_hash());
    _tablet_set.insert(tablet_info);
    return OLAP_SUCCESS;
}

OLAPStatus DataDir::deregister_tablet(Tablet* tablet) {
    std::lock_guard<std::mutex> l(_mutex);

    TabletInfo tablet_info(tablet->tablet_id(), tablet->schema_hash());
    _tablet_set.erase(tablet_info);
    return OLAP_SUCCESS;
}

void DataDir::clear_tablets(std::vector<TabletInfo>* tablet_infos) {
     for (auto& tablet : _tablet_set) {
        tablet_infos->push_back(tablet);
    }
    _tablet_set.clear();
}

std::string DataDir::get_absolute_shard_path(const std::string& shard_string) {
    return _path + DATA_PREFIX + "/" + shard_string;
}

std::string DataDir::get_absolute_tablet_path(TabletMeta* tablet_meta, bool with_schema_hash) {
    if (with_schema_hash) {
        return _path + DATA_PREFIX + "/" + std::to_string(tablet_meta->shard_id())
            + "/" + std::to_string(tablet_meta->tablet_id()) + "/" + std::to_string(tablet_meta->schema_hash());

    } else {
        return _path + DATA_PREFIX + "/" + std::to_string(tablet_meta->shard_id())
            + "/" + std::to_string(tablet_meta->tablet_id());
    }
}

std::string DataDir::get_absolute_tablet_path(TabletMetaPB* tablet_meta, bool with_schema_hash) {
    if (with_schema_hash) {
        return _path + DATA_PREFIX + "/" + std::to_string(tablet_meta->shard_id())
            + "/" + std::to_string(tablet_meta->tablet_id())
            + "/" + std::to_string(tablet_meta->schema_hash());

    } else {
        return _path + DATA_PREFIX + "/" + std::to_string(tablet_meta->shard_id())
            + "/" + std::to_string(tablet_meta->tablet_id());
    }
}

std::string DataDir::get_absolute_tablet_path(OLAPHeaderMessage& olap_header_msg, bool with_schema_hash) {
    if (with_schema_hash) {
        return _path + DATA_PREFIX + "/" + std::to_string(olap_header_msg.shard())
            + "/" + std::to_string(olap_header_msg.tablet_id()) + "/" + std::to_string(olap_header_msg.schema_hash());

    } else {
        return _path + DATA_PREFIX + "/" + std::to_string(olap_header_msg.shard())
            + "/" + std::to_string(olap_header_msg.tablet_id());
    }
}

void DataDir::find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths) {
    // path: /root_path/trash/time_label/tablet_id/schema_hash
    std::string trash_path = _path + TRASH_PREFIX;
    std::vector<std::string> sub_dirs;
    FileUtils::scan_dir(trash_path, &sub_dirs);
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
    return schema_hash_path_in_trash.parent_path().parent_path().parent_path().parent_path().string();
}

OLAPStatus DataDir::_clean_unfinished_converting_data() {
    auto clean_unifinished_tablet_meta_func = [this](long tablet_id,
        long schema_hash, const std::string& value) -> bool {
        TabletMetaManager::remove(this, tablet_id, schema_hash, HEADER_PREFIX);
        LOG(INFO) << "successfully clean temp tablet meta for tablet="
                  << tablet_id << "." << schema_hash
                  << "from data dir: " << _path;
        return true;
    };
    OLAPStatus clean_unfinished_meta_status = TabletMetaManager::traverse_headers(_meta,
        clean_unifinished_tablet_meta_func, HEADER_PREFIX);
    if (clean_unfinished_meta_status != OLAP_SUCCESS) {
        // If failed to clean meta just skip the error, there will be useless metas in rocksdb column family
        LOG(WARNING) << "there is failure when clean temp tablet meta from data dir=" << _path;
    } else {
        LOG(INFO) << "successfully clean temp tablet meta from data dir=" << _path;
    }
    auto clean_unifinished_rowset_meta_func = [this](RowsetId rowset_id, const std::string& value) -> bool {
        RowsetMetaManager::remove(_meta, rowset_id);
        LOG(INFO) << "successfully clean temp rowset meta for rowset_id="
                  << rowset_id << " from data dir=" << _path;
        return true;
    };
    OLAPStatus clean_unfinished_rowset_meta_status = RowsetMetaManager::traverse_rowset_metas(_meta,
        clean_unifinished_rowset_meta_func);
    if (clean_unfinished_rowset_meta_status != OLAP_SUCCESS) {
        // If failed to clean meta just skip the error, there will be useless metas in rocksdb column family
        LOG(WARNING) << "fail to clean temp rowset meta from data dir=" << _path;
    } else {
        LOG(INFO) << "success to clean temp rowset meta from data dir=" << _path;
    }
    return OLAP_SUCCESS;
}

// convert old tablet and its files to new tablet meta and rowset format
// if any error occurred during converting, stop it and break.
OLAPStatus DataDir::_convert_old_tablet() {
    auto convert_tablet_func = [this](long tablet_id,
        long schema_hash, const std::string& value) -> bool {
        OlapSnapshotConverter converter;
        // convert olap header and files
        OLAPHeaderMessage olap_header_msg;
        TabletMetaPB tablet_meta_pb;
        vector<RowsetMetaPB> pending_rowsets;
        bool parsed = olap_header_msg.ParseFromString(value);
        if (!parsed) {
            LOG(WARNING) << "convert olap header to tablet meta failed when load olap header tablet="
                         << tablet_id << "." << schema_hash;
            return false;
        }
        string old_data_path_prefix = get_absolute_tablet_path(olap_header_msg, true);
        OLAPStatus status = converter.to_new_snapshot(olap_header_msg, old_data_path_prefix,
            old_data_path_prefix, *this, &tablet_meta_pb, &pending_rowsets);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "convert olap header to tablet meta failed when convert header and files tablet=" 
                         << tablet_id << "." << schema_hash;
            return false;
        }

        // write pending rowset to olap meta
        for (auto& rowset_pb : pending_rowsets) {
            string meta_binary;
            rowset_pb.SerializeToString(&meta_binary);
            status = RowsetMetaManager::save(_meta, rowset_pb.rowset_id() , meta_binary);
            if (status != OLAP_SUCCESS) {
                LOG(WARNING) << "convert olap header to tablet meta failed when save rowset meta tablet=" 
                             << tablet_id << "." << schema_hash;
                return false;
            }
        }

        // write converted tablet meta to olap meta
        string meta_binary;
        tablet_meta_pb.SerializeToString(&meta_binary);
        status = TabletMetaManager::save(this, tablet_meta_pb.tablet_id(), tablet_meta_pb.schema_hash(), meta_binary);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "convert olap header to tablet meta failed when save tablet meta tablet=" 
                         << tablet_id << "." << schema_hash;
            return false;
        } else {
            LOG(INFO) << "convert olap header to tablet meta successfully and save tablet meta to meta tablet=" 
                      << tablet_id << "." << schema_hash;
        }
        return true;
    };
    OLAPStatus convert_tablet_status = TabletMetaManager::traverse_headers(_meta, 
        convert_tablet_func, OLD_HEADER_PREFIX);
    if (convert_tablet_status != OLAP_SUCCESS) {
        LOG(WARNING) << "there is failure when convert old tablet, data dir:" << _path;
        return convert_tablet_status;
    } else {
        LOG(INFO) << "successfully convert old tablet, data dir: " << _path;
    }
    return OLAP_SUCCESS;
}

OLAPStatus DataDir::_remove_old_meta_and_files() {
    // clean old meta(olap header message) 
    auto clean_old_meta_func = [this](long tablet_id,
        long schema_hash, const std::string& value) -> bool {
        TabletMetaManager::remove(this, tablet_id, schema_hash, OLD_HEADER_PREFIX);
        LOG(INFO) << "successfully clean old tablet meta(olap header) for tablet=" 
                  << tablet_id << "." << schema_hash
                  << "from data dir: " << _path;
        return true;
    };
    OLAPStatus clean_old_meta_status = TabletMetaManager::traverse_headers(_meta,
        clean_old_meta_func, OLD_HEADER_PREFIX);
    if (clean_old_meta_status != OLAP_SUCCESS) {
        // If failed to clean meta just skip the error, there will be useless metas in rocksdb column family
        LOG(WARNING) << "there is failure when clean old tablet meta(olap header) from data dir:" << _path;
    } else {
        LOG(INFO) << "successfully clean old tablet meta(olap header) from data dir: " << _path;
    }

    // clean old files because they have hard links in new file name format
    auto clean_old_files_func = [this](long tablet_id,
        long schema_hash, const std::string& value) -> bool {
        TabletMetaPB tablet_meta_pb;
        bool parsed = tablet_meta_pb.ParseFromString(value);
        if (!parsed) {
            // if errors when load, just skip it
            LOG(WARNING) << "failed to load tablet meta from meta store to tablet=" << tablet_id << "." << schema_hash;
            return true;
        }

        TabletSchema tablet_schema;
        tablet_schema.init_from_pb(tablet_meta_pb.schema());
        string data_path_prefix = get_absolute_tablet_path(&tablet_meta_pb, true);

        // convert visible pdelta file to rowsets and remove old files
        for (auto& visible_rowset : tablet_meta_pb.rs_metas()) {
            RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
            alpha_rowset_meta->init_from_pb(visible_rowset);
            AlphaRowset rowset(&tablet_schema, data_path_prefix, this, alpha_rowset_meta);
            rowset.init();
            rowset.load();
            std::vector<std::string> old_files;
            rowset.remove_old_files(&old_files);
        }

        // convert inc delta file to rowsets and remove old files
        for (auto& inc_rs_meta : tablet_meta_pb.inc_rs_metas()) {
            RowsetMetaSharedPtr alpha_rowset_meta(new AlphaRowsetMeta());
            alpha_rowset_meta->init_from_pb(inc_rs_meta);
            AlphaRowset rowset(&tablet_schema, data_path_prefix, this, alpha_rowset_meta);
            rowset.init();
            rowset.load();  // check if the rowset is successfully converted
            // incremental delta is saved in a seperate incremental folder
            // create a mock rowset to delete its files
            AlphaRowset inc_rowset(&tablet_schema, data_path_prefix + "/incremental_delta", 
                this, alpha_rowset_meta);
            inc_rowset.init();
            std::vector<std::string> old_files;
            inc_rowset.remove_old_files(&old_files);
        }
        return true;
    };
    OLAPStatus clean_old_tablet_status = TabletMetaManager::traverse_headers(_meta,
        clean_old_files_func, HEADER_PREFIX);
    if (clean_old_tablet_status != OLAP_SUCCESS) {
        LOG(WARNING) << "there is failure when loading tablet and clean old files:" << _path;
    } else {
        LOG(INFO) << "load rowset from meta finished, data dir: " << _path;
    }
    return OLAP_SUCCESS;
}

// TODO(ygl): deal with rowsets and tablets when load failed
OLAPStatus DataDir::load() {
    // check if this is an old data path
    bool is_tablet_convert_finished = false;
    OLAPStatus res = _meta->get_tablet_convert_finished(is_tablet_convert_finished);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "get convert flag from meta failed dir=" << _path;
        return res;
    }

    if (!is_tablet_convert_finished) {
        _clean_unfinished_converting_data();
        res = _convert_old_tablet();
        if (res != OLAP_SUCCESS) {
            LOG(FATAL) << "convert old tablet failed for  dir = " << _path;
            return res;
        }
        // TODO(ygl): should load tablet successfully and then set convert flag and clean old files
        res = _meta->set_tablet_convert_finished();
        if (res != OLAP_SUCCESS) {
            LOG(FATAL) << "save convert flag failed after convert old tablet. dir=" << _path;
            return res;
        }
        // convert may be successfully, but crashed before remove old files
        // depend on gc thread to recycle the old files
        // _remove_old_meta_and_files(data_dir);
    } else {
        LOG(INFO) << "tablets have been converted, skip convert process";
    }

    LOG(INFO) << "start to load tablets from " << _path;
    // load rowset meta from meta env and create rowset
    // COMMITTED: add to txn manager
    // VISIBLE: add to tablet
    // if one rowset load failed, then the total data dir will not be loaded
    std::vector<RowsetMetaSharedPtr> dir_rowset_metas;
    LOG(INFO) << "begin loading rowset from meta";
    auto load_rowset_func = [this, &dir_rowset_metas](RowsetId rowset_id, const std::string& meta_str) -> bool {
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
    OLAPStatus load_rowset_status = RowsetMetaManager::traverse_rowset_metas(_meta, load_rowset_func);

    if (load_rowset_status != OLAP_SUCCESS) {
        LOG(WARNING) << "errors when load rowset meta from meta env, skip this data dir:" << _path;
    } else {
        LOG(INFO) << "load rowset from meta finished, data dir: " << _path;
    }

    // load tablet
    // create tablet from tablet meta and add it to tablet mgr
    LOG(INFO) << "begin loading tablet from meta";
    auto load_tablet_func = [this](long tablet_id,
        long schema_hash, const std::string& value) -> bool {
        OLAPStatus status = StorageEngine::instance()->tablet_manager()->load_tablet_from_meta(
                                this, tablet_id, schema_hash, value);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "load tablet from header failed. status:" << status
                << ", tablet=" << tablet_id << "." << schema_hash;
        };
        return true;
    };
    OLAPStatus load_tablet_status = TabletMetaManager::traverse_headers(_meta, load_tablet_func);
    if (load_tablet_status != OLAP_SUCCESS) {
        LOG(WARNING) << "there is failure when loading tablet headers, path:" << _path;
    } else {
        LOG(INFO) << "load rowset from meta finished, data dir: " << _path;
    }

    // tranverse rowset 
    // 1. add committed rowset to txn map
    // 2. add visible rowset to tablet
    // ignore any errors when load tablet or rowset, because fe will repair them after report
    for (auto rowset_meta : dir_rowset_metas) {
        TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(
                                    rowset_meta->tablet_id(), rowset_meta->tablet_schema_hash());
        // tablet maybe dropped, but not drop related rowset meta
        if (tablet.get() == NULL) {
            LOG(WARNING) << "could not find tablet id: " << rowset_meta->tablet_id()
                         << ", schema hash: " << rowset_meta->tablet_schema_hash()
                         << ", for rowset: " << rowset_meta->rowset_id()
                         << ", skip this rowset";
            continue;
        }
        RowsetSharedPtr rowset;
        OLAPStatus create_status = RowsetFactory::load_rowset(tablet->tablet_schema(), 
                                                             tablet->tablet_path(), 
                                                             tablet->data_dir(), 
                                                             rowset_meta, &rowset);
        if (create_status != OLAP_SUCCESS) {
            LOG(WARNING) << "could not create rowset from rowsetmeta: "
                         << " rowset_id: " << rowset_meta->rowset_id()
                         << " rowset_type: " << rowset_meta->rowset_type()
                         << " rowset_state: " << rowset_meta->rowset_state();
            continue;
        }
        if (rowset_meta->rowset_state() == RowsetStatePB::COMMITTED) {
            OLAPStatus commit_txn_status = StorageEngine::instance()->txn_manager()->commit_txn(
                _meta,
                rowset_meta->partition_id(), rowset_meta->txn_id(), 
                rowset_meta->tablet_id(), rowset_meta->tablet_schema_hash(), 
                rowset_meta->load_id(), rowset, true);
            if (commit_txn_status != OLAP_SUCCESS && commit_txn_status != OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST) {
                LOG(WARNING) << "failed to add committed rowset: " << rowset_meta->rowset_id()
                             << " to tablet: " << rowset_meta->tablet_id() 
                             << " for txn: " << rowset_meta->txn_id();
            } else {
                LOG(INFO) << "successfully to add committed rowset: " << rowset_meta->rowset_id()
                             << " to tablet: " << rowset_meta->tablet_id() 
                             << " schema hash: " << rowset_meta->tablet_schema_hash()
                             << " for txn: " << rowset_meta->txn_id();
            }
        } else if (rowset_meta->rowset_state() == RowsetStatePB::VISIBLE) {
            // add visible rowset to tablet, it maybe use in the future
            // there should be only preparing rowset in meta env because visible 
            // rowset is persist with tablet meta currently
            OLAPStatus publish_status = tablet->add_inc_rowset(rowset);
            if (publish_status != OLAP_SUCCESS) {
                LOG(WARNING) << "add visilbe rowset to tablet failed rowset_id:" << rowset->rowset_id()
                             << " tablet id: " << rowset_meta->tablet_id()
                             << " txn id:" << rowset_meta->txn_id()
                             << " start_version: " << rowset_meta->version().first
                             << " end_version: " << rowset_meta->version().second;
            } else {
                LOG(INFO) << "successfully to add visible rowset: " << rowset_meta->rowset_id()
                          << " to tablet: " << rowset_meta->tablet_id()
                          << " txn id:" << rowset_meta->txn_id()
                          << " start_version: " << rowset_meta->version().first
                          << " end_version: " << rowset_meta->version().second;
            }
        } else {
            LOG(WARNING) << "find invalid rowset: " << rowset_meta->rowset_id()
                         << " with tablet id: " << rowset_meta->tablet_id() 
                         << " schema hash: " << rowset_meta->tablet_schema_hash()
                         << " txn: " << rowset_meta->txn_id();
        }
    }
    return OLAP_SUCCESS;
}

} // namespace doris
