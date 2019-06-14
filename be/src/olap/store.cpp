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

#include "olap/store.h"

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
#include "olap/utils.h" // for check_dir_existed
#include "service/backend_options.h"
#include "util/file_utils.h"
#include "util/string_util.h"
#include "olap/olap_header_manager.h"

namespace doris {

static const char* const kMtabPath = "/etc/mtab";
static const char* const kTestFilePath = "/.testfile";

OlapStore::OlapStore(const std::string& path, int64_t capacity_bytes)
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

OlapStore::~OlapStore() {
    free(_test_file_read_buf);
    free(_test_file_write_buf);
    if (_meta != nullptr) {
        delete _meta;
    }
}

Status OlapStore::load() {
    _rand_seed = static_cast<uint32_t>(time(NULL));
    if (posix_memalign((void**)&_test_file_write_buf,
                       DIRECT_IO_ALIGNMENT,
                       TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "fail to allocate memory. size=" <<  TEST_FILE_BUF_SIZE;
        return Status::InternalError("No memory");
    }
    if (posix_memalign((void**)&_test_file_read_buf,
                       DIRECT_IO_ALIGNMENT,
                       TEST_FILE_BUF_SIZE) != 0) {
        LOG(WARNING) << "fail to allocate memory. size=" <<  TEST_FILE_BUF_SIZE;
        return Status::InternalError("No memory");
    }
    RETURN_IF_ERROR(_check_path_exist());
    std::string align_tag_path = _path + ALIGN_TAG_PREFIX;
    if (access(align_tag_path.c_str(), F_OK) == 0) {
        LOG(WARNING) << "align tag was found, path=" << _path;
        return Status::InternalError("invalid root path: ");
    }

    RETURN_IF_ERROR(_init_cluster_id());
    RETURN_IF_ERROR(_init_extension_and_capacity());
    RETURN_IF_ERROR(_init_file_system());
    RETURN_IF_ERROR(_init_meta());

    _is_used = true;
    return Status::OK();
}

Status OlapStore::_check_path_exist() {
    DIR* dirp = opendir(_path.c_str());
    if (dirp == nullptr) {
        char buf[64];
        LOG(WARNING) << "opendir failed, path=" << _path
            << ", errno=" << errno << ", errmsg=" << strerror_r(errno, buf, 64);
        return Status::InternalError("opendir failed");
    }
    struct dirent dirent;
    struct dirent* result = nullptr;
    if (readdir_r(dirp, &dirent, &result) != 0) {
        char buf[64];
        LOG(WARNING) << "readdir failed, path=" << _path
            << ", errno=" << errno << ", errmsg=" << strerror_r(errno, buf, 64);
        closedir(dirp);
        return Status::InternalError("readdir failed");
    }
    closedir(dirp);
    return Status::OK();
}

Status OlapStore::_init_cluster_id() {
    std::string cluster_id_path = _path + CLUSTER_ID_PREFIX;
    if (access(cluster_id_path.c_str(), F_OK) != 0) {
        int fd = open(cluster_id_path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (fd < 0 || close(fd) < 0) {
            char errmsg[64];
            LOG(WARNING) << "fail to create file. [path='" << cluster_id_path
                << "' err='" << strerror_r(errno, errmsg, 64) << "']";
            return Status::InternalError("invalid store path: create cluster id failed");
        }
    }

    // obtain lock of all cluster id paths
    FILE* fp = NULL;
    fp = fopen(cluster_id_path.c_str(), "r+b");
    if (fp == NULL) {
        LOG(WARNING) << "fail to open cluster id path. path=" << cluster_id_path;
        return Status::InternalError("invalid store path: open cluster id failed");
    }

    int lock_res = flock(fp->_fileno, LOCK_EX | LOCK_NB);
    if (lock_res < 0) {
        LOG(WARNING) << "fail to lock file descriptor. path=" << cluster_id_path;
        fclose(fp);
        fp = NULL;
        return Status::InternalError("invalid store path: flock cluster id failed");
    }

    // obtain cluster id of all root paths
    auto st = _read_cluster_id(cluster_id_path, &_cluster_id);
    fclose(fp);
    return st;
}

Status OlapStore::_read_cluster_id(const std::string& path, int32_t* cluster_id) {
    int32_t tmp_cluster_id = -1;

    std::fstream fs(path.c_str(), std::fstream::in);
    if (!fs.is_open()) {
        LOG(WARNING) << "fail to open cluster id path. [path='" << path << "']";
        return Status::InternalError("open file failed");
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
        return Status::InternalError("cluster id file corrupt");
    }
    return Status::OK();
}

Status OlapStore::_init_extension_and_capacity() {
    boost::filesystem::path boost_path = _path;
    std::string extension = boost::filesystem::canonical(boost_path).extension().string();
    if (extension != "") {
        if (boost::iequals(extension, ".ssd")) {
            _storage_medium = TStorageMedium::SSD;
        } else if (boost::iequals(extension, ".hdd")) {
            _storage_medium = TStorageMedium::HDD;
        } else {
            LOG(WARNING) << "store path has wrong extension. path=" << _path;
            return Status::InternalError("invalid sotre path: invalid extension");
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
        return Status::InternalError("invalid store path: invalid capacity");
    }

    std::string data_path = _path + DATA_PREFIX;
    if (!check_dir_existed(data_path) && create_dir(data_path) != OLAP_SUCCESS) {
        LOG(WARNING) << "failed to create data root path. path=" << data_path;
        return Status::InternalError("invalid store path: failed to create data directory");
    }

    return Status::OK();
}

Status OlapStore::_init_file_system() {
    struct stat s;
    if (stat(_path.c_str(), &s) != 0) {
        char errmsg[64];
        LOG(WARNING) << "stat failed, path=" << _path
            << ", errno=" << errno << ", errmsg=" << strerror_r(errno, errmsg, 64);
        return Status::InternalError("invalid store path: stat failed");
    }

    dev_t mount_device;
    if ((s.st_mode & S_IFMT) == S_IFBLK) {
        mount_device = s.st_rdev;
    } else {
        mount_device = s.st_dev;
    }

    FILE* mount_table = nullptr;
    if ((mount_table = setmntent(kMtabPath, "r")) == NULL) {
        char errmsg[64];
        LOG(WARNING) << "setmntent failed, path=" << kMtabPath
            << ", errno=" << errno << ", errmsg=" << strerror_r(errno, errmsg, 64);
        return Status::InternalError("invalid store path: setmntent failed");
    }

    bool is_find = false;
    struct mntent* mount_entry = NULL;
    while ((mount_entry = getmntent(mount_table)) != NULL) {
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

    endmntent(mount_table);

    if (!is_find) {
        LOG(WARNING) << "fail to find file system, path=" << _path;
        return Status::InternalError("invalid store path: find file system failed");
    }

    _file_system = mount_entry->mnt_fsname;

    return Status::OK();
}

Status OlapStore::_init_meta() {
    // init path hash
    _path_hash = hash_of_path(BackendOptions::get_localhost(), _path);
    LOG(INFO) << "get hash of path: " << _path
              << ": " << _path_hash;

    // init meta
    _meta = new(std::nothrow) OlapMeta(_path);
    if (_meta == nullptr) {
        LOG(WARNING) << "new olap meta failed";
        return Status::InternalError("new olap meta failed");
    }
    OLAPStatus res = _meta->init();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "init meta failed";
        return Status::InternalError("init meta failed");
    }
    return Status::OK();
}

Status OlapStore::set_cluster_id(int32_t cluster_id) {
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

Status OlapStore::_write_cluster_id_to_path(const std::string& path, int32_t cluster_id) {
    std::fstream fs(path.c_str(), std::fstream::out);
    if (!fs.is_open()) {
        LOG(WARNING) << "fail to open cluster id path. path=" << path;
        return Status::InternalError("IO Error");
    }
    fs << cluster_id;
    fs.close();
    return Status::OK();
}

void OlapStore::health_check() {
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

OLAPStatus OlapStore::_read_and_write_test_file() {
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

OLAPStatus OlapStore::get_shard(uint64_t* shard) {
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

OlapMeta* OlapStore::get_meta() {
    return _meta;
}

OLAPStatus OlapStore::register_table(OLAPTable* table) {
    std::lock_guard<std::mutex> l(_mutex);

    TabletInfo tablet_info(table->tablet_id(), table->schema_hash());
    _tablet_set.insert(tablet_info);
    return OLAP_SUCCESS;
}

OLAPStatus OlapStore::deregister_table(OLAPTable* table) {
    std::lock_guard<std::mutex> l(_mutex);

    TabletInfo tablet_info(table->tablet_id(), table->schema_hash());
    _tablet_set.erase(tablet_info);
    return OLAP_SUCCESS;
}

std::string OlapStore::get_shard_path_from_header(const std::string& shard_string) {
    return _path + DATA_PREFIX + "/" + shard_string;
}

std::string OlapStore::get_tablet_schema_hash_path_from_header(OLAPHeader* header) {
    return _path + DATA_PREFIX + "/" + std::to_string(header->shard())
            + "/" + std::to_string(header->tablet_id()) + "/" + std::to_string(header->schema_hash());
}

std::string OlapStore::get_tablet_path_from_header(OLAPHeader* header) {
    return _path + DATA_PREFIX + "/" + std::to_string(header->shard())
            + "/" + std::to_string(header->tablet_id());
}

void OlapStore::find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths) {
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

std::string OlapStore::get_root_path_from_schema_hash_path_in_trash(
        const std::string& schema_hash_dir_in_trash) {
    boost::filesystem::path schema_hash_path_in_trash(schema_hash_dir_in_trash);
    return schema_hash_path_in_trash.parent_path().parent_path().parent_path().parent_path().string();
}

void OlapStore::_deal_with_header_error(TTabletId tablet_id, TSchemaHash schema_hash, int shard) {
    // path: store_path/shard/tablet_id/schema_hash
    std::string schema_hash_path = path() + "/" + std::to_string(shard)
            + "/" + std::to_string(tablet_id) + "/" + std::to_string(schema_hash);
    std::string header_path = schema_hash_path + "/" + std::to_string(tablet_id) + ".hdr";
    OLAPStatus res = OlapHeaderManager::dump_header(this, tablet_id, schema_hash, header_path);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "dump header failed. tablet_id:" << tablet_id
                     << "schema_hash:" << schema_hash
                     << "store path:" << path();
    } else {
        LOG(INFO) << "dump header successfully. move path:" << schema_hash_path << " to trash.";
        if (move_to_trash(schema_hash_path, schema_hash_path) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to delete table. [table_path=" << schema_hash_path << "]";
        }
    }
    res = OlapHeaderManager::remove(this, tablet_id, schema_hash);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "remove header failed. tablet_id:" << tablet_id
                     << "schema_hash:" << schema_hash
                     << "store path:" << path();
    } else {
        LOG(INFO) << "remove tablet header successfully. tablet:" << tablet_id << "_" << schema_hash;
    }
}

OLAPStatus OlapStore::_load_table_from_header(OLAPEngine* engine, TTabletId tablet_id,
        TSchemaHash schema_hash, const std::string& header) {
    std::unique_ptr<OLAPHeader> olap_header(new OLAPHeader());
    OLAPStatus res = OLAP_SUCCESS;
    bool parsed = olap_header->ParseFromString(header);
    if (!parsed) {
        // here we can not get shard id
        // so just remove invalid header from meta
        // the related tablet path should be removed by gc 
        LOG(WARNING) << "parse header string failed for tablet_id:" << tablet_id
                     << " schema_hash:" << schema_hash;
        res = OlapHeaderManager::remove(this, tablet_id, schema_hash);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "remove header failed. tablet_id:" << tablet_id
                << " schema_hash:" << schema_hash
                << " store path:" << path();
        } else {
            LOG(INFO) << "remove tablet header successfully. tablet:" << tablet_id << "_" << schema_hash;
        }
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }
    if (olap_header->file_version_size() != 0) {
        olap_header->change_file_version_to_delta();
        res = OlapHeaderManager::save(this, tablet_id, schema_hash, olap_header.get());
        if (res != OLAP_SUCCESS) {
            LOG(FATAL) << "fail to save header, tablet_id:" << tablet_id
                << ", schema_hash:" << schema_hash << " to path:" << path();
            return OLAP_ERR_HEADER_PUT;
        }
    }
    
    // init must be called
    res = olap_header->init();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init header, tablet_id:" << tablet_id
                     << ", schema_hash:" << schema_hash;
        _deal_with_header_error(tablet_id, schema_hash, olap_header->shard());
        return OLAP_ERR_HEADER_INIT_FAILED;
    }
    OLAPTablePtr olap_table =
        OLAPTable::create_from_header(olap_header.release(), this);
    if (olap_table == nullptr) {
        LOG(WARNING) << "fail to new table. tablet_id=" << tablet_id << ", schema_hash:" << schema_hash;
        _deal_with_header_error(tablet_id, schema_hash, olap_header->shard());
        return OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR;
    }

    if (olap_table->lastest_version() == nullptr && !olap_table->is_schema_changing()) {
        LOG(WARNING) << "tablet not in schema change state without delta is invalid."
                     << "tablet=" << olap_table->full_name();
        // tablet state is invalid, drop tablet
        olap_table->mark_dropped();
        return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
    }

    res = engine->add_table(tablet_id, schema_hash, olap_table);
    if (res != OLAP_SUCCESS) {
        // insert existed tablet return OLAP_SUCCESS
        if (res == OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE) {
            LOG(WARNING) << "add duplicate table. table=" << olap_table->full_name();
        }

        LOG(WARNING) << "failed to add table. table=" << olap_table->full_name();
        return res;
    }
    res = engine->register_table_into_root_path(olap_table.get());
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to register table into root path. root_path=" << olap_table->storage_root_path_name();

        if (engine->drop_table(tablet_id, schema_hash) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to drop table when create table failed. "
                <<"tablet=" << tablet_id << " schema_hash=" << schema_hash;
        }

        return res;
    }
    // load pending data (for realtime push), will add transaction relationship into engine
    olap_table->load_pending_data();

    return OLAP_SUCCESS;
}

OLAPStatus OlapStore::load_tables(OLAPEngine* engine) {
    auto load_table_func = [this, engine](long tablet_id,
            long schema_hash, const std::string& value) -> bool {
        OLAPStatus status = _load_table_from_header(engine, tablet_id, schema_hash, value);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "load table from header failed. status:" << status
                << "tablet=" << tablet_id << "." << schema_hash;
        };
        return true;
    };
    OLAPStatus status = OlapHeaderManager::traverse_headers(_meta, load_table_func);
    return status;
}

OLAPStatus OlapStore::check_none_row_oriented_table_in_store(OLAPEngine* engine) {
    auto load_table_func = [this, engine](long tablet_id,
            long schema_hash, const std::string& value) -> bool {
        OLAPStatus status = _check_none_row_oriented_table_in_store(engine, tablet_id, schema_hash, value);
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "load table from header failed. status:" << status
                << "tablet=" << tablet_id << "." << schema_hash;
        };
        return true;
    };
    OLAPStatus status = OlapHeaderManager::traverse_headers(_meta, load_table_func);
    return status;
}

OLAPStatus OlapStore::_check_none_row_oriented_table_in_store(
                        OLAPEngine* engine, TTabletId tablet_id,
                        TSchemaHash schema_hash, const std::string& header) {
    std::unique_ptr<OLAPHeader> olap_header(new OLAPHeader());
    bool parsed = olap_header->ParseFromString(header);
    if (!parsed) {
        LOG(WARNING) << "parse header string failed for tablet_id:" << tablet_id << " schema_hash:" << schema_hash;
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }
    // init must be called
    RETURN_NOT_OK(olap_header->init());
    if (olap_header->data_file_type() == OLAP_DATA_FILE) {
        LOG(FATAL) << "Not support row-oriented table any more. Please convert it to column-oriented table."
                   << "tablet=" << tablet_id << "." << schema_hash;
    }

    return OLAP_SUCCESS;
}

} // namespace doris
