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

#include "runtime/snapshot_loader.h"

#include <stdint.h>

#include "common/logging.h"
#include "env/env.h"
#include "exec/broker_reader.h"
#include "exec/broker_writer.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/FrontendService_types.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "olap/file_helper.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "runtime/broker_mgr.h"
#include "runtime/exec_env.h"
#include "util/broker_storage_backend.h"
#include "util/file_utils.h"
#include "util/s3_storage_backend.h"
#include "util/thrift_rpc_helper.h"

namespace doris {

SnapshotLoader::SnapshotLoader(ExecEnv* env, int64_t job_id, int64_t task_id,
                               const TNetworkAddress& broker_addr,
                               const std::map<std::string, std::string>& broker_prop)
        : _env(env),
          _job_id(job_id),
          _task_id(task_id),
          _broker_addr(broker_addr),
          _prop(broker_prop) {
    _storage_backend.reset(new BrokerStorageBackend(_env, broker_addr, broker_prop));
}

SnapshotLoader::SnapshotLoader(ExecEnv* env, int64_t job_id, int64_t task_id)
        : _env(env),
          _job_id(job_id),
          _task_id(task_id),
          _broker_addr(TNetworkAddress()),
          _prop(std::map<std::string, std::string>()),
          _storage_backend(nullptr) {}

SnapshotLoader::SnapshotLoader(ExecEnv* env, int64_t job_id, int64_t task_id, const std::map<std::string, std::string>& prop)
        : _env(env),
          _job_id(job_id),
          _task_id(task_id),
          _broker_addr(TNetworkAddress()),
          _prop(prop) {
              _storage_backend.reset(new S3StorageBackend(prop));
          }

SnapshotLoader::~SnapshotLoader() {}

Status SnapshotLoader::upload(const std::map<std::string, std::string>& src_to_dest_path,
                              std::map<int64_t, std::vector<std::string>>* tablet_files) {
    if (!_storage_backend) {
        return Status::InternalError("Storage backend not initialized.");
    }
    LOG(INFO) << "begin to upload snapshot files. num: " << src_to_dest_path.size()
              << ", broker addr: " << _broker_addr << ", job: " << _job_id << ", task" << _task_id;

    // check if job has already been cancelled
    int tmp_counter = 1;
    RETURN_IF_ERROR(_report_every(0, &tmp_counter, 0, 0, TTaskType::type::UPLOAD));

    Status status = Status::OK();
    // 1. validate local tablet snapshot paths
    RETURN_IF_ERROR(_check_local_snapshot_paths(src_to_dest_path, true));

    // 2. for each src path, upload it to remote storage
    // we report to frontend for every 10 files, and we will cancel the job if
    // the job has already been cancelled in frontend.
    int report_counter = 0;
    int total_num = src_to_dest_path.size();
    int finished_num = 0;
    for (auto iter = src_to_dest_path.begin(); iter != src_to_dest_path.end(); iter++) {
        const std::string& src_path = iter->first;
        const std::string& dest_path = iter->second;

        int64_t tablet_id = 0;
        int32_t schema_hash = 0;
        RETURN_IF_ERROR(
                _get_tablet_id_and_schema_hash_from_file_path(src_path, &tablet_id, &schema_hash));

        // 2.1 get existing files from remote path
        std::map<std::string, FileStat> remote_files;
        RETURN_IF_ERROR(_storage_backend->list(dest_path, &remote_files));

        for (auto& tmp : remote_files) {
            VLOG_CRITICAL << "get remote file: " << tmp.first << ", checksum: " << tmp.second.md5;
        }

        // 2.2 list local files
        std::vector<std::string> local_files;
        std::vector<std::string> local_files_with_checksum;
        RETURN_IF_ERROR(_get_existing_files_from_local(src_path, &local_files));

        // 2.3 iterate local files
        for (auto it = local_files.begin(); it != local_files.end(); it++) {
            RETURN_IF_ERROR(_report_every(10, &report_counter, finished_num, total_num,
                                          TTaskType::type::UPLOAD));

            const std::string& local_file = *it;
            // calc md5sum of localfile
            std::string md5sum;
            status = FileUtils::md5sum(src_path + "/" + local_file, &md5sum);
            if (!status.ok()) {
                std::stringstream ss;
                ss << "failed to get md5sum of file: " << local_file << ": "
                   << status.get_error_msg();
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
            VLOG_CRITICAL << "get file checksum: " << local_file << ": " << md5sum;
            local_files_with_checksum.push_back(local_file + "." + md5sum);

            // check if this local file need upload
            bool need_upload = false;
            auto find = remote_files.find(local_file);
            if (find != remote_files.end()) {
                if (md5sum != find->second.md5) {
                    // remote storage file exist, but with different checksum
                    LOG(WARNING) << "remote file checksum is invalid. remote: " << find->first
                                 << ", local: " << md5sum;
                    // TODO(cmy): save these files and delete them later
                    need_upload = true;
                }
            } else {
                need_upload = true;
            }

            if (!need_upload) {
                VLOG_CRITICAL << "file exist in remote path, no need to upload: " << local_file;
                continue;
            }

            // upload
            std::string full_remote_file = dest_path + "/" + local_file;
            std::string full_local_file = src_path + "/" + local_file;
            RETURN_IF_ERROR(_storage_backend->upload_with_checksum(full_local_file, full_remote_file, md5sum));
        } // end for each tablet's local files

        tablet_files->emplace(tablet_id, local_files_with_checksum);
        finished_num++;
        LOG(INFO) << "finished to write tablet to remote. local path: " << src_path
                  << ", remote path: " << dest_path;
    } // end for each tablet path

    LOG(INFO) << "finished to upload snapshots. job: " << _job_id << ", task id: " << _task_id;
    return status;
}

/*
 * Download snapshot files from remote.
 * After downloaded, the local dir should contains all files existing in remote,
 * may also contains severval useless files.
 */
Status SnapshotLoader::download(const std::map<std::string, std::string>& src_to_dest_path,
                                std::vector<int64_t>* downloaded_tablet_ids) {
    if (!_storage_backend) {
        return Status::InternalError("Storage backend not initialized.");
    }
    LOG(INFO) << "begin to download snapshot files. num: " << src_to_dest_path.size()
              << ", broker addr: " << _broker_addr << ", job: " << _job_id
              << ", task id: " << _task_id;

    // check if job has already been cancelled
    int tmp_counter = 1;
    RETURN_IF_ERROR(_report_every(0, &tmp_counter, 0, 0, TTaskType::type::DOWNLOAD));

    Status status = Status::OK();
    // 1. validate local tablet snapshot paths
    RETURN_IF_ERROR(_check_local_snapshot_paths(src_to_dest_path, false));

    // 2. for each src path, download it to local storage
    int report_counter = 0;
    int total_num = src_to_dest_path.size();
    int finished_num = 0;
    for (auto iter = src_to_dest_path.begin(); iter != src_to_dest_path.end(); iter++) {
        const std::string& remote_path = iter->first;
        const std::string& local_path = iter->second;

        int64_t local_tablet_id = 0;
        int32_t schema_hash = 0;
        RETURN_IF_ERROR(_get_tablet_id_and_schema_hash_from_file_path(local_path, &local_tablet_id,
                                                                      &schema_hash));
        downloaded_tablet_ids->push_back(local_tablet_id);

        int64_t remote_tablet_id;
        RETURN_IF_ERROR(_get_tablet_id_from_remote_path(remote_path, &remote_tablet_id));
        VLOG_CRITICAL << "get local tablet id: " << local_tablet_id << ", schema hash: " << schema_hash
                << ", remote tablet id: " << remote_tablet_id;

        // 2.1. get local files
        std::vector<std::string> local_files;
        RETURN_IF_ERROR(_get_existing_files_from_local(local_path, &local_files));

        // 2.2. get remote files
        std::map<std::string, FileStat> remote_files;
        RETURN_IF_ERROR(_storage_backend->list(remote_path, &remote_files));
        if (remote_files.empty()) {
            std::stringstream ss;
            ss << "get nothing from remote path: " << remote_path;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        TabletSharedPtr tablet =
                _env->storage_engine()->tablet_manager()->get_tablet(local_tablet_id, schema_hash);
        if (tablet == nullptr) {
            std::stringstream ss;
            ss << "failed to get local tablet: " << local_tablet_id;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        DataDir* data_dir = tablet->data_dir();

        for (auto& iter : remote_files) {
            RETURN_IF_ERROR(_report_every(10, &report_counter, finished_num, total_num,
                                          TTaskType::type::DOWNLOAD));

            bool need_download = false;
            const std::string& remote_file = iter.first;
            const FileStat& file_stat = iter.second;
            auto find = std::find(local_files.begin(), local_files.end(), remote_file);
            if (find == local_files.end()) {
                // remote file does not exist in local, download it
                need_download = true;
            } else {
                if (_end_with(remote_file, ".hdr")) {
                    // this is a header file, download it.
                    need_download = true;
                } else {
                    // check checksum
                    std::string local_md5sum;
                    Status st = FileUtils::md5sum(local_path + "/" + remote_file, &local_md5sum);
                    if (!st.ok()) {
                        LOG(WARNING) << "failed to get md5sum of local file: " << remote_file
                                     << ". msg: " << st.get_error_msg() << ". download it";
                        need_download = true;
                    } else {
                        VLOG_CRITICAL << "get local file checksum: " << remote_file << ": "
                                << local_md5sum;
                        if (file_stat.md5 != local_md5sum) {
                            // file's checksum does not equal, download it.
                            need_download = true;
                        }
                    }
                }
            }

            if (!need_download) {
                LOG(INFO) << "remote file already exist in local, no need to download."
                          << ", file: " << remote_file;
                continue;
            }

            // begin to download
            std::string full_remote_file = remote_path + "/" + remote_file + "." + file_stat.md5;
            std::string local_file_name;
            // we need to replace the tablet_id in remote file name with local tablet id
            RETURN_IF_ERROR(_replace_tablet_id(remote_file, local_tablet_id, &local_file_name));
            std::string full_local_file = local_path + "/" + local_file_name;
            LOG(INFO) << "begin to download from " << full_remote_file << " to " << full_local_file;
            size_t file_len = file_stat.size;

            // check disk capacity
            if (data_dir->reach_capacity_limit(file_len)) {
                return Status::InternalError("capacity limit reached");
            }
            // remove file which will be downloaded now.
            // this file will be added to local_files if it be downloaded successfully.
            local_files.erase(find);
            RETURN_IF_ERROR(_storage_backend->download(full_remote_file, full_local_file));

            // 3. check md5 of the downloaded file
            std::string downloaded_md5sum;
            status = FileUtils::md5sum(full_local_file, &downloaded_md5sum);
            if (!status.ok()) {
                std::stringstream ss;
                ss << "failed to get md5sum of file: " << full_local_file << ", err: " << status.get_error_msg();
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
            VLOG_CRITICAL << "get downloaded file checksum: " << full_local_file << ": "
                    << downloaded_md5sum;
            if (downloaded_md5sum != file_stat.md5) {
                std::stringstream ss;
                ss << "invalid md5 of downloaded file: " << full_local_file
                   << ", expected: " << file_stat.md5 << ", get: " << downloaded_md5sum;
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }

            // local_files always keep the updated local files
            local_files.push_back(local_file_name);
            LOG(INFO) << "finished to download file via broker. file: " << full_local_file
                      << ", length: " << file_len;
        } // end for all remote files

        // finally, delete local files which are not in remote
        for (const auto& local_file : local_files) {
            // replace the tablet id in local file name with the remote tablet id,
            // in order to compare the file name.
            std::string new_name;
            Status st = _replace_tablet_id(local_file, remote_tablet_id, &new_name);
            if (!st.ok()) {
                LOG(WARNING) << "failed to replace tablet id. unknown local file: "
                             << st.get_error_msg() << ". ignore it";
                continue;
            }
            VLOG_CRITICAL << "new file name after replace tablet id: " << new_name;
            const auto& find = remote_files.find(new_name);
            if (find != remote_files.end()) {
                continue;
            }

            // delete
            std::string full_local_file = local_path + "/" + local_file;
            VLOG_CRITICAL << "begin to delete local snapshot file: " << full_local_file
                    << ", it does not exist in remote";
            if (remove(full_local_file.c_str()) != 0) {
                LOG(WARNING) << "failed to delete unknown local file: " << full_local_file
                             << ", ignore it";
            }
        }

        finished_num++;
    } // end for src_to_dest_path

    LOG(INFO) << "finished to download snapshots. job: " << _job_id << ", task id: " << _task_id;
    return status;
}

// move the snapshot files in snapshot_path
// to tablet_path
// If overwrite, just replace the tablet_path with snapshot_path,
// else: (TODO)
//
// MUST hold tablet's header lock, push lock, cumulative lock and base compaction lock
Status SnapshotLoader::move(const std::string& snapshot_path, TabletSharedPtr tablet,
                            bool overwrite) {
    std::string tablet_path = tablet->tablet_path();
    std::string store_path = tablet->data_dir()->path();
    LOG(INFO) << "begin to move snapshot files. from: " << snapshot_path << ", to: " << tablet_path
              << ", store: " << store_path << ", job: " << _job_id << ", task id: " << _task_id;

    Status status = Status::OK();

    // validate snapshot_path and tablet_path
    int64_t snapshot_tablet_id = 0;
    int32_t snapshot_schema_hash = 0;
    RETURN_IF_ERROR(_get_tablet_id_and_schema_hash_from_file_path(
            snapshot_path, &snapshot_tablet_id, &snapshot_schema_hash));

    int64_t tablet_id = 0;
    int32_t schema_hash = 0;
    RETURN_IF_ERROR(
            _get_tablet_id_and_schema_hash_from_file_path(tablet_path, &tablet_id, &schema_hash));

    if (tablet_id != snapshot_tablet_id || schema_hash != snapshot_schema_hash) {
        std::stringstream ss;
        ss << "path does not match. snapshot: " << snapshot_path
           << ", tablet path: " << tablet_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    DataDir* store = StorageEngine::instance()->get_store(store_path);
    if (store == nullptr) {
        std::stringstream ss;
        ss << "failed to get store by path: " << store_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    boost::filesystem::path tablet_dir(tablet_path);
    boost::filesystem::path snapshot_dir(snapshot_path);
    if (!boost::filesystem::exists(tablet_dir)) {
        std::stringstream ss;
        ss << "tablet path does not exist: " << tablet_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    if (!boost::filesystem::exists(snapshot_dir)) {
        std::stringstream ss;
        ss << "snapshot path does not exist: " << snapshot_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    // rename the rowset ids and tabletid info in rowset meta
    OLAPStatus convert_status =
            SnapshotManager::instance()->convert_rowset_ids(snapshot_path, tablet_id, schema_hash);
    if (convert_status != OLAP_SUCCESS) {
        std::stringstream ss;
        ss << "failed to convert rowsetids in snapshot: " << snapshot_path
           << ", tablet path: " << tablet_path;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    if (overwrite) {
        std::vector<std::string> snapshot_files;
        RETURN_IF_ERROR(_get_existing_files_from_local(snapshot_path, &snapshot_files));

        // 1. simply delete the old dir and replace it with the snapshot dir
        try {
            // This remove seems soft enough, because we already get
            // tablet id and schema hash from this path, which
            // means this path is a valid path.
            boost::filesystem::remove_all(tablet_dir);
            VLOG_CRITICAL << "remove dir: " << tablet_dir;
            boost::filesystem::create_directory(tablet_dir);
            VLOG_CRITICAL << "re-create dir: " << tablet_dir;
        } catch (const boost::filesystem::filesystem_error& e) {
            std::stringstream ss;
            ss << "failed to move tablet path: " << tablet_path << ". err: " << e.what();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // link files one by one
        // files in snapshot dir will be moved in snapshot clean process
        std::vector<std::string> linked_files;
        for (auto& file : snapshot_files) {
            std::string full_src_path = snapshot_path + "/" + file;
            std::string full_dest_path = tablet_path + "/" + file;
            if (link(full_src_path.c_str(), full_dest_path.c_str()) != 0) {
                LOG(WARNING) << "failed to link file from " << full_src_path << " to "
                             << full_dest_path << ", err: " << std::strerror(errno);

                // clean the already linked files
                for (auto& linked_file : linked_files) {
                    remove(linked_file.c_str());
                }

                return Status::InternalError("move tablet failed");
            }
            linked_files.push_back(full_dest_path);
            VLOG_CRITICAL << "link file from " << full_src_path << " to " << full_dest_path;
        }

    } else {
        LOG(FATAL) << "only support overwrite now";
    }

    // snapshot loader not need to change tablet uid
    // fixme: there is no header now and can not call load_one_tablet here
    // reload header
    OLAPStatus ost = StorageEngine::instance()->tablet_manager()->load_tablet_from_dir(
            store, tablet_id, schema_hash, tablet_path, true);
    if (ost != OLAP_SUCCESS) {
        std::stringstream ss;
        ss << "failed to reload header of tablet: " << tablet_id;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    LOG(INFO) << "finished to reload header of tablet: " << tablet_id;

    return status;
}

bool SnapshotLoader::_end_with(const std::string& str, const std::string& match) {
    if (str.size() >= match.size() &&
        str.compare(str.size() - match.size(), match.size(), match) == 0) {
        return true;
    }
    return false;
}

Status SnapshotLoader::_get_tablet_id_and_schema_hash_from_file_path(const std::string& src_path,
                                                                     int64_t* tablet_id,
                                                                     int32_t* schema_hash) {
    // path should be like: /path/.../tablet_id/schema_hash
    // we try to extract tablet_id from path
    size_t pos = src_path.find_last_of("/");
    if (pos == std::string::npos || pos == src_path.length() - 1) {
        return Status::InternalError("failed to get tablet id from path: " + src_path);
    }

    std::string schema_hash_str = src_path.substr(pos + 1);
    std::stringstream ss1;
    ss1 << schema_hash_str;
    ss1 >> *schema_hash;

    // skip schema hash part
    size_t pos2 = src_path.find_last_of("/", pos - 1);
    if (pos2 == std::string::npos) {
        return Status::InternalError("failed to get tablet id from path: " + src_path);
    }

    std::string tablet_str = src_path.substr(pos2 + 1, pos - pos2);
    std::stringstream ss2;
    ss2 << tablet_str;
    ss2 >> *tablet_id;

    VLOG_CRITICAL << "get tablet id " << *tablet_id << ", schema hash: " << *schema_hash
            << " from path: " << src_path;
    return Status::OK();
}

Status SnapshotLoader::_check_local_snapshot_paths(
        const std::map<std::string, std::string>& src_to_dest_path, bool check_src) {
    for (const auto& pair : src_to_dest_path) {
        std::string path;
        if (check_src) {
            path = pair.first;
        } else {
            path = pair.second;
        }
        if (!FileUtils::is_dir(path)) {
            std::stringstream ss;
            ss << "snapshot path is not directory or does not exist: " << path;
            LOG(WARNING) << ss.str();
            return Status::RuntimeError(ss.str());
        }
    }
    LOG(INFO) << "all local snapshot paths are existing. num: " << src_to_dest_path.size();
    return Status::OK();
}

Status SnapshotLoader::_get_existing_files_from_local(const std::string& local_path,
                                                      std::vector<std::string>* local_files) {
    Status status = FileUtils::list_files(Env::Default(), local_path, local_files);
    if (!status.ok()) {
        std::stringstream ss;
        ss << "failed to list files in local path: " << local_path
           << ", msg: " << status.get_error_msg();
        LOG(WARNING) << ss.str();
        return status;
    }
    LOG(INFO) << "finished to list files in local path: " << local_path
              << ", file num: " << local_files->size();
    return Status::OK();
}

void SnapshotLoader::_assemble_file_name(const std::string& snapshot_path,
                                         const std::string& tablet_path, int64_t tablet_id,
                                         int64_t start_version, int64_t end_version,
                                         int64_t vesion_hash, int32_t seg_num,
                                         const std::string suffix, std::string* snapshot_file,
                                         std::string* tablet_file) {
    std::stringstream ss1;
    ss1 << snapshot_path << "/" << tablet_id << "_" << start_version << "_" << end_version << "_"
        << vesion_hash << "_" << seg_num << suffix;
    *snapshot_file = ss1.str();

    std::stringstream ss2;
    ss2 << tablet_path << "/" << tablet_id << "_" << start_version << "_" << end_version << "_"
        << vesion_hash << "_" << seg_num << suffix;
    *tablet_file = ss2.str();

    VLOG_CRITICAL << "assemble file name: " << *snapshot_file << ", " << *tablet_file;
}

Status SnapshotLoader::_replace_tablet_id(const std::string& file_name, int64_t tablet_id,
                                          std::string* new_file_name) {
    // eg:
    // 10007.hdr
    // 10007_2_2_0_0.idx
    // 10007_2_2_0_0.dat
    if (_end_with(file_name, ".hdr")) {
        std::stringstream ss;
        ss << tablet_id << ".hdr";
        *new_file_name = ss.str();
        return Status::OK();
    } else if (_end_with(file_name, ".idx") || _end_with(file_name, ".dat")) {
        *new_file_name = file_name;
        return Status::OK();
    } else {
        return Status::InternalError("invalid tablet file name: " + file_name);
    }
}

Status SnapshotLoader::_get_tablet_id_from_remote_path(const std::string& remote_path,
                                                       int64_t* tablet_id) {
    // eg:
    // bos://xxx/../__tbl_10004/__part_10003/__idx_10004/__10005
    size_t pos = remote_path.find_last_of("_");
    if (pos == std::string::npos) {
        return Status::InternalError("invalid remove file path: " + remote_path);
    }

    std::string tablet_id_str = remote_path.substr(pos + 1);
    std::stringstream ss;
    ss << tablet_id_str;
    ss >> *tablet_id;

    return Status::OK();
}

// only return CANCELLED if FE return that job is cancelled.
// otherwise, return OK
Status SnapshotLoader::_report_every(int report_threshold, int* counter, int32_t finished_num,
                                     int32_t total_num, TTaskType::type type) {
    ++*counter;
    if (*counter <= report_threshold) {
        return Status::OK();
    }

    LOG(INFO) << "report to frontend. job id: " << _job_id << ", task id: " << _task_id
              << ", finished num: " << finished_num << ", total num:" << total_num;

    TNetworkAddress master_addr = _env->master_info()->network_address;

    TSnapshotLoaderReportRequest request;
    request.job_id = _job_id;
    request.task_id = _task_id;
    request.task_type = type;
    request.__set_finished_num(finished_num);
    request.__set_total_num(total_num);
    TStatus report_st;

    Status rpcStatus = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &report_st](FrontendServiceConnection& client) {
                client->snapshotLoaderReport(report_st, request);
            },
            10000);

    if (!rpcStatus.ok()) {
        // rpc failed, ignore
        return Status::OK();
    }

    // reset
    *counter = 0;
    if (report_st.status_code == TStatusCode::CANCELLED) {
        LOG(INFO) << "job is cancelled. job id: " << _job_id << ", task id: " << _task_id;
        return Status::Cancelled("Cancelled");
    }
    return Status::OK();
}

} // end namespace doris
