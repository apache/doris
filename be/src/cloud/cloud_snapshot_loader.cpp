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

#include "cloud/cloud_snapshot_loader.h"

#include <gen_cpp/Types_types.h>

#include <unordered_map>

#include "cloud/cloud_snapshot_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "common/logging.h"
#include "io/fs/broker_file_system.h"
#include "io/fs/file_system.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "io/fs/s3_file_system.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/tablet.h"
#include "util/slice.h"

namespace doris {
#include "common/compile_check_avoid_begin.h"
namespace {
bool _end_with(std::string_view str, std::string_view match) {
    return str.size() >= match.size() &&
           str.compare(str.size() - match.size(), match.size(), match) == 0;
}
} // namespace

CloudSnapshotLoader::CloudSnapshotLoader(CloudStorageEngine& engine, ExecEnv* env, int64_t job_id,
                                         int64_t task_id, const TNetworkAddress& broker_addr,
                                         const std::map<std::string, std::string>& broker_prop)
        : BaseSnapshotLoader(env, job_id, task_id, broker_addr, broker_prop), _engine(engine) {};

Status CloudSnapshotLoader::init(TStorageBackendType::type type, const std::string& location,
                                 std::string vault_id) {
    RETURN_IF_ERROR(BaseSnapshotLoader::init(type, location));
    _storage_resource = _engine.get_storage_resource(vault_id);
    if (!_storage_resource) {
        return Status::InternalError("vault id not found, vault id {}", vault_id);
    }
    return Status::OK();
}

io::RemoteFileSystemSPtr CloudSnapshotLoader::storage_fs() {
    return _storage_resource->fs;
}

Status CloudSnapshotLoader::upload(const std::map<std::string, std::string>& src_to_dest_path,
                                   std::map<int64_t, std::vector<std::string>>* tablet_files) {
    return Status::NotSupported("upload not supported");
}

Status CloudSnapshotLoader::download(const std::map<std::string, std::string>& src_to_dest_path,
                                     std::vector<int64_t>* downloaded_tablet_ids) {
    if (!_remote_fs || !_storage_resource) {
        return Status::InternalError("Storage backend not initialized.");
    }

    LOG(INFO) << "begin to transfer snapshot files. num: " << src_to_dest_path.size()
              << ", broker addr: " << _broker_addr << ", job: " << _job_id
              << ", task id: " << _task_id;

    // check if job has already been cancelled
    int tmp_counter = 1;
    RETURN_IF_ERROR(_report_every(0, &tmp_counter, 0, 0, TTaskType::type::DOWNLOAD));

    Status status = Status::OK();

    // 1. for each src path, transfer files to target path
    int report_counter = 0;
    int total_num = src_to_dest_path.size();
    int finished_num = 0;
    for (const auto& iter : src_to_dest_path) {
        // remote_path eg:
        // cos://xxx/__palo_repository_xxx/_ss_xxx/_ss_content/__db_10000/
        // __tbl_10001/__part_10002/_idx_10001/__10003
        const std::string& remote_path = iter.first;
        const std::string& tablet_str = iter.second;
        int64_t target_tablet_id = -1;
        try {
            target_tablet_id = std::stoll(tablet_str);
        } catch (std::exception& e) {
            return Status::InternalError("failed to parse target tablet id {}, {}", tablet_str,
                                         e.what());
        }
        const std::string target_path = _storage_resource->remote_tablet_path(target_tablet_id);

        // 1.1. check target path not exists
        bool target_path_exist = false;
        if (!storage_fs()->exists(target_path, &target_path_exist).ok() || target_path_exist) {
            std::stringstream ss;
            ss << "failed to download snapshot files, target path already exists: " << target_path;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        downloaded_tablet_ids->push_back(target_tablet_id);

        int64_t remote_tablet_id;
        RETURN_IF_ERROR(_get_tablet_id_from_remote_path(remote_path, &remote_tablet_id));
        VLOG_CRITICAL << "get target tablet id: " << target_tablet_id
                      << ", remote tablet id: " << remote_tablet_id;

        // 1.2. get remote files
        std::map<std::string, FileStat> remote_files;
        RETURN_IF_ERROR(_list_with_checksum(remote_path, &remote_files));
        if (remote_files.empty()) {
            std::stringstream ss;
            ss << "get nothing from remote path: " << remote_path;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        auto remote_hdr_file_path = [&remote_files, &remote_path](std::string& full_hdr_path,
                                                                  size_t* hdr_file_len) {
            for (auto iter = remote_files.begin(); iter != remote_files.end();) {
                if (_end_with(iter->first, ".hdr")) {
                    *hdr_file_len = iter->second.size;
                    full_hdr_path = remote_path + "/" + iter->first + "." + iter->second.md5;
                    // remove hdr file from remote_files
                    iter = remote_files.erase(iter);
                    return true;
                } else {
                    ++iter;
                }
            }
            return false;
        };

        size_t hdr_file_len;
        std::string full_remote_hdr_path;
        if (!remote_hdr_file_path(full_remote_hdr_path, &hdr_file_len)) {
            std::stringstream ss;
            ss << "failed to find hdr file from remote_path: " << remote_path;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        // 1.3. download hdr file
        io::FileReaderOptions reader_options {
                .cache_type = io::FileCachePolicy::NO_CACHE,
                .is_doris_table = false,
                .cache_base_path = "",
                .file_size = static_cast<int64_t>(hdr_file_len),
        };
        LOG(INFO) << "download hdr file: " << full_remote_hdr_path;
        io::FileReaderSPtr hdr_reader = nullptr;
        RETURN_IF_ERROR(_remote_fs->open_file(full_remote_hdr_path, &hdr_reader, &reader_options));
        std::unique_ptr<char[]> read_buf = std::make_unique_for_overwrite<char[]>(hdr_file_len);
        size_t read_len = 0;
        Slice hdr_slice(read_buf.get(), hdr_file_len);
        RETURN_IF_ERROR(hdr_reader->read_at(0, hdr_slice, &read_len));
        if (read_len != hdr_file_len) {
            std::stringstream ss;
            ss << "failed to read hdr file: " << full_remote_hdr_path;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        RETURN_IF_ERROR(
                _report_every(0, &tmp_counter, finished_num, total_num, TTaskType::type::DOWNLOAD));

        // 1.4. make snapshot
        std::unordered_map<std::string, std::string> file_mapping;
        RETURN_IF_ERROR(_engine.cloud_snapshot_mgr().make_snapshot(
                target_tablet_id, *_storage_resource, file_mapping, true, &hdr_slice));

        LOG(INFO) << "finish to make snapshot for tablet: " << target_tablet_id;

        // 1.5. download files
        for (auto& iter : remote_files) {
            RETURN_IF_ERROR(_report_every(10, &report_counter, finished_num, total_num,
                                          TTaskType::type::DOWNLOAD));
            const std::string& remote_file = iter.first;
            const FileStat& file_stat = iter.second;
            auto find = file_mapping.find(remote_file);
            if (find == file_mapping.end()) {
                continue;
            }
            std::string target_file = find->second;
            std::string full_remote_file = remote_path + "/" + remote_file + "." + file_stat.md5;
            std::string full_target_file = target_path + "/" + target_file;
            LOG(INFO) << "begin to download from " << full_remote_file << " to "
                      << full_target_file;
            io::FileReaderOptions reader_options {
                    .cache_type = io::FileCachePolicy::NO_CACHE,
                    .is_doris_table = false,
                    .cache_base_path = "",
                    .file_size = static_cast<int64_t>(file_stat.size),
            };
            io::FileReaderSPtr file_reader = nullptr;
            RETURN_IF_ERROR(_remote_fs->open_file(full_remote_file, &file_reader, &reader_options));
            io::FileWriterPtr file_writer = nullptr;
            RETURN_IF_ERROR(storage_fs()->create_file(full_target_file, &file_writer));
            size_t buf_size = config::s3_file_system_local_upload_buffer_size;
            std::unique_ptr<char[]> transfer_buffer =
                    std::make_unique_for_overwrite<char[]>(buf_size);
            size_t cur_offset = 0;
            // (TODO) Add Verification that the length of the source file is consistent
            // with the length of the target file
            while (true) {
                size_t read_len = 0;
                RETURN_IF_ERROR(file_reader->read_at(
                        cur_offset, Slice {transfer_buffer.get(), buf_size}, &read_len));
                cur_offset += read_len;
                if (read_len == 0) {
                    break;
                }
                RETURN_IF_ERROR(file_writer->append({transfer_buffer.get(), read_len}));
            }
            RETURN_IF_ERROR(file_writer->close());
        }

        finished_num++;

        // (TODO) Add bvar metrics to track download time
    } // end for src_to_dest_path

    LOG(INFO) << "finished to download snapshots. job: " << _job_id << ", task id: " << _task_id;
    return status;
}

#include "common/compile_check_avoid_end.h"
} // end namespace doris
