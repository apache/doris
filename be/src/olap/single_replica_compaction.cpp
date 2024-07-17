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

#include "olap/single_replica_compaction.h"

#include <curl/curl.h>

#include "common/logging.h"
#include "gen_cpp/Types_constants.h"
#include "gen_cpp/internal_service.pb.h"
#include "gutil/strings/split.h"
#include "http/http_client.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/snapshot_manager.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "service/brpc.h"
#include "task/engine_clone_task.h"
#include "util/brpc_client_cache.h"
#include "util/doris_metrics.h"
#include "util/thrift_rpc_helper.h"
#include "util/trace.h"

namespace doris {
using namespace ErrorCode;

SingleReplicaCompaction::SingleReplicaCompaction(StorageEngine& engine,
                                                 const TabletSharedPtr& tablet,
                                                 CompactionType compaction_type)
        : CompactionMixin(engine, tablet,
                          "SingleReplicaCompaction:" + std::to_string(tablet->tablet_id())),
          _compaction_type(compaction_type) {}

SingleReplicaCompaction::~SingleReplicaCompaction() = default;

Status SingleReplicaCompaction::prepare_compact() {
    VLOG_CRITICAL << _tablet->tablet_id() << " prepare single replcia compaction and pick rowsets!";
    if (!tablet()->init_succeeded()) {
        return Status::Error<CUMULATIVE_INVALID_PARAMETERS, false>("_tablet init failed");
    }

    // Single replica compaction does not require picking _input_rowsets
    // _input_rowsets depends on the fetched _output_version
    return Status::OK();
}

Status SingleReplicaCompaction::execute_compact() {
    if (!tablet()->should_fetch_from_peer()) {
        return Status::Aborted("compaction should be performed locally");
    }
    std::unique_lock<std::mutex> lock_cumu(tablet()->get_cumulative_compaction_lock(),
                                           std::try_to_lock);
    if (!lock_cumu.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED, false>(
                "The tablet is under cumulative compaction. tablet={}", _tablet->tablet_id());
    }

    std::unique_lock<std::mutex> lock_base(tablet()->get_base_compaction_lock(), std::try_to_lock);
    if (!lock_base.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED, false>(
                "another base compaction is running. tablet={}", _tablet->tablet_id());
    }

    SCOPED_ATTACH_TASK(_mem_tracker);

    // do single replica compaction
    RETURN_IF_ERROR(_do_single_replica_compaction());

    _state = CompactionState::SUCCESS;

    return Status::OK();
}

Status SingleReplicaCompaction::_do_single_replica_compaction() {
    tablet()->data_dir()->disks_compaction_num_increment(1);
    Status st = _do_single_replica_compaction_impl();
    tablet()->data_dir()->disks_compaction_num_increment(-1);

    return st;
}

Status SingleReplicaCompaction::_do_single_replica_compaction_impl() {
    DBUG_EXECUTE_IF("do_single_compaction_return_ok", { return Status::OK(); });
    TReplicaInfo addr;
    std::string token;
    //  1. get peer replica info
    DBUG_EXECUTE_IF("single_compaction_failed_get_peer",
                    { return Status::Aborted("tablet don't have peer replica"); });
    if (!_engine.get_peer_replica_info(_tablet->tablet_id(), &addr, &token)) {
        LOG(WARNING) << _tablet->tablet_id() << " tablet don't have peer replica";
        return Status::Aborted("tablet don't have peer replica");
    }

    // 2. get verisons from peer
    std::vector<Version> peer_versions;
    RETURN_IF_ERROR(_get_rowset_verisons_from_peer(addr, &peer_versions));

    Version proper_version;
    // 3. find proper version to fetch
    if (!_find_rowset_to_fetch(peer_versions, &proper_version)) {
        return Status::Cancelled("no matched versions for single replica compaction");
    }

    // 4. fetch compaction result
    RETURN_IF_ERROR(_fetch_rowset(addr, token, proper_version));
    // 5. modify rowsets in memory
    RETURN_IF_ERROR(modify_rowsets());

    // 6. update last success compaction time
    if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
        tablet()->set_last_cumu_compaction_success_time(UnixMillis());
    } else if (compaction_type() == ReaderType::READER_BASE_COMPACTION) {
        tablet()->set_last_base_compaction_success_time(UnixMillis());
    } else if (compaction_type() == ReaderType::READER_FULL_COMPACTION) {
        tablet()->set_last_full_compaction_success_time(UnixMillis());
    }

    tablet()->set_last_fetched_version(_output_rowset->version());

    int64_t current_max_version = -1;
    {
        std::shared_lock rdlock(_tablet->get_header_lock());
        if (RowsetSharedPtr max_rowset = _tablet->get_rowset_with_max_version()) {
            current_max_version = max_rowset->end_version();
        }
    }

    LOG(INFO) << "succeed to do single replica compaction"
              << ". tablet=" << _tablet->tablet_id() << ", output_version=" << _output_version
              << ", current_max_version=" << current_max_version
              << ", input_rowset_size=" << _input_rowsets_size
              << ", input_row_num=" << _input_row_num
              << ", input_segments_num=" << _input_num_segments
              << ", _input_index_size=" << _input_index_size
              << ", output_rowset_data_size=" << _output_rowset->data_disk_size()
              << ", output_row_num=" << _output_rowset->num_rows()
              << ", output_segments_num=" << _output_rowset->num_segments();
    return Status::OK();
}

Status SingleReplicaCompaction::_get_rowset_verisons_from_peer(
        const TReplicaInfo& addr, std::vector<Version>* peer_versions) {
    DBUG_EXECUTE_IF("single_compaction_failed_get_peer_versions",
                    { return Status::Aborted("tablet failed get peer versions"); });
    PGetTabletVersionsRequest request;
    request.set_tablet_id(_tablet->tablet_id());
    PGetTabletVersionsResponse response;
    std::shared_ptr<PBackendService_Stub> stub =
            ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(addr.host,
                                                                             addr.brpc_port);
    if (stub == nullptr) {
        return Status::Aborted("get rpc stub failed");
    }

    brpc::Controller cntl;
    stub->get_tablet_rowset_versions(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status::Aborted("open brpc connection failed");
    }
    if (response.status().status_code() != 0) {
        return Status::Aborted("peer don't have tablet");
    }
    if (response.versions_size() == 0) {
        return Status::Aborted("no peer version");
    }
    for (int i = 0; i < response.versions_size(); ++i) {
        (*peer_versions).emplace_back(response.versions(i).first(), response.versions(i).second());
    }
    return Status::OK();
}

bool SingleReplicaCompaction::_find_rowset_to_fetch(const std::vector<Version>& peer_versions,
                                                    Version* proper_version) {
    //  already sorted
    std::vector<Version> local_versions = tablet()->get_all_local_versions();
    for (const auto& v : local_versions) {
        VLOG_CRITICAL << _tablet->tablet_id() << " tablet local version: " << v.first << " - "
                      << v.second;
    }
    for (const auto& v : peer_versions) {
        VLOG_CRITICAL << _tablet->tablet_id() << " tablet peer version: " << v.first << " - "
                      << v.second;
    }

    bool find = false;
    int index_peer = 0;
    int index_local = 0;
    // peer_versions  [0-0] [1-1] [2-2] [3-5] [6-7]
    // local_versions [0-0] [1-1] [2-2] [3-3] [4-4] [5-5] [6-7]
    // return output_version [3-5]
    //  1: skip same versions
    while (index_local < local_versions.size() && index_peer < peer_versions.size()) {
        if (peer_versions[index_peer].first == local_versions[index_local].first &&
            peer_versions[index_peer].second == local_versions[index_local].second) {
            ++index_peer;
            ++index_local;
            continue;
        }
        break;
    }
    if (index_peer >= peer_versions.size() || index_local >= local_versions.size()) {
        return false;
    }
    //  2: first match
    if (peer_versions[index_peer].first != local_versions[index_local].first) {
        return false;
    }
    //  3: second match
    if (peer_versions[index_peer].contains(local_versions[index_local])) {
        ++index_local;
        while (index_local < local_versions.size()) {
            if (peer_versions[index_peer].contains(local_versions[index_local])) {
                ++index_local;
                continue;
            }
            break;
        }
        --index_local;
        if (local_versions[index_local].second == peer_versions[index_peer].second) {
            *proper_version = peer_versions[index_peer];
            find = true;
        }
    }
    if (find) {
        //  4. reset input rowsets
        _input_rowsets.clear();
        tablet()->traverse_rowsets([this, &proper_version](const auto& rs) {
            // only need rowset in proper_version
            if (rs->is_local() && proper_version->contains(rs->version())) {
                this->_input_rowsets.emplace_back(rs);
            }
        });
        std::sort(_input_rowsets.begin(), _input_rowsets.end(), Rowset::comparator);
        DCHECK_EQ(_input_rowsets.front()->start_version(), proper_version->first);
        DCHECK_EQ(_input_rowsets.back()->end_version(), proper_version->second);
        if (_input_rowsets.front()->start_version() != proper_version->first ||
            _input_rowsets.back()->end_version() != proper_version->second) {
            LOG(WARNING) << fmt::format(
                    "single compaction input rowsets error, tablet_id={}, input rowset = [{}-{}], "
                    "remote rowset = {}",
                    tablet()->tablet_id(), _input_rowsets.front()->start_version(),
                    _input_rowsets.back()->end_version(), proper_version->to_string());
            return false;
        }
        for (auto& rowset : _input_rowsets) {
            _input_rowsets_size += rowset->data_disk_size();
            _input_row_num += rowset->num_rows();
            _input_num_segments += rowset->num_segments();
            _input_index_size += rowset->index_disk_size();
        }
        _output_version = *proper_version;
    }
    return find;
}

Status SingleReplicaCompaction::_fetch_rowset(const TReplicaInfo& addr, const std::string& token,
                                              const Version& rowset_version) {
    LOG(INFO) << "begin to fetch compaction result, tablet_id=" << _tablet->tablet_id()
              << ", addr=" << addr.host << ", version=" << rowset_version;
    std::shared_lock migration_rlock(tablet()->get_migration_lock(), std::try_to_lock);
    if (!migration_rlock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED, false>("got migration_rlock failed. tablet={}",
                                                     _tablet->tablet_id());
    }

    std::string local_data_path = tablet()->tablet_path() + CLONE_PREFIX;
    std::string local_path = local_data_path + "/";
    std::string snapshot_path;
    int timeout_s = 0;
    // 1: make snapshot
    RETURN_IF_ERROR(_make_snapshot(addr.host, addr.be_port, _tablet->tablet_id(),
                                   _tablet->schema_hash(), timeout_s, rowset_version,
                                   &snapshot_path));
    Defer defer {[&, this] {
        // TODO(plat1ko): Async release snapshot
        auto st = _release_snapshot(addr.host, addr.be_port, snapshot_path);
        if (!st.ok()) [[unlikely]] {
            LOG_WARNING("failed to release snapshot in remote BE")
                    .tag("host", addr.host)
                    .tag("port", addr.be_port)
                    .tag("snapshot_path", snapshot_path)
                    .error(st);
        }
    }};
    // 2: download snapshot
    std::string remote_url_prefix;
    {
        std::stringstream ss;
        ss << "http://" << addr.host << ":" << addr.http_port << HTTP_REQUEST_PREFIX
           << HTTP_REQUEST_TOKEN_PARAM << token << HTTP_REQUEST_FILE_PARAM << snapshot_path << "/"
           << _tablet->tablet_id() << "/" << _tablet->schema_hash() << "/";
        remote_url_prefix = ss.str();
    }
    RETURN_IF_ERROR(_download_files(tablet()->data_dir(), remote_url_prefix, local_path));
    _pending_rs_guards = DORIS_TRY(_engine.snapshot_mgr()->convert_rowset_ids(
            local_path, _tablet->tablet_id(), tablet()->replica_id(), _tablet->table_id(),
            _tablet->partition_id(), _tablet->schema_hash()));
    // 4: finish_clone: create output_rowset and link file
    return _finish_clone(local_data_path, rowset_version);
}

Status SingleReplicaCompaction::_make_snapshot(const std::string& ip, int port, TTableId tablet_id,
                                               TSchemaHash schema_hash, int timeout_s,
                                               const Version& version, std::string* snapshot_path) {
    VLOG_NOTICE << "single replica compaction make snapshot ip=" << ip
                << ", tablet_id=" << tablet_id;
    TSnapshotRequest request;
    request.__set_tablet_id(tablet_id);
    request.__set_schema_hash(schema_hash);
    request.__set_preferred_snapshot_version(g_Types_constants.TPREFER_SNAPSHOT_REQ_VERSION);
    request.__set_start_version(version.first);
    request.__set_end_version(version.second);

    if (timeout_s > 0) {
        request.__set_timeout(timeout_s);
    }

    TAgentResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<BackendServiceClient>(
            ip, port, [&request, &result](BackendServiceConnection& client) {
                client->make_snapshot(result, request);
            }));
    if (result.status.status_code != TStatusCode::OK) {
        return Status::create(result.status);
    }

    if (result.__isset.snapshot_path) {
        *snapshot_path = result.snapshot_path;
        if (snapshot_path->at(snapshot_path->length() - 1) != '/') {
            snapshot_path->append("/");
        }
        DBUG_EXECUTE_IF("single_compaction_failed_make_snapshot",
                        { return Status::InternalError("failed snapshot"); });
    } else {
        return Status::InternalError("success snapshot without snapshot path");
    }
    return Status::OK();
}

Status SingleReplicaCompaction::_download_files(DataDir* data_dir,
                                                const std::string& remote_url_prefix,
                                                const std::string& local_path) {
    // Check local path exist, if exist, remove it, then create the dir
    // local_file_full_path = tabletid/clone， for a specific tablet, there should be only one folder
    // if this folder exists, then should remove it
    // for example, BE clone from BE 1 to download file 1 with version (2,2), but clone from BE 1 failed
    // then it will try to clone from BE 2, but it will find the file 1 already exist, but file 1 with same
    // name may have different versions.
    VLOG_DEBUG << "single replica compaction begin to download files, remote path="
               << _mask_token(remote_url_prefix) << " local_path=" << local_path;
    RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(local_path));
    RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(local_path));

    // Get remote dir file list
    string file_list_str;
    auto list_files_cb = [&remote_url_prefix, &file_list_str](HttpClient* client) {
        RETURN_IF_ERROR(client->init(remote_url_prefix));
        client->set_timeout_ms(LIST_REMOTE_FILE_TIMEOUT * 1000);
        RETURN_IF_ERROR(client->execute(&file_list_str));
        return Status::OK();
    };
    RETURN_IF_ERROR(HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, list_files_cb));
    std::vector<string> file_name_list =
            strings::Split(file_list_str, "\n", strings::SkipWhitespace());

    // If the header file is not exist, the table couldn't loaded by olap engine.
    // Avoid of data is not complete, we copy the header file at last.
    // The header file's name is end of .hdr.
    for (int i = 0; i < file_name_list.size() - 1; ++i) {
        if (file_name_list[i].ends_with(".hdr")) {
            std::swap(file_name_list[i], file_name_list[file_name_list.size() - 1]);
            break;
        }
    }

    // Get copy from remote
    uint64_t total_file_size = 0;
    MonotonicStopWatch watch;
    watch.start();
    auto curl = std::unique_ptr<CURL, decltype(&curl_easy_cleanup)>(curl_easy_init(),
                                                                    &curl_easy_cleanup);
    if (!curl) {
        return Status::InternalError("single compaction init curl failed");
    }
    for (auto& file_name : file_name_list) {
        // The file name of the variant column with the inverted index contains %
        // such as: 020000000000003f624c4c322c568271060f9b5b274a4a95_0_10133@properties%2Emessage.idx
        //  {rowset_id}_{seg_num}_{index_id}_{variant_column_name}{%2E}{extracted_column_name}.idx
        // We need to handle %, otherwise it will cause an HTTP 404 error.
        // Because the percent ("%") character serves as the indicator for percent-encoded octets,
        // it must be percent-encoded as "%25" for that octet to be used as data within a URI.
        // https://datatracker.ietf.org/doc/html/rfc3986
        auto output = std::unique_ptr<char, decltype(&curl_free)>(
                curl_easy_escape(curl.get(), file_name.c_str(), file_name.length()), &curl_free);
        if (!output) {
            return Status::InternalError("escape file name failed, file name={}", file_name);
        }
        std::string encoded_filename(output.get());
        auto remote_file_url = remote_url_prefix + encoded_filename;

        // get file length
        uint64_t file_size = 0;
        auto get_file_size_cb = [&remote_file_url, &file_size](HttpClient* client) {
            RETURN_IF_ERROR(client->init(remote_file_url));
            client->set_timeout_ms(GET_LENGTH_TIMEOUT * 1000);
            RETURN_IF_ERROR(client->head());
            RETURN_IF_ERROR(client->get_content_length(&file_size));
            return Status::OK();
        };
        RETURN_IF_ERROR(
                HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, get_file_size_cb));
        // check disk capacity
        if (data_dir->reach_capacity_limit(file_size)) {
            return Status::Error<EXCEEDED_LIMIT>(
                    "reach the capacity limit of path {}, file_size={}", data_dir->path(),
                    file_size);
        }

        total_file_size += file_size;
        uint64_t estimate_timeout = file_size / config::download_low_speed_limit_kbps / 1024;
        if (estimate_timeout < config::download_low_speed_time) {
            estimate_timeout = config::download_low_speed_time;
        }

        std::string local_file_path = local_path + file_name;

        LOG(INFO) << "single replica compaction begin to download file from: "
                  << _mask_token(remote_file_url) << " to: " << local_file_path
                  << ". size(B): " << file_size << ", timeout(s): " << estimate_timeout;

        auto download_cb = [this, &remote_file_url, estimate_timeout, &local_file_path,
                            file_size](HttpClient* client) {
            RETURN_IF_ERROR(client->init(remote_file_url));
            client->set_timeout_ms(estimate_timeout * 1000);
            RETURN_IF_ERROR(client->download(local_file_path));

            DBUG_EXECUTE_IF("single_compaction_failed_download_file",
                            { return Status::InternalError("failed to download file"); });
            // Check file length
            uint64_t local_file_size = std::filesystem::file_size(local_file_path);
            if (local_file_size != file_size) {
                LOG(WARNING) << "download file length error"
                             << ", remote_path=" << _mask_token(remote_file_url)
                             << ", file_size=" << file_size
                             << ", local_file_size=" << local_file_size;
                return Status::InternalError("downloaded file size is not equal");
            }
            return io::global_local_filesystem()->permission(local_file_path,
                                                             io::LocalFileSystem::PERMS_OWNER_RW);
        };
        RETURN_IF_ERROR(HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, download_cb));
    } // Clone files from remote backend

    uint64_t total_time_ms = watch.elapsed_time() / 1000 / 1000;
    total_time_ms = total_time_ms > 0 ? total_time_ms : 0;
    double copy_rate = 0.0;
    if (total_time_ms > 0) {
        copy_rate = total_file_size / ((double)total_time_ms) / 1000;
    }
    LOG(INFO) << "succeed to single replica compaction copy tablet " << _tablet->tablet_id()
              << ", total file size: " << total_file_size << " B"
              << ", cost: " << total_time_ms << " ms"
              << ", rate: " << copy_rate << " MB/s";
    return Status::OK();
}

Status SingleReplicaCompaction::_release_snapshot(const std::string& ip, int port,
                                                  const std::string& snapshot_path) {
    TAgentResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<BackendServiceClient>(
            ip, port, [&snapshot_path, &result](BackendServiceConnection& client) {
                client->release_snapshot(result, snapshot_path);
            }));
    return Status::create(result.status);
}

Status SingleReplicaCompaction::_finish_clone(const string& clone_dir,
                                              const Version& output_version) {
    Status res = Status::OK();
    std::vector<string> linked_success_files;
    {
        do {
            // check clone dir existed
            bool exists = true;
            RETURN_IF_ERROR(io::global_local_filesystem()->exists(clone_dir, &exists));
            if (!exists) {
                return Status::InternalError("clone dir not existed. clone_dir={}", clone_dir);
            }

            // Load src header.
            // The tablet meta info is downloaded from source BE as .hdr file.
            // So we load it and generate cloned_tablet_meta.
            auto cloned_tablet_meta_file =
                    fmt::format("{}/{}.hdr", clone_dir, _tablet->tablet_id());
            auto cloned_tablet_meta = std::make_shared<TabletMeta>();
            RETURN_IF_ERROR(cloned_tablet_meta->create_from_file(cloned_tablet_meta_file));

            // remove the cloned meta file
            RETURN_IF_ERROR(io::global_local_filesystem()->delete_file(cloned_tablet_meta_file));

            RowsetMetaSharedPtr output_rs_meta =
                    cloned_tablet_meta->acquire_rs_meta_by_version(output_version);
            if (output_rs_meta == nullptr) {
                LOG(WARNING) << "version not found in cloned tablet meta when do single compaction";
                return Status::InternalError("version not found in cloned tablet meta");
            }
            res = tablet()->create_rowset(output_rs_meta, &_output_rowset);
            if (!res.ok()) {
                LOG(WARNING) << "fail to init rowset. version=" << output_version;
                return res;
            }

            // check all files in /clone and /tablet
            std::vector<io::FileInfo> clone_files;
            RETURN_IF_ERROR(
                    io::global_local_filesystem()->list(clone_dir, true, &clone_files, &exists));
            std::unordered_set<std::string> clone_file_names;
            for (auto& file : clone_files) {
                clone_file_names.insert(file.file_name);
            }

            std::vector<io::FileInfo> local_files;
            const auto& tablet_dir = tablet()->tablet_path();
            RETURN_IF_ERROR(
                    io::global_local_filesystem()->list(tablet_dir, true, &local_files, &exists));
            std::unordered_set<std::string> local_file_names;
            for (auto& file : local_files) {
                local_file_names.insert(file.file_name);
            }

            /// Traverse all downloaded clone files in CLONE dir.
            /// If it does not exist in local tablet dir, link the file to local tablet dir
            /// And save all linked files in linked_success_files.
            for (const string& clone_file : clone_file_names) {
                if (local_file_names.find(clone_file) != local_file_names.end()) {
                    VLOG_NOTICE << "find same file when clone, skip it. "
                                << "tablet=" << _tablet->tablet_id()
                                << ", clone_file=" << clone_file;
                    continue;
                }

                auto from = fmt::format("{}/{}", clone_dir, clone_file);
                auto to = fmt::format("{}/{}", tablet_dir, clone_file);
                RETURN_IF_ERROR(io::global_local_filesystem()->link_file(from, to));
                linked_success_files.emplace_back(std::move(to));
            }

            if (!res.ok()) {
                break;
            }
        } while (false);

        // clear linked files if errors happen
        if (!res.ok()) {
            std::vector<io::Path> paths;
            for (auto& file : linked_success_files) {
                paths.emplace_back(file);
            }
            RETURN_IF_ERROR(io::global_local_filesystem()->batch_delete(paths));
        }
    }
    // clear clone dir
    std::filesystem::path clone_dir_path(clone_dir);
    std::error_code ec;
    std::filesystem::remove_all(clone_dir_path, ec);
    if (ec) {
        LOG(WARNING) << "failed to remove=" << clone_dir_path << " msg=" << ec.message();
        return Status::IOError("failed to remove {}, due to {}", clone_dir, ec.message());
    }
    LOG(INFO) << "finish to clone data, clear downloaded data. res=" << res
              << ", tablet=" << _tablet->tablet_id() << ", clone_dir=" << clone_dir;
    return res;
}

std::string SingleReplicaCompaction::_mask_token(const std::string& str) {
    std::regex pattern("token=[\\w|-]+");
    return regex_replace(str, pattern, "token=******");
}

} // namespace doris
