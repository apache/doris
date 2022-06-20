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

#include "olap/compaction.h"

#include "common/status.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/FrontendService.h"
#include "gen_cpp/Types_constants.h"
#include "gutil/strings/split.h"
#include "gutil/strings/stringpiece.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/snapshot_manager.h"
#include "olap/tablet.h"
#include "runtime/client_cache.h"
#include "service/brpc.h"
#include "task/engine_clone_task.h"
#include "util/brpc_client_cache.h"
#include "util/file_utils.h"
#include "util/thrift_rpc_helper.h"
#include "util/time.h"
#include "util/trace.h"

using std::vector;

namespace doris {

Compaction::Compaction(TabletSharedPtr tablet, const std::string& label)
        : _tablet(tablet),
          _input_rowsets_size(0),
          _input_row_num(0),
          _input_segments_num(0),
          _state(CompactionState::INITED),
          _single_compaction_succ(false),
          _doing(false) {
#ifndef BE_TEST
    _mem_tracker = std::make_shared<MemTrackerLimiter>(
            -1, label, StorageEngine::instance()->compaction_mem_tracker());
#else
    _mem_tracker = std::make_shared<MemTrackerLimiter>(-1, label);
#endif
}

Compaction::~Compaction() {
#ifndef BE_TEST
    // Compaction tracker cannot be completely accurate, offset the global impact.
    StorageEngine::instance()->compaction_mem_tracker()->consumption_revise(
            -_mem_tracker->consumption());
#endif
}

Status Compaction::compact() {
    RETURN_NOT_OK(prepare_compact());
    RETURN_NOT_OK(execute_compact());
    return Status::OK();
}

Status Compaction::execute_compact() {
    Status st = execute_compact_impl();
    if (!st.ok()) {
        gc_output_rowset();
    }
    return st;
}

Status Compaction::quick_rowsets_compact() {
    std::unique_lock<std::mutex> lock(_tablet->get_cumulative_compaction_lock(), std::try_to_lock);
    if (!lock.owns_lock()) {
        LOG(WARNING) << "The tablet is under cumulative compaction. tablet="
                     << _tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_CE_TRY_CE_LOCK_ERROR);
    }

    // Clone task may happen after compaction task is submitted to thread pool, and rowsets picked
    // for compaction may change. In this case, current compaction task should not be executed.
    if (_tablet->get_clone_occurred()) {
        _tablet->set_clone_occurred(false);
        return Status::OLAPInternalError(OLAP_ERR_CUMULATIVE_CLONE_OCCURRED);
    }

    _input_rowsets.clear();
    int version_count = _tablet->version_count();
    MonotonicStopWatch watch;
    watch.start();
    int64_t permits = 0;
    _tablet->pick_quick_compaction_rowsets(&_input_rowsets, &permits);
    std::vector<Version> missedVersions;
    find_longest_consecutive_version(&_input_rowsets, &missedVersions);
    if (missedVersions.size() != 0) {
        LOG(WARNING) << "quick_rowsets_compaction, find missed version"
                     << ",input_size:" << _input_rowsets.size();
    }
    int nums = _input_rowsets.size();
    if (_input_rowsets.size() >= config::quick_compaction_min_rowsets) {
        Status st = check_version_continuity(_input_rowsets);
        if (!st.ok()) {
            LOG(WARNING) << "quick_rowsets_compaction failed, cause version not continuous";
            return st;
        }
        st = do_compaction(permits);
        if (!st.ok()) {
            gc_output_rowset();
            LOG(WARNING) << "quick_rowsets_compaction failed";
        } else {
            LOG(INFO) << "quick_compaction succ"
                      << ", before_versions:" << version_count
                      << ", after_versions:" << _tablet->version_count()
                      << ", cost:" << (watch.elapsed_time() / 1000 / 1000) << "ms"
                      << ", merged: " << nums << ", batch:" << config::quick_compaction_batch_size
                      << ", segments:" << permits << ", tabletid:" << _tablet->tablet_id();
            _tablet->set_last_quick_compaction_success_time(UnixMillis());
        }
    }
    return Status::OK();
}

bool Compaction::is_compaction_doing() {
    return _doing;
}

Status Compaction::do_compaction(int64_t permits) {
    TRACE("start to do compaction");
    _tablet->data_dir()->disks_compaction_score_increment(permits);
    _tablet->data_dir()->disks_compaction_num_increment(1);
    Status st = do_compaction_impl(permits);
    _doing = false;
    _tablet->data_dir()->disks_compaction_score_increment(-permits);
    _tablet->data_dir()->disks_compaction_num_increment(-1);
    return st;
}

Status Compaction::_get_replicas_and_token_rpc(std::vector<TBackend>* replicas,
                                               std::string* token) {
    TGetTabletReplicasRequest request;
    request.__set_tablet_id(_tablet->tablet_id());
    TGetTabletReplicasResult result;
    TMasterInfo* master_info = ExecEnv::GetInstance()->master_info();
    if (master_info == nullptr) {
        return Status::Cancelled("Have not get FE Master heartbeat yet");
    }
    TNetworkAddress master_addr = master_info->network_address;
    // TODO check master_info here if it is the same with that of heartbeat rpc
    if (master_addr.hostname == "" || master_addr.port == 0) {
        return Status::Cancelled("Have not get FE Master heartbeat yet");
    }
    int64_t get_tablet_replicas_start_nano = MonotonicNanos();
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) {
                client->getTabletReplicas(result, request);
            }));
    auto cost = MonotonicNanos() - get_tablet_replicas_start_nano;
    VLOG_DEBUG << "get replicas from master cost: " << cost / 1000 / 1000 << "ms";
    *token = result.token;
    for (const auto& backend : result.backends) {
        VLOG_DEBUG << "replica addr: " << backend.host << " port: " << backend.be_port
                   << " token=" << *token;
        replicas->push_back(backend);
    }
    return Status::OK();
}

Status Compaction::_check_replica_compaction_status(const TBackend& addr,
                                                    const PCheckCompactionStatusRequest& request,
                                                    PCheckCompactionStatusResponse* response) {
    std::shared_ptr<PBackendService_Stub> stub =
            ExecEnv::GetInstance()->brpc_internal_client_cache()->get_client(addr.host,
                                                                             addr.brpc_port);

    brpc::Controller cntl;
    stub->check_compaction_status(&cntl, &request, response, nullptr);
    if (cntl.Failed()) {
        LOG(WARNING) << "open brpc connection to " << addr.host << " failed: " << cntl.ErrorText();
        return Status::InternalError("failed to open brpc");
    }
    return Status::OK();
}

Status Compaction::_make_snapshot(const std::string& ip, int port, TTableId tablet_id,
                                  TSchemaHash schema_hash, int timeout_s, const Version& version,
                                  std::string* snapshot_path) {
    VLOG_DEBUG << "compaction make snapshot ip=" << ip << ", tablet_id=" << tablet_id;
    TSnapshotRequest request;
    request.__set_tablet_id(tablet_id);
    request.__set_schema_hash(schema_hash);
    request.__set_preferred_snapshot_version(g_Types_constants.TPREFER_SNAPSHOT_REQ_VERSION);
    request.__set_is_compaction_clone(true);
    request.__set_compaction_clone_start_version(version.first);
    request.__set_compaction_clone_end_version(version.second);

    if (timeout_s > 0) {
        request.__set_timeout(timeout_s);
    }

    TAgentResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<BackendServiceClient>(
            ip, port, [&request, &result](BackendServiceConnection& client) {
                client->make_snapshot(result, request);
            }));
    if (result.status.status_code != TStatusCode::OK) {
        return Status(result.status);
    }

    if (result.__isset.snapshot_path) {
        *snapshot_path = result.snapshot_path;
        if (snapshot_path->at(snapshot_path->length() - 1) != '/') {
            snapshot_path->append("/");
        }
    } else {
        return Status::InternalError("success snapshot without snapshot path");
    }
    return Status::OK();
}

Status Compaction::_download_files(DataDir* data_dir, const std::string& remote_url_prefix,
                                   const std::string& local_path) {
    // Check local path exist, if exist, remove it, then create the dir
    // local_file_full_path = tabletid/clone， for a specific tablet, there should be only one folder
    // if this folder exists, then should remove it
    // for example, BE clone from BE 1 to download file 1 with version (2,2), but clone from BE 1 failed
    // then it will try to clone from BE 2, but it will find the file 1 already exist, but file 1 with same
    // name may have different versions.
    VLOG_DEBUG << "compaction begin to download files, remote path=" << remote_url_prefix
               << " local_path=" << local_path;
    RETURN_IF_ERROR(FileUtils::remove_all(local_path));
    RETURN_IF_ERROR(FileUtils::create_dir(local_path));

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
        StringPiece sp(file_name_list[i]);
        if (sp.ends_with(".hdr")) {
            std::swap(file_name_list[i], file_name_list[file_name_list.size() - 1]);
            break;
        }
    }

    // Get copy from remote
    uint64_t total_file_size = 0;
    MonotonicStopWatch watch;
    watch.start();
    for (auto& file_name : file_name_list) {
        auto remote_file_url = remote_url_prefix + file_name;

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
            return Status::InternalError("Disk reach capacity limit");
        }

        total_file_size += file_size;
        uint64_t estimate_timeout = file_size / config::download_low_speed_limit_kbps / 1024;
        if (estimate_timeout < config::download_low_speed_time) {
            estimate_timeout = config::download_low_speed_time;
        }

        std::string local_file_path = local_path + file_name;

        LOG(INFO) << "clone begin to download file from: " << remote_file_url
                  << " to: " << local_file_path << ". size(B): " << file_size
                  << ", timeout(s): " << estimate_timeout;

        auto download_cb = [&remote_file_url, estimate_timeout, &local_file_path,
                            file_size](HttpClient* client) {
            RETURN_IF_ERROR(client->init(remote_file_url));
            client->set_timeout_ms(estimate_timeout * 1000);
            RETURN_IF_ERROR(client->download(local_file_path));

            // Check file length
            uint64_t local_file_size = std::filesystem::file_size(local_file_path);
            if (local_file_size != file_size) {
                LOG(WARNING) << "download file length error"
                             << ", remote_path=" << remote_file_url << ", file_size=" << file_size
                             << ", local_file_size=" << local_file_size;
                return Status::InternalError("downloaded file size is not equal");
            }
            chmod(local_file_path.c_str(), S_IRUSR | S_IWUSR);
            return Status::OK();
        };
        RETURN_IF_ERROR(HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, download_cb));
    } // Clone files from remote backend

    uint64_t total_time_ms = watch.elapsed_time() / 1000 / 1000;
    total_time_ms = total_time_ms > 0 ? total_time_ms : 0;
    double copy_rate = 0.0;
    if (total_time_ms > 0) {
        copy_rate = total_file_size / ((double)total_time_ms) / 1000;
    }
    LOG(INFO) << "succeed to copy tablet " << _tablet->tablet_id()
              << ", total file size: " << total_file_size << " B"
              << ", cost: " << total_time_ms << " ms"
              << ", rate: " << copy_rate << " MB/s";
    return Status::OK();
}

Status Compaction::_release_snapshot(const std::string& ip, int port,
                                     const std::string& snapshot_path) {
    TAgentResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<BackendServiceClient>(
            ip, port, [&snapshot_path, &result](BackendServiceConnection& client) {
                client->release_snapshot(result, snapshot_path);
            }));
    return Status(result.status);
}

// fetch compaction result and modify meta
Status Compaction::_fetch_compaction_result(const TBackend& addr, const std::string& token,
                                            const Version& output_version) {
    LOG(INFO) << "begin to fetch compaction result, tablet_id=" << _tablet->tablet_id()
              << ", addr=" << addr.host << ", version=" << output_version;
    std::shared_lock migration_rlock(_tablet->get_migration_lock(), std::try_to_lock);
    if (!migration_rlock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_RWLOCK_ERROR);
    }
    // get single replica compaction lock to avoid base&cumu do
    // fetch compaction result concurrently, which will make some
    // Fatal error
    std::unique_lock single_replica_compaction_lock(_tablet->get_single_replica_compaction_lock(),
                                                    std::try_to_lock);
    if (!single_replica_compaction_lock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_MUTEX_ERROR);
    }
    string local_data_path = _tablet->tablet_path() + CLONE_PREFIX;
    std::string local_path = local_data_path + "/";
    std::string snapshot_path;
    int timeout_s = 0;
    Status status = Status::OK();
    // make snapshot
    auto st = _make_snapshot(addr.host, addr.be_port, _tablet->tablet_id(), _tablet->schema_hash(),
                             timeout_s, output_version, &snapshot_path);
    if (st.ok()) {
        status = Status::OK();
    } else {
        LOG(WARNING) << "fail to make snapshot";
        status = Status::InternalError("Failed to make snapshot");
        return status;
    }
    std::string remote_url_prefix;
    {
        std::stringstream ss;
        ss << "http://" << addr.host << ":" << addr.http_port << HTTP_REQUEST_PREFIX
           << HTTP_REQUEST_TOKEN_PARAM << token << HTTP_REQUEST_FILE_PARAM << snapshot_path << "/"
           << _tablet->tablet_id() << "/" << _tablet->schema_hash() << "/";

        remote_url_prefix = ss.str();
    }

    // download snapshot
    st = _download_files(_tablet->data_dir(), remote_url_prefix, local_path);
    if (!st.ok()) {
        LOG(WARNING) << "fail to download and convert tablet, remote=" << remote_url_prefix
                     << ", error=" << st.to_string();
        status = Status::InternalError("Fail to download and convert tablet");
        // when there is an error, keep this program executing to release snapshot
    }
    if (status.ok()) {
        // change all rowset ids because they maybe its id same with local rowset
        auto olap_st = SnapshotManager::instance()->convert_rowset_ids(
                local_path, _tablet->tablet_id(), _tablet->replica_id(), _tablet->schema_hash());
        if (olap_st != Status::OK()) {
            LOG(WARNING) << "fail to convert rowset ids, path=" << local_path
                         << ", tablet_id=" << _tablet->tablet_id() << ", error=" << olap_st;
            status = Status::InternalError("Failed to convert rowset ids");
        }
    }
    st = _release_snapshot(addr.host, addr.be_port, snapshot_path);
    if (status.ok()) {
        Status olap_status = _finish_clone(local_data_path, output_version);
        if (!olap_status.ok()) {
            LOG(WARNING) << "failed to finish clone. [table=" << _tablet->full_name()
                         << " res=" << olap_status << "]";
            status = Status::InternalError("Failed to finish clone");
        }
    }
    return status;
}

Status Compaction::_finish_clone(const string& clone_dir, const Version& output_version) {
    Status res = Status::OK();
    std::vector<string> linked_success_files;
    {
        do {
            // check clone dir existed
            if (!FileUtils::check_exist(clone_dir)) {
                res = Status::OLAPInternalError(OLAP_ERR_DIR_NOT_EXIST);
                LOG(WARNING) << "clone dir not existed when clone. clone_dir=" << clone_dir.c_str();
                break;
            }

            // Load src header.
            // The tablet meta info is downloaded from source BE as .hdr file.
            // So we load it and generate cloned_tablet_meta.
            string cloned_tablet_meta_file =
                    clone_dir + "/" + std::to_string(_tablet->tablet_id()) + ".hdr";
            TabletMeta cloned_tablet_meta;
            res = cloned_tablet_meta.create_from_file(cloned_tablet_meta_file);
            if (!res.ok()) {
                LOG(WARNING) << "fail to load src header when clone. "
                             << ", cloned_tablet_meta_file=" << cloned_tablet_meta_file;
                break;
            }
            // remove the cloned meta file
            FileUtils::remove(cloned_tablet_meta_file);

            RowsetMetaSharedPtr output_rs_meta =
                    cloned_tablet_meta.acquire_rs_meta_by_version(output_version);
            if (output_rs_meta == nullptr) {
                LOG(WARNING) << "version not found in cloned tablet meta when do single compaction";
                return Status::OLAPInternalError(OLAP_ERR_WRITE_PROTOBUF_ERROR);
            }
            res = RowsetFactory::create_rowset(_tablet->tablet_schema(), _tablet->tablet_path(),
                                               output_rs_meta, &_output_rowset);
            if (!res.ok()) {
                LOG(WARNING) << "fail to init rowset. version=" << output_version;
                return res;
            }

            // check all files in /clone and /tablet
            set<string> clone_files;
            Status ret =
                    FileUtils::list_dirs_files(clone_dir, nullptr, &clone_files, Env::Default());
            if (!ret.ok()) {
                LOG(WARNING) << "failed to list clone dir when clone. [clone_dir=" << clone_dir
                             << "]"
                             << " error: " << ret.to_string();
                res = Status::OLAPInternalError(OLAP_ERR_DISK_FAILURE);
                break;
            }

            set<string> local_files;
            string tablet_dir = _tablet->tablet_path();
            ret = FileUtils::list_dirs_files(tablet_dir, nullptr, &local_files, Env::Default());
            if (!ret.ok()) {
                LOG(WARNING) << "failed to list local tablet dir when clone. [tablet_dir="
                             << tablet_dir << "]"
                             << " error: " << ret.to_string();
                res = Status::OLAPInternalError(OLAP_ERR_DISK_FAILURE);
                break;
            }

            /// Traverse all downloaded clone files in CLONE dir.
            /// If it does not exist in local tablet dir, link the file to local tablet dir
            /// And save all linked files in linked_success_files.
            for (const string& clone_file : clone_files) {
                if (local_files.find(clone_file) != local_files.end()) {
                    VLOG_NOTICE << "find same file when clone, skip it. "
                                << "tablet=" << _tablet->full_name()
                                << ", clone_file=" << clone_file;
                    continue;
                }

                string from = clone_dir + "/" + clone_file;
                string to = tablet_dir + "/" + clone_file;
                LOG(INFO) << "src file:" << from << " dest file:" << to;
                if (link(from.c_str(), to.c_str()) != 0) {
                    LOG(WARNING) << "fail to create hard link when clone. "
                                 << " from=" << from.c_str() << " to=" << to.c_str();
                    res = Status::OLAPInternalError(OLAP_ERR_OS_ERROR);
                    break;
                }
                linked_success_files.emplace_back(std::move(to));
            }

            if (!res.ok()) {
                break;
            }
        } while (0);

        // clear linked files if errors happen
        if (!res.ok()) {
            FileUtils::remove_paths(linked_success_files);
        }
    }
    // clear clone dir
    std::filesystem::path clone_dir_path(clone_dir);
    std::filesystem::remove_all(clone_dir_path);
    LOG(INFO) << "finish to clone data, clear downloaded data. res=" << res
              << ", tablet=" << _tablet->full_name() << ", clone_dir=" << clone_dir;
    return res;
}

// single replica compaction
// step 1: ask fe to get peer replica info
// step 2: ask peer replica too check if compaction already done or started,
//         if not, do it myself.
// step 3: wait to get compaction result from peer replica
// step 4: build engine clone task and get compaction result
// step 5: build output rowset
Status Compaction::_handle_single_replica_compaction() {
    VLOG_DEBUG << "begin to handle single replica compaction, tablet_id=" << _tablet->tablet_id()
               << ", version: [" << _input_rowsets.front()->start_version() << ","
               << _input_rowsets.back()->end_version() << "]";
    // get peer replica info and token
    std::vector<TBackend> replica_addrs;
    std::string token;
    RETURN_IF_ERROR(_get_replicas_and_token_rpc(&replica_addrs, &token));

    // ask peer replica compaction info
    TBackend dst_addr;
    bool version_matched = false;
    Version dst_version;

    PCheckCompactionStatusRequest request;
    request.set_tablet_id(_tablet->tablet_id());
    for (auto& rowset : _input_rowsets) {
        request.add_versions(rowset->start_version());
        request.add_versions(rowset->end_version());
    }
    request.set_compaction_name(compaction_name());
    Status st = Status::OK();

    for (const auto& addr : replica_addrs) {
        PCheckCompactionStatusResponse response;
        st = _check_replica_compaction_status(addr, request, &response);
        if (!st.ok()) {
            LOG(WARNING) << "failed to check peer compaction status, tablet="
                         << _tablet->tablet_id();
            continue;
        }
        if (response.compaction_status() == PCompactionStatus::COMPACTION_NONE) {
            VLOG_DEBUG << "can't find proper version in peer replica";
            continue;
        }
        if (response.compaction_status() == PCompactionStatus::COMPACTION_OK) {
            if (response.has_expect_start_version() && response.has_expect_end_version()) {
                Version peer_version(response.expect_start_version(),
                                     response.expect_end_version());
                VLOG_DEBUG << "find peer exist version:" << peer_version;
                if (_tablet->should_fetch_from_peer(peer_version)) {
                    dst_addr = addr;
                    version_matched = true;
                    dst_version = peer_version;
                    break;
                }
            }
            if (response.has_compaction_start_version() && response.has_compaction_end_version()) {
                VLOG_DEBUG << "peer compaction version: [" << response.compaction_start_version()
                           << "," << response.compaction_end_version() << "]";
                // wait
                st = Status::OLAPInternalError(OLAP_ERR_COMPACTION_WAIT_PEER_FINISH);
            }
        }
    }
    if (version_matched) {
        RETURN_IF_ERROR(_fetch_compaction_result(dst_addr, token, dst_version));
        adjust_input_rowset(dst_version);
        _single_compaction_succ = true;
        return Status::OK();
    }
    if (st.precise_code() == OLAP_ERR_COMPACTION_WAIT_PEER_FINISH) {
        return st;
    }
    return Status::Aborted("cancel");
}

void Compaction::adjust_input_rowset(const Version& expect_version) {
    VLOG_DEBUG << "adjust input rowset, version=" << expect_version;
    bool adjusted = false;
    auto rs_iter = _input_rowsets.begin();
    while (rs_iter != _input_rowsets.end()) {
        if (expect_version.contains((*rs_iter)->version())) {
            ++rs_iter;
            continue;
        }
        VLOG_DEBUG << "erase rowset, version=" << (*rs_iter)->version();
        rs_iter = _input_rowsets.erase(rs_iter);
        adjusted = true;
    }
    if (adjusted) {
        clean_input_rowset_info();
        for (auto& rowset : _input_rowsets) {
            _input_rowsets_size += rowset->data_disk_size();
            _input_row_num += rowset->num_rows();
            _input_segments_num += rowset->num_segments();
        }
        _output_version = Version(_input_rowsets.front()->start_version(),
                                  _input_rowsets.back()->end_version());
    }
}

void Compaction::clean_input_rowset_info() {
    _input_rowsets_size = 0;
    _input_row_num = 0;
    _input_segments_num = 0;
}

bool Compaction::ready_to_check_peer_status() {
    int64_t now = UnixMillis();
    if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
        if (now - _tablet->last_single_replica_cumu_compaction_wait_millis() <
            config::single_replica_cumu_compaction_check_peer_interval_ms) {
            return false;
        }
    } else {
        if (now - _tablet->last_single_replica_base_compaction_wait_millis() <
            config::single_replica_base_compaction_check_peer_interval_ms) {
            return false;
        }
    }
    return true;
}

Status Compaction::do_compaction_impl(int64_t permits) {
    OlapStopWatch watch;

    // 1. prepare input and output parameters
    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_row_num += rowset->num_rows();
        _input_segments_num += rowset->num_segments();
    }

    _output_version =
            Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());

    _oldest_write_timestamp = _input_rowsets.front()->oldest_write_timestamp();
    _newest_write_timestamp = _input_rowsets.back()->newest_write_timestamp();

    auto use_vectorized_compaction = config::enable_vectorized_compaction;
    string merge_type = use_vectorized_compaction ? "v" : "";

    // get cur schema if rowset schema exist, rowset schema must be newer than tablet schema
    std::vector<RowsetMetaSharedPtr> rowset_metas(_input_rowsets.size());
    std::transform(_input_rowsets.begin(), _input_rowsets.end(), rowset_metas.begin(),
                   [](const RowsetSharedPtr& rowset) { return rowset->rowset_meta(); });
    TabletSchemaSPtr cur_tablet_schema =
            _tablet->rowset_meta_with_max_schema_version(rowset_metas)->tablet_schema();

    Status res = Status::OK();
    bool enter_single_replica_compaction =
            config::enable_single_replica_compaction &&
            _input_rowsets_size > config::single_replica_compaction_min_size_mbytes * 1024;
    if (enter_single_replica_compaction) {
        if (!ready_to_check_peer_status()) {
            clean_input_rowset_info();
            return Status::OK();
        }
        res = _handle_single_replica_compaction();
        // two case we can wait for next round
        // 1. peer compaction overlapped with current compaction version
        // 2. cumu compaction hold compaction lock, base compaction can wait
        if (res.precise_code() == OLAP_ERR_COMPACTION_WAIT_PEER_FINISH ||
            (res.precise_code() == OLAP_ERR_MUTEX_ERROR &&
             compaction_type() == ReaderType::READER_BASE_COMPACTION)) {
            int64_t now = UnixMillis();
            if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
                _tablet->set_last_single_replica_cumu_compaction_wait_millis(now);
            } else {
                _tablet->set_last_single_replica_base_compaction_wait_millis(now);
            }
            clean_input_rowset_info();
            return Status::OK();
        }
    }

    if (!enter_single_replica_compaction || !res.ok()) {
        LOG(INFO) << "enter origin compaction, tablet=" << _tablet->tablet_id();
        _doing = true;
        TRACE_COUNTER_INCREMENT("input_rowsets_data_size", _input_rowsets_size);
        TRACE_COUNTER_INCREMENT("input_row_num", _input_row_num);
        TRACE_COUNTER_INCREMENT("input_segments_num", _input_segments_num);

        LOG(INFO) << "start " << merge_type << compaction_name()
                  << ". tablet=" << _tablet->full_name() << ", output_version=" << _output_version
                  << ", permits: " << permits;

        RETURN_NOT_OK(construct_output_rowset_writer(cur_tablet_schema));
        RETURN_NOT_OK(construct_input_rowset_readers());
        TRACE("prepare finished");

        // 2. write merged rows to output rowset
        // The test results show that merger is low-memory-footprint, there is no need to tracker its mem pool
        Merger::Statistics stats;
        Status res;
        if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
            _tablet->enable_unique_key_merge_on_write()) {
            stats.rowid_conversion = &_rowid_conversion;
        }

        if (use_vectorized_compaction) {
            res = Merger::vmerge_rowsets(_tablet, compaction_type(), cur_tablet_schema.get(),
                                         _input_rs_readers, _output_rs_writer.get(), &stats);
        } else {
            res = Merger::merge_rowsets(_tablet, compaction_type(), cur_tablet_schema.get(),
                                        _input_rs_readers, _output_rs_writer.get(), &stats);
        }

        if (!res.ok()) {
            LOG(WARNING) << "fail to do " << merge_type << compaction_name() << ". res=" << res
                         << ", tablet=" << _tablet->full_name()
                         << ", output_version=" << _output_version;
            return res;
        }
        TRACE("merge rowsets finished");
        TRACE_COUNTER_INCREMENT("merged_rows", stats.merged_rows);
        TRACE_COUNTER_INCREMENT("filtered_rows", stats.filtered_rows);

        _output_rowset = _output_rs_writer->build();
        if (_output_rowset == nullptr) {
            LOG(WARNING) << "rowset writer build failed. writer version:"
                         << ", output_version=" << _output_version;
            return Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
        }
        TRACE_COUNTER_INCREMENT("output_rowset_data_size", _output_rowset->data_disk_size());
        TRACE_COUNTER_INCREMENT("output_row_num", _output_rowset->num_rows());
        TRACE_COUNTER_INCREMENT("output_segments_num", _output_rowset->num_segments());
        TRACE("output rowset built");

        // 3. check correctness
        RETURN_NOT_OK(check_correctness(stats));
        TRACE("check correctness finished");
    }

    // 4. modify rowsets in memory
    RETURN_NOT_OK(modify_rowsets());
    TRACE("modify rowsets finished");

    // 5. update last success compaction time
    int64_t now = UnixMillis();
    // TODO(yingchun): do the judge in Tablet class
    if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
        _tablet->set_last_cumu_compaction_success_time(now);
    } else {
        _tablet->set_last_base_compaction_success_time(now);
    }

    int64_t current_max_version;
    {
        std::shared_lock rdlock(_tablet->get_header_lock());
        RowsetSharedPtr max_rowset = _tablet->rowset_with_max_version();
        if (max_rowset == nullptr) {
            current_max_version = -1;
        } else {
            current_max_version = _tablet->rowset_with_max_version()->end_version();
        }
    }

    LOG(INFO) << "succeed to do " << merge_type << compaction_name()
              << ". tablet=" << _tablet->full_name() << ", output_version=" << _output_version
              << ", current_max_version=" << current_max_version
              << ", input_rowset_size=" << _input_rowsets_size
              << ", input_row_num=" << _input_row_num
              << ", input_segments_num=" << _input_segments_num
              << ", output_rowset_data_size=" << _output_rowset->data_disk_size()
              << ", output_row_num=" << _output_rowset->num_rows()
              << ", output_segments_num=" << _output_rowset->num_segments()
              << ", disk=" << _tablet->data_dir()->path() << ", segments=" << _input_segments_num
              << ". elapsed time=" << watch.get_elapse_second()
              << "s. cumulative_compaction_policy="
              << _tablet->cumulative_compaction_policy()->name()
              << ", is_single_replica_compaction=" << _single_compaction_succ;
    return Status::OK();
}

Status Compaction::construct_output_rowset_writer(TabletSchemaSPtr schema) {
    return _tablet->create_rowset_writer(_output_version, VISIBLE, NONOVERLAPPING, schema,
                                         _oldest_write_timestamp, _newest_write_timestamp,
                                         &_output_rs_writer);
}

Status Compaction::construct_input_rowset_readers() {
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_NOT_OK(rowset->create_reader(&rs_reader));
        _input_rs_readers.push_back(std::move(rs_reader));
    }
    return Status::OK();
}

Status Compaction::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);
    std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());

    // update dst rowset delete bitmap
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        _tablet->tablet_meta()->update_delete_bitmap(_input_rowsets, _output_rs_writer->version(),
                                                     _rowid_conversion);
    }

    RETURN_NOT_OK(_tablet->modify_rowsets(output_rowsets, _input_rowsets, true));
    _tablet->save_meta();
    return Status::OK();
}

void Compaction::gc_output_rowset() {
    if (_state != CompactionState::SUCCESS && _output_rowset != nullptr) {
        StorageEngine::instance()->add_unused_rowset(_output_rowset);
    }
}

// Find the longest consecutive version path in "rowset", from beginning.
// Two versions before and after the missing version will be saved in missing_version,
// if missing_version is not null.
Status Compaction::find_longest_consecutive_version(std::vector<RowsetSharedPtr>* rowsets,
                                                    std::vector<Version>* missing_version) {
    if (rowsets->empty()) {
        return Status::OK();
    }
    RowsetSharedPtr prev_rowset = rowsets->front();
    size_t i = 1;
    for (; i < rowsets->size(); ++i) {
        RowsetSharedPtr rowset = (*rowsets)[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            if (missing_version != nullptr) {
                missing_version->push_back(prev_rowset->version());
                missing_version->push_back(rowset->version());
            }
            break;
        }
        prev_rowset = rowset;
    }

    rowsets->resize(i);
    return Status::OK();
}

Status Compaction::check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (size_t i = 1; i < rowsets.size(); ++i) {
        RowsetSharedPtr rowset = rowsets[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            LOG(WARNING) << "There are missed versions among rowsets. "
                         << "prev_rowset version=" << prev_rowset->start_version() << "-"
                         << prev_rowset->end_version()
                         << ", rowset version=" << rowset->start_version() << "-"
                         << rowset->end_version();
            return Status::OLAPInternalError(OLAP_ERR_CUMULATIVE_MISS_VERSION);
        }
        prev_rowset = rowset;
    }

    return Status::OK();
}

Status Compaction::check_correctness(const Merger::Statistics& stats) {
    // 1. check row number
    if (_input_row_num != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
        LOG(WARNING) << "row_num does not match between cumulative input and output! "
                     << "input_row_num=" << _input_row_num
                     << ", merged_row_num=" << stats.merged_rows
                     << ", filtered_row_num=" << stats.filtered_rows
                     << ", output_row_num=" << _output_rowset->num_rows();
        return Status::OLAPInternalError(OLAP_ERR_CHECK_LINES_ERROR);
    }
    return Status::OK();
}

int64_t Compaction::get_compaction_permits() {
    int64_t permits = 0;
    for (auto rowset : _input_rowsets) {
        permits += rowset->rowset_meta()->get_compaction_score();
    }
    return permits;
}

} // namespace doris
