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

#include "olap/task/engine_clone_task.h"

#include <set>

#include "env/env.h"
#include "gen_cpp/Types_constants.h"
#include "gen_cpp/BackendService.h"
#include "gutil/strings/stringpiece.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "http/http_client.h"
#include "http/http_client.h"
#include "olap/olap_snapshot_converter.h"
#include "olap/snapshot_manager.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "runtime/client_cache.h"
#include "util/thrift_rpc_helper.h"

using std::set;
using std::stringstream;
using strings::Substitute;
using strings::Split;
using strings::SkipWhitespace;

namespace doris {

const std::string HTTP_REQUEST_PREFIX = "/api/_tablet/_download?";
const std::string HTTP_REQUEST_TOKEN_PARAM = "token=";
const std::string HTTP_REQUEST_FILE_PARAM = "&file=";
const uint32_t DOWNLOAD_FILE_MAX_RETRY = 3;
const uint32_t LIST_REMOTE_FILE_TIMEOUT = 15;
const uint32_t GET_LENGTH_TIMEOUT = 10;

EngineCloneTask::EngineCloneTask(const TCloneReq& clone_req,
                    const TMasterInfo& master_info,
                    int64_t signature,
                    vector<string>* error_msgs,
                    vector<TTabletInfo>* tablet_infos,
                    AgentStatus* res_status) :
    _clone_req(clone_req),
    _error_msgs(error_msgs),
    _tablet_infos(tablet_infos),
    _res_status(res_status),
    _signature(signature),
    _master_info(master_info) {}

OLAPStatus EngineCloneTask::execute() {
    AgentStatus status = DORIS_SUCCESS;
    string src_file_path;
    TBackend src_host;
    // Check local tablet exist or not
    TabletSharedPtr tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(
            _clone_req.tablet_id, _clone_req.schema_hash);
    bool is_new_tablet = tablet == nullptr;
    // try to repair a tablet with missing version
    if (tablet != nullptr) {
        ReadLock migration_rlock(tablet->get_migration_lock_ptr(), TRY_LOCK);
        if (!migration_rlock.own_lock()) {
            return OLAP_ERR_RWLOCK_ERROR;
        }
        LOG(INFO) << "clone tablet exist yet, begin to incremental clone. "
                    << "signature:" << _signature
                    << ", tablet_id:" << _clone_req.tablet_id
                    << ", schema_hash:" << _clone_req.schema_hash
                    << ", committed_version:" << _clone_req.committed_version;

        // get download path
        string local_data_path = tablet->tablet_path() + CLONE_PREFIX;
        bool allow_incremental_clone = false;
        // check if current tablet has version == 2 and version hash == 0
        // version 2 may be an invalid rowset
        Version clone_version = {_clone_req.committed_version, _clone_req.committed_version};
        RowsetSharedPtr clone_rowset = tablet->get_rowset_by_version(clone_version);
        if (clone_rowset == nullptr) {
            // try to incremental clone
            vector<Version> missed_versions;
            tablet->calc_missed_versions(_clone_req.committed_version, &missed_versions);
            LOG(INFO) << "finish to calculate missed versions when clone. "
                    << "tablet=" << tablet->full_name()
                    << ", committed_version=" << _clone_req.committed_version
                    << ", missed_versions_size=" << missed_versions.size();
            // if missed version size is 0, then it is useless to clone from remote be, it means local data is
            // completed. Or remote be will just return header not the rowset files. clone will failed.
            if (missed_versions.size() == 0) {
                LOG(INFO) << "missed version size = 0, skip clone and return success";
                _set_tablet_info(DORIS_SUCCESS, is_new_tablet);
                return OLAP_SUCCESS;
            }
            status = _clone_copy(*(tablet->data_dir()), local_data_path,
                                &src_host, &src_file_path, _error_msgs,
                                &missed_versions,
                                &allow_incremental_clone);
        } else {
            LOG(INFO) << "current tablet has invalid rowset that's version == commit_version but version hash not equal"
                      << " clone req commit_version=" <<  _clone_req.committed_version
                      << " tablet info = " << tablet->full_name();
        }
        if (status == DORIS_SUCCESS && allow_incremental_clone) {
            OLAPStatus olap_status = _finish_clone(tablet.get(), local_data_path, _clone_req.committed_version, allow_incremental_clone);
            if (olap_status != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to finish incremental clone. [table=" << tablet->full_name()
                             << " res=" << olap_status << "]";
                _error_msgs->push_back("incremental clone error.");
                status = DORIS_ERROR;
            }
        } else {
            bool allow_incremental_clone = false;
            // begin to full clone if incremental failed
            LOG(INFO) << "begin to full clone. [table=" << tablet->full_name();
            status = _clone_copy(*(tablet->data_dir()), local_data_path,
                                &src_host, &src_file_path,  _error_msgs,
                                NULL, &allow_incremental_clone);
            if (status == DORIS_SUCCESS) {
                LOG(INFO) << "download successfully when full clone. [table=" << tablet->full_name()
                          << " src_host=" << src_host.host << " src_file_path=" << src_file_path
                          << " local_data_path=" << local_data_path << "]";

                OLAPStatus olap_status = _finish_clone(tablet.get(), local_data_path, _clone_req.committed_version, false);

                if (olap_status != OLAP_SUCCESS) {
                    LOG(WARNING) << "fail to finish full clone. [table=" << tablet->full_name()
                                 << " res=" << olap_status << "]";
                    _error_msgs->push_back("full clone error.");
                    status = DORIS_ERROR;
                }
            }
        }
    } else {
        LOG(INFO) << "clone tablet not exist, begin clone a new tablet from remote be. "
                    << "signature:" << _signature
                    << ", tablet_id:" << _clone_req.tablet_id
                    << ", schema_hash:" << _clone_req.schema_hash
                    << ", committed_version:" << _clone_req.committed_version;
        // create a new tablet in this be
        // Get local disk from olap
        string local_shard_root_path;
        DataDir* store = nullptr;
        OLAPStatus olap_status = StorageEngine::instance()->obtain_shard_path(
            _clone_req.storage_medium, &local_shard_root_path, &store);
        if (olap_status != OLAP_SUCCESS) {
            LOG(WARNING) << "clone get local root path failed. signature: " << _signature;
            _error_msgs->push_back("clone get local root path failed.");
            status = DORIS_ERROR;
        }
        stringstream tablet_dir_stream;
        tablet_dir_stream << local_shard_root_path
                            << "/" << _clone_req.tablet_id
                            << "/" << _clone_req.schema_hash;

        if (status == DORIS_SUCCESS) {
            bool allow_incremental_clone = false;
            status = _clone_copy(*store,
                                tablet_dir_stream.str(),
                                &src_host,
                                &src_file_path,
                                _error_msgs,
                                nullptr, &allow_incremental_clone);
        }

        if (status == DORIS_SUCCESS) {
            LOG(INFO) << "clone copy done. src_host: " << src_host.host
                        << " src_file_path: " << src_file_path;
            stringstream schema_hash_path_stream;
            schema_hash_path_stream << local_shard_root_path
                                    << "/" << _clone_req.tablet_id
                                    << "/" << _clone_req.schema_hash;
            string header_path = TabletMeta::construct_header_file_path(schema_hash_path_stream.str(),
                _clone_req.tablet_id);
            OLAPStatus reset_id_status = TabletMeta::reset_tablet_uid(header_path);
            if (reset_id_status != OLAP_SUCCESS) {
                LOG(WARNING) << "errors while set tablet uid: '" << header_path;
                _error_msgs->push_back("errors while set tablet uid.");
                status = DORIS_ERROR;
            } else {
                OLAPStatus load_header_status =  StorageEngine::instance()->tablet_manager()->load_tablet_from_dir(
                    store, _clone_req.tablet_id, _clone_req.schema_hash, schema_hash_path_stream.str(), false);
                if (load_header_status != OLAP_SUCCESS) {
                    LOG(WARNING) << "load header failed. local_shard_root_path: '" << local_shard_root_path
                                << "' schema_hash: " << _clone_req.schema_hash << ". status: " << load_header_status
                                << ". signature: " << _signature;
                    _error_msgs->push_back("load header failed.");
                    status = DORIS_ERROR;
                }
            }
            // clone success, delete .hdr file because tablet meta is stored in rocksdb
            string cloned_meta_file = tablet_dir_stream.str() + "/" + std::to_string(_clone_req.tablet_id) + ".hdr";
            FileUtils::remove(cloned_meta_file);
        }
        // Clean useless dir, if failed, ignore it.
        if (status != DORIS_SUCCESS && status != DORIS_CREATE_TABLE_EXIST) {
            stringstream local_data_path_stream;
            local_data_path_stream << local_shard_root_path
                                    << "/" << _clone_req.tablet_id;
            string local_data_path = local_data_path_stream.str();
            LOG(INFO) << "clone failed. want to delete local dir: " << local_data_path
                        << ". signature: " << _signature;
            try {
                boost::filesystem::path local_path(local_data_path);
                if (boost::filesystem::exists(local_path)) {
                    boost::filesystem::remove_all(local_path);
                }
            } catch (boost::filesystem::filesystem_error e) {
                // Ignore the error, OLAP will delete it
                LOG(WARNING) << "clone delete useless dir failed. "
                             << " error: " << e.what()
                             << " local dir: " << local_data_path.c_str()
                             << " signature: " << _signature;
            }
        }
    }
    _set_tablet_info(status, is_new_tablet);
    return OLAP_SUCCESS;
}

void EngineCloneTask::_set_tablet_info(AgentStatus status, bool is_new_tablet) {
    // Get clone tablet info
    if (status == DORIS_SUCCESS || status == DORIS_CREATE_TABLE_EXIST) {
        TTabletInfo tablet_info;
        tablet_info.__set_tablet_id(_clone_req.tablet_id);
        tablet_info.__set_schema_hash(_clone_req.schema_hash);
        OLAPStatus get_tablet_info_status = StorageEngine::instance()->tablet_manager()->report_tablet_info(&tablet_info);
        if (get_tablet_info_status != OLAP_SUCCESS) {
            LOG(WARNING) << "clone success, but get tablet info failed."
                         << " tablet id: " <<  _clone_req.tablet_id
                         << " schema hash: " << _clone_req.schema_hash
                         << " signature: " << _signature;
            _error_msgs->push_back("clone success, but get tablet info failed.");
            status = DORIS_ERROR;
        } else if (_clone_req.__isset.committed_version
                   && tablet_info.version < _clone_req.committed_version) {
            LOG(WARNING) << "failed to clone tablet. tablet_id:" << _clone_req.tablet_id
                      << ", schema_hash:" << _clone_req.schema_hash
                      << ", signature:" << _signature
                      << ", version:" << tablet_info.version
                      << ", expected_version: " << _clone_req.committed_version;
            // if it is a new tablet and clone failed, then remove the tablet
            // if it is incremental clone, then must not drop the tablet
            if (is_new_tablet) {
                // we need to check if this cloned table's version is what we expect.
                // if not, maybe this is a stale remaining table which is waiting for drop.
                // we drop it.
                LOG(WARNING) << "begin to drop the stale tablet. tablet_id:" << _clone_req.tablet_id
                            << ", schema_hash:" << _clone_req.schema_hash
                            << ", signature:" << _signature
                            << ", version:" << tablet_info.version
                            << ", expected_version: " << _clone_req.committed_version;
                OLAPStatus drop_status = StorageEngine::instance()->tablet_manager()->drop_tablet(_clone_req.tablet_id,
                    _clone_req.schema_hash);
                if (drop_status != OLAP_SUCCESS && drop_status != OLAP_ERR_TABLE_NOT_FOUND) {
                    // just log
                    LOG(WARNING) << "drop stale cloned table failed! tabelt id: " << _clone_req.tablet_id;
                }
            }
            status = DORIS_ERROR;
        } else {
            LOG(INFO) << "clone get tablet info success. tablet_id:" << _clone_req.tablet_id
                        << ", schema_hash:" << _clone_req.schema_hash
                        << ", signature:" << _signature
                        << ", version:" << tablet_info.version;
            _tablet_infos->push_back(tablet_info);
        }
    }
    *_res_status = status;
}

AgentStatus EngineCloneTask::_clone_copy(
        DataDir& data_dir,
        const string& local_data_path,
        TBackend* src_host,
        string* snapshot_path,
        vector<string>* error_msgs,
        const vector<Version>* missed_versions,
        bool* allow_incremental_clone) {
    AgentStatus status = DORIS_SUCCESS;

    std::string local_path = local_data_path + "/";
    const auto& token = _master_info.token;

    int timeout_s = 0;
    if (_clone_req.__isset.timeout_s) {
        timeout_s = _clone_req.timeout_s;
    }

    for (auto& src : _clone_req.src_backends) {
        // Make snapshot in remote olap engine
        *src_host = src;
        int32_t snapshot_version = 0;
        // make snapsthot
        auto st = _make_snapshot(src.host, src.be_port,
                                 _clone_req.tablet_id, _clone_req.schema_hash,
                                 timeout_s,
                                 missed_versions,
                                 snapshot_path,
                                 allow_incremental_clone,
                                 &snapshot_version);
        if (st.ok()) {
            LOG(INFO) << "success to make snapshot. ip=" << src.host << ", port=" << src.be_port
                << ", tablet=" << _clone_req.tablet_id << ", schema_hash=" << _clone_req.schema_hash
                << ", snapshot_path=" << *snapshot_path
                << ", signature=" << _signature;
        } else {
            LOG(WARNING) << "fail to make snapshot, ip=" << src.host << ", port=" << src.be_port
                << ", tablet=" << _clone_req.tablet_id << ", schema_hash=" << _clone_req.schema_hash
                << ", signature=" << _signature << ", error=" << st.to_string();
            error_msgs->push_back("make snapshot failed. backend_ip: " + src_host->host);

            status = DORIS_ERROR;
            continue;
        }

        std::string remote_url_prefix;
        {
            // TODO(zc): if snapshot path has been returned from source, it is some strange to
            // concat tablet_id and schema hash here.
            std::stringstream ss;
            ss << "http://" << src.host << ":" << src.http_port
                << HTTP_REQUEST_PREFIX
                << HTTP_REQUEST_TOKEN_PARAM << token
                << HTTP_REQUEST_FILE_PARAM
                << *snapshot_path
                << "/" << _clone_req.tablet_id
                << "/" << _clone_req.schema_hash << "/";

            remote_url_prefix = ss.str();
        }

        st = _download_files(&data_dir, remote_url_prefix, local_path);
        if (!st.ok()) {
            LOG(WARNING) << "fail to download and convert tablet, remote="
                << remote_url_prefix << ", error=" << st.to_string();
            status = DORIS_ERROR;
            // when there is an error, keep this program executing to release snapshot
        }

        if (status == DORIS_SUCCESS && snapshot_version == 1) {
            auto olap_st = _convert_to_new_snapshot(local_path, _clone_req.tablet_id);
            if (olap_st != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to convert to new snapshot, path=" << local_path
                    << ", tablet_id=" << _clone_req.tablet_id
                    << ", error=" << olap_st;
                status = DORIS_ERROR;
            }
        }
        if (status == DORIS_SUCCESS) {
            // change all rowset ids because they maybe its id same with local rowset
            auto olap_st = SnapshotManager::instance()->convert_rowset_ids(
                local_path, _clone_req.tablet_id, _clone_req.schema_hash);
            if (olap_st != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to convert rowset ids, path=" << local_path
                    << ", tablet_id=" << _clone_req.tablet_id
                    << ", schema_hash=" << _clone_req.schema_hash
                    << ", error=" << olap_st;
                status = DORIS_ERROR;
            }
        }

        // Release snapshot, if failed, ignore it. OLAP engine will drop useless snapshot
        st = _release_snapshot(src.host, src.be_port, *snapshot_path);
        if (st.ok()) {
            LOG(INFO) << "success to release snapshot, ip=" << src.host << ", port=" << src.be_port
                << ", snapshot_path=" << snapshot_path;
        } else {
            LOG(WARNING) << "fail to release snapshot, ip=" << src.host << ", port=" << src.be_port
                << ", snapshot_path=" << snapshot_path << ", error=" << st.to_string();
            // DON'T change the status
        }
        if (status == DORIS_SUCCESS) {
            break;
        }
    } // clone copy from one backend
    return status;
}

Status EngineCloneTask::_make_snapshot(
        const std::string& ip, int port,
        TTableId tablet_id,
        TSchemaHash schema_hash,
        int timeout_s,
        const std::vector<Version>* missed_versions,
        std::string* snapshot_path,
        bool* allow_incremental_clone,
        int32_t* snapshot_version) {
    TSnapshotRequest request;
    request.__set_tablet_id(tablet_id);
    request.__set_schema_hash(schema_hash);
    request.__set_preferred_snapshot_version(g_Types_constants.TPREFER_SNAPSHOT_REQ_VERSION);
    if (missed_versions != nullptr) {
        // TODO: missing version composed of singleton delta.
        // if not, this place should be rewrote.
        request.__isset.missing_version = true;
        for (auto& version : *missed_versions) {
            request.missing_version.push_back(version.first);
        }
    }
    if (timeout_s > 0) {
        request.__set_timeout(timeout_s);
    }

    TAgentResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<BackendServiceClient>(
        ip, port,
        [&request, &result] (BackendServiceConnection& client) {
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
    if (result.__isset.allow_incremental_clone) {
        // During upgrading, some BE nodes still be installed an old previous old.
        // which incremental clone is not ready in those nodes.
        // should add a symbol to indicate it.
        *allow_incremental_clone = result.allow_incremental_clone;
    }
    *snapshot_version = result.snapshot_version;
    return Status::OK();
}

Status EngineCloneTask::_release_snapshot(
        const std::string& ip, int port,
        const std::string& snapshot_path) {
    TAgentResult result;
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<BackendServiceClient>(
            ip, port,
            [&snapshot_path, &result] (BackendServiceConnection& client) {
                client->release_snapshot(result, snapshot_path);
            }));
    return Status(result.status);
}

Status EngineCloneTask::_download_files(
        DataDir* data_dir,
        const std::string& remote_url_prefix,
        const std::string& local_path) {
    // Check local path exist, if exist, remove it, then create the dir
    // local_file_full_path = tabletid/clone， for a specific tablet, there should be only one folder
    // if this folder exists, then should remove it
    // for example, BE clone from BE 1 to download file 1 with version (2,2), but clone from BE 1 failed
    // then it will try to clone from BE 2, but it will find the file 1 already exist, but file 1 with same
    // name may have different versions.
    RETURN_IF_ERROR(FileUtils::remove_all(local_path));
    RETURN_IF_ERROR(FileUtils::create_dir(local_path));

    // Get remote dir file list
    string file_list_str;
    auto list_files_cb = [&remote_url_prefix, &file_list_str] (HttpClient* client) {
        RETURN_IF_ERROR(client->init(remote_url_prefix));
        client->set_timeout_ms(LIST_REMOTE_FILE_TIMEOUT * 1000);
        RETURN_IF_ERROR(client->execute(&file_list_str));
        return Status::OK();
    };
    RETURN_IF_ERROR(HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, list_files_cb));
    vector<string> file_name_list = strings::Split(file_list_str, "\n", strings::SkipWhitespace());

    // If the header file is not exist, the table could't loaded by olap engine.
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
        auto get_file_size_cb = [&remote_file_url, &file_size] (HttpClient* client) {
            RETURN_IF_ERROR(client->init(remote_file_url));
            client->set_timeout_ms(GET_LENGTH_TIMEOUT * 1000);
            RETURN_IF_ERROR(client->head());
            file_size = client->get_content_length();
            return Status::OK();
        };
        RETURN_IF_ERROR(HttpClient::execute_with_retry(DOWNLOAD_FILE_MAX_RETRY, 1, get_file_size_cb));
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

        LOG(INFO) << "clone begin to download file from: " << remote_file_url << " to: "
            << local_file_path << ". size(B): " << file_size << ", timeout(s): " << estimate_timeout;

        auto download_cb = [&remote_file_url,
             estimate_timeout,
             &local_file_path,
             file_size] (HttpClient* client) {
                 RETURN_IF_ERROR(client->init(remote_file_url));
                 client->set_timeout_ms(estimate_timeout * 1000);
                 RETURN_IF_ERROR(client->download(local_file_path));

                 // Check file length
                 uint64_t local_file_size = boost::filesystem::file_size(local_file_path);
                 if (local_file_size != file_size) {
                     LOG(WARNING) << "download file length error"
                         << ", remote_path=" << remote_file_url
                         << ", file_size=" << file_size
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
        copy_rate = total_file_size / ((double) total_time_ms) / 1000;
    }
    _copy_size = (int64_t) total_file_size;
    _copy_time_ms = (int64_t) total_time_ms;
    LOG(INFO) << "succeed to copy tablet " << _signature
        << ", total file size: " << total_file_size << " B"
        << ", cost: " << total_time_ms << " ms"
        << ", rate: " << copy_rate << " MB/s";
    return Status::OK();
}

OLAPStatus EngineCloneTask::_convert_to_new_snapshot(const string& clone_dir, int64_t tablet_id) {
    OLAPStatus res = OLAP_SUCCESS;
    // check clone dir existed
    if (!FileUtils::check_exist(clone_dir)) {
        res = OLAP_ERR_DIR_NOT_EXIST;
        LOG(WARNING) << "clone dir not existed when clone. clone_dir=" << clone_dir.c_str();
        return res;
    }

    // load src header
    string cloned_meta_file = clone_dir + "/" + std::to_string(tablet_id) + ".hdr";
    FileHeader<OLAPHeaderMessage> file_header;
    FileHandler file_handler;
    OLAPHeaderMessage olap_header_msg;
    if (file_handler.open(cloned_meta_file.c_str(), O_RDONLY) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to open ordinal file. file=" << cloned_meta_file;
        return OLAP_ERR_IO_ERROR;
    }

    // In file_header.unserialize(), it validates file length, signature, checksum of protobuf.
    if (file_header.unserialize(&file_handler) != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to unserialize tablet_meta. file='" << cloned_meta_file;
        return OLAP_ERR_PARSE_PROTOBUF_ERROR;
    }

    set<string> clone_files;

    RETURN_WITH_WARN_IF_ERROR(
            FileUtils::list_dirs_files(clone_dir, NULL, &clone_files, Env::Default()),
            OLAP_ERR_DISK_FAILURE,
            "failed to dir walk when clone. clone_dir=" + clone_dir);

    try {
       olap_header_msg.CopyFrom(file_header.message());
    } catch (...) {
        LOG(WARNING) << "fail to copy protocol buffer object. file='" << cloned_meta_file;
        return OLAP_ERR_PARSE_PROTOBUF_ERROR;
    }
    OlapSnapshotConverter converter;
    TabletMetaPB tablet_meta_pb;
    vector<RowsetMetaPB> pending_rowsets;
    res = converter.to_new_snapshot(olap_header_msg, clone_dir, clone_dir,
                                    &tablet_meta_pb, &pending_rowsets, false);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to convert snapshot to new format. dir='" << clone_dir;
        return res;
    }
    vector<string> files_to_delete;
    for (auto file_name : clone_files) {
        string full_file_path = clone_dir + "/" + file_name;
        files_to_delete.push_back(full_file_path);
    }
    // remove all files
    RETURN_WITH_WARN_IF_ERROR(FileUtils::remove_paths(files_to_delete), OLAP_ERR_IO_ERROR,
            "remove paths failed.")

    res = TabletMeta::save(cloned_meta_file, tablet_meta_pb);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to save converted tablet meta to dir='" << clone_dir;
        return res;
    }

    return OLAP_SUCCESS;
}

// only incremental clone use this method
OLAPStatus EngineCloneTask::_finish_clone(Tablet* tablet, const string& clone_dir,
                                         int64_t committed_version, bool is_incremental_clone) {
    OLAPStatus res = OLAP_SUCCESS;
    vector<string> linked_success_files;

    // clone and compaction operation should be performed sequentially
    tablet->obtain_base_compaction_lock();
    tablet->obtain_cumulative_lock();

    tablet->obtain_push_lock();
    tablet->obtain_header_wrlock();
    do {
        // check clone dir existed
        if (!FileUtils::check_exist(clone_dir)) {
            res = OLAP_ERR_DIR_NOT_EXIST;
            LOG(WARNING) << "clone dir not existed when clone. clone_dir=" << clone_dir.c_str();
            break;
        }

        // load src header
        string cloned_tablet_meta_file = clone_dir + "/" + std::to_string(tablet->tablet_id()) + ".hdr";
        TabletMeta cloned_tablet_meta;
        if ((res = cloned_tablet_meta.create_from_file(cloned_tablet_meta_file)) != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to load src header when clone. "
                         << ", cloned_tablet_meta_file=" << cloned_tablet_meta_file;
            break;
        }
        // remove the cloned meta file
        FileUtils::remove(cloned_tablet_meta_file);

        // TODO(ygl): convert old format file into rowset
        // check all files in /clone and /tablet
        set<string> clone_files;
        Status ret = FileUtils::list_dirs_files(clone_dir, NULL, &clone_files, Env::Default());
        if (!ret.ok()) {
            LOG(WARNING) << "failed to dir walk when clone. [clone_dir=" << clone_dir << "]"
                         << " error: " << ret.to_string();
            res = OLAP_ERR_DISK_FAILURE;
            break;
        }

        set<string> local_files;
        string tablet_dir = tablet->tablet_path();
        ret = FileUtils::list_dirs_files(tablet_dir, NULL, &local_files, Env::Default());
        if (!ret.ok()) {
            LOG(WARNING) << "failed to dir walk when clone. [tablet_dir=" << tablet_dir << "]"
                         << " error: " << ret.to_string();
            res = OLAP_ERR_DISK_FAILURE;
            break;
        }

        // link files from clone dir, if file exists, skip it
        for (const string& clone_file : clone_files) {
            if (local_files.find(clone_file) != local_files.end()) {
                VLOG(3) << "find same file when clone, skip it. "
                        << "tablet=" << tablet->full_name()
                        << ", clone_file=" << clone_file;
                continue;
            }

            string from = clone_dir + "/" + clone_file;
            string to = tablet_dir + "/" + clone_file;
            LOG(INFO) << "src file:" << from << " dest file:" << to;
            if (link(from.c_str(), to.c_str()) != 0) {
                LOG(WARNING) << "fail to create hard link when clone. "
                             << " from=" << from.c_str()
                             << " to=" << to.c_str();
                res = OLAP_ERR_OS_ERROR;
                break;
            }
            linked_success_files.emplace_back(std::move(to));
        }

        if (res != OLAP_SUCCESS) {
            break;
        }

        if (is_incremental_clone) {
            res = _clone_incremental_data(tablet, cloned_tablet_meta, committed_version);
        } else {
            res = _clone_full_data(tablet, const_cast<TabletMeta*>(&cloned_tablet_meta));
        }

        // if full clone success, need to update cumulative layer point
        if (!is_incremental_clone && res == OLAP_SUCCESS) {
            tablet->set_cumulative_layer_point(-1);
        }

    } while (0);

    // clear linked files if errors happen
    if (res != OLAP_SUCCESS) {
        FileUtils::remove_paths(linked_success_files);
    }
    tablet->release_header_lock();
    tablet->release_push_lock();

    tablet->release_cumulative_lock();
    tablet->release_base_compaction_lock();

    // clear clone dir
    boost::filesystem::path clone_dir_path(clone_dir);
    boost::filesystem::remove_all(clone_dir_path);
    LOG(INFO) << "finish to clone data, clear downloaded data. res=" << res
              << ", tablet=" << tablet->full_name()
              << ", clone_dir=" << clone_dir;
    return res;
}

OLAPStatus EngineCloneTask::_clone_incremental_data(Tablet* tablet, const TabletMeta& cloned_tablet_meta,
                                              int64_t committed_version) {
    LOG(INFO) << "begin to incremental clone. tablet=" << tablet->full_name()
              << ", committed_version=" << committed_version;

    vector<Version> missed_versions;
    tablet->calc_missed_versions_unlocked(committed_version, &missed_versions);

    vector<Version> versions_to_delete;
    vector<RowsetMetaSharedPtr> rowsets_to_clone;

    VLOG(3) << "get missed versions again when finish incremental clone. "
            << "tablet=" << tablet->full_name()
            << ", committed_version=" << committed_version
            << ", missed_versions_size=" << missed_versions.size();

    // check missing versions exist in clone src
    for (Version version : missed_versions) {
        RowsetMetaSharedPtr inc_rs_meta = cloned_tablet_meta.acquire_inc_rs_meta_by_version(version);
        if (inc_rs_meta == nullptr) {
            LOG(WARNING) << "missed version is not found in cloned tablet meta."
                         << ", missed_version=" << version.first << "-" << version.second;
            return OLAP_ERR_VERSION_NOT_EXIST;
        }

        rowsets_to_clone.push_back(inc_rs_meta);
    }

    // clone_data to tablet
    OLAPStatus clone_res = tablet->revise_tablet_meta(rowsets_to_clone, versions_to_delete);
    LOG(INFO) << "finish to incremental clone. [tablet=" << tablet->full_name() << " res=" << clone_res << "]";
    return clone_res;
}

OLAPStatus EngineCloneTask::_clone_full_data(Tablet* tablet, TabletMeta* cloned_tablet_meta) {
    Version cloned_max_version = cloned_tablet_meta->max_version();
    LOG(INFO) << "begin to full clone. tablet=" << tablet->full_name()
              << ", cloned_max_version=" << cloned_max_version.first
              << "-" << cloned_max_version.second;
    vector<Version> versions_to_delete;
    vector<RowsetMetaSharedPtr> rs_metas_found_in_src;
    // check local versions
    for (auto& rs_meta : tablet->tablet_meta()->all_rs_metas()) {
        Version local_version(rs_meta->start_version(), rs_meta->end_version());
        LOG(INFO) << "check local delta when full clone."
            << "tablet=" << tablet->full_name()
            << ", local_version=" << local_version.first << "-" << local_version.second;

        // if local version cross src latest, clone failed
        // if local version is : 0-0, 1-1, 2-10, 12-14, 15-15,16-16
        // cloned max version is 13-13, this clone is failed, because could not
        // fill local data by using cloned data.
        // It should not happen because if there is a hole, the following delta will not
        // do compaction.
        if (local_version.first <= cloned_max_version.second
            && local_version.second > cloned_max_version.second) {
            LOG(WARNING) << "stop to full clone, version cross src latest."
                    << "tablet=" << tablet->full_name()
                    << ", local_version=" << local_version.first << "-" << local_version.second;
            return OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR;

        } else if (local_version.second <= cloned_max_version.second) {
            // if local version smaller than src, check if existed in src, will not clone it
            bool existed_in_src = false;

            // if delta labeled with local_version is same with the specified version in clone header,
            // there is no necessity to clone it.
            for (auto& rs_meta : cloned_tablet_meta->all_rs_metas()) {
                if (rs_meta->version().first == local_version.first
                    && rs_meta->version().second == local_version.second) {
                    existed_in_src = true;
                    break;
                }
            }

            if (existed_in_src) {
                cloned_tablet_meta->delete_rs_meta_by_version(local_version, &rs_metas_found_in_src);
                LOG(INFO) << "Delta has already existed in local header, no need to clone."
                    << "tablet=" << tablet->full_name()
                    << ", version='" << local_version.first<< "-" << local_version.second;
            } else {
                // Delta labeled in local_version is not existed in clone header,
                // some overlapping delta will be cloned to replace it.
                // And also, the specified delta should deleted from local header.
                versions_to_delete.push_back(local_version);
                LOG(INFO) << "Delete delta not included by the clone header, should delete it from local header."
                          << "tablet=" << tablet->full_name() << ","
                          << ", version=" << local_version.first<< "-" << local_version.second;
            }
        }
    }
    vector<RowsetMetaSharedPtr> rowsets_to_clone;
    for (auto& rs_meta : cloned_tablet_meta->all_rs_metas()) {
        rowsets_to_clone.push_back(rs_meta);
        LOG(INFO) << "Delta to clone."
                  << "tablet=" << tablet->full_name()
                  << ", version=" << rs_meta->version().first << "-"
                  << rs_meta->version().second;
    }

    // clone_data to tablet
    // only replace rowet info, must not modify other info such as alter task info. for example
    // 1. local tablet finished alter task
    // 2. local tablet has error in push
    // 3. local tablet cloned rowset from other nodes
    // 4. if cleared alter task info, then push will not write to new tablet, the report info is error
    OLAPStatus clone_res = tablet->revise_tablet_meta(rowsets_to_clone, versions_to_delete);
    LOG(INFO) << "finish to full clone. tablet=" << tablet->full_name() << ", res=" << clone_res;
    // in previous step, copy all files from CLONE_DIR to tablet dir
    // but some rowset is useless, so that remove them here
    for (auto& rs_meta_ptr : rs_metas_found_in_src) {
        RowsetSharedPtr rowset_to_remove;
        auto s = RowsetFactory::create_rowset(&(cloned_tablet_meta->tablet_schema()),
                                              tablet->tablet_path(),
                                              rs_meta_ptr,
                                              &rowset_to_remove);
        if (s != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to init rowset to remove: " << rs_meta_ptr->rowset_id().to_string();
            continue;
        }
        s = rowset_to_remove->remove();
        if (s != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to remove rowset " << rs_meta_ptr->rowset_id().to_string() << ", res=" << s;
        }
    }
    return clone_res;
}

} // doris
