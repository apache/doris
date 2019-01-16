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

using std::set;
using std::stringstream;

namespace doris {

const std::string HTTP_REQUEST_PREFIX = "/api/_tablet/_download?";
const std::string HTTP_REQUEST_TOKEN_PARAM = "token=";
const std::string HTTP_REQUEST_FILE_PARAM = "&file=";
const uint32_t DOWNLOAD_FILE_MAX_RETRY = 3;
const uint32_t LIST_REMOTE_FILE_TIMEOUT = 15;

EngineCloneTask::EngineCloneTask(const TCloneReq& clone_req, vector<string>* error_msgs, 
                    vector<TTabletInfo>* tablet_infos, 
                    AgentStatus* res_status, 
                    int64_t signature,
                    const TMasterInfo& master_info) :
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
            TabletManager::instance()->get_tablet(
            _clone_req.tablet_id, _clone_req.schema_hash);
    if (tablet.get() != NULL) {
        LOG(INFO) << "clone tablet exist yet, begin to incremental clone. "
                    << "signature:" << _signature
                    << ", tablet_id:" << _clone_req.tablet_id
                    << ", schema_hash:" << _clone_req.schema_hash
                    << ", committed_version:" << _clone_req.committed_version;

        // try to incremental clone
        vector<Version> missed_versions;
        string local_data_path = _get_info_before_incremental_clone(tablet, _clone_req.committed_version, &missed_versions);

        bool allow_incremental_clone = false;
        status = _clone_copy(_clone_req,
                                                _signature,
                                                local_data_path,
                                                &src_host,
                                                &src_file_path,
                                                _error_msgs,
                                                &missed_versions,
                                                &allow_incremental_clone);
        if (status == DORIS_SUCCESS) {
            OLAPStatus olap_status = _finish_clone(tablet, local_data_path, _clone_req.committed_version, allow_incremental_clone);
            if (olap_status != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to finish incremental clone. [table=" << tablet->full_name()
                                << " res=" << olap_status << "]";
                _error_msgs->push_back("incremental clone error.");
                status = DORIS_ERROR;
            }
        } else {
            // begin to full clone if incremental failed
            LOG(INFO) << "begin to full clone. [table=" << tablet->full_name();
            status = _clone_copy(_clone_req,
                                                    _signature,
                                                    local_data_path,
                                                    &src_host,
                                                    &src_file_path,
                                                    _error_msgs,
                                                    NULL, NULL);
            if (status == DORIS_SUCCESS) {
                LOG(INFO) << "download successfully when full clone. [table=" << tablet->full_name()
                            << " src_host=" << src_host.host << " src_file_path=" << src_file_path
                            << " local_data_path=" << local_data_path << "]";

                OLAPStatus olap_status = _finish_clone(tablet, local_data_path, _clone_req.committed_version, false);

                if (olap_status != OLAP_SUCCESS) {
                    LOG(WARNING) << "fail to finish full clone. [table=" << tablet->full_name()
                                    << " res=" << olap_status << "]";
                    _error_msgs->push_back("full clone error.");
                    status = DORIS_ERROR;
                }
            }
        }
    } else {

        // Get local disk from olap
        string local_shard_root_path;
        DataDir* store = nullptr;
        OLAPStatus olap_status = StorageEngine::get_instance()->obtain_shard_path(
            _clone_req.storage_medium, &local_shard_root_path, &store);
        if (olap_status != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("clone get local root path failed. signature: %ld",
                                _signature);
            _error_msgs->push_back("clone get local root path failed.");
            status = DORIS_ERROR;
        }

        if (status == DORIS_SUCCESS) {
            stringstream tablet_dir_stream;
            tablet_dir_stream << local_shard_root_path
                                << "/" << _clone_req.tablet_id
                                << "/" << _clone_req.schema_hash;
            status = _clone_copy(_clone_req,
                                                    _signature,
                                                    tablet_dir_stream.str(),
                                                    &src_host,
                                                    &src_file_path,
                                                    _error_msgs,
                                                    NULL, NULL);
        }

        if (status == DORIS_SUCCESS) {
            LOG(INFO) << "clone copy done. src_host: " << src_host.host
                        << " src_file_path: " << src_file_path;
            // Load header
            OLAPStatus load_header_status =
                StorageEngine::get_instance()->load_header(
                    store,
                    local_shard_root_path,
                    _clone_req.tablet_id,
                    _clone_req.schema_hash);
            if (load_header_status != OLAP_SUCCESS) {
                LOG(WARNING) << "load header failed. local_shard_root_path: '" << local_shard_root_path
                                << "' schema_hash: " << _clone_req.schema_hash << ". status: " << load_header_status
                                << ". signature: " << _signature;
                _error_msgs->push_back("load header failed.");
                status = DORIS_ERROR;
            }
        }

#ifndef BE_TEST
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
                OLAP_LOG_WARNING("clone delete useless dir failed. "
                                    "error: %s, local dir: %s, signature: %ld",
                                    e.what(), local_data_path.c_str(),
                                    _signature);
            }
        }
#endif
    }

    // Get clone tablet info
    if (status == DORIS_SUCCESS || status == DORIS_CREATE_TABLE_EXIST) {
        TTabletInfo tablet_info;
        tablet_info.__set_tablet_id(_clone_req.tablet_id);
        tablet_info.__set_schema_hash(_clone_req.schema_hash);
        OLAPStatus get_tablet_info_status = TabletManager::instance()->report_tablet_info(&tablet_info);
        if (get_tablet_info_status != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("clone success, but get tablet info failed."
                                "tablet id: %ld, schema hash: %ld, signature: %ld",
                                _clone_req.tablet_id, _clone_req.schema_hash,
                                _signature);
            _error_msgs->push_back("clone success, but get tablet info failed.");
            status = DORIS_ERROR;
        } else if (
            (_clone_req.__isset.committed_version
                    && _clone_req.__isset.committed_version_hash)
                    && (tablet_info.version < _clone_req.committed_version ||
                        (tablet_info.version == _clone_req.committed_version
                        && tablet_info.version_hash != _clone_req.committed_version_hash))) {

            // we need to check if this cloned table's version is what we expect.
            // if not, maybe this is a stale remaining table which is waiting for drop.
            // we drop it.
            LOG(INFO) << "begin to drop the stale table. tablet_id:" << _clone_req.tablet_id
                        << ", schema_hash:" << _clone_req.schema_hash
                        << ", signature:" << _signature
                        << ", version:" << tablet_info.version
                        << ", version_hash:" << tablet_info.version_hash
                        << ", expected_version: " << _clone_req.committed_version
                        << ", version_hash:" << _clone_req.committed_version_hash;
            OLAPStatus drop_status = TabletManager::instance()->drop_tablet(_clone_req.tablet_id, _clone_req.schema_hash);
            if (drop_status != OLAP_SUCCESS && drop_status != OLAP_ERR_TABLE_NOT_FOUND) {
                // just log
                OLAP_LOG_WARNING(
                    "drop stale cloned table failed! tabelt id: %ld", _clone_req.tablet_id);
            }

            status = DORIS_ERROR;
        } else {
            LOG(INFO) << "clone get tablet info success. tablet_id:" << _clone_req.tablet_id
                        << ", schema_hash:" << _clone_req.schema_hash
                        << ", signature:" << _signature
                        << ", version:" << tablet_info.version
                        << ", version_hash:" << tablet_info.version_hash;
            _tablet_infos->push_back(tablet_info);
        }
    }
    *_res_status = status;
    return OLAP_SUCCESS;
}

string EngineCloneTask::_get_info_before_incremental_clone(TabletSharedPtr tablet,
    int64_t committed_version, vector<Version>* missed_versions) {
    tablet->calc_missed_versions(committed_version, missed_versions);
    LOG(INFO) << "finish to calculate missed versions when clone. "
              << "tablet=" << tablet->full_name()
              << ", committed_version=" << committed_version
              << ", missed_versions_size=" << missed_versions->size();

    // get download path
    return tablet->tablet_path() + CLONE_PREFIX;
}

AgentStatus EngineCloneTask::_clone_copy(
        const TCloneReq& clone_req,
        int64_t signature,
        const string& local_data_path,
        TBackend* src_host,
        string* src_file_path,
        vector<string>* error_msgs,
        const vector<Version>* missed_versions,
        bool* allow_incremental_clone) {
    AgentStatus status = DORIS_SUCCESS;

    std::string token = _master_info.token;
    for (auto src_backend : clone_req.src_backends) {
        stringstream http_host_stream;
        http_host_stream << "http://" << src_backend.host << ":" << src_backend.http_port;
        string http_host = http_host_stream.str();
        // Make snapshot in remote olap engine
        *src_host = src_backend;
        AgentServerClient agent_client(*src_host);
        TAgentResult make_snapshot_result;
        status = DORIS_SUCCESS;

        LOG(INFO) << "pre make snapshot. backend_ip: " << src_host->host;
        TSnapshotRequest snapshot_request;
        snapshot_request.__set_tablet_id(clone_req.tablet_id);
        snapshot_request.__set_schema_hash(clone_req.schema_hash);
        if (missed_versions != NULL) {
            // TODO: missing version composed of singleton delta.
            // if not, this place should be rewrote.
            vector<int64_t> snapshot_versions;
            for (Version version : *missed_versions) {
               snapshot_versions.push_back(version.first); 
            } 
            snapshot_request.__set_missing_version(snapshot_versions);
        }
        agent_client.make_snapshot(
                snapshot_request,
                &make_snapshot_result);

        if (make_snapshot_result.__isset.allow_incremental_clone) {
            // During upgrading, some BE nodes still be installed an old previous old.
            // which incremental clone is not ready in those nodes.
            // should add a symbol to indicate it.
            *allow_incremental_clone = make_snapshot_result.allow_incremental_clone;
        }
        if (make_snapshot_result.status.status_code == TStatusCode::OK) {
            if (make_snapshot_result.__isset.snapshot_path) {
                *src_file_path = make_snapshot_result.snapshot_path;
                if (src_file_path->at(src_file_path->length() - 1) != '/') {
                    src_file_path->append("/");
                }
                LOG(INFO) << "make snapshot success. backend_ip: " << src_host->host << ". src_file_path: "
                          << *src_file_path << ". signature: " << signature;
            } else {
                OLAP_LOG_WARNING("clone make snapshot success, "
                                 "but get src file path failed. signature: %ld",
                                 signature);
                status = DORIS_ERROR;
                continue;
            }
        } else {
            LOG(WARNING) << "make snapshot failed. tablet_id: " << clone_req.tablet_id
                         << ". schema_hash: " << clone_req.schema_hash << ". backend_ip: " << src_host->host
                         << ". backend_port: " << src_host->be_port << ". signature: " << signature;
            error_msgs->push_back("make snapshot failed. backend_ip: " + src_host->host);
            status = DORIS_ERROR;
            continue;
        }

        // Get remote and local full path
        stringstream src_file_full_path_stream;
        stringstream local_file_full_path_stream;

        if (status == DORIS_SUCCESS) {
            src_file_full_path_stream << *src_file_path
                                      << "/" << clone_req.tablet_id
                                      << "/" << clone_req.schema_hash << "/";
            local_file_full_path_stream << local_data_path  << "/";
        }
        string src_file_full_path = src_file_full_path_stream.str();
        string local_file_full_path = local_file_full_path_stream.str();

#ifndef BE_TEST
        // Check local path exist, if exist, remove it, then create the dir
        if (status == DORIS_SUCCESS) {
            boost::filesystem::path local_file_full_dir(local_file_full_path);
            if (boost::filesystem::exists(local_file_full_dir)) {
                boost::filesystem::remove_all(local_file_full_dir);
            }
            boost::filesystem::create_directories(local_file_full_dir);
        }
#endif

        // Get remove dir file list
        FileDownloader::FileDownloaderParam downloader_param;
        downloader_param.remote_file_path = http_host + HTTP_REQUEST_PREFIX
            + HTTP_REQUEST_TOKEN_PARAM + token
            + HTTP_REQUEST_FILE_PARAM + src_file_full_path;
        downloader_param.curl_opt_timeout = LIST_REMOTE_FILE_TIMEOUT;

#ifndef BE_TEST
        FileDownloader* file_downloader_ptr = new FileDownloader(downloader_param);
        if (file_downloader_ptr == NULL) {
            OLAP_LOG_WARNING("clone copy create file downloader failed. try next backend");
            status = DORIS_ERROR;
        }
#endif

        string file_list_str;
        AgentStatus download_status = DORIS_SUCCESS;
        uint32_t download_retry_time = 0;
        while (status == DORIS_SUCCESS && download_retry_time < DOWNLOAD_FILE_MAX_RETRY) {
#ifndef BE_TEST
            download_status = file_downloader_ptr->list_file_dir(&file_list_str);
#else
            download_status = _file_downloader_ptr->list_file_dir(&file_list_str);
#endif
            if (download_status != DORIS_SUCCESS) {
                OLAP_LOG_WARNING("clone get remote file list failed. backend_ip: %s, "
                                 "src_file_path: %s, signature: %ld",
                                 src_host->host.c_str(),
                                 downloader_param.remote_file_path.c_str(),
                                 signature);
                ++download_retry_time;
#ifndef BE_TEST
                sleep(download_retry_time);
#endif
            } else {
                break;
            }
        }

#ifndef BE_TEST
        if (file_downloader_ptr != NULL) {
            delete file_downloader_ptr;
            file_downloader_ptr = NULL;
        }
#endif

        vector<string> file_name_list;
        if (download_status != DORIS_SUCCESS) {
            OLAP_LOG_WARNING("clone get remote file list failed over max time. backend_ip: %s, "
                             "src_file_path: %s, signature: %ld",
                             src_host->host.c_str(),
                             downloader_param.remote_file_path.c_str(),
                             signature);
            status = DORIS_ERROR;
        } else {
            size_t start_position = 0;
            size_t end_position = file_list_str.find("\n");

            // Split file name from file_list_str
            while (end_position != string::npos) {
                string file_name = file_list_str.substr(
                        start_position, end_position - start_position);
                // If the header file is not exist, the table could't loaded by olap engine.
                // Avoid of data is not complete, we copy the header file at last.
                // The header file's name is end of .hdr.
                if (file_name.size() > 4 && file_name.substr(file_name.size() - 4, 4) == ".hdr") {
                    file_name_list.push_back(file_name);
                } else {
                    file_name_list.insert(file_name_list.begin(), file_name);
                }

                start_position = end_position + 1;
                end_position = file_list_str.find("\n", start_position);
            }
            if (start_position != file_list_str.size()) {
                string file_name = file_list_str.substr(
                        start_position, file_list_str.size() - start_position);
                if (file_name.size() > 4 && file_name.substr(file_name.size() - 4, 4) == ".hdr") {
                    file_name_list.push_back(file_name);
                } else {
                    file_name_list.insert(file_name_list.begin(), file_name);
                }
            }
        }

        // Get copy from remote
        for (auto file_name : file_name_list) {
            download_retry_time = 0;
            downloader_param.remote_file_path = http_host + HTTP_REQUEST_PREFIX
                + HTTP_REQUEST_TOKEN_PARAM + token
                + HTTP_REQUEST_FILE_PARAM + src_file_full_path + file_name;
            downloader_param.local_file_path = local_file_full_path + file_name;

            // Get file length
            uint64_t file_size = 0;
            uint64_t estimate_time_out = 0;

            downloader_param.curl_opt_timeout = GET_LENGTH_TIMEOUT;
#ifndef BE_TEST
            file_downloader_ptr = new FileDownloader(downloader_param);
            if (file_downloader_ptr == NULL) {
                OLAP_LOG_WARNING("clone copy create file downloader failed. try next backend");
                status = DORIS_ERROR;
                break;
            }
#endif
            while (download_retry_time < DOWNLOAD_FILE_MAX_RETRY) {
#ifndef BE_TEST
                download_status = file_downloader_ptr->get_length(&file_size);
#else
                download_status = _file_downloader_ptr->get_length(&file_size);
#endif
                if (download_status != DORIS_SUCCESS) {
                    OLAP_LOG_WARNING("clone copy get file length failed. backend_ip: %s, "
                                     "src_file_path: %s, signature: %ld",
                                     src_host->host.c_str(),
                                     downloader_param.remote_file_path.c_str(),
                                     signature);
                    ++download_retry_time;
#ifndef BE_TEST
                    sleep(download_retry_time);
#endif
                } else {
                    break;
                }
            }

#ifndef BE_TEST
            if (file_downloader_ptr != NULL) {
                delete file_downloader_ptr;
                file_downloader_ptr = NULL;
            }
#endif
            if (download_status != DORIS_SUCCESS) {
                OLAP_LOG_WARNING("clone copy get file length failed over max time. "
                                 "backend_ip: %s, src_file_path: %s, signature: %ld",
                                 src_host->host.c_str(),
                                 downloader_param.remote_file_path.c_str(),
                                 signature);
                status = DORIS_ERROR;
                break;
            }

            estimate_time_out = file_size / config::download_low_speed_limit_kbps / 1024;
            if (estimate_time_out < config::download_low_speed_time) {
                estimate_time_out = config::download_low_speed_time;
            }

            // Download the file
            download_retry_time = 0;
            downloader_param.curl_opt_timeout = estimate_time_out;
#ifndef BE_TEST
            file_downloader_ptr = new FileDownloader(downloader_param);
            if (file_downloader_ptr == NULL) {
                OLAP_LOG_WARNING("clone copy create file downloader failed. try next backend");
                status = DORIS_ERROR;
                break;
            }
#endif
            while (download_retry_time < DOWNLOAD_FILE_MAX_RETRY) {
#ifndef BE_TEST
                download_status = file_downloader_ptr->download_file();
#else
                download_status = _file_downloader_ptr->download_file();
#endif
                if (download_status != DORIS_SUCCESS) {
                    OLAP_LOG_WARNING("download file failed. backend_ip: %s, "
                                     "src_file_path: %s, signature: %ld",
                                     src_host->host.c_str(),
                                     downloader_param.remote_file_path.c_str(),
                                     signature);
                } else {
                    // Check file length
                    boost::filesystem::path local_file_path(downloader_param.local_file_path);
                    uint64_t local_file_size = boost::filesystem::file_size(local_file_path);
                    if (local_file_size != file_size) {
                        OLAP_LOG_WARNING("download file length error. backend_ip: %s, "
                                         "src_file_path: %s, signature: %ld,"
                                         "remote file size: %d, local file size: %d",
                                         src_host->host.c_str(),
                                         downloader_param.remote_file_path.c_str(),
                                         signature, file_size, local_file_size);
                        download_status = DORIS_FILE_DOWNLOAD_FAILED;
                    } else {
                        chmod(downloader_param.local_file_path.c_str(), S_IRUSR | S_IWUSR);
                        break;
                    }
                }
                ++download_retry_time;
#ifndef BE_TEST
                sleep(download_retry_time);
#endif
            } // Try to download a file from remote backend

#ifndef BE_TEST
            if (file_downloader_ptr != NULL) {
                delete file_downloader_ptr;
                file_downloader_ptr = NULL;
            }
#endif

            if (download_status != DORIS_SUCCESS) {
                OLAP_LOG_WARNING("download file failed over max retry. backend_ip: %s, "
                                 "src_file_path: %s, signature: %ld",
                                 src_host->host.c_str(),
                                 downloader_param.remote_file_path.c_str(),
                                 signature);
                status = DORIS_ERROR;
                break;
            }
        } // Clone files from remote backend

        // Release snapshot, if failed, ignore it. OLAP engine will drop useless snapshot
        TAgentResult release_snapshot_result;
        agent_client.release_snapshot(
                make_snapshot_result.snapshot_path,
                &release_snapshot_result);
        if (release_snapshot_result.status.status_code != TStatusCode::OK) {
            LOG(WARNING) << "release snapshot failed. src_file_path: " << *src_file_path
                         << ". signature: " << signature;
        }

        if (status == DORIS_SUCCESS) {
            break;
        }
    } // clone copy from one backend
    return status;
}


OLAPStatus EngineCloneTask::_finish_clone(TabletSharedPtr tablet, const string& clone_dir,
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
        if (!check_dir_existed(clone_dir)) {
            res = OLAP_ERR_DIR_NOT_EXIST;
            OLAP_LOG_WARNING("clone dir not existed when clone. [clone_dir=%s]",
                             clone_dir.c_str());
            break;
        }

        // load src header
        string clone_header_file = clone_dir + "/" + std::to_string(tablet->tablet_id()) + ".hdr";
        TabletMeta clone_header(clone_header_file);
        if ((res = clone_header.load_and_init()) != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to load src header when clone. [clone_header_file=%s]",
                             clone_header_file.c_str());
            break;
        }

        // check all files in /clone and /tablet
        set<string> clone_files;
        if ((res = dir_walk(clone_dir, NULL, &clone_files)) != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to dir walk when clone. [clone_dir=" << clone_dir << "]";
            break;
        }

        set<string> local_files;
        string tablet_dir = tablet->tablet_path();
        if ((res = dir_walk(tablet_dir, NULL, &local_files)) != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to dir walk when clone. [tablet_dir=" << tablet_dir << "]";
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
            LOG(INFO) << "src file:" << from << "dest file:" << to;
            if (link(from.c_str(), to.c_str()) != 0) {
                OLAP_LOG_WARNING("fail to create hard link when clone. [from=%s to=%s]",
                                 from.c_str(), to.c_str());
                res = OLAP_ERR_OS_ERROR;
                break;
            }
            linked_success_files.emplace_back(std::move(to));
        }

        if (res != OLAP_SUCCESS) {
            break;
        }

        if (is_incremental_clone) {
            res = _clone_incremental_data(
                                              tablet, clone_header, committed_version);
        } else {
            res = _clone_full_data(tablet, clone_header);
        }

        // if full clone success, need to update cumulative layer point
        if (!is_incremental_clone && res == OLAP_SUCCESS) {
            tablet->set_cumulative_layer_point(clone_header.cumulative_layer_point());
        }

    } while (0);

    // clear linked files if errors happen
    if (res != OLAP_SUCCESS) {
        remove_files(linked_success_files);
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


OLAPStatus EngineCloneTask::_clone_incremental_data(TabletSharedPtr tablet, TabletMeta& clone_header,
                                              int64_t committed_version) {
    LOG(INFO) << "begin to incremental clone. tablet=" << tablet->full_name()
              << ", committed_version=" << committed_version;

    vector<Version> missed_versions;
    tablet->calc_missed_versions(committed_version, &missed_versions);
    
    vector<Version> versions_to_delete;
    vector<const PDelta*> versions_to_clone;

    VLOG(3) << "get missed versions again when incremental clone. "
            << "tablet=" << tablet->full_name() 
            << ", committed_version=" << committed_version
            << ", missed_versions_size=" << missed_versions.size();

    // check missing versions exist in clone src
    for (Version version : missed_versions) {
        const PDelta* clone_src_version = clone_header.get_incremental_version(version);
        if (clone_src_version == NULL) {
           LOG(WARNING) << "missing version not found in clone src."
                        << "clone_header_file=" << clone_header.file_name()
                        << ", missing_version=" << version.first << "-" << version.second;
            return OLAP_ERR_VERSION_NOT_EXIST;
        }

        versions_to_clone.push_back(clone_src_version);
    }

    // clone_data to tablet
    OLAPStatus clone_res = tablet->clone_data(clone_header, versions_to_clone, versions_to_delete);
    LOG(INFO) << "finish to incremental clone. [tablet=" << tablet->full_name() << " res=" << clone_res << "]";
    return clone_res;
}

OLAPStatus EngineCloneTask::_clone_full_data(TabletSharedPtr tablet, TabletMeta& clone_header) {
    Version clone_latest_version = clone_header.get_latest_version();
    LOG(INFO) << "begin to full clone. tablet=" << tablet->full_name() << ","
        << "clone_latest_version=" << clone_latest_version.first << "-" << clone_latest_version.second;
    vector<Version> versions_to_delete;

    // check local versions
    for (int i = 0; i < tablet->file_delta_size(); i++) {
        Version local_version(tablet->get_delta(i)->start_version(),
                              tablet->get_delta(i)->end_version());
        VersionHash local_version_hash = tablet->get_delta(i)->version_hash();
        LOG(INFO) << "check local delta when full clone."
            << "tablet=" << tablet->full_name()
            << ", local_version=" << local_version.first << "-" << local_version.second;

        // if local version cross src latest, clone failed
        if (local_version.first <= clone_latest_version.second
            && local_version.second > clone_latest_version.second) {
            LOG(WARNING) << "stop to full clone, version cross src latest."
                    << "tablet=" << tablet->full_name()
                    << ", local_version=" << local_version.first << "-" << local_version.second;
            return OLAP_ERR_TABLE_VERSION_DUPLICATE_ERROR;

        } else if (local_version.second <= clone_latest_version.second) {
            // if local version smaller than src, check if existed in src, will not clone it
            bool existed_in_src = false;

            // if delta labeled with local_version is same with the specified version in clone header,
            // there is no necessity to clone it.
            for (int j = 0; j < clone_header.file_delta_size(); ++j) {
                if (clone_header.get_delta(j)->start_version() == local_version.first
                    && clone_header.get_delta(j)->end_version() == local_version.second
                    && clone_header.get_delta(j)->version_hash() == local_version_hash) {
                    existed_in_src = true;
                    LOG(INFO) << "Delta has already existed in local header, no need to clone."
                        << "tablet=" << tablet->full_name()
                        << ", version='" << local_version.first<< "-" << local_version.second
                        << ", version_hash=" << local_version_hash;

                    OLAPStatus delete_res = clone_header.delete_version(local_version);
                    if (delete_res != OLAP_SUCCESS) {
                        LOG(WARNING) << "failed to delete existed version from clone src when full clone. "
                                  << "clone_header_file=" << clone_header.file_name()
                                  << "version=" << local_version.first << "-" << local_version.second;
                        return delete_res;
                    }
                    break;
                }
            }

            // Delta labeled in local_version is not existed in clone header,
            // some overlapping delta will be cloned to replace it.
            // And also, the specified delta should deleted from local header.
            if (!existed_in_src) {
                versions_to_delete.push_back(local_version);
                LOG(INFO) << "Delete delta not included by the clone header, should delete it from local header."
                          << "tablet=" << tablet->full_name() << ","
                          << ", version=" << local_version.first<< "-" << local_version.second
                          << ", version_hash=" << local_version_hash;
            }
        }
    }
    vector<const PDelta*> clone_deltas;
    for (int i = 0; i < clone_header.file_delta_size(); ++i) {
        clone_deltas.push_back(clone_header.get_delta(i));
        LOG(INFO) << "Delta to clone."
            << "tablet=" << tablet->full_name() << ","
            << ", version=" << clone_header.get_delta(i)->start_version() << "-"
                << clone_header.get_delta(i)->end_version()
            << ", version_hash=" << clone_header.get_delta(i)->version_hash();
    }

    // clone_data to tablet
    OLAPStatus clone_res = tablet->clone_data(clone_header, clone_deltas, versions_to_delete);
    LOG(INFO) << "finish to full clone. [tablet=" << tablet->full_name() << ", res=" << clone_res << "]";
    return clone_res;
}

} // doris
