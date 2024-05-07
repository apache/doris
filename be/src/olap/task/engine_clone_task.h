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

#pragma once

#include <gen_cpp/Types_types.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/tablet_fwd.h"
#include "olap/task/engine_task.h"

namespace doris {
class DataDir;
class TCloneReq;
class TMasterInfo;
class TTabletInfo;
class Tablet;
struct Version;
class StorageEngine;

const std::string HTTP_REQUEST_PREFIX = "/api/_tablet/_download?";
const std::string HTTP_REQUEST_TOKEN_PARAM = "token=";
const std::string HTTP_REQUEST_FILE_PARAM = "&file=";
const uint32_t DOWNLOAD_FILE_MAX_RETRY = 3;
const uint32_t LIST_REMOTE_FILE_TIMEOUT = 15;
const uint32_t GET_LENGTH_TIMEOUT = 10;

// base class for storage engine
// add "Engine" as task prefix to prevent duplicate name with agent task
class EngineCloneTask final : public EngineTask {
public:
    Status execute() override;

    EngineCloneTask(StorageEngine& engine, const TCloneReq& clone_req,
                    const TMasterInfo& master_info, int64_t signature,
                    std::vector<TTabletInfo>* tablet_infos);
    ~EngineCloneTask() override = default;

private:
    Status _do_clone();

    virtual Status _finish_clone(Tablet* tablet, const std::string& clone_dir, int64_t version,
                                 bool is_incremental_clone);

    Status _finish_incremental_clone(Tablet* tablet, const TabletMetaSharedPtr& cloned_tablet_meta,
                                     int64_t version);

    Status _finish_full_clone(Tablet* tablet, const TabletMetaSharedPtr& cloned_tablet_meta);

    Status _make_and_download_snapshots(DataDir& data_dir, const std::string& local_data_path,
                                        TBackend* src_host, std::string* src_file_path,
                                        const std::vector<Version>& missing_versions,
                                        bool* allow_incremental_clone);

    Status _set_tablet_info(bool is_new_tablet);

    // Download tablet files from
    Status _download_files(DataDir* data_dir, const std::string& remote_url_prefix,
                           const std::string& local_path);

    Status _make_snapshot(const std::string& ip, int port, TTableId tablet_id,
                          TSchemaHash schema_hash, int timeout_s,
                          const std::vector<Version>& missing_versions, std::string* snapshot_path,
                          bool* allow_incremental_clone);

    Status _release_snapshot(const std::string& ip, int port, const std::string& snapshot_path);

    std::string _mask_token(const std::string& str);

private:
    StorageEngine& _engine;
    const TCloneReq& _clone_req;
    std::vector<TTabletInfo>* _tablet_infos = nullptr;
    int64_t _signature;
    const TMasterInfo& _master_info;
    int64_t _copy_size;
    int64_t _copy_time_ms;
    std::vector<PendingRowsetGuard> _pending_rs_guards;
}; // EngineTask

} // namespace doris