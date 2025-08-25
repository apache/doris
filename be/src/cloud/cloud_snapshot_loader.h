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

#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "olap/storage_policy.h"
#include "runtime/snapshot_loader.h"

namespace doris {

class CloudStorageEngine;

class CloudSnapshotLoader : public BaseSnapshotLoader {
public:
    CloudSnapshotLoader(CloudStorageEngine& engine, ExecEnv* env, int64_t job_id, int64_t task_id,
                        const TNetworkAddress& broker_addr = {},
                        const std::map<std::string, std::string>& broker_prop = {});

    ~CloudSnapshotLoader() override {};

    io::RemoteFileSystemSPtr storage_fs();

    Status init(TStorageBackendType::type type, const std::string& location, std::string vault_id);

    Status upload(const std::map<std::string, std::string>& src_to_dest_path,
                  std::map<int64_t, std::vector<std::string>>* tablet_files) override;

    Status download(const std::map<std::string, std::string>& src_to_dest_path,
                    std::vector<int64_t>* downloaded_tablet_ids) override;

protected:
    std::optional<StorageResource> _storage_resource;

private:
    CloudStorageEngine& _engine;
};

} // end namespace doris
