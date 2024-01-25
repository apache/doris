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

#include <memory>

#include "olap/storage_engine.h"

namespace doris {
namespace cloud {
class CloudMetaMgr;
}

class CloudTabletMgr;

class CloudStorageEngine final : public BaseStorageEngine {
public:
    CloudStorageEngine(const UniqueId& backend_uid);

    ~CloudStorageEngine() override;

    Status open() override;
    void stop() override;
    bool stopped() override;

    Result<BaseTabletSPtr> get_tablet(int64_t tablet_id) override;

    Status start_bg_threads() override;

    Status set_cluster_id(int32_t cluster_id) override {
        _effective_cluster_id = cluster_id;
        return Status::OK();
    }

    cloud::CloudMetaMgr& meta_mgr() { return *_meta_mgr; }

    CloudTabletMgr& tablet_mgr() { return *_tablet_mgr; }

private:
    std::atomic_bool _stopped {false};

    std::unique_ptr<cloud::CloudMetaMgr> _meta_mgr;
    std::unique_ptr<CloudTabletMgr> _tablet_mgr;
};

} // namespace doris
