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

#include "cloud/cloud_storage_engine.h"

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"

namespace doris {

CloudStorageEngine::CloudStorageEngine(const UniqueId& backend_uid)
        : BaseStorageEngine(Type::CLOUD, backend_uid),
          _meta_mgr(std::make_unique<cloud::CloudMetaMgr>()),
          _tablet_mgr(std::make_unique<CloudTabletMgr>(*this)) {}

CloudStorageEngine::~CloudStorageEngine() = default;

Status CloudStorageEngine::open() {
    // TODO(plat1ko)
    return Status::OK();
}

void CloudStorageEngine::stop() {
    if (_stopped) {
        return;
    }

    _stopped = true;
    // TODO(plat1ko)
}

bool CloudStorageEngine::stopped() {
    return _stopped;
}

Result<BaseTabletSPtr> CloudStorageEngine::get_tablet(int64_t tablet_id) {
    return _tablet_mgr->get_tablet(tablet_id, false).transform([](auto&& t) {
        return static_pointer_cast<BaseTablet>(std::move(t));
    });
}

Status CloudStorageEngine::start_bg_threads() {
    // TODO(plat1ko)
    return Status::OK();
}

} // namespace doris
