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

#include "io/object_storage/obj_storage_client_holder.h"

#include <mutex>
#include <utility>

#include "common/logging.h"
#include "cpp/obj-storage/rate_limited_obj_storage_client.h"
#include "io/object_storage/obj_storage_client_factory.h"
#include "io/object_storage/obj_storage_rate_limiter_manager.h"

namespace doris::io {

ObjStorageClientHolder::ObjStorageClientHolder(ObjStorageConfig conf)
        : _rate_limit_policy(ObjStorageRateLimiterManager::policy_for(
                  storage_config_base(conf).is_internal_bucket)),
          _conf(std::move(conf)) {}

ObjStorageClientHolder::~ObjStorageClientHolder() = default;

Result<std::shared_ptr<ObjStorageClient>> ObjStorageClientHolder::create_client(
        const ObjStorageConfig& conf) const {
    auto client = ObjStorageClientFactory::instance().create(conf);
    if (client && _rate_limit_policy) {
        *client = std::make_shared<RateLimitedObjStorageClient>(std::move(*client),
                                                                _rate_limit_policy);
    }
    return client;
}

Status ObjStorageClientHolder::init() {
    _client = DORIS_TRY(create_client(_conf));
    return Status::OK();
}

Status ObjStorageClientHolder::reset(const ObjStorageConfig& conf) {
    std::lock_guard update_lock(_update_mutex);
    if (!same_storage_target(_conf, conf)) {
        return Status::InvalidArgument(
                "Cannot change storage target on an open object storage filesystem");
    }
    if (conf == _conf) {
        return Status::OK();
    }

    auto client = DORIS_TRY(create_client(conf));
    LOG(INFO) << "reset object storage client with new conf: "
              << obj_storage_config_to_string(conf);
    std::lock_guard lock(_mtx);
    _client = std::move(client);
    _conf = conf;
    return Status::OK();
}

} // namespace doris::io
