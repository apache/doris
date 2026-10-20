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

#include "io/obj_storage/obj_client_factory.h"

#include <fmt/format.h>

#include <variant>

#include "cloud/config.h"
#include "common/config.h"
#include "cpp/obj-client/rate_limited_obj_storage_client.h"
#include "util/s3_rate_limiter_manager.h"
#include "util/s3_util.h"

namespace doris::io {
namespace {

ObjStorageResponse make_be_rate_limit_response(S3RateLimitType type,
                                               S3RateLimitRejectReason reason) {
    const auto* limit_type = reason == S3RateLimitRejectReason::QPS ? "QPS" : "bytes";
    // A local admission rejection is not an S3 HTTP 429. Keep the merged #65420 behavior so S3
    // readers do not retry it as provider throttling.
    return ObjStorageResponse::rate_limit(
            ErrorCode::EXCEEDED_LIMIT, 0,
            fmt::format("s3 {} request exceeds {} limit, rejected by BE rate limiter",
                        to_string(type), limit_type));
}

class BeObjStorageRateLimitPolicy final : public ObjStorageRateLimitPolicy {
public:
    ObjStorageAdmission acquire(S3RateLimitType type, size_t estimated_bytes) const override {
        auto guard = std::make_shared<S3RateLimitGuard>(type, estimated_bytes);
        if (!guard->ok()) {
            return ObjStorageAdmission {
                    .resp = make_be_rate_limit_response(type, guard->reject_reason()),
            };
        }
        return ObjStorageAdmission {
                .settle = [guard = std::move(guard)](
                                  size_t actual_bytes) { guard->settle(actual_bytes); },
        };
    }
};

} // namespace

ObjClientFactory::ObjClientFactory() {
    // Complete AWS SDK initialization before this singleton finishes construction. Function
    // local statics are destroyed in reverse order of construction, so the cached AWS
    // clients held by the providers are released before S3ClientFactory shuts the SDK down.
    S3ClientFactory::instance();
}

ObjClientFactory& ObjClientFactory::instance() {
    static ObjClientFactory ret;
    return ret;
}

Result<std::shared_ptr<ObjStorageClient>> ObjClientFactory::get_or_create(
        const ObjClientConfig& config) {
    return std::visit([&](const auto& backend) { return _dispatch(config, backend); },
                      config.backend);
}

Result<std::shared_ptr<ObjStorageClient>> ObjClientFactory::_dispatch(const ObjClientConfig& config,
                                                                      const S3ClientConfig& s3) {
    return _s3.get_or_create(config, s3);
}

Result<std::shared_ptr<ObjStorageClient>> ObjClientFactory::_dispatch(
        const ObjClientConfig& config, const AzureClientConfig& azure) {
    return _azure.get_or_create(config, azure);
}

std::shared_ptr<ObjStorageClient> apply_rate_limit_policy(std::shared_ptr<ObjStorageClient> client,
                                                          ClientUsage usage) {
    if (!config::is_cloud_mode() || usage == ClientUsage::INTERNAL_STORAGE) {
        return std::make_shared<RateLimitedObjStorageClient>(
                std::move(client), std::make_shared<BeObjStorageRateLimitPolicy>());
    }
    return client;
}

} // namespace doris::io

namespace doris::config {

// A lowered capacity takes effect at once instead of on the next client publish, so a cache that
// only serves hits still shrinks. The callback runs after the new value passed its validator, so
// it is always positive.
DEFINE_ON_UPDATE(azure_client_cache_capacity, [](int32_t /*old_value*/, int32_t new_value) {
    io::ObjClientFactory::instance().shrink_azure_client_cache(static_cast<size_t>(new_value));
});

} // namespace doris::config
