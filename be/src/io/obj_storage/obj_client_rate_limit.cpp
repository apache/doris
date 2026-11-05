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

#include "io/obj_storage/obj_client_rate_limit.h"

#include <fmt/format.h>

#include <utility>

#include "cloud/config.h"
#include "cpp/obj-client/rate_limited_obj_storage_client.h"
#include "util/s3_rate_limiter_manager.h"

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

std::shared_ptr<ObjStorageClient> apply_rate_limit_policy(std::shared_ptr<ObjStorageClient> client,
                                                          ClientUsage usage) {
    if (!config::is_cloud_mode() || usage == ClientUsage::INTERNAL_STORAGE) {
        return std::make_shared<RateLimitedObjStorageClient>(
                std::move(client), std::make_shared<BeObjStorageRateLimitPolicy>());
    }
    return client;
}

} // namespace doris::io
