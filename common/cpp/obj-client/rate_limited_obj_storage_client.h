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

#include <functional>
#include <memory>
#include <utility>

#include "cpp/token_bucket_rate_limiter.h"
#include "obj_storage_client.h"

namespace doris {

struct ObjStorageAdmission {
    ObjStorageResponse resp = ObjStorageResponse::OK();
    std::function<void(size_t)> settle {};

    void settle_bytes(size_t actual_bytes) const {
        if (settle) {
            settle(actual_bytes);
        }
    }
};

// Implementations are in be/src/util/s3_util.cpp and cloud/src/recycler/s3_accessor.cpp.
class ObjStorageRateLimitPolicy {
public:
    virtual ~ObjStorageRateLimitPolicy() = default;
    virtual ObjStorageAdmission acquire(S3RateLimitType type, size_t estimated_bytes) const = 0;
};

class RateLimitedObjStorageClient final : public ObjStorageClient {
public:
    RateLimitedObjStorageClient(std::shared_ptr<ObjStorageClient> inner,
                                std::shared_ptr<const ObjStorageRateLimitPolicy> rate_limit_policy)
            : inner_(std::move(inner)), rate_limit_policy_(std::move(rate_limit_policy)) {}
    ~RateLimitedObjStorageClient() override = default;

    ObjStorageUploadResult create_multipart_upload(const ObjStoragePath& opts) override;
    ObjStorageResponse put_object(const ObjStoragePath& opts, std::string_view stream) override;
    ObjStorageUploadResult upload_part(const ObjStoragePath& opts, const std::string& upload_id,
                                       std::string_view stream, int part_num) override;
    ObjStorageResponse complete_multipart_upload(
            const ObjStoragePath& opts, const std::string& upload_id,
            const std::vector<ObjStorageCompletedPart>& completed_parts) override;
    ObjStorageHeadResult head_object(const ObjStoragePath& opts) override;
    ObjStorageResponse get_object(const ObjStoragePath& opts, void* buffer, size_t offset,
                                  size_t bytes_read, size_t* size_return) override;
    ObjStorageResponse delete_objects(const ObjStoragePath& opts,
                                      std::vector<std::string> objs) override;
    ObjStorageResponse delete_object(const ObjStoragePath& opts) override;
    ObjStorageCapabilities capabilities() const override { return inner_->capabilities(); }
    std::string generate_presigned_url(const ObjStoragePath& opts,
                                       int64_t expiration_secs) override;
    ObjStorageResponse get_lifecycle(const std::string& bucket, int64_t* expiration_days) override;
    ObjStorageResponse check_versioning(const std::string& bucket) override;
    ObjStorageResponse abort_multipart_upload(const ObjStoragePath& opts,
                                              const std::string& upload_id) override;

protected:
    ObjStorageListPageResult list_objects_page(const ObjStoragePath& opts,
                                               std::string_view continuation_token) override;

private:
    ObjStorageAdmission acquire(S3RateLimitType type, size_t estimated_bytes = 0) const;

    std::shared_ptr<ObjStorageClient> inner_;
    std::shared_ptr<const ObjStorageRateLimitPolicy> rate_limit_policy_;
};

} // namespace doris

namespace doris::io {
using ::doris::ObjStorageAdmission;
using ::doris::ObjStorageRateLimitPolicy;
using ::doris::RateLimitedObjStorageClient;
using ::doris::S3RateLimitType;
} // namespace doris::io
