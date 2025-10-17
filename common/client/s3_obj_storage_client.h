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

#include <aws/core/client/AWSError.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CopyObjectResult.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/Error.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationResult.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <cpp/s3_rate_limiter.h>
#include <fmt/core.h>
#include <gen_cpp/Status_types.h>
#include <glog/logging.h>

#include <memory>
#include <ranges>

#include "client_bvar.h"
#include "cpp/stopwatch.h"
#include "cpp/sync_point.h"
#include "obj_storage_client.h"
#include "s3_common.h"

namespace Aws::S3 {
class S3Client;
namespace Model {
class CompletedPart;
}
} // namespace Aws::S3

namespace doris {

class S3ObjStorageClient final : public ObjStorageClient {
public:
    S3ObjStorageClient(std::shared_ptr<Aws::S3::S3Client> client, ObjectClientConfig config)
            : _config(std::move(config)), _client(std::move(client)) {}
    ~S3ObjStorageClient() override = default;
    ObjectStorageUploadResponse create_multipart_upload(
            const ObjectStoragePathOptions& opts) override;
    ObjectStorageResponse put_object(const ObjectStoragePathOptions& opts,
                                     std::string_view stream) override;
    ObjectStorageUploadResponse upload_part(const ObjectStoragePathOptions& opts, std::string_view,
                                            int partNum) override;
    ObjectStorageResponse complete_multipart_upload(
            const ObjectStoragePathOptions& opts,
            const std::vector<ObjectCompleteMultiPart>& completed_parts) override;
    ObjectStorageHeadResponse head_object(const ObjectStoragePathOptions& opts) override;
    ObjectStorageResponse get_object(const ObjectStoragePathOptions& opts, void* buffer,
                                     size_t offset, size_t bytes_read,
                                     size_t* size_return) override;
    std::unique_ptr<ObjectListIterator> list_objects(const ObjectStoragePathOptions& opts) override;
    ObjectStorageResponse delete_objects(const ObjectStoragePathOptions& opts,
                                         std::vector<std::string> objs) override;
    ObjectStorageResponse delete_object(const ObjectStoragePathOptions& opts) override;
    ObjectStorageResponse delete_objects_recursively(const ObjectStoragePathOptions& opts,
                                                     const std::string& prefix) override;
    std::string generate_presigned_url(const ObjectStoragePathOptions& opts,
                                       int64_t expiration_secs) override;
    ObjectStorageResponse get_life_cycle(const std::string& endpoint, const std::string& bucket,
                                         int64_t* expiration_days) override;

    ObjectStorageResponse check_versioning(const std::string& endpoint_,
                                           const std::string& bucket) override;

    ObjectStorageResponse abort_multipart_upload(const ObjectStoragePathOptions& opts,
                                                 const std::string& upload_id) override;

private:
    ObjectClientConfig _config;
    std::shared_ptr<Aws::S3::S3Client> _client;
};

class S3ObjListIterator final : public ObjectListIterator {
public:
    S3ObjListIterator(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket,
                      std::string prefix, std::string endpoint)
            : client_(std::move(client)), endpoint_(std::move(endpoint)) {
        req_.WithBucket(std::move(bucket)).WithPrefix(std::move(prefix));
        TEST_SYNC_POINT_CALLBACK("S3ObjListIterator", &req_);
    }

    ~S3ObjListIterator() override = default;

    ObjectStorageResponse has_next() override {
        if (!is_valid_) {
            return ObjectStorageResponse {
                    .status = ObjectStorageStatus {TStatusCode::INTERNAL_ERROR,
                                                   "Iterator is invalid"},
                    .http_code = 0,
                    .request_id = ""};
        }

        if (!results_.empty()) {
            return ObjectStorageResponse::OK();
        }

        if (!has_more_) {
            return ObjectStorageResponse {
                    .status = ObjectStorageStatus {TStatusCode::NOT_FOUND, "No more results"},
                    .http_code = 404,
                    .request_id = ""};
        }

        auto outcome = [&]() {
            SCOPED_BVAR_LATENCY(client_bvar::s3_list_latency);
            return client_->ListObjectsV2(req_);
        }();

        const auto& request_id = outcome.IsSuccess() ? outcome.GetResult().GetRequestId()
                                                     : outcome.GetError().GetRequestId();
        if (!outcome.IsSuccess()) {
            LOG(WARNING) << fmt::format(
                    "failed to list objects, endpoint: {}, bucket: {}, prefix: {}, responseCode: "
                    "{}, error: {}, request_id: {}",
                    endpoint_, req_.GetBucket(), req_.GetPrefix(),
                    static_cast<int>(outcome.GetError().GetResponseCode()),
                    outcome.GetError().GetMessage(), request_id);
            is_valid_ = false;
            return ObjectStorageResponse {
                    .status = ObjectStorageStatus {TStatusCode::INTERNAL_ERROR,
                                                   fmt::format(
                                                           "failed to list objects: {}, prefix: {}",
                                                           req_.GetBucket(), req_.GetPrefix())},
                    .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                    .request_id = request_id,
            };
        }

        if (outcome.GetResult().GetIsTruncated() &&
            outcome.GetResult().GetNextContinuationToken().empty()) {
            LOG(WARNING) << fmt::format(
                    "failed to list objects, isTruncated but no continuation token, "
                    "bucket: {}, prefix: {}, request_id: {}",
                    endpoint_, req_.GetBucket(), req_.GetPrefix(), request_id);
            is_valid_ = false;
            return ObjectStorageResponse {
                    .status = ObjectStorageStatus {TStatusCode::INTERNAL_ERROR,
                                                   fmt::format(
                                                           "failed to list objects: {}, prefix: {}",
                                                           req_.GetBucket(), req_.GetPrefix())},
                    .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                    .request_id = request_id,
            };
        }

        has_more_ = outcome.GetResult().GetIsTruncated();
        req_.SetContinuationToken(std::move(
                const_cast<std::string&&>(outcome.GetResult().GetNextContinuationToken())));

        auto&& content = outcome.GetResult().GetContents();
        DCHECK(!(has_more_ && content.empty()))
                << has_more_ << ' ' << content.empty() << " request_id=" << request_id;

        results_.reserve(content.size());
        for (auto&& obj : std::ranges::reverse_view(content)) {
            DCHECK(obj.GetKey().starts_with(req_.GetPrefix()))
                    << obj.GetKey() << ' ' << req_.GetPrefix();
            results_.emplace_back(
                    ObjectMeta {.file_path = std::move(const_cast<std::string&&>(obj.GetKey())),
                                .size = obj.GetSize(),
                                .mtime_s = obj.GetLastModified().Seconds()});
        }

        return ObjectStorageResponse::OK();
    }

private:
    std::shared_ptr<Aws::S3::S3Client> client_;
    Aws::S3::Model::ListObjectsV2Request req_;
    std::string endpoint_;
};

} // namespace doris
