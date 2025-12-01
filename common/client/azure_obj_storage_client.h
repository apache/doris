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

#include <aws/core/utils/Array.h>
#include <aws/core/utils/HashingUtils.h>
#include <cpp/s3_rate_limiter.h>
#include <cpp/sync_point.h>
#include <fmt/core.h>
#include <gen_cpp/Status_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <azure/core/http/http.hpp>
#include <azure/core/http/http_status_code.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_batch.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#ifdef USE_AZURE
#include <azure/core/diagnostics/logger.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#endif
#include <azure/storage/blobs/blob_sas_builder.hpp>
#include <azure/storage/blobs/rest_client.hpp>
#include <azure/storage/common/account_sas_builder.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/storage/common/storage_exception.hpp>
#include <chrono>
#include <exception>
#include <iterator>
#include <ranges>

#include "client_bvar.h"
#include "cpp/stopwatch.h"
#include "cpp/util.h"
#include "obj_storage_client.h"
#include "s3_common.h"

namespace Azure::Storage::Blobs {
class BlobContainerClient;
} // namespace Azure::Storage::Blobs

namespace doris {

using namespace Azure::Storage::Blobs;

static const Azure::DateTime SystemClockEpoch {1970, 1, 1};

class AzureListIterator final : public ObjectListIterator {
public:
    AzureListIterator(std::shared_ptr<BlobContainerClient> client, std::string prefix)
            : client_(std::move(client)) {
        req_.Prefix = std::move(prefix);
        TEST_SYNC_POINT_CALLBACK("AzureListIterator", &req_);
    }

    ~AzureListIterator() override = default;

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

        try {
            auto resp = [&]() {
                SCOPED_BVAR_LATENCY(client_bvar::s3_list_latency);
                return client_->ListBlobs(req_);
            }();
            has_more_ = resp.NextPageToken.HasValue();
            DCHECK(!(has_more_ && resp.Blobs.empty())) << has_more_ << ' ' << resp.Blobs.empty();
            req_.ContinuationToken = std::move(resp.NextPageToken);
            results_.reserve(resp.Blobs.size());
            for (auto&& item : std::ranges::reverse_view(resp.Blobs)) {
                DCHECK(item.Name.starts_with(*req_.Prefix)) << item.Name << ' ' << *req_.Prefix;
                results_.emplace_back(ObjectMeta {
                        .file_path = std::move(item.Name),
                        .size = item.BlobSize,
                        // `Azure::DateTime` adds the offset of `SystemClockEpoch` to the given Unix timestamp,
                        // so here we need to subtract this offset to obtain the Unix timestamp of the mtime.
                        // https://github.com/Azure/azure-sdk-for-cpp/blob/azure-core_1.12.0/sdk/core/azure-core/inc/azure/core/datetime.hpp#L129
                        .mtime_s = duration_cast<std::chrono::seconds>(item.Details.LastModified -
                                                                       SystemClockEpoch)
                                           .count()});
            }
        } catch (Azure::Core::RequestFailedException& e) {
            LOG(WARNING) << fmt::format("Azure request failed because {}, url: {}, prefix: {}",
                                        e.what(), client_->GetUrl(), req_.Prefix.Value());
            is_valid_ = false;
            return ObjectStorageResponse {
                    .status = ObjectStorageStatus {TStatusCode::INTERNAL_ERROR, e.Message},
                    .http_code = static_cast<int>(e.StatusCode),
                    .request_id = std::move(e.RequestId),
            };
        } catch (std::exception& e) {
            LOG(WARNING) << fmt::format("Azure request failed because {}, url: {}, prefix: {}",
                                        e.what(), client_->GetUrl(), req_.Prefix.Value());
            is_valid_ = false;
            return ObjectStorageResponse {
                    .status = ObjectStorageStatus {TStatusCode::INTERNAL_ERROR, e.what()},
                    .http_code = 0,
                    .request_id = "",
            };
        }

        return ObjectStorageResponse::OK();
    }

private:
    std::shared_ptr<BlobContainerClient> client_;
    ListBlobsOptions req_;
};

class ObjClientHolder;

class AzureObjStorageClient final : public ObjStorageClient {
public:
    AzureObjStorageClient(std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> client,
                          ObjectClientConfig config)
            : _config(std::move(config)), _client(std::move(client)) {}
    ~AzureObjStorageClient() override = default;
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
    std::unique_ptr<ObjectListIterator> list_objects(const ObjectStoragePathOptions& path) override;
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
    std::shared_ptr<Azure::Storage::Blobs::BlobContainerClient> _client;
};

} // namespace doris