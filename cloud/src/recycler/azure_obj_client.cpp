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

#include "recycler/azure_obj_client.h"

#include <fmt/core.h>
#include <glog/logging.h>

#include <algorithm>
#include <azure/core/datetime.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/blobs/rest_client.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/storage/common/storage_exception.hpp>
#include <iterator>
#include <ranges>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/s3_rate_limiter.h"
#include "cpp/sync_point.h"
#include "recycler/s3_accessor.h"

using namespace Azure::Storage::Blobs;

namespace doris::cloud {

template <typename Func>
auto s3_rate_limit(S3RateLimitType op, Func callback) -> decltype(callback()) {
    using T = decltype(callback());
    if (!config::enable_s3_rate_limiter) {
        return callback();
    }
    auto sleep_duration = AccessorRateLimiter::instance().rate_limiter(op)->add(1);
    if (sleep_duration < 0) {
        throw std::runtime_error("Azure exceeds request limit");
    }
    return callback();
}

template <typename Func>
auto s3_get_rate_limit(Func callback) -> decltype(callback()) {
    return s3_rate_limit(S3RateLimitType::GET, std::move(callback));
}

template <typename Func>
auto s3_put_rate_limit(Func callback) -> decltype(callback()) {
    return s3_rate_limit(S3RateLimitType::PUT, std::move(callback));
}

static constexpr size_t BlobBatchMaxOperations = 256;
static constexpr char BlobNotFound[] = "BlobNotFound";

template <typename Func>
ObjectStorageResponse do_azure_client_call(Func f, std::string_view url, std::string_view key) {
    try {
        f();
    } catch (Azure::Core::RequestFailedException& e) {
        auto msg = fmt::format(
                "Azure request failed because {}, http_code: {}, request_id: {}, url: {}, "
                "key: {}",
                e.Message, static_cast<int>(e.StatusCode), e.RequestId, url, key);
        LOG_WARNING(msg);
        return {-1, std::move(msg)};
    } catch (std::exception& e) {
        auto msg = fmt::format("Azure request failed because {}, url: {}, key: {}", e.what(), url,
                               key);
        LOG_WARNING(msg);
        return {-1, std::move(msg)};
    }
    return {};
}

static const Azure::DateTime SystemClockEpoch {1970, 1, 1};

class AzureListIterator final : public ObjectListIterator {
public:
    AzureListIterator(std::shared_ptr<BlobContainerClient> client, std::string prefix)
            : client_(std::move(client)), req_({.Prefix = std::move(prefix)}) {
        TEST_SYNC_POINT_CALLBACK("AzureListIterator", &req_);
    }

    ~AzureListIterator() override = default;

    bool is_valid() override { return is_valid_; }

    bool has_next() override {
        if (!is_valid_) {
            return false;
        }

        if (!results_.empty()) {
            return true;
        }

        if (!has_more_) {
            return false;
        }

        try {
            auto resp = s3_get_rate_limit([&]() { return client_->ListBlobs(req_); });
            has_more_ = resp.NextPageToken.HasValue();
            DCHECK(!(has_more_ && resp.Blobs.empty())) << has_more_ << ' ' << resp.Blobs.empty();
            req_.ContinuationToken = std::move(resp.NextPageToken);
            results_.reserve(resp.Blobs.size());
            for (auto&& item : std::ranges::reverse_view(resp.Blobs)) {
                DCHECK(item.Name.starts_with(*req_.Prefix)) << item.Name << ' ' << *req_.Prefix;
                results_.emplace_back(ObjectMeta {
                        .key = std::move(item.Name),
                        .size = item.BlobSize,
                        // `Azure::DateTime` adds the offset of `SystemClockEpoch` to the given Unix timestamp,
                        // so here we need to subtract this offset to obtain the Unix timestamp of the mtime.
                        // https://github.com/Azure/azure-sdk-for-cpp/blob/azure-core_1.12.0/sdk/core/azure-core/inc/azure/core/datetime.hpp#L129
                        .mtime_s = duration_cast<std::chrono::seconds>(item.Details.LastModified -
                                                                       SystemClockEpoch)
                                           .count()});
            }
        } catch (Azure::Storage::StorageException& e) {
            LOG_WARNING(
                    "Azure request failed because {}, http_code: {}, request_id: {}, url: {}, "
                    "prefix: {}",
                    e.Message, static_cast<int>(e.StatusCode), e.RequestId, client_->GetUrl(),
                    req_.Prefix.Value());
            is_valid_ = false;
            return false;
        }

        return !results_.empty();
    }

    std::optional<ObjectMeta> next() override {
        std::optional<ObjectMeta> res;
        if (!has_next()) {
            return res;
        }

        res = std::move(results_.back());
        results_.pop_back();
        return res;
    }

private:
    std::shared_ptr<BlobContainerClient> client_;
    ListBlobsOptions req_;
    std::vector<ObjectMeta> results_;
    bool is_valid_ {true};
    bool has_more_ {true};
};

AzureObjClient::~AzureObjClient() = default;

ObjectStorageResponse AzureObjClient::put_object(ObjectStoragePathRef path,
                                                 std::string_view stream) {
    auto client = client_->GetBlockBlobClient(path.key);
    return do_azure_client_call(
            [&]() {
                s3_put_rate_limit([&]() {
                    return client.UploadFrom(reinterpret_cast<const uint8_t*>(stream.data()),
                                             stream.size());
                });
            },
            client_->GetUrl(), path.key);
}

ObjectStorageResponse AzureObjClient::head_object(ObjectStoragePathRef path, ObjectMeta* res) {
    try {
        auto&& properties = s3_get_rate_limit(
                [&]() { return client_->GetBlockBlobClient(path.key).GetProperties().Value; });
        res->key = path.key;
        res->mtime_s = properties.LastModified.time_since_epoch().count();
        res->size = properties.BlobSize;
        return {0};
    } catch (Azure::Storage::StorageException& e) {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
            return {1};
        }
        auto msg = fmt::format(
                "Head azure blob failed because {}, http_code: {}, request_id: {}, url: {}, "
                "key: {}",
                e.Message, static_cast<int>(e.StatusCode), e.RequestId, client_->GetUrl(),
                path.key);
        LOG_WARNING(msg);
        return {-1, std::move(msg)};
    }
}

std::unique_ptr<ObjectListIterator> AzureObjClient::list_objects(ObjectStoragePathRef path) {
    return std::make_unique<AzureListIterator>(client_, path.key);
}

// As Azure's doc said, the batch size is 256
// You can find out the num in https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=microsoft-entra-id
// > Each batch request supports a maximum of 256 subrequests.
ObjectStorageResponse AzureObjClient::delete_objects(const std::string& bucket,
                                                     std::vector<std::string> keys) {
    if (keys.empty()) {
        return {0};
    }

    // TODO(ByteYue) : use range to adate this code when compiler is ready
    // auto chunkedView = objs | std::views::chunk(BlobBatchMaxOperations);
    auto begin = std::begin(keys);
    auto end = std::end(keys);

    while (begin != end) {
        auto batch = client_->CreateBatch();
        auto chunk_end = begin;
        size_t batch_size = BlobBatchMaxOperations;
        TEST_SYNC_POINT_CALLBACK("AzureObjClient::delete_objects", &batch_size);
        std::advance(chunk_end,
                     std::min(batch_size, static_cast<size_t>(std::distance(begin, end))));
        std::vector<Azure::Storage::DeferredResponse<Models::DeleteBlobResult>> deferred_resps;
        deferred_resps.reserve(std::distance(begin, chunk_end));
        for (auto it = begin; it != chunk_end; ++it) {
            deferred_resps.emplace_back(batch.DeleteBlob(*it));
        }
        auto resp = do_azure_client_call(
                [&]() { s3_put_rate_limit([&]() { return client_->SubmitBatch(batch); }); },
                client_->GetUrl(), *begin);
        if (resp.ret != 0) {
            return resp;
        }
        for (auto&& defer : deferred_resps) {
            try {
                auto r = defer.GetResponse();
                if (!r.Value.Deleted) {
                    LOG_INFO("Azure batch delete failed, url {}", client_->GetUrl());
                    return {-1};
                }
            } catch (Azure::Storage::StorageException& e) {
                if (Azure::Core::Http::HttpStatusCode::NotFound == e.StatusCode &&
                    0 == strcmp(e.ErrorCode.c_str(), BlobNotFound)) {
                    continue;
                }
                auto msg = fmt::format(
                        "Azure request failed because {}, http code {}, request id {}, url {}",
                        e.Message, static_cast<int>(e.StatusCode), e.RequestId, client_->GetUrl());
                LOG_WARNING(msg);
                return {-1, std::move(msg)};
            }
        }

        begin = chunk_end;
    }

    return {0};
}

ObjectStorageResponse AzureObjClient::delete_object(ObjectStoragePathRef path) {
    return do_azure_client_call(
            [&]() {
                if (auto r = s3_put_rate_limit([&]() { return client_->DeleteBlob(path.key); });
                    !r.Value.Deleted) {
                    throw std::runtime_error("Delete azure blob failed");
                }
            },
            client_->GetUrl(), path.key);
}

ObjectStorageResponse AzureObjClient::delete_objects_recursively(ObjectStoragePathRef path,
                                                                 int64_t expiration_time) {
    return delete_objects_recursively_(path, expiration_time, BlobBatchMaxOperations);
}

ObjectStorageResponse AzureObjClient::get_life_cycle(const std::string& bucket,
                                                     int64_t* expiration_days) {
    // TODO(plat1ko)
    *expiration_days = INT64_MAX;
    return {0};
}

ObjectStorageResponse AzureObjClient::check_versioning(const std::string& bucket) {
    // TODO(plat1ko)
    return {0};
}

} // namespace doris::cloud