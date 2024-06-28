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
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/blobs/rest_client.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/storage/common/storage_exception.hpp>
#include <iterator>
#include <stdexcept>

#include "common/logging.h"
#include "recycler/obj_store_accessor.h"

using namespace Azure::Storage::Blobs;

namespace doris::cloud {

constexpr size_t BlobBatchMaxOperations = 256;
constexpr char BlobNotFound[] = "BlobNotFound";

template <typename Func>
ObjectStorageResponse do_azure_client_call(Func f, const ObjectStoragePathOptions& opts) {
    try {
        f();
    } catch (Azure::Storage::StorageException& e) {
        auto msg = fmt::format(
                "Azure request failed because {}, http code {}, request id {}, bucket {}, key {}, "
                "prefix {}, endpoint {}",
                e.Message, static_cast<int>(e.StatusCode), e.RequestId, opts.bucket, opts.key,
                opts.prefix, opts.endpoint);
        LOG_WARNING(msg);
        return {-1, std::move(msg)};
    } catch (std::exception& e) {
        auto msg = fmt::format(
                "Azure request failed because {}, bucket {}, key {}, prefix {}, endpoint {}",
                e.what(), opts.bucket, opts.key, opts.prefix, opts.endpoint);
        LOG_WARNING(msg);
        return {-1, std::move(msg)};
    }
    return ObjectStorageResponse::OK();
}

struct AzureBatchDeleter {
    AzureBatchDeleter(BlobContainerClient* client, const ObjectStoragePathOptions& opts)
            : _client(client), _batch(client->CreateBatch()), _opts(opts) {}
    // Submit one blob to be deleted in `AzureBatchDeleter::execute`
    void delete_blob(const std::string& blob_name) {
        deferred_resps.emplace_back(_batch.DeleteBlob(blob_name));
    }
    ObjectStorageResponse execute() {
        if (deferred_resps.empty()) {
            return ObjectStorageResponse::OK();
        }
        auto resp = do_azure_client_call([&]() { _client->SubmitBatch(_batch); }, _opts);
        if (resp.ret != 0) {
            return resp;
        }

        for (auto&& defer_response : deferred_resps) {
            try {
                auto r = defer_response.GetResponse();
                if (!r.Value.Deleted) {
                    LOG_INFO("Azure batch delete failed, bucket {}, key {}, prefix {}, endpoint {}",
                             _opts.bucket, _opts.key, _opts.prefix, _opts.endpoint);
                    return {-1};
                }
            } catch (Azure::Storage::StorageException& e) {
                if (Azure::Core::Http::HttpStatusCode::NotFound == e.StatusCode &&
                    0 == strcmp(e.ErrorCode.c_str(), BlobNotFound)) {
                    continue;
                }
                auto msg = fmt::format(
                        "Azure request failed because {}, http code {}, request id {}, bucket {}, "
                        "key {}, "
                        "prefix {}, endpoint {}",
                        e.Message, static_cast<int>(e.StatusCode), e.RequestId, _opts.bucket,
                        _opts.key, _opts.prefix, _opts.endpoint);
                LOG_WARNING(msg);
                return {-1, std::move(msg)};
            }
        }

        return ObjectStorageResponse::OK();
    }

private:
    BlobContainerClient* _client;
    BlobContainerBatch _batch;
    const ObjectStoragePathOptions& _opts;
    std::vector<Azure::Storage::DeferredResponse<Models::DeleteBlobResult>> deferred_resps;
};

ObjectStorageResponse AzureObjClient::put_object(const ObjectStoragePathOptions& opts,
                                                 std::string_view stream) {
    auto client = _client->GetBlockBlobClient(opts.key);
    return do_azure_client_call(
            [&]() {
                client.UploadFrom(reinterpret_cast<const uint8_t*>(stream.data()), stream.size());
            },
            opts);
}

ObjectStorageResponse AzureObjClient::head_object(const ObjectStoragePathOptions& opts) {
    try {
        Models::BlobProperties properties =
                _client->GetBlockBlobClient(opts.key).GetProperties().Value;
        return ObjectStorageResponse::OK();
    } catch (Azure::Storage::StorageException& e) {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
            return {1};
        }
        auto msg = fmt::format(
                "Failed to head azure blob due to {}, http code {}, request id {}, "
                "bucket {}, key {}, prefix {}, endpoint {}",
                e.Message, static_cast<int>(e.StatusCode), e.RequestId, opts.bucket, opts.key,
                opts.prefix, opts.endpoint);
        LOG_WARNING(msg);
        return {-1, std::move(msg)};
    }
}

ObjectStorageResponse AzureObjClient::list_objects(const ObjectStoragePathOptions& opts,
                                                   std::vector<ObjectMeta>* files) {
    auto get_object_meta = [&](auto&& resp) {
        std::ranges::transform(
                resp.Blobs, std::back_inserter(*files), [](auto&& blob_item) -> ObjectMeta {
                    return {.path = std::move(blob_item.Name),
                            .size = blob_item.BlobSize,
                            .last_modify_second =
                                    blob_item.Details.LastModified.time_since_epoch().count()};
                });
    };
    return do_azure_client_call(
            [&]() {
                ListBlobsOptions list_opts;
                list_opts.Prefix = opts.prefix;
                auto resp = _client->ListBlobs(list_opts);
                get_object_meta(resp);
                while (!resp.NextPageToken->empty()) {
                    list_opts.ContinuationToken = resp.NextPageToken;
                    resp = _client->ListBlobs(list_opts);
                    get_object_meta(resp);
                }
            },
            opts);
}

// As Azure's doc said, the batch size is 256
// You can find out the num in https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=microsoft-entra-id
// > Each batch request supports a maximum of 256 subrequests.
ObjectStorageResponse AzureObjClient::delete_objects(const ObjectStoragePathOptions& opts,
                                                     std::vector<std::string> objs) {
    if (objs.empty()) {
        LOG_INFO("No objects to delete").tag("endpoint", opts.endpoint).tag("bucket", opts.bucket);
        return ObjectStorageResponse::OK();
    }
    // TODO(ByteYue) : use range to adate this code when compiler is ready
    // auto chunkedView = objs | std::views::chunk(BlobBatchMaxOperations);
    auto begin = std::begin(objs);
    auto end = std::end(objs);

    while (begin != end) {
        auto deleter = AzureBatchDeleter(_client.get(), opts);
        auto chunk_end = begin;
        std::advance(chunk_end, std::min(BlobBatchMaxOperations,
                                         static_cast<size_t>(std::distance(begin, end))));

        std::ranges::for_each(std::ranges::subrange(begin, chunk_end),
                              [&](const std::string& obj) { deleter.delete_blob(obj); });
        begin = chunk_end;
        if (auto resp = deleter.execute(); resp.ret != 0) {
            return resp;
        }
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse AzureObjClient::delete_object(const ObjectStoragePathOptions& opts) {
    return do_azure_client_call(
            [&]() {
                auto resp = _client->DeleteBlob(opts.key);
                if (!resp.Value.Deleted) {
                    throw std::runtime_error("Delete azure blob failed");
                }
            },
            opts);
}

ObjectStorageResponse AzureObjClient::delete_objects_recursively(
        const ObjectStoragePathOptions& opts) {
    ListBlobsOptions list_opts;
    list_opts.Prefix = opts.prefix;
    list_opts.PageSizeHint = BlobBatchMaxOperations;
    auto delete_func = [&](const std::vector<Models::BlobItem>& blobs) -> ObjectStorageResponse {
        auto deleter = AzureBatchDeleter(_client.get(), opts);
        auto batch = _client->CreateBatch();
        for (auto&& blob_item : blobs) {
            deleter.delete_blob(blob_item.Name);
        }
        if (auto response = deleter.execute(); response.ret != 0) {
            return response;
        }
        return ObjectStorageResponse::OK();
    };
    auto resp = _client->ListBlobs(list_opts);
    if (auto response = delete_func(resp.Blobs); response.ret != 0) {
        return response;
    }
    while (!resp.NextPageToken->empty()) {
        list_opts.ContinuationToken = resp.NextPageToken;
        resp = _client->ListBlobs(list_opts);
        auto response = delete_func(resp.Blobs);
        if (response.ret != 0) {
            return response;
        }
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse AzureObjClient::delete_expired(const ObjectStorageDeleteExpiredOptions& opts,
                                                     int64_t expired_time) {
    ListBlobsOptions list_opts;
    list_opts.Prefix = opts.path_opts.prefix;
    list_opts.PageSizeHint = BlobBatchMaxOperations;
    auto delete_func = [&](const std::vector<Models::BlobItem>& blobs) -> ObjectStorageResponse {
        auto deleter = AzureBatchDeleter(_client.get(), opts.path_opts);
        auto batch = _client->CreateBatch();
        for (auto&& blob_item : blobs) {
            if (blob_item.Details.LastModified.time_since_epoch().count() < expired_time) {
                batch.DeleteBlob(blob_item.Name);
            }
        }
        if (auto response = deleter.execute(); response.ret != 0) {
            return response;
        }
        return ObjectStorageResponse::OK();
    };
    auto resp = _client->ListBlobs(list_opts);
    auto response = delete_func(resp.Blobs);
    if (response.ret != 0) {
        return response;
    }
    while (!resp.NextPageToken->empty()) {
        list_opts.ContinuationToken = resp.NextPageToken;
        resp = _client->ListBlobs(list_opts);
        auto response = delete_func(resp.Blobs);
        if (response.ret != 0) {
            return response;
        }
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse AzureObjClient::get_life_cycle(const ObjectStoragePathOptions& opts,
                                                     int64_t* expiration_days) {
    return {-1};
}

ObjectStorageResponse AzureObjClient::check_versioning(const ObjectStoragePathOptions& opts) {
    return {-1};
}

const std::shared_ptr<Aws::S3::S3Client>& AzureObjClient::s3_client() {
    CHECK(true) << "Currently this is unreachable";
    // TODO(ByteYue): use std::unreachable() instead when compiler supports it
    __builtin_unreachable();
}

} // namespace doris::cloud