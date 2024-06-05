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

#include "common/logging.h"
#include "recycler/obj_store_accessor.h"

namespace doris::cloud {

constexpr size_t BlobBatchMaxOperations = 256;

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
    }
    return {};
}

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
        Azure::Storage::Blobs::Models::BlobProperties properties =
                _client->GetBlockBlobClient(opts.key).GetProperties().Value;
        return {};
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
                Azure::Storage::Blobs::ListBlobsOptions list_opts;
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
        return {};
    }
    // TODO(ByteYue) : use range to adate this code when compiler is ready
    // auto chunkedView = objs | std::views::chunk(BlobBatchMaxOperations);
    auto begin = std::begin(objs);
    auto end = std::end(objs);

    while (begin != end) {
        auto batch = _client->CreateBatch();
        auto chunkEnd = begin;
        std::advance(chunkEnd, std::min(BlobBatchMaxOperations,
                                        static_cast<size_t>(std::distance(begin, end))));
        for (auto it = begin; it != chunkEnd; ++it) {
            batch.DeleteBlob(*it);
        }
        begin = chunkEnd;
        auto resp = do_azure_client_call([&]() { _client->SubmitBatch(batch); }, opts);
        if (resp.ret != 0) {
            return resp;
        }
    }
    return {};
}

ObjectStorageResponse AzureObjClient::delete_object(const ObjectStoragePathOptions& opts) {
    return do_azure_client_call([&]() { _client->DeleteBlob(opts.key); }, opts);
}

ObjectStorageResponse AzureObjClient::delete_objects_recursively(
        const ObjectStoragePathOptions& opts) {
    Azure::Storage::Blobs::ListBlobsOptions list_opts;
    list_opts.Prefix = opts.prefix;
    list_opts.PageSizeHint = BlobBatchMaxOperations;
    auto resp = _client->ListBlobs(list_opts);
    auto batch = _client->CreateBatch();
    for (auto&& blob_item : resp.Blobs) {
        batch.DeleteBlob(blob_item.Name);
    }
    auto response = do_azure_client_call([&]() { _client->SubmitBatch(batch); }, opts);
    if (response.ret != 0) {
        return response;
    }
    while (!resp.NextPageToken->empty()) {
        batch = _client->CreateBatch();
        list_opts.ContinuationToken = resp.NextPageToken;
        resp = _client->ListBlobs(list_opts);
        for (auto&& blob_item : resp.Blobs) {
            batch.DeleteBlob(blob_item.Name);
        }
        auto response = do_azure_client_call([&]() { _client->SubmitBatch(batch); }, opts);
        if (response.ret != 0) {
            return response;
        }
    }
    return {};
}

ObjectStorageResponse AzureObjClient::delete_expired(const ObjectStorageDeleteExpiredOptions& opts,
                                                     int64_t expired_time) {
    Azure::Storage::Blobs::ListBlobsOptions list_opts;
    list_opts.Prefix = opts.path_opts.prefix;
    list_opts.PageSizeHint = BlobBatchMaxOperations;
    auto resp = _client->ListBlobs(list_opts);
    auto batch = _client->CreateBatch();
    for (auto&& blob_item : resp.Blobs) {
        batch.DeleteBlob(blob_item.Name);
    }
    auto response = do_azure_client_call([&]() { _client->SubmitBatch(batch); }, opts.path_opts);
    if (response.ret != 0) {
        return response;
    }
    while (!resp.NextPageToken->empty()) {
        batch = _client->CreateBatch();
        list_opts.ContinuationToken = resp.NextPageToken;
        resp = _client->ListBlobs(list_opts);
        for (auto&& blob_item : resp.Blobs) {
            if (blob_item.Details.LastModified.time_since_epoch().count() < expired_time) {
                batch.DeleteBlob(blob_item.Name);
            }
        }
        auto response =
                do_azure_client_call([&]() { _client->SubmitBatch(batch); }, opts.path_opts);
        if (response.ret != 0) {
            return response;
        }
    }
    return {};
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