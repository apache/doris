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

#include "io/fs/azure_obj_storage_client.h"

#include <aws/core/utils/Array.h>
#include <aws/core/utils/HashingUtils.h>

#include <algorithm>
#include <azure/core/http/http.hpp>
#include <azure/core/http/http_status_code.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_batch.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/blobs/blob_sas_builder.hpp>
#include <azure/storage/blobs/rest_client.hpp>
#include <azure/storage/common/account_sas_builder.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/storage/common/storage_exception.hpp>
#include <chrono>
#include <exception>
#include <iterator>
#include <ranges>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/obj_storage_client.h"
#include "util/bvar_helper.h"
#include "util/s3_util.h"

using namespace Azure::Storage::Blobs;

namespace {
std::string wrap_object_storage_path_msg(const doris::io::ObjectStoragePathOptions& opts) {
    return fmt::format("bucket {}, key {}, prefix {}, path {}", opts.bucket, opts.key, opts.prefix,
                       opts.path.native());
}

auto base64_encode_part_num(int part_num) {
    return Aws::Utils::HashingUtils::Base64Encode(
            {reinterpret_cast<unsigned char*>(&part_num), sizeof(part_num)});
}

template <typename Func>
auto s3_rate_limit(doris::S3RateLimitType op, Func callback) -> decltype(callback()) {
    if (!doris::config::enable_s3_rate_limiter) {
        return callback();
    }
    auto sleep_duration = doris::S3ClientFactory::instance().rate_limiter(op)->add(1);
    if (sleep_duration < 0) {
        throw std::runtime_error("Azure exceeds request limit");
    }
    return callback();
}

template <typename Func>
auto s3_get_rate_limit(Func callback) -> decltype(callback()) {
    return s3_rate_limit(doris::S3RateLimitType::GET, std::move(callback));
}

template <typename Func>
auto s3_put_rate_limit(Func callback) -> decltype(callback()) {
    return s3_rate_limit(doris::S3RateLimitType::PUT, std::move(callback));
}

constexpr char SAS_TOKEN_URL_TEMPLATE[] = "https://{}.blob.core.windows.net/{}/{}{}";
constexpr char BlobNotFound[] = "BlobNotFound";
} // namespace

namespace doris::io {

// As Azure's doc said, the batch size is 256
// You can find out the num in https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=microsoft-entra-id
// > Each batch request supports a maximum of 256 subrequests.
constexpr size_t BlobBatchMaxOperations = 256;

template <typename Func>
ObjectStorageResponse do_azure_client_call(Func f, const ObjectStoragePathOptions& opts) {
    try {
        f();
    } catch (Azure::Core::RequestFailedException& e) {
        auto msg = fmt::format(
                "Azure request failed because {}, error msg {}, http code {}, path msg {}",
                e.what(), e.Message, static_cast<int>(e.StatusCode),
                wrap_object_storage_path_msg(opts));
        LOG_WARNING(msg);
        return {.status = convert_to_obj_response(Status::InternalError<false>(std::move(msg))),
                .http_code = static_cast<int>(e.StatusCode),
                .request_id = std::move(e.RequestId)};
    } catch (std::exception& e) {
        auto msg = fmt::format("Azure request failed because {}, path msg {}", e.what(),
                               wrap_object_storage_path_msg(opts));
        LOG_WARNING(msg);
        return {.status = convert_to_obj_response(Status::InternalError<false>(std::move(msg)))};
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
        auto resp = do_azure_client_call(
                [&]() {
                    s3_put_rate_limit([&]() {
                        SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_objects_latency);
                        _client->SubmitBatch(_batch);
                    });
                },
                _opts);
        if (resp.status.code != ErrorCode::OK) {
            return resp;
        }

        for (auto&& defer_response : deferred_resps) {
            try {
                auto r = defer_response.GetResponse();
                if (!r.Value.Deleted) {
                    auto msg = fmt::format("Azure batch delete failed, path msg {}",
                                           wrap_object_storage_path_msg(_opts));
                    LOG_WARNING(msg);
                    return {.status = convert_to_obj_response(
                                    Status::InternalError<false>(std::move(msg)))};
                }
            } catch (Azure::Core::RequestFailedException& e) {
                if (Azure::Core::Http::HttpStatusCode::NotFound == e.StatusCode &&
                    0 == strcmp(e.ErrorCode.c_str(), BlobNotFound)) {
                    continue;
                }
                auto msg = fmt::format(
                        "Azure request failed because {}, error msg {}, http code {}, path msg {}",
                        e.what(), e.Message, static_cast<int>(e.StatusCode),
                        wrap_object_storage_path_msg(_opts));
                LOG_WARNING(msg);
                return {.status = convert_to_obj_response(
                                Status::InternalError<false>(std::move(msg))),
                        .http_code = static_cast<int>(e.StatusCode),
                        .request_id = std::move(e.RequestId)};
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

// Azure would do nothing
ObjectStorageUploadResponse AzureObjStorageClient::create_multipart_upload(
        const ObjectStoragePathOptions& opts) {
    return ObjectStorageUploadResponse {
            .resp = ObjectStorageResponse::OK(),
    };
}

ObjectStorageResponse AzureObjStorageClient::put_object(const ObjectStoragePathOptions& opts,
                                                        std::string_view stream) {
    auto client = _client->GetBlockBlobClient(opts.key);
    return do_azure_client_call(
            [&]() {
                s3_put_rate_limit([&]() {
                    SCOPED_BVAR_LATENCY(s3_bvar::s3_put_latency);
                    client.UploadFrom(reinterpret_cast<const uint8_t*>(stream.data()),
                                      stream.size());
                });
            },
            opts);
}

ObjectStorageUploadResponse AzureObjStorageClient::upload_part(const ObjectStoragePathOptions& opts,
                                                               std::string_view stream,
                                                               int part_num) {
    auto client = _client->GetBlockBlobClient(opts.key);
    try {
        Azure::Core::IO::MemoryBodyStream memory_body(
                reinterpret_cast<const uint8_t*>(stream.data()), stream.size());
        // The blockId must be base64 encoded
        s3_put_rate_limit([&]() {
            SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
            client.StageBlock(base64_encode_part_num(part_num), memory_body);
        });
    } catch (Azure::Core::RequestFailedException& e) {
        auto msg = fmt::format(
                "Azure request failed because {}, error msg {}, http code {}, path msg {}",
                e.what(), e.Message, static_cast<int>(e.StatusCode),
                wrap_object_storage_path_msg(opts));
        LOG_WARNING(msg);
        // clang-format off
        return {
            .resp = {
                .status = convert_to_obj_response(
                        Status::InternalError<false>(std::move(msg))),
                .http_code = static_cast<int>(e.StatusCode),
                .request_id = std::move(e.RequestId),
            },
        };
        // clang-format on
    }
    return ObjectStorageUploadResponse {
            .resp = ObjectStorageResponse::OK(),
    };
}

ObjectStorageResponse AzureObjStorageClient::complete_multipart_upload(
        const ObjectStoragePathOptions& opts,
        const std::vector<ObjectCompleteMultiPart>& completed_parts) {
    auto client = _client->GetBlockBlobClient(opts.key);
    std::vector<std::string> string_block_ids;
    std::ranges::transform(
            completed_parts, std::back_inserter(string_block_ids),
            [](const ObjectCompleteMultiPart& i) { return base64_encode_part_num(i.part_num); });
    return do_azure_client_call(
            [&]() {
                s3_put_rate_limit([&]() {
                    SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
                    client.CommitBlockList(string_block_ids);
                });
            },
            opts);
}

ObjectStorageHeadResponse AzureObjStorageClient::head_object(const ObjectStoragePathOptions& opts) {
    try {
        Models::BlobProperties properties = s3_get_rate_limit([&]() {
            SCOPED_BVAR_LATENCY(s3_bvar::s3_head_latency);
            return _client->GetBlockBlobClient(opts.key).GetProperties().Value;
        });
        return {.file_size = properties.BlobSize};
    } catch (Azure::Core::RequestFailedException& e) {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
            return ObjectStorageHeadResponse {
                    .resp = {.status = convert_to_obj_response(Status::NotFound<false>("")),
                             .http_code = static_cast<int>(e.StatusCode),
                             .request_id = std::move(e.RequestId)},
            };
        }
        auto msg = fmt::format(
                "Azure request failed because {}, error msg {}, http code {}, path msg {}",
                e.what(), e.Message, static_cast<int>(e.StatusCode),
                wrap_object_storage_path_msg(opts));
        return ObjectStorageHeadResponse {
                .resp = {.status = convert_to_obj_response(
                                 Status::InternalError<false>(std::move(msg))),
                         .http_code = static_cast<int>(e.StatusCode),
                         .request_id = std::move(e.RequestId)},
        };
    }
}

ObjectStorageResponse AzureObjStorageClient::get_object(const ObjectStoragePathOptions& opts,
                                                        void* buffer, size_t offset,
                                                        size_t bytes_read, size_t* size_return) {
    auto client = _client->GetBlockBlobClient(opts.key);
    return do_azure_client_call(
            [&]() {
                DownloadBlobToOptions download_opts;
                Azure::Core::Http::HttpRange range {static_cast<int64_t>(offset), bytes_read};
                download_opts.Range = range;
                auto resp = s3_get_rate_limit([&]() {
                    SCOPED_BVAR_LATENCY(s3_bvar::s3_get_latency);
                    return client.DownloadTo(reinterpret_cast<uint8_t*>(buffer), bytes_read,
                                             download_opts);
                });
                *size_return = resp.Value.ContentRange.Length.Value();
            },
            opts);
}

ObjectStorageResponse AzureObjStorageClient::list_objects(const ObjectStoragePathOptions& opts,
                                                          std::vector<FileInfo>* files) {
    auto get_file_file = [&](ListBlobsPagedResponse& resp) {
        std::ranges::transform(resp.Blobs, std::back_inserter(*files), [](auto&& blob_item) {
            return FileInfo {
                    .file_name = blob_item.Name, .file_size = blob_item.BlobSize, .is_file = true};
        });
    };
    return do_azure_client_call(
            [&]() {
                ListBlobsOptions list_opts;
                list_opts.Prefix = opts.prefix;
                auto resp = s3_get_rate_limit([&]() {
                    SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
                    return _client->ListBlobs(list_opts);
                });
                get_file_file(resp);
                while (!resp.NextPageToken->empty()) {
                    list_opts.ContinuationToken = resp.NextPageToken;
                    resp = s3_get_rate_limit([&]() {
                        SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
                        return _client->ListBlobs(list_opts);
                    });
                    get_file_file(resp);
                }
            },
            opts);
}

// As Azure's doc said, the batch size is 256
// You can find out the num in https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=microsoft-entra-id
// > Each batch request supports a maximum of 256 subrequests.
ObjectStorageResponse AzureObjStorageClient::delete_objects(const ObjectStoragePathOptions& opts,
                                                            std::vector<std::string> objs) {
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
        if (auto resp = deleter.execute(); resp.status.code != ErrorCode::OK) {
            return resp;
        }
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse AzureObjStorageClient::delete_object(const ObjectStoragePathOptions& opts) {
    return do_azure_client_call(
            [&]() {
                auto resp = s3_put_rate_limit([&]() {
                    SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_object_latency);
                    return _client->DeleteBlob(opts.key);
                });
                if (!resp.Value.Deleted) {
                    throw Exception(Status::IOError<false>("Delete azure blob failed"));
                }
            },
            opts);
}

ObjectStorageResponse AzureObjStorageClient::delete_objects_recursively(
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
        if (auto response = deleter.execute(); response.status.code != ErrorCode::OK) {
            return response;
        }
        return ObjectStorageResponse::OK();
    };
    auto resp = s3_get_rate_limit([&]() {
        SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
        return _client->ListBlobs(list_opts);
    });
    if (auto response = delete_func(resp.Blobs); response.status.code != ErrorCode::OK) {
        return response;
    }

    while (!resp.NextPageToken->empty()) {
        list_opts.ContinuationToken = resp.NextPageToken;
        resp = s3_get_rate_limit([&]() {
            SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
            return _client->ListBlobs(list_opts);
        });

        if (auto response = delete_func(resp.Blobs); response.status.code != ErrorCode::OK) {
            return response;
        }
    }
    return ObjectStorageResponse::OK();
}

std::string AzureObjStorageClient::generate_presigned_url(const ObjectStoragePathOptions& opts,
                                                          int64_t expiration_secs,
                                                          const S3ClientConf& conf) {
    Azure::Storage::Sas::BlobSasBuilder sas_builder;
    sas_builder.ExpiresOn =
            std::chrono::system_clock::now() + std::chrono::seconds(expiration_secs);
    sas_builder.BlobContainerName = opts.bucket;
    sas_builder.BlobName = opts.key;
    sas_builder.Resource = Azure::Storage::Sas::BlobSasResource::Blob;
    sas_builder.Protocol = Azure::Storage::Sas::SasProtocol::HttpsOnly;
    sas_builder.SetPermissions(Azure::Storage::Sas::BlobSasPermissions::Read);

    std::string sasToken = sas_builder.GenerateSasToken(
            Azure::Storage::StorageSharedKeyCredential(conf.ak, conf.sk));

    return fmt::format(SAS_TOKEN_URL_TEMPLATE, conf.ak, conf.bucket, opts.key, sasToken);
}
} // namespace doris::io