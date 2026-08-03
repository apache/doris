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
#include <array>
#include <azure/core/http/http.hpp>
#include <azure/core/http/http_status_code.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_batch.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/blobs/blob_lease_client.hpp>
#include <azure/storage/blobs/blob_sas_builder.hpp>
#include <azure/storage/blobs/rest_client.hpp>
#include <azure/storage/common/account_sas_builder.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/storage/common/storage_exception.hpp>
#include <cctype>
#include <cstdint>
#include <exception>
#include <iterator>
#include <ranges>
#include <string_view>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "cpp/obj_retry_strategy.h"
#include "io/fs/obj_storage_client.h"
#include "util/bvar_helper.h"
#include "util/s3_util.h"

using namespace Azure::Storage::Blobs;

namespace {
std::string wrap_object_storage_path_msg(const doris::io::ObjectStoragePathOptions& opts) {
    return fmt::format("bucket {}, key {}, prefix {}, path {}", opts.bucket, opts.key, opts.prefix,
                       opts.path.native());
}

std::string to_lower_ascii(std::string_view input) {
    std::string lowered(input);
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return lowered;
}

std::string encode_azure_block_id(std::string_view upload_id, int part_num) {
    uint32_t upload_namespace = 0x811C9DC5U;
    for (unsigned char byte : upload_id) {
        upload_namespace = (upload_namespace ^ byte) * 0x01000193U;
    }
    uint32_t namespaced_part = upload_namespace + static_cast<uint32_t>(part_num);
    // Four decoded bytes remain compatible with legacy residual blocks. Writer isolation is
    // enforced by the target blob lease because no 32-bit namespace can identify every upload.
    std::array<unsigned char, sizeof(namespaced_part)> raw_id {};
    for (size_t i = 0; i < raw_id.size(); ++i) {
        raw_id[i] = static_cast<unsigned char>(namespaced_part >> (i * 8));
    }
    Aws::Utils::ByteBuffer bytes(raw_id.data(), raw_id.size());
    return Aws::Utils::HashingUtils::Base64Encode(bytes);
}

constexpr std::string_view MULTIPART_LEASE_PREFIX = "doris-azure-lease-v1:";
constexpr std::chrono::seconds MULTIPART_LEASE_DURATION {60};

std::optional<std::string_view> azure_multipart_lease_id(std::string_view upload_id) {
    if (upload_id.starts_with(MULTIPART_LEASE_PREFIX) &&
        upload_id.size() > MULTIPART_LEASE_PREFIX.size()) {
        return upload_id.substr(MULTIPART_LEASE_PREFIX.size());
    }
    return std::nullopt;
}

// Rate limiting is applied by RateLimitedObjStorageClient, the decorator that
// S3ClientFactory wraps around this client when the bucket is subject to limiting.

constexpr char SAS_TOKEN_URL_TEMPLATE[] = "{}/{}/{}{}";
constexpr char BlobNotFound[] = "BlobNotFound";
} // namespace

namespace doris::io {

std::string azure_multipart_block_id(std::string_view upload_id, int part_num) {
    return encode_azure_block_id(upload_id, part_num);
}

// As Azure's doc said, the batch size is 256
// You can find out the num in https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch?tabs=microsoft-entra-id
// > Each batch request supports a maximum of 256 subrequests.
constexpr size_t BlobBatchMaxOperations = 256;

bool is_azure_tls_ca_error_message(std::string_view message) {
    std::string lower = to_lower_ascii(message);
    return lower.find("ssl ca cert") != std::string::npos ||
           lower.find("peer failed verification") != std::string::npos ||
           lower.find("unable to get local issuer certificate") != std::string::npos ||
           lower.find("problem with the ssl ca cert") != std::string::npos;
}

std::string build_azure_tls_debug_suffix(std::string_view error_message,
                                         std::string_view tls_debug_context) {
    if (tls_debug_context.empty() || !is_azure_tls_ca_error_message(error_message)) {
        return "";
    }
    return fmt::format(", {}", tls_debug_context);
}

template <typename Func>
ObjectStorageResponse do_azure_client_call(Func f, const ObjectStoragePathOptions& opts,
                                           std::string_view tls_debug_context) {
    try {
        f();
    } catch (Azure::Core::RequestFailedException& e) {
        doris::record_object_request_failed(static_cast<int>(e.StatusCode));
        auto tls_debug_suffix = build_azure_tls_debug_suffix(
                fmt::format("{} {}", e.what(), e.Message), tls_debug_context);
        auto msg = fmt::format(
                "Azure request failed because {}, error msg {}, http code {}, path msg {}{}",
                e.what(), e.Message, static_cast<int>(e.StatusCode),
                wrap_object_storage_path_msg(opts), tls_debug_suffix);
        LOG_WARNING(msg);
        return {.status = convert_to_obj_response(Status::InternalError<false>(std::move(msg))),
                .http_code = static_cast<int>(e.StatusCode),
                .request_id = std::move(e.RequestId)};
    } catch (std::exception& e) {
        auto msg = fmt::format("Azure request failed because {}, path msg {}{}", e.what(),
                               wrap_object_storage_path_msg(opts),
                               build_azure_tls_debug_suffix(e.what(), tls_debug_context));
        LOG_WARNING(msg);
        return {.status = convert_to_obj_response(Status::InternalError<false>(std::move(msg)))};
    }
    return ObjectStorageResponse::OK();
}

struct AzureBatchDeleter {
    AzureBatchDeleter(BlobContainerClient* client, const ObjectStoragePathOptions& opts,
                      std::string_view tls_debug_context)
            : _client(client),
              _batch(client->CreateBatch()),
              _opts(opts),
              _tls_debug_context(tls_debug_context) {}
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
                    SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_objects_latency);
                    _client->SubmitBatch(_batch);
                },
                _opts, _tls_debug_context);
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
                doris::record_object_request_failed(static_cast<int>(e.StatusCode));
                auto msg = fmt::format(
                        "Azure request failed because {}, error msg {}, http code {}, path msg "
                        "{}{}",
                        e.what(), e.Message, static_cast<int>(e.StatusCode),
                        wrap_object_storage_path_msg(_opts),
                        build_azure_tls_debug_suffix(fmt::format("{} {}", e.what(), e.Message),
                                                     _tls_debug_context));
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
    std::string_view _tls_debug_context;
    std::vector<Azure::Storage::DeferredResponse<Models::DeleteBlobResult>> deferred_resps;
};

ObjectStorageUploadResponse AzureObjStorageClient::create_multipart_upload(
        const ObjectStoragePathOptions& opts) {
    auto target_blob = _client->GetBlobClient(opts.key);
    auto target_client = target_blob.AsBlockBlobClient();
    std::string lease_id = BlobLeaseClient::CreateUniqueLeaseId();
    std::string upload_id = fmt::format("{}{}", MULTIPART_LEASE_PREFIX, lease_id);
    auto resp = do_azure_client_call(
            [&]() {
                uint8_t empty = 0;
                Azure::Core::IO::MemoryBodyStream empty_body(&empty, 0);
                // The reservation makes an absent blob leaseable but remains uncommitted and
                // invisible to normal listings until Put Block List publishes the real data.
                target_client.StageBlock(azure_multipart_block_id(upload_id, 0), empty_body);
                auto lease =
                        BlobLeaseClient(target_blob, lease_id).Acquire(MULTIPART_LEASE_DURATION);
                upload_id = fmt::format("{}{}", MULTIPART_LEASE_PREFIX, lease.Value.LeaseId);
            },
            opts, _tls_debug_context);
    return ObjectStorageUploadResponse {
            .resp = resp,
            .upload_id = resp.status.code == ErrorCode::OK
                                 ? std::make_optional(std::move(upload_id))
                                 : std::nullopt,
    };
}

ObjectStorageResponse AzureObjStorageClient::put_object(const ObjectStoragePathOptions& opts,
                                                        std::string_view stream) {
    auto client = _client->GetBlockBlobClient(opts.key);
    return do_azure_client_call(
            [&]() {
                SCOPED_BVAR_LATENCY(s3_bvar::s3_put_latency);
                client.UploadFrom(reinterpret_cast<const uint8_t*>(stream.data()), stream.size());
            },
            opts, _tls_debug_context);
}

ObjectStorageUploadResponse AzureObjStorageClient::upload_part(const ObjectStoragePathOptions& opts,
                                                               std::string_view stream,
                                                               int part_num) {
    DCHECK(opts.upload_id.has_value());
    auto target_blob = _client->GetBlobClient(opts.key);
    auto client = target_blob.AsBlockBlobClient();
    std::string block_id = azure_multipart_block_id(*opts.upload_id, part_num);
    auto resp = do_azure_client_call(
            [&]() {
                Azure::Core::IO::MemoryBodyStream memory_body(
                        reinterpret_cast<const uint8_t*>(stream.data()), stream.size());
                // The blockId must be base64 encoded
                SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
                auto lease_id = azure_multipart_lease_id(*opts.upload_id);
                if (lease_id.has_value()) {
                    BlobLeaseClient(target_blob, std::string(*lease_id)).Renew();
                    StageBlockOptions stage_opts;
                    stage_opts.AccessConditions.LeaseId = std::string(*lease_id);
                    client.StageBlock(block_id, memory_body, stage_opts);
                } else {
                    client.StageBlock(block_id, memory_body);
                }
            },
            opts, _tls_debug_context);
    return ObjectStorageUploadResponse {
            .resp = resp,
            // Hive defers completion to FE, so the exact staged ID must cross that boundary.
            .etag = block_id,
    };
}

ObjectStorageResponse AzureObjStorageClient::complete_multipart_upload(
        const ObjectStoragePathOptions& opts,
        const std::vector<ObjectCompleteMultiPart>& completed_parts) {
    DCHECK(opts.upload_id.has_value());
    auto target_blob = _client->GetBlobClient(opts.key);
    auto target_client = target_blob.AsBlockBlobClient();
    std::vector<std::string> string_block_ids;
    std::ranges::transform(completed_parts, std::back_inserter(string_block_ids),
                           [&opts](const ObjectCompleteMultiPart& i) {
                               return azure_multipart_block_id(*opts.upload_id, i.part_num);
                           });
    auto resp = do_azure_client_call(
            [&]() {
                SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
                // Put Block List atomically replaces the committed blob; no scan-visible staging blob exists.
                auto lease_id = azure_multipart_lease_id(*opts.upload_id);
                if (lease_id.has_value()) {
                    BlobLeaseClient(target_blob, std::string(*lease_id)).Renew();
                    CommitBlockListOptions commit_opts;
                    commit_opts.AccessConditions.LeaseId = std::string(*lease_id);
                    target_client.CommitBlockList(string_block_ids, commit_opts);
                } else {
                    target_client.CommitBlockList(string_block_ids);
                }
            },
            opts, _tls_debug_context);
    if (resp.status.code == ErrorCode::OK) {
        if (auto lease_id = azure_multipart_lease_id(*opts.upload_id); lease_id.has_value()) {
            auto release_resp = do_azure_client_call(
                    [&]() { BlobLeaseClient(target_blob, std::string(*lease_id)).Release(); }, opts,
                    _tls_debug_context);
            if (release_resp.status.code != ErrorCode::OK) {
                LOG(WARNING) << "Azure multipart commit succeeded but its finite lease could not "
                                "be released; it will expire automatically";
            }
        }
    }
    return resp;
}

ObjectStorageResponse AzureObjStorageClient::abort_multipart_upload(
        const ObjectStoragePathOptions& opts) {
    DCHECK(opts.upload_id.has_value());
    if (auto lease_id = azure_multipart_lease_id(*opts.upload_id); lease_id.has_value()) {
        auto target_blob = _client->GetBlobClient(opts.key);
        return do_azure_client_call(
                [&]() { BlobLeaseClient(target_blob, std::string(*lease_id)).Release(); }, opts,
                _tls_debug_context);
    }
    // Azure cannot delete one upload's uncommitted blocks without changing the committed blob.
    // Leaving them to service GC preserves the last successfully published value.
    return ObjectStorageResponse::OK();
}

ObjectStorageHeadResponse AzureObjStorageClient::head_object(const ObjectStoragePathOptions& opts) {
    Models::BlobProperties properties {};
    auto resp = do_azure_client_call(
            [&]() {
                SCOPED_BVAR_LATENCY(s3_bvar::s3_head_latency);
                properties = _client->GetBlockBlobClient(opts.key).GetProperties().Value;
            },
            opts, _tls_debug_context);
    if (resp.http_code == static_cast<int>(Azure::Core::Http::HttpStatusCode::NotFound)) {
        return ObjectStorageHeadResponse {
                .resp = {.status = convert_to_obj_response(
                                 Status::Error<ErrorCode::NOT_FOUND, false>(""))},
                .file_size = properties.BlobSize,
        };
    }

    return ObjectStorageHeadResponse {
            .resp = resp,
            .file_size = properties.BlobSize,
    };
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
                SCOPED_BVAR_LATENCY(s3_bvar::s3_get_latency);
                auto resp = client.DownloadTo(reinterpret_cast<uint8_t*>(buffer), bytes_read,
                                              download_opts);
                *size_return = resp.Value.ContentRange.Length.Value();
            },
            opts, _tls_debug_context);
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
                ListBlobsPagedResponse resp;
                {
                    SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
                    resp = _client->ListBlobs(list_opts);
                }
                get_file_file(resp);
                while (resp.NextPageToken.HasValue()) {
                    list_opts.ContinuationToken = resp.NextPageToken;
                    {
                        SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
                        resp = _client->ListBlobs(list_opts);
                    }
                    get_file_file(resp);
                }
            },
            opts, _tls_debug_context);
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
        auto deleter = AzureBatchDeleter(_client.get(), opts, _tls_debug_context);
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
                SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_object_latency);
                auto resp = _client->DeleteBlob(opts.key);
                if (!resp.Value.Deleted) {
                    throw Exception(Status::IOError<false>("Delete azure blob failed"));
                }
            },
            opts, _tls_debug_context);
}

ObjectStorageResponse AzureObjStorageClient::delete_objects_recursively(
        const ObjectStoragePathOptions& opts) {
    ListBlobsOptions list_opts;
    list_opts.Prefix = opts.prefix;
    list_opts.PageSizeHint = BlobBatchMaxOperations;
    auto delete_func = [&](const std::vector<Models::BlobItem>& blobs) -> ObjectStorageResponse {
        auto deleter = AzureBatchDeleter(_client.get(), opts, _tls_debug_context);
        auto batch = _client->CreateBatch();
        for (auto&& blob_item : blobs) {
            deleter.delete_blob(blob_item.Name);
        }
        if (auto response = deleter.execute(); response.status.code != ErrorCode::OK) {
            return response;
        }
        return ObjectStorageResponse::OK();
    };

    ListBlobsPagedResponse resp;
    auto list_resp = do_azure_client_call(
            [&]() {
                SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
                resp = _client->ListBlobs(list_opts);
            },
            opts, _tls_debug_context);
    if (list_resp.status.code != ErrorCode::OK) {
        return list_resp;
    }

    if (auto response = delete_func(resp.Blobs); response.status.code != ErrorCode::OK) {
        return response;
    }

    while (resp.NextPageToken.HasValue()) {
        list_opts.ContinuationToken = resp.NextPageToken;
        list_resp = do_azure_client_call(
                [&]() {
                    SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
                    resp = _client->ListBlobs(list_opts);
                },
                opts, _tls_debug_context);
        if (list_resp.status.code != ErrorCode::OK) {
            return list_resp;
        }

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

    std::string endpoint = conf.endpoint;
    auto sasURL = fmt::format(SAS_TOKEN_URL_TEMPLATE, endpoint, conf.bucket, opts.key, sasToken);
    if (sasURL.find("://") == std::string::npos) {
        sasURL = "https://" + sasURL;
    }
    return sasURL;
}
} // namespace doris::io
