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

#include "azure_obj_storage_client.h"

#include <absl/strings/escaping.h>
#include <butil/guid.h>

#include <azure/core/diagnostics/logger.hpp>
#include <azure/core/http/curl_transport.hpp>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string_view>
#include <vector>

#include "cpp/obj-storage/auth/azure_auth_factory.h"
#include "cpp/obj-storage/obj_storage_config.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/util.h"

using namespace Azure::Storage::Blobs;

namespace {
std::string wrap_object_storage_path_msg(const doris::ObjStoragePath& opts) {
    return fmt::format("bucket {}, key {}, prefix {}, path {}", opts.bucket, opts.key, opts.prefix,
                       opts.path.native());
}

doris::ObjStoragePath with_object_key(const doris::ObjStoragePath& opts, std::string_view key) {
    auto path = opts;
    path.key = key;
    return path;
}

std::string to_lower_ascii(std::string_view input) {
    std::string lowered(input);
    std::transform(lowered.begin(), lowered.end(), lowered.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return lowered;
}

std::string encode_azure_block_id(std::string_view upload_id, int part_num) {
    // Azure has no multipart upload namespace. Include the writer UUID in every block ID so
    // concurrent writers for the same key cannot stage interchangeable blocks.
    std::string raw_id(upload_id);
    auto part = static_cast<uint32_t>(part_num);
    for (size_t i = 0; i < sizeof(part); ++i) {
        raw_id.push_back(static_cast<char>(part >> (i * 8)));
    }
    return absl::Base64Escape(raw_id);
}

constexpr char BlobNotFound[] = "BlobNotFound";
} // namespace

namespace doris {

std::string azure_multipart_block_id(std::string_view upload_id, int part_num) {
    return encode_azure_block_id(upload_id, part_num);
}

std::string build_azure_batch_delete_failure_message(const ObjStoragePath& opts,
                                                     std::string_view key) {
    return fmt::format("Azure batch delete failed, path msg {}",
                       wrap_object_storage_path_msg(with_object_key(opts, key)));
}

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

static ObjStorageResponse make_azure_std_exception_response(const std::exception& e,
                                                            const ObjStoragePath& opts,
                                                            std::string_view tls_debug_context) {
    auto msg = fmt::format("Azure request failed because {}, path msg {}{}", e.what(),
                           wrap_object_storage_path_msg(opts),
                           build_azure_tls_debug_suffix(e.what(), tls_debug_context));
    LOG(WARNING) << msg;
    return {.status = ObjStorageStatus {ObjStorageStatus::INTERNAL_ERROR, std::move(msg)},
            .http_code = 0,
            .request_id = ""};
}

template <typename Func>
ObjStorageResponse do_azure_client_call(Func f, const ObjStoragePath& opts,
                                        std::string_view tls_debug_context) {
    try {
        f();
    } catch (Azure::Core::RequestFailedException& e) {
        doris::record_object_request_failed(static_cast<int>(e.StatusCode));
        auto msg = fmt::format(
                "Azure request failed because {}, error msg {}, http code {}, path msg {}{}",
                e.what(), e.Message, static_cast<int>(e.StatusCode),
                wrap_object_storage_path_msg(opts),
                build_azure_tls_debug_suffix(fmt::format("{} {}", e.what(), e.Message),
                                             tls_debug_context));
        LOG(WARNING) << msg;
        return {.status = obj_storage_status_from_http_code(static_cast<int>(e.StatusCode),
                                                            std::move(msg)),
                .http_code = static_cast<int>(e.StatusCode),
                .request_id = std::move(e.RequestId)};
    } catch (const std::exception& e) {
        return make_azure_std_exception_response(e, opts, tls_debug_context);
    }
    return ObjStorageResponse::OK();
}

struct AzureBatchDeleter {
    AzureBatchDeleter(BlobContainerClient* client, const ObjStoragePath& opts,
                      std::string_view representative_key, std::string_view tls_debug_context)
            : _client(client),
              _batch(client->CreateBatch()),
              _opts(with_object_key(opts, representative_key)),
              _tls_debug_context(tls_debug_context) {}
    // Submit one blob to be deleted in `AzureBatchDeleter::execute`
    void delete_blob(const std::string& blob_name) {
        deferred_resps.emplace_back(DeferredDeleteResponse {
                .key = blob_name, .response = _batch.DeleteBlob(blob_name)});
    }
    ObjStorageResponse execute(const Azure::Core::Context& context) {
        if (deferred_resps.empty()) {
            return ObjStorageResponse::OK();
        }
        auto resp = do_azure_client_call(
                [&]() {
                    client_bvar::ScopedLatency scoped_latency(
                            client_bvar::s3_delete_objects_latency);
                    _client->SubmitBatch(_batch, {}, context);
                },
                _opts, _tls_debug_context);
        if (resp.status.code != ObjStorageStatus::OK) {
            return resp;
        }

        for (auto&& deferred : deferred_resps) {
            try {
                auto r = deferred.response.GetResponse();
                if (!r.Value.Deleted) {
                    auto msg = build_azure_batch_delete_failure_message(_opts, deferred.key);
                    LOG(WARNING) << msg;
                    return {.status = ObjStorageStatus {ObjStorageStatus::INTERNAL_ERROR,
                                                        std::move(msg)},
                            .http_code = 0,
                            .request_id = ""};
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
                        wrap_object_storage_path_msg(with_object_key(_opts, deferred.key)),
                        build_azure_tls_debug_suffix(fmt::format("{} {}", e.what(), e.Message),
                                                     _tls_debug_context));
                LOG(WARNING) << msg;
                return {.status = obj_storage_status_from_http_code(static_cast<int>(e.StatusCode),
                                                                    std::move(msg)),
                        .http_code = static_cast<int>(e.StatusCode),
                        .request_id = std::move(e.RequestId)};
            }
        }

        return ObjStorageResponse::OK();
    }

private:
    struct DeferredDeleteResponse {
        std::string key;
        Azure::Storage::DeferredResponse<Models::DeleteBlobResult> response;
    };

    BlobContainerClient* _client;
    BlobContainerBatch _batch;
    ObjStoragePath _opts;
    std::string_view _tls_debug_context;
    std::vector<DeferredDeleteResponse> deferred_resps;
};

namespace {
std::string env_or_empty(const char* env_name) {
    if (const char* value = std::getenv(env_name); value != nullptr) {
        return value;
    }
    return "";
}

std::string build_azure_tls_debug_context(const std::string& selected_ca_file) {
    bool selected_ca_exists = false;
    bool selected_ca_readable = false;
    if (!selected_ca_file.empty()) {
        std::error_code ec;
        selected_ca_exists = std::filesystem::exists(selected_ca_file, ec) && !ec;
        std::ifstream input(selected_ca_file);
        selected_ca_readable = input.good();
    }

    return fmt::format(
            "tls_debug(selected_ca_file='{}', selected_ca_exists={}, "
            "selected_ca_readable={}, SSL_CERT_FILE='{}', CURL_CA_BUNDLE='{}', SSL_CERT_DIR='{}')",
            selected_ca_file, selected_ca_exists, selected_ca_readable,
            env_or_empty("SSL_CERT_FILE"), env_or_empty("CURL_CA_BUNDLE"),
            env_or_empty("SSL_CERT_DIR"));
}

void init_azure_logging(int log_level) {
    auto azureLogLevel = static_cast<Azure::Core::Diagnostics::Logger::Level>(log_level);
    Azure::Core::Diagnostics::Logger::SetLevel(azureLogLevel);
    Azure::Core::Diagnostics::Logger::SetListener(
            [](Azure::Core::Diagnostics::Logger::Level level, const std::string& message) {
                switch (level) {
                case Azure::Core::Diagnostics::Logger::Level::Verbose:
                    LOG(INFO) << message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Informational:
                    LOG(INFO) << message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Warning:
                    LOG(WARNING) << message;
                    break;
                case Azure::Core::Diagnostics::Logger::Level::Error:
                    LOG(ERROR) << message;
                    break;
                default:
                    LOG(WARNING) << "Unknown level: " << static_cast<int>(level)
                                 << ", message: " << message;
                    break;
                }
            });
}

} // namespace

void initialize_azure_logging(int log_level) {
    static std::once_flag init_logging;
    std::call_once(init_logging, init_azure_logging, log_level);
}

ObjStorageClientBuildResult AzureObjStorageClient::create(const AzureObjStorageConfig& config) {
    const auto& connection = config.connection;
    Azure::Storage::Blobs::BlobClientOptions options;
    if (connection.max_retries) {
        options.Retry.MaxRetries = *connection.max_retries;
    }
    if (config.retry_too_many_requests) {
        options.Retry.StatusCodes.insert(Azure::Core::Http::HttpStatusCode::TooManyRequests);
    }
    options.PerRetryPolicies.emplace_back(std::make_unique<AzureRetryRecordPolicy>());
    if (connection.ca_cert_file || connection.connect_timeout) {
        Azure::Core::Http::CurlTransportOptions curl_options;
        curl_options.CAInfo = connection.ca_cert_file.value_or("");
        if (connection.connect_timeout) {
            curl_options.ConnectionTimeout = *connection.connect_timeout;
        }
        options.Transport.Transport =
                std::make_shared<Azure::Core::Http::CurlTransport>(std::move(curl_options));
    }
    ObjStorageEndpointInfo endpoint_info {.endpoint = connection.endpoint,
                                          .tls_debug_context = build_azure_tls_debug_context(
                                                  connection.ca_cert_file.value_or(""))};
    auto built = AzureAuthFactory::create(connection.endpoint, config.auth, std::move(options));
    if (!built) {
        return {.error = std::move(built.error)};
    }
    return {.client = std::make_shared<AzureObjStorageClient>(
                    std::move(built.service_client), std::move(endpoint_info),
                    std::move(built.shared_key_credential), connection.request_timeout)};
}

Azure::Core::Context AzureObjStorageClient::request_context() const {
    return _request_timeout
                   ? Azure::Core::Context(std::chrono::system_clock::now() + *_request_timeout)
                   : Azure::Core::Context();
}

ObjStorageUploadResult AzureObjStorageClient::create_multipart_upload(const ObjStoragePath&) {
    // Azure has no provider-side multipart session. This local UUID namespaces the writer's
    // staged block IDs and is carried through the same interface as an S3 upload ID.
    auto upload_id = butil::GenerateGUID();
    if (upload_id.empty()) {
        return {.resp = {.status = {ObjStorageStatus::INTERNAL_ERROR,
                                    "failed to generate Azure multipart upload ID"}}};
    }
    return ObjStorageUploadResult {
            .resp = ObjStorageResponse::OK(),
            .upload_id = std::move(upload_id),
    };
}

ObjStorageResponse AzureObjStorageClient::put_object(const ObjStoragePath& opts,
                                                     std::string_view stream) {
    auto client = _client->GetBlobContainerClient(opts.bucket).GetBlockBlobClient(opts.key);
    return do_azure_client_call(
            [&]() {
                client_bvar::ScopedLatency scoped_latency(client_bvar::s3_put_latency);
                client.UploadFrom(reinterpret_cast<const uint8_t*>(stream.data()), stream.size(),
                                  {}, request_context());
            },
            opts, _config.tls_debug_context);
}

ObjStorageUploadResult AzureObjStorageClient::upload_part(const ObjStoragePath& opts,
                                                          const std::string& upload_id,
                                                          std::string_view stream, int part_num) {
    DCHECK(!upload_id.empty());
    auto client = _client->GetBlobContainerClient(opts.bucket).GetBlockBlobClient(opts.key);
    std::string block_id = azure_multipart_block_id(upload_id, part_num);
    try {
        Azure::Core::IO::MemoryBodyStream memory_body(
                reinterpret_cast<const uint8_t*>(stream.data()), stream.size());
        // The blockId must be base64 encoded
        client_bvar::ScopedLatency scoped_latency(client_bvar::s3_multi_part_upload_latency);
        client.StageBlock(block_id, memory_body, {}, request_context());
    } catch (Azure::Core::RequestFailedException& e) {
        record_object_request_failed(static_cast<int>(e.StatusCode));
        auto tls_debug_suffix = build_azure_tls_debug_suffix(
                fmt::format("{} {}", e.what(), e.Message), _config.tls_debug_context);
        auto msg = fmt::format(
                "Azure request failed because {}, error msg {}, http code {}, path msg {}{}",
                e.what(), e.Message, static_cast<int>(e.StatusCode),
                wrap_object_storage_path_msg(opts), tls_debug_suffix);
        LOG(WARNING) << msg;
        // clang-format off
        return {
            .resp = {
                .status = obj_storage_status_from_http_code(static_cast<int>(e.StatusCode),
                                                            std::move(msg)),
                .http_code = static_cast<int>(e.StatusCode),
                .request_id = std::move(e.RequestId),
            },
        };
        // clang-format on
    } catch (const std::exception& e) {
        return {.resp = make_azure_std_exception_response(e, opts, _config.tls_debug_context)};
    }
    return ObjStorageUploadResult {.resp = ObjStorageResponse::OK(), .etag = std::move(block_id)};
}

ObjStorageResponse AzureObjStorageClient::complete_multipart_upload(
        const ObjStoragePath& opts, const std::string& upload_id,
        const std::vector<ObjStorageCompletedPart>& completed_parts) {
    DCHECK(!upload_id.empty());
    auto client = _client->GetBlobContainerClient(opts.bucket).GetBlockBlobClient(opts.key);
    std::vector<std::string> string_block_ids;
    std::ranges::transform(completed_parts, std::back_inserter(string_block_ids),
                           [&upload_id](const ObjStorageCompletedPart& i) {
                               return azure_multipart_block_id(upload_id, i.part_num);
                           });
    return do_azure_client_call(
            [&]() {
                client_bvar::ScopedLatency scoped_latency(
                        client_bvar::s3_multi_part_upload_latency);
                client.CommitBlockList(string_block_ids, {}, request_context());
            },
            opts, _config.tls_debug_context);
}

ObjStorageHeadResult AzureObjStorageClient::head_object(const ObjStoragePath& opts) {
    try {
        Models::BlobProperties properties = [&]() {
            client_bvar::ScopedLatency scoped_latency(client_bvar::s3_head_latency);
            return _client->GetBlobContainerClient(opts.bucket)
                    .GetBlockBlobClient(opts.key)
                    .GetProperties({}, request_context())
                    .Value;
        }();
        return {.resp = ObjStorageResponse::OK(), .file_size = properties.BlobSize};
    } catch (Azure::Core::RequestFailedException& e) {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound) {
            return ObjStorageHeadResult {
                    .resp = {.status = obj_storage_status_from_http_code(
                                     static_cast<int>(e.StatusCode), ""),
                             .http_code = static_cast<int>(e.StatusCode),
                             .request_id = std::move(e.RequestId)},
            };
        }
        record_object_request_failed(static_cast<int>(e.StatusCode));
        auto tls_debug_suffix = build_azure_tls_debug_suffix(
                fmt::format("{} {}", e.what(), e.Message), _config.tls_debug_context);
        auto msg = fmt::format(
                "Azure request failed because {}, error msg {}, http code {}, path msg {}{}",
                e.what(), e.Message, static_cast<int>(e.StatusCode),
                wrap_object_storage_path_msg(opts), tls_debug_suffix);
        LOG(WARNING) << msg << ", request_id=" << e.RequestId;
        return ObjStorageHeadResult {
                .resp = {.status = obj_storage_status_from_http_code(static_cast<int>(e.StatusCode),
                                                                     std::move(msg)),
                         .http_code = static_cast<int>(e.StatusCode),
                         .request_id = std::move(e.RequestId)},
        };
    } catch (const std::exception& e) {
        return {.resp = make_azure_std_exception_response(e, opts, _config.tls_debug_context)};
    }
}

ObjStorageResponse AzureObjStorageClient::get_object(const ObjStoragePath& opts, void* buffer,
                                                     size_t offset, size_t bytes_read,
                                                     size_t* size_return) {
    auto client = _client->GetBlobContainerClient(opts.bucket).GetBlockBlobClient(opts.key);
    auto response = do_azure_client_call(
            [&]() {
                DownloadBlobToOptions download_opts;
                Azure::Core::Http::HttpRange range {.Offset = static_cast<int64_t>(offset),
                                                    .Length = bytes_read};
                download_opts.Range = range;
                client_bvar::ScopedLatency scoped_latency(client_bvar::s3_get_latency);
                auto resp = client.DownloadTo(reinterpret_cast<uint8_t*>(buffer), bytes_read,
                                              download_opts, request_context());
                *size_return = resp.Value.ContentRange.Length.Value();
            },
            opts, _config.tls_debug_context);
    if (!response.ok() || *size_return == bytes_read) {
        return response;
    }
    return {
            .status = {ObjStorageStatus::INTERNAL_ERROR,
                       fmt::format("incomplete read from {}, expect {}, got {}",
                                   wrap_object_storage_path_msg(opts), bytes_read, *size_return)},
            .http_code = response.http_code,
            .request_id = std::move(response.request_id),
    };
}

ObjStorageListPageResult AzureObjStorageClient::list_objects_page(
        const ObjStoragePath& opts, std::string_view continuation_token) {
    const auto& prefix = opts.prefix.empty() ? opts.key : opts.prefix;
    ListBlobsOptions request;
    request.Prefix = prefix;
    request.PageSizeHint = static_cast<int32_t>(capabilities().max_list_page);
    if (!continuation_token.empty()) {
        request.ContinuationToken = std::string(continuation_token);
    }
    TEST_SYNC_POINT_CALLBACK("AzureObjStorageClient::list_objects", &request);

    try {
        auto response = [&]() {
            client_bvar::ScopedLatency scoped_latency(client_bvar::s3_list_latency);
            return _client->GetBlobContainerClient(opts.bucket)
                    .ListBlobs(request, request_context());
        }();
        const bool has_more = response.NextPageToken.HasValue();
        auto next_token = has_more ? response.NextPageToken.Value() : std::string {};
        if (has_more && next_token.empty()) {
            return {
                    .resp = {.status = {ObjStorageStatus::INTERNAL_ERROR,
                                        "Azure list response has an empty continuation token"},
                             .http_code = 0},
            };
        }
        ObjStorageListPageResult page {.resp = ObjStorageResponse::OK(),
                                       .continuation_token = std::move(next_token),
                                       .has_more = has_more};
        page.objects.reserve(response.Blobs.size());
        for (auto&& item : response.Blobs) {
            DCHECK(item.Name.starts_with(*request.Prefix)) << item.Name << ' ' << *request.Prefix;
            page.objects.emplace_back(ObjectMeta {
                    .key = std::move(item.Name),
                    .size = item.BlobSize,
                    // `Azure::DateTime` adds the offset of `SystemClockEpoch` to the given Unix timestamp,
                    // so here we need to subtract this offset to obtain the Unix timestamp of the mtime.
                    // https://github.com/Azure/azure-sdk-for-cpp/blob/azure-core_1.12.0/sdk/core/azure-core/inc/azure/core/datetime.hpp#L129
                    .mtime_s = duration_cast<std::chrono::seconds>(item.Details.LastModified -
                                                                   SystemClockEpoch)
                                       .count()});
        }
        return page;
    } catch (Azure::Core::RequestFailedException& e) {
        record_object_request_failed(static_cast<int>(e.StatusCode));
        auto tls_debug_suffix = build_azure_tls_debug_suffix(
                fmt::format("{} {}", e.what(), e.Message), _config.tls_debug_context);
        LOG(WARNING) << fmt::format("Azure request failed because {}, url: {}, prefix: {}{}",
                                    e.what(), _client->GetUrl(), request.Prefix.Value(),
                                    tls_debug_suffix);
        return {
                .resp = {.status = obj_storage_status_from_http_code(static_cast<int>(e.StatusCode),
                                                                     e.Message + tls_debug_suffix),
                         .http_code = static_cast<int>(e.StatusCode),
                         .request_id = std::move(e.RequestId)},
        };
    } catch (std::exception& e) {
        LOG(WARNING) << fmt::format("Azure request failed because {}, url: {}, prefix: {}",
                                    e.what(), _client->GetUrl(), request.Prefix.Value());
        return {
                .resp = {.status = {ObjStorageStatus::INTERNAL_ERROR, e.what()},
                         .http_code = 0,
                         .request_id = ""},
        };
    }
}

ObjStorageResponse AzureObjStorageClient::delete_objects_batch(const ObjStoragePath& opts,
                                                               std::span<std::string> objs) {
    auto container = _client->GetBlobContainerClient(opts.bucket);
    AzureBatchDeleter deleter(&container, opts, objs.front(), _config.tls_debug_context);
    for (const auto& key : objs) {
        deleter.delete_blob(key);
    }
    return deleter.execute(request_context());
}

ObjStorageResponse AzureObjStorageClient::delete_object(const ObjStoragePath& opts) {
    try {
        auto resp = [&]() {
            client_bvar::ScopedLatency scoped_latency(client_bvar::s3_delete_object_latency);
            return _client->GetBlobContainerClient(opts.bucket)
                    .DeleteBlob(opts.key, {}, request_context());
        }();
        if (!resp.Value.Deleted) {
            return {
                    .status = ObjStorageStatus {ObjStorageStatus::IO_ERROR,
                                                "Delete azure blob failed"},
                    .http_code = 0,
                    .request_id = "",
            };
        }
        return ObjStorageResponse::OK();
    } catch (Azure::Core::RequestFailedException& e) {
        if (e.StatusCode == Azure::Core::Http::HttpStatusCode::NotFound &&
            e.ErrorCode == BlobNotFound) {
            return ObjStorageResponse::OK();
        }
        record_object_request_failed(static_cast<int>(e.StatusCode));
        auto tls_debug_suffix = build_azure_tls_debug_suffix(
                fmt::format("{} {}", e.what(), e.Message), _config.tls_debug_context);
        auto msg = fmt::format(
                "Azure request failed because {}, error msg {}, http code {}, path msg {}{}",
                e.what(), e.Message, static_cast<int>(e.StatusCode),
                wrap_object_storage_path_msg(opts), tls_debug_suffix);
        LOG(WARNING) << msg;
        return {
                .status = obj_storage_status_from_http_code(static_cast<int>(e.StatusCode),
                                                            std::move(msg)),
                .http_code = static_cast<int>(e.StatusCode),
                .request_id = std::move(e.RequestId),
        };
    } catch (std::exception& e) {
        auto msg = fmt::format("Azure request failed because {}, path msg {}{}", e.what(),
                               wrap_object_storage_path_msg(opts),
                               build_azure_tls_debug_suffix(e.what(), _config.tls_debug_context));
        LOG(WARNING) << msg;
        return {
                .status = ObjStorageStatus {ObjStorageStatus::INTERNAL_ERROR, std::move(msg)},
                .http_code = 0,
                .request_id = "",
        };
    }
}

std::string AzureObjStorageClient::generate_presigned_url(const ObjStoragePath& opts,
                                                          int64_t expiration_secs) {
    Azure::Storage::Sas::BlobSasBuilder sas_builder;
    sas_builder.ExpiresOn =
            std::chrono::system_clock::now() + std::chrono::seconds(expiration_secs);
    sas_builder.BlobContainerName = opts.bucket;
    sas_builder.BlobName = opts.key;
    sas_builder.Resource = Azure::Storage::Sas::BlobSasResource::Blob;
    sas_builder.Protocol = Azure::Storage::Sas::SasProtocol::HttpsOnly;
    sas_builder.SetPermissions(Azure::Storage::Sas::BlobSasPermissions::Read);

    auto blob = _client->GetBlobContainerClient(opts.bucket).GetBlobClient(opts.key);
    return blob.GetUrl() + sas_builder.GenerateSasToken(*_credential);
}

ObjStorageResponse AzureObjStorageClient::get_lifecycle(const std::string& /*bucket*/,
                                                        int64_t* expiration_days) {
    // TODO(plat1ko)
    *expiration_days = INT64_MAX;
    return ObjStorageResponse::OK();
}

ObjStorageResponse AzureObjStorageClient::check_versioning(const std::string& /*bucket*/) {
    // TODO(plat1ko)
    return ObjStorageResponse::OK();
}

ObjStorageResponse AzureObjStorageClient::abort_multipart_upload(const ObjStoragePath& opts,
                                                                 const std::string& upload_id) {
    // delete uncommitted blobs
    // https://learn.microsoft.com/en-us/rest/api/storageservices/delete-blob?tabs=microsoft-entra-id#remarks
    return delete_object(opts);
}
} // namespace doris
