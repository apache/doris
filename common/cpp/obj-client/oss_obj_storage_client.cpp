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

#include "oss_obj_storage_client.h"

#include <alibabacloud/oss2/ClientConfiguration.h>
#include <alibabacloud/oss2/OSSClient.h>
#include <alibabacloud/oss2/credentials/CredentialsProvider.h>
#include <alibabacloud/oss2/io/ByteWriter.h>
#include <alibabacloud/oss2/transport/curl/CurlTransportFactory.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/utils/DateTime.h>
#include <fmt/format.h>

#include <pugixml.hpp>

namespace doris {
namespace oss = alibabacloud::oss2;
namespace model = oss::models;
namespace {
ObjStorageResponse failure(const oss::OperationError& error) {
    int code = error.getStatusCode();
    return {.status = obj_storage_status_from_http_code(
                    code, error.getCode() + ": " + error.getMessage()),
            .http_code = code,
            .request_id = error.getRequestId()};
}

template <typename Outcome>
ObjStorageResponse response(const Outcome& outcome) {
    return outcome.has_value() ? ObjStorageResponse::OK() : failure(outcome.error());
}

ObjStorageResponse invalid_response(std::string message) {
    return {.status = {ObjStorageStatus::IO_ERROR, std::move(message)}};
}

bool lifecycle_prefix_overlaps(pugi::xml_node rule, std::string_view vault_prefix) {
    std::string_view rule_prefix = rule.child_value("Prefix");
    if (!vault_prefix.starts_with(rule_prefix) && !rule_prefix.starts_with(vault_prefix)) {
        return false;
    }
    // The longer prefix describes the intersection (e.g. "vault/" and "vault/cold/").
    // A prefix-only Not can exclude all of it. Tags and size filters need object metadata,
    // so keep potentially matching rules for the checker's earliest-expiration alarm.
    auto intersection = vault_prefix.size() >= rule_prefix.size() ? vault_prefix : rule_prefix;
    for (auto excluded : rule.child("Filter").children("Not")) {
        if (!excluded.child("Tag") && excluded.child("Prefix") &&
            intersection.starts_with(excluded.child_value("Prefix"))) {
            return false;
        }
    }
    return true;
}
} // namespace

OssObjStorageClient::OssObjStorageClient(
        const Aws::Client::ClientConfiguration& config,
        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials, bool use_virtual_addressing,
        long max_retries) {
    oss::ClientConfiguration options;
    options.region = config.region;
    if (!config.endpointOverride.empty()) {
        options.endpoint = config.endpointOverride;
    }
    options.disableSsl = config.scheme == Aws::Http::Scheme::HTTP;
    options.usePathStyle = !use_virtual_addressing;
    options.retryMaxAttempts = max_retries + 1;
    oss::CurlTransportOptions curl;
    curl.connectTimeout = config.connectTimeoutMs;
    curl.readWriteTimeout = config.requestTimeoutMs;
    curl.maxConnections = config.maxConnections;
    if (!config.caFile.empty()) {
        curl.caFile = config.caFile;
    }
    options.httpTransport = oss::CurlTransportFactory::createHttpTransport(curl);
    if (std::dynamic_pointer_cast<Aws::Auth::AnonymousAWSCredentialsProvider>(credentials)) {
        options.credentialsProvider = std::make_shared<oss::AnonymousCredentialsProvider>();
    } else {
        // Resolve on each signature so expiring session credentials can be refreshed.
        options.credentialsProvider = std::make_shared<oss::CredentialsProviderFunc>(
                [credentials = std::move(credentials)] {
                    auto keys = credentials->GetAWSCredentials();
                    if (keys.IsEmpty()) {
                        return oss::Credentials::withError("No credentials available for OSS");
                    }
                    return oss::Credentials(keys.GetAWSAccessKeyId(), keys.GetAWSSecretKey(),
                                            keys.GetSessionToken());
                });
    }
    _client = std::make_unique<oss::OSSClient>(options);
}

OssObjStorageClient::~OssObjStorageClient() = default;

ObjStorageResponse OssObjStorageClient::put_object(const ObjStoragePath& path,
                                                   std::string_view data) {
    return response(_client->putObject(model::PutObjectRequest()
                                               .setBucket(path.bucket)
                                               .setKey(path.key)
                                               .setBody(oss::RequestBody::fromMemory(data))));
}

ObjStorageUploadResult OssObjStorageClient::create_multipart_upload(const ObjStoragePath& path) {
    auto result = _client->initiateMultipartUpload(
            model::InitiateMultipartUploadRequest().setBucket(path.bucket).setKey(path.key));
    if (!result.has_value()) {
        return {.resp = failure(result.error())};
    }
    return {.upload_id = result.value().getUploadId()};
}

ObjStorageUploadResult OssObjStorageClient::upload_part(const ObjStoragePath& path,
                                                        const std::string& upload_id,
                                                        std::string_view data, int part_num) {
    auto result = _client->uploadPart(model::UploadPartRequest()
                                              .setBucket(path.bucket)
                                              .setKey(path.key)
                                              .setUploadId(upload_id)
                                              .setPartNumber(part_num)
                                              .setBody(oss::RequestBody::fromMemory(data)));
    if (!result.has_value()) {
        return {.resp = failure(result.error())};
    }
    return {.etag = result.value().getETag()};
}

ObjStorageResponse OssObjStorageClient::complete_multipart_upload(
        const ObjStoragePath& path, const std::string& upload_id,
        const std::vector<ObjStorageCompletedPart>& parts) {
    model::CompleteMultipartUpload upload;
    for (const auto& part : parts) {
        upload.parts.push_back(model::Part().setPartNumber(part.part_num).setETag(part.etag));
    }
    return response(_client->completeMultipartUpload(
            model::CompleteMultipartUploadRequest()
                    .setBucket(path.bucket)
                    .setKey(path.key)
                    .setUploadId(upload_id)
                    .setCompleteMultipartUpload(std::move(upload))));
}

ObjStorageResponse OssObjStorageClient::abort_multipart_upload(const ObjStoragePath& path,
                                                               const std::string& upload_id) {
    auto result = _client->abortMultipartUpload(model::AbortMultipartUploadRequest()
                                                        .setBucket(path.bucket)
                                                        .setKey(path.key)
                                                        .setUploadId(upload_id));
    if (!result.has_value() && result.error().getStatusCode() == 404) {
        return ObjStorageResponse::OK();
    }
    return response(result);
}

ObjStorageHeadResult OssObjStorageClient::head_object(const ObjStoragePath& path) {
    auto result =
            _client->headObject(model::HeadObjectRequest().setBucket(path.bucket).setKey(path.key));
    if (!result.has_value()) {
        return {.resp = failure(result.error())};
    }
    return {.file_size = result.value().getContentLength()};
}

ObjStorageResponse OssObjStorageClient::get_object(const ObjStoragePath& path, void* buffer,
                                                   size_t offset, size_t length,
                                                   size_t* size_return) {
    *size_return = 0;
    if (length == 0) {
        return ObjStorageResponse::OK();
    }
    std::shared_ptr<oss::MemoryWriter> writer;
    oss::SinkFactory sink;
    sink.isOneShot = false;
    sink.supplier = [&](int64_t, const oss::HeaderCollection&) {
        writer = std::make_shared<oss::MemoryWriter>(static_cast<uint8_t*>(buffer), length);
        return writer;
    };
    auto result = _client->getObject(
            model::GetObjectRequest()
                    .setBucket(path.bucket)
                    .setKey(path.key)
                    .setRange(fmt::format("bytes={}-{}", offset, offset + length - 1))
                    .setRangeBehavior("standard")
                    .setSinkFactory(std::move(sink)));
    if (!result.has_value()) {
        return failure(result.error());
    }
    if (!writer || writer->written() != length ||
        (offset != 0 && result.value().getStatusCode() != 206)) {
        return invalid_response("Incomplete or ignored OSS range read");
    }
    *size_return = writer->written();
    return ObjStorageResponse::OK();
}

ObjStorageListPageResult OssObjStorageClient::list_objects_page(const ObjStoragePath& path,
                                                                std::string_view token) {
    model::ListObjectsV2Request request;
    request.setBucket(path.bucket)
            .setPrefix(path.prefix.empty() ? path.key : path.prefix)
            .setMaxKeys(static_cast<int>(capabilities().max_list_page));
    if (!token.empty()) {
        request.setContinuationToken(std::string(token));
    }
    auto result = _client->listObjectsV2(request);
    if (!result.has_value()) {
        return {.resp = failure(result.error())};
    }
    ObjStorageListPageResult page;
    page.has_more = result.value().getIsTruncated();
    page.continuation_token = result.value().getNextContinuationToken();
    if (page.has_more && (page.continuation_token.empty() || page.continuation_token == token)) {
        return {.resp = invalid_response("Truncated OSS listing has no new continuation token")};
    }
    for (const auto& object : result.value().getContents()) {
        Aws::Utils::DateTime modified(object.lastModified.c_str(),
                                      Aws::Utils::DateFormat::ISO_8601);
        if (!modified.WasParseSuccessful()) {
            return {.resp = invalid_response("Invalid OSS LastModified timestamp")};
        }
        page.objects.push_back(
                {.key = object.key, .size = object.size, .mtime_s = modified.Seconds()});
    }
    return page;
}

ObjStorageResponse OssObjStorageClient::delete_object(const ObjStoragePath& path) {
    return response(_client->deleteObject(
            model::DeleteObjectRequest().setBucket(path.bucket).setKey(path.key)));
}

ObjStorageResponse OssObjStorageClient::delete_objects(const ObjStoragePath& path,
                                                       std::vector<std::string> keys) {
    for (size_t i = 0; i < keys.size();) {
        model::Delete batch;
        batch.quiet = false;
        auto end = std::min(keys.size(), i + capabilities().max_delete_batch);
        for (; i < end; ++i) {
            batch.objects.push_back(model::ObjectIdentifier().setKey(std::move(keys[i])));
        }
        auto count = batch.objects.size();
        auto result = _client->deleteMultipleObjects(model::DeleteMultipleObjectsRequest()
                                                             .setBucket(path.bucket)
                                                             .setDelete(std::move(batch)));
        if (!result.has_value()) {
            return failure(result.error());
        }
        if (result.value().getDeletedObjects().size() != count) {
            return invalid_response("Incomplete OSS batch deletion");
        }
    }
    return ObjStorageResponse::OK();
}

std::string OssObjStorageClient::generate_presigned_url(const ObjStoragePath& path,
                                                        int64_t expiration_secs) {
    model::PresignOptions options;
    options.setExpirationDuration(std::chrono::seconds(expiration_secs));
    auto result = _client->presign(
            model::GetObjectRequest().setBucket(path.bucket).setKey(path.key), &options);
    return result.has_value() ? result.value().getUrl() : std::string {};
}

ObjStorageResponse OssObjStorageClient::check_versioning(const std::string& bucket) {
    auto result =
            _client->getBucketVersioning(model::GetBucketVersioningRequest().setBucket(bucket));
    if (!result.has_value()) {
        return failure(result.error());
    }
    if (result.value().getVersioningConfiguration().status != "Enabled") {
        return invalid_response("OSS bucket versioning is disabled");
    }
    return ObjStorageResponse::OK();
}

ObjStorageResponse OssObjStorageClient::get_lifecycle(const ObjStoragePath& path,
                                                      int64_t* expiration_days) {
    // OSS SDK 0.2.0 does not yet expose a typed GetBucketLifecycle operation.
    oss::OperationInput input;
    input.opName = "GetBucketLifecycle";
    input.method = "GET";
    input.bucket = path.bucket;
    input.parameters["lifecycle"] = "";
    auto result = _client->invokeOperation(input);
    if (auto* error = std::get_if<oss::OperationError>(&result)) {
        return failure(*error);
    }
    auto& body = std::get<oss::OperationOutput>(result).body;
    pugi::xml_document doc;
    if (!body || !doc.load(*body)) {
        return invalid_response("Invalid OSS bucket lifecycle XML");
    }
    std::optional<int64_t> shortest;
    for (auto rule : doc.child("LifecycleConfiguration").children("Rule")) {
        auto expiration = rule.child("NoncurrentVersionExpiration").child("NoncurrentDays");
        if (!expiration || std::string_view(rule.child_value("Status")) != "Enabled" ||
            !lifecycle_prefix_overlaps(rule, path.prefix)) {
            continue;
        }
        int64_t days = expiration.text().as_llong();
        if (days <= 0) {
            return invalid_response("Invalid OSS noncurrent version expiration");
        }
        shortest = shortest ? std::min(*shortest, days) : days;
    }
    if (!shortest) {
        return invalid_response(
                fmt::format("No enabled OSS noncurrent version expiration for {}/{}", path.bucket,
                            path.prefix));
    }
    *expiration_days = *shortest;
    return ObjStorageResponse::OK();
}

} // namespace doris
