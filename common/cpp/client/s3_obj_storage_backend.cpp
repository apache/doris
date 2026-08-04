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

#include "s3_obj_storage_backend.h"

#include <cpp/client/obj_storage_client.h>
#include <gen_cpp/Status_types.h>

#include <algorithm>
#include <chrono>

#include "client_bvar.h"
#include "cpp/obj_retry_strategy.h"

namespace Aws::S3::Model {
class DeleteObjectRequest;
} // namespace Aws::S3::Model

using Aws::S3::Model::CompletedPart;
using Aws::S3::Model::CompletedMultipartUpload;
using Aws::S3::Model::CompleteMultipartUploadRequest;
using Aws::S3::Model::CreateMultipartUploadRequest;
using Aws::S3::Model::UploadPartRequest;
using Aws::S3::Model::UploadPartOutcome;

namespace doris {
using namespace Aws::S3::Model;
namespace {

constexpr int64_t S3_REQUEST_THRESHOLD_MS = 5000;

int64_t elapsed_time_milliseconds(std::chrono::steady_clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                                 start)
            .count();
}

void record_s3_request_failed(const Aws::S3::S3Error& error) {
    record_object_request_failed(static_cast<int>(error.GetResponseCode()));
}

} // namespace

ObjectStorageStatus s3fs_error(const Aws::S3::S3Error& err, std::string_view msg) {
    using namespace Aws::Http;
    switch (err.GetResponseCode()) {
    case HttpResponseCode::NOT_FOUND:
        return {TStatusCode::NOT_FOUND,
                fmt::format("{}: {} {}", msg, err.GetExceptionName(), err.GetMessage())};
    case HttpResponseCode::FORBIDDEN:
        // TODO: no permission and other 4xx errors should be handled separately
        return {TStatusCode::NOT_AUTHORIZED,
                fmt::format("{}: {} {}", msg, err.GetExceptionName(), err.GetMessage())};
    case HttpResponseCode::REQUEST_NOT_MADE:
        return {-1, fmt::format("{}: {} {}", msg, err.GetExceptionName(), err.GetMessage())};
    default:
        return {TStatusCode::INTERNAL_ERROR,
                fmt::format("{}: {} {}", msg, err.GetExceptionName(), err.GetMessage())};
    }
}

ObjectStorageUploadResponse S3ObjStorageBackend::create_multipart_upload(
        const ObjectStoragePathOptions& opts) {
    CreateMultipartUploadRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    request.SetContentType("application/octet-stream");

    const auto start = std::chrono::steady_clock::now();
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            [&]() {
                client_bvar::ScopedLatency scoped_latency(
                        client_bvar::s3_multi_part_upload_latency);
                return _client->CreateMultipartUpload(request);
            }(),
            "s3_file_writer::create_multi_part_upload", std::cref(request).get());
    SYNC_POINT_CALLBACK("s3_file_writer::_open", &outcome);
    const auto elapsed_ms = elapsed_time_milliseconds(start);

    const auto& request_id = outcome.IsSuccess() ? outcome.GetResult().GetRequestId()
                                                 : outcome.GetError().GetRequestId();

    LOG_IF(INFO, elapsed_ms > S3_REQUEST_THRESHOLD_MS)
            << "CreateMultipartUpload cost=" << elapsed_ms << "ms"
            << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key;

    if (!outcome.IsSuccess()) {
        record_s3_request_failed(outcome.GetError());
        auto st = s3fs_error(outcome.GetError(), fmt::format("failed to CreateMultipartUpload: {} ",
                                                             opts.path.native()));
        LOG(WARNING) << st.code << " request_id=" << request_id;
        return ObjectStorageUploadResponse {
                .resp = {.status = st,
                         .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                         .request_id = outcome.GetError().GetRequestId()},
        };
    }

    return ObjectStorageUploadResponse {.resp = ObjectStorageResponse::OK(),
                                        .upload_id {outcome.GetResult().GetUploadId()}};
}

ObjectStorageResponse S3ObjStorageBackend::put_object(const ObjectStoragePathOptions& opts,
                                                      std::string_view stream) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    auto string_view_stream = std::make_shared<StringViewStream>(stream.data(), stream.size());
    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*string_view_stream));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));
    request.SetBody(string_view_stream);
    request.SetContentLength(stream.size());
    request.SetContentType("application/octet-stream");

    const auto start = std::chrono::steady_clock::now();
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            [&]() {
                client_bvar::ScopedLatency scoped_latency(client_bvar::s3_put_latency);
                return _client->PutObject(request);
            }(),
            "s3_file_writer::put_object", std::cref(request).get(), &stream);
    const auto elapsed_ms = elapsed_time_milliseconds(start);

    const auto& request_id = outcome.IsSuccess() ? outcome.GetResult().GetRequestId()
                                                 : outcome.GetError().GetRequestId();

    if (!outcome.IsSuccess()) {
        record_s3_request_failed(outcome.GetError());
        auto st = s3fs_error(outcome.GetError(),
                             fmt::format("failed to put object: {}", opts.path.native()));
        LOG(WARNING) << st.code << ", request_id=" << request_id;
        return ObjectStorageResponse {
                .status = st,
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId()};
    }

    LOG_IF(INFO, elapsed_ms > S3_REQUEST_THRESHOLD_MS)
            << "PutObject cost=" << elapsed_ms << "ms"
            << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key;
    return ObjectStorageResponse::OK();
}

ObjectStorageUploadResponse S3ObjStorageBackend::upload_part(const ObjectStoragePathOptions& opts,
                                                             std::string_view stream,
                                                             int part_num) {
    UploadPartRequest request;
    request.WithBucket(opts.bucket)
            .WithKey(opts.key)
            .WithPartNumber(part_num)
            .WithUploadId(*opts.upload_id);
    auto string_view_stream = std::make_shared<StringViewStream>(stream.data(), stream.size());

    request.SetBody(string_view_stream);

    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*string_view_stream));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    request.SetContentLength(stream.size());
    request.SetContentType("application/octet-stream");

    const auto start = std::chrono::steady_clock::now();
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            [&]() {
                client_bvar::ScopedLatency scoped_latency(
                        client_bvar::s3_multi_part_upload_latency);

                return _client->UploadPart(request);
            }(),
            "s3_file_writer::upload_part", std::cref(request).get(), &stream);
    const auto elapsed_ms = elapsed_time_milliseconds(start);

    const auto& request_id = outcome.IsSuccess() ? outcome.GetResult().GetRequestId()
                                                 : outcome.GetError().GetRequestId();

    TEST_SYNC_POINT_CALLBACK("S3FileWriter::_upload_one_part", &outcome);
    if (!outcome.IsSuccess()) {
        record_s3_request_failed(outcome.GetError());
        auto st = s3fs_error(outcome.GetError(),
                             fmt::format("failed to UploadPart: {}, part_num {}, upload_id={}",
                                         opts.path.native(), part_num, *opts.upload_id));

        LOG(WARNING) << st.code << ", request_id=" << request_id;
        return ObjectStorageUploadResponse {
                .resp = {.status = st,
                         .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                         .request_id = outcome.GetError().GetRequestId()}};
    }
    LOG_IF(INFO, elapsed_ms > S3_REQUEST_THRESHOLD_MS)
            << "UploadPart cost=" << elapsed_ms << "ms"
            << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key
            << ", part_num=" << part_num << ", upload_id=" << *opts.upload_id;
    return ObjectStorageUploadResponse {.resp = ObjectStorageResponse::OK(),
                                        .etag = outcome.GetResult().GetETag()};
}

ObjectStorageResponse S3ObjStorageBackend::complete_multipart_upload(
        const ObjectStoragePathOptions& opts,
        const std::vector<ObjectCompleteMultiPart>& completed_parts) {
    CompleteMultipartUploadRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key).WithUploadId(*opts.upload_id);

    CompletedMultipartUpload completed_upload;
    std::vector<CompletedPart> complete_parts;
    std::ranges::transform(completed_parts, std::back_inserter(complete_parts),
                           [](const ObjectCompleteMultiPart& part_ptr) {
                               CompletedPart part;
                               part.SetPartNumber(part_ptr.part_num);
                               part.SetETag(part_ptr.etag);
                               return part;
                           });
    completed_upload.SetParts(std::move(complete_parts));
    request.WithMultipartUpload(completed_upload);

    TEST_SYNC_POINT_RETURN_WITH_VALUE("S3FileWriter::_complete:3", ObjectStorageResponse(), this);

    const auto start = std::chrono::steady_clock::now();
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            [&]() {
                client_bvar::ScopedLatency scoped_latency(
                        client_bvar::s3_multi_part_upload_latency);
                return _client->CompleteMultipartUpload(request);
            }(),
            "s3_file_writer::complete_multi_part", std::cref(request).get());
    const auto elapsed_ms = elapsed_time_milliseconds(start);

    const auto& request_id = outcome.IsSuccess() ? outcome.GetResult().GetRequestId()
                                                 : outcome.GetError().GetRequestId();

    if (!outcome.IsSuccess()) {
        record_s3_request_failed(outcome.GetError());
        auto st = s3fs_error(outcome.GetError(),
                             fmt::format("failed to CompleteMultipartUpload: {}, upload_id={}",
                                         opts.path.native(), *opts.upload_id));
        LOG(WARNING) << st.code << ", request_id=" << request_id;
        return {.status = st,
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId()};
    }

    LOG_IF(INFO, elapsed_ms > S3_REQUEST_THRESHOLD_MS)
            << "CompleteMultipartUpload cost=" << elapsed_ms << "ms"
            << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key
            << ", upload_id=" << *opts.upload_id;
    return ObjectStorageResponse::OK();
}

ObjectStorageHeadResponse S3ObjStorageBackend::head_object(const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);

    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            [&]() {
                client_bvar::ScopedLatency scoped_latency(client_bvar::s3_head_latency);
                return _client->HeadObject(request);
            }(),
            "s3_file_system::head_object", std::ref(request).get());

    if (outcome.IsSuccess()) {
        return {.resp = ObjectStorageResponse::OK(),
                .file_size = outcome.GetResult().GetContentLength()};
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return {.resp = {.status = TStatusCode::NOT_FOUND}, .file_size = 0};
    } else {
        record_s3_request_failed(outcome.GetError());
        LOG(WARNING) << "failed to head object"
                     << "bucket " << opts.bucket << " key " << opts.key << " responseCode "
                     << outcome.GetError() << " error " << outcome.GetError().GetMessage()
                     << " request_id " << outcome.GetError().GetRequestId();
        return {.resp = {.status = s3fs_error(
                                 outcome.GetError(),
                                 fmt::format("failed to head object: {}", opts.path.native())),
                         .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                         .request_id = outcome.GetError().GetRequestId()},
                .file_size = -1};
    }
}

ObjectStorageResponse S3ObjStorageBackend::get_object(const ObjectStoragePathOptions& opts,
                                                      void* buffer, size_t offset,
                                                      size_t bytes_read, size_t* size_return) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    request.SetRange(fmt::format("bytes={}-{}", offset, offset + bytes_read - 1));
    request.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer, bytes_read));

    auto outcome = [&]() {
        client_bvar::ScopedLatency scoped_latency(client_bvar::s3_get_latency);
        return _client->GetObject(request);
    }();
    if (!outcome.IsSuccess()) {
        record_s3_request_failed(outcome.GetError());
        return ObjectStorageResponse {
                .status = s3fs_error(outcome.GetError(),
                                     fmt::format("failed to get object: {}", opts.path.native())),
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId(),
        };
    }
    *size_return = outcome.GetResult().GetContentLength();
    SYNC_POINT_CALLBACK("s3_obj_storage_client::get_object", size_return);
    if (*size_return != bytes_read) {
        return ObjectStorageResponse {
                .status = {TStatusCode::INTERNAL_ERROR,
                           fmt::format("incomplete read from {}, expect {}, got {}",
                                       opts.path.native(), bytes_read, *size_return)}};
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageListPage S3ObjStorageBackend::list_objects(const ObjectStoragePathOptions& opts,
                                                        std::string_view continuation_token) {
    const auto& prefix = opts.prefix.empty() ? opts.key : opts.prefix;
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(opts.bucket).WithPrefix(prefix).WithMaxKeys(OBJECT_LIST_PAGE_SIZE);
    if (!continuation_token.empty()) {
        request.SetContinuationToken(std::string(continuation_token));
    }
    TEST_SYNC_POINT_CALLBACK("S3ObjStorageBackend::list_objects", &request);

    auto outcome = [&]() {
        client_bvar::ScopedLatency scoped_latency(client_bvar::s3_list_latency);
        return _client->ListObjectsV2(request);
    }();

    const auto& request_id = outcome.IsSuccess() ? outcome.GetResult().GetRequestId()
                                                 : outcome.GetError().GetRequestId();
    if (!outcome.IsSuccess()) {
        // Some S3-compatible providers (for example TOS) return NoSuchKey instead of an empty page
        // when a prefix does not exist.
        if (outcome.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY) {
            LOG(INFO) << fmt::format(
                    "NoSuchKey when listing objects, treat as empty response, endpoint: {}, "
                    "bucket: {}, prefix: {}, request_id: {}",
                    _config.endpoint, request.GetBucket(), request.GetPrefix(), request_id);
            return {.resp = ObjectStorageResponse::OK()};
        }
        record_object_request_failed(static_cast<int>(outcome.GetError().GetResponseCode()));
        const auto status = s3fs_error(outcome.GetError(),
                                       fmt::format("failed to list objects: {}, prefix: {}",
                                                   request.GetBucket(), request.GetPrefix()));
        LOG(WARNING) << fmt::format(
                "failed to list objects, endpoint: {}, bucket: {}, prefix: {}, responseCode: {}, "
                "error: {}, request_id: {}",
                _config.endpoint, request.GetBucket(), request.GetPrefix(),
                static_cast<int>(outcome.GetError().GetResponseCode()),
                outcome.GetError().GetMessage(), request_id);
        return {
                .resp = {.status = status,
                         .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                         .request_id = request_id},
        };
    }

    const auto& result = outcome.GetResult();
    if (result.GetIsTruncated() && result.GetNextContinuationToken().empty()) {
        LOG(WARNING) << fmt::format(
                "failed to list objects, isTruncated but no continuation token, endpoint: {}, "
                "bucket: {}, prefix: {}, request_id: {}",
                _config.endpoint, request.GetBucket(), request.GetPrefix(), request_id);
        return {
                .resp = {.status = {TStatusCode::INTERNAL_ERROR,
                                    fmt::format("failed to list objects: {}, prefix: {}",
                                                request.GetBucket(), request.GetPrefix())},
                         .http_code = 0,
                         .request_id = request_id},
        };
    }

    ObjectStorageListPage page {
            .resp = ObjectStorageResponse::OK(),
            .continuation_token = result.GetNextContinuationToken(),
            .has_more = result.GetIsTruncated(),
    };
    const auto& content = result.GetContents();
    page.objects.reserve(content.size());
    for (const auto& obj : content) {
        DCHECK(obj.GetKey().starts_with(request.GetPrefix()))
                << obj.GetKey() << ' ' << request.GetPrefix();
        page.objects.emplace_back(ObjectMeta {.file_path = obj.GetKey(),
                                              .size = obj.GetSize(),
                                              .mtime_s = obj.GetLastModified().Seconds()});
    }
    return page;
}

ObjectStorageResponse S3ObjStorageBackend::delete_objects(const ObjectStoragePathOptions& opts,
                                                          std::vector<std::string> objs) {
    size_t max_delete_batch = 1000;
    TEST_SYNC_POINT_CALLBACK("S3ObjClient::delete_objects", &max_delete_batch);
    TEST_SYNC_POINT_CALLBACK("S3ObjStorageClient::delete_objects", &max_delete_batch);
    max_delete_batch = std::max<size_t>(1, max_delete_batch);
    for (size_t begin = 0; begin < objs.size(); begin += max_delete_batch) {
        const size_t end = std::min(begin + max_delete_batch, objs.size());
        if (end - begin == 1) {
            auto single_opts = opts;
            single_opts.key = std::move(objs[begin]);
            auto resp = delete_object(single_opts);
            if (!resp.ok()) {
                return resp;
            }
            continue;
        }

        Aws::S3::Model::DeleteObjectsRequest delete_request;
        delete_request.SetBucket(opts.bucket);
        Aws::S3::Model::Delete del;
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        objects.reserve(end - begin);
        for (size_t i = begin; i < end; ++i) {
            Aws::S3::Model::ObjectIdentifier object;
            object.SetKey(std::move(objs[i]));
            objects.emplace_back(std::move(object));
        }
        del.WithObjects(std::move(objects)).SetQuiet(true);
        delete_request.SetDelete(std::move(del));

        auto delete_outcome = [&]() {
            client_bvar::ScopedLatency scoped_latency(client_bvar::s3_delete_objects_latency);
            return _client->DeleteObjects(delete_request);
        }();
        SYNC_POINT_CALLBACK("s3_obj_storage_client::delete_objects", &delete_outcome);
        SYNC_POINT_CALLBACK("s3_obj_storage_client::delete_objects_recursively", &delete_outcome);
        if (!delete_outcome.IsSuccess()) {
            record_s3_request_failed(delete_outcome.GetError());
            return ObjectStorageResponse {
                    .status = s3fs_error(delete_outcome.GetError(),
                                         fmt::format("failed to delete dir {}", opts.key)),
                    .http_code = static_cast<int>(delete_outcome.GetError().GetResponseCode()),
                    .request_id = delete_outcome.GetError().GetRequestId()};
        }
        if (!delete_outcome.GetResult().GetErrors().empty()) {
            const auto& error = delete_outcome.GetResult().GetErrors().front();
            return ObjectStorageResponse {
                    .status = {TStatusCode::INTERNAL_ERROR,
                               fmt::format("failed to delete object {}: {}, request_id={}",
                                           error.GetKey(), error.GetMessage(),
                                           delete_outcome.GetResult().GetRequestId())}};
        }
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse S3ObjStorageBackend::delete_object(const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);

    auto outcome = [&]() {
        client_bvar::ScopedLatency scoped_latency(client_bvar::s3_delete_object_latency);

        return _client->DeleteObject(request);
    }();
    TEST_SYNC_POINT_CALLBACK("S3ObjClient::delete_object", &outcome);
    TEST_SYNC_POINT_CALLBACK("S3ObjStorageClient::delete_object", &outcome);
    if (outcome.IsSuccess() ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return ObjectStorageResponse::OK();
    }
    record_s3_request_failed(outcome.GetError());
    return ObjectStorageResponse {
            .status = {TStatusCode::INTERNAL_ERROR,
                       fmt::format("failed to delete object {}: {}, request_id={}", opts.key,
                                   outcome.GetError().GetMessage(),
                                   outcome.GetError().GetRequestId())},
            .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
            .request_id = outcome.GetError().GetRequestId()};
}

std::string S3ObjStorageBackend::generate_presigned_url(const ObjectStoragePathOptions& opts,
                                                        int64_t expiration_secs) {
    return _client->GeneratePresignedUrl(opts.bucket, opts.key, Aws::Http::HttpMethod::HTTP_GET,
                                         expiration_secs);
}

ObjectStorageResponse S3ObjStorageBackend::check_versioning(const std::string& bucket) {
    Aws::S3::Model::GetBucketVersioningRequest request;
    request.SetBucket(bucket);

    auto outcome = _client->GetBucketVersioning(request);

    if (outcome.IsSuccess()) {
        const auto& versioning_configuration = outcome.GetResult().GetStatus();
        if (versioning_configuration != Aws::S3::Model::BucketVersioningStatus::Enabled) {
            LOG(WARNING) << "Err for check interval: bucket doesn't enable bucket versioning"
                         << " endpoint=" << _config.endpoint << " bucket=" << bucket;
            return ObjectStorageResponse {
                    .status = {TStatusCode::INTERNAL_ERROR,
                               fmt::format("bucket versioning is not enabled: {}", bucket)}};
        }
    } else {
        record_s3_request_failed(outcome.GetError());
        LOG(WARNING) << "Err for check interval: failed to get status of bucket versioning"
                     << " endpoint=" << _config.endpoint << " bucket=" << bucket
                     << " responseCode=" << static_cast<int>(outcome.GetError().GetResponseCode())
                     << " error=" << outcome.GetError().GetMessage()
                     << " request_id=" << outcome.GetError().GetRequestId();
        return ObjectStorageResponse {
                .status = {-1},
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId()};
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse S3ObjStorageBackend::abort_multipart_upload(
        const ObjectStoragePathOptions& opts, const std::string& upload_id) {
    Aws::S3::Model::AbortMultipartUploadRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key).WithUploadId(upload_id);

    auto outcome = _client->AbortMultipartUpload(request);
    if (!outcome.IsSuccess()) {
        LOG(WARNING) << "failed to abort multipart upload"
                     << " endpoint=" << _config.endpoint << " bucket=" << opts.bucket
                     << " key=" << opts.key << " upload_id=" << upload_id
                     << " responseCode=" << static_cast<int>(outcome.GetError().GetResponseCode())
                     << " error=" << outcome.GetError().GetMessage()
                     << " request_id=" << outcome.GetError().GetRequestId();
        if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
            return ObjectStorageResponse::OK();
        }
        record_s3_request_failed(outcome.GetError());
        return ObjectStorageResponse {
                .status = {TStatusCode::INTERNAL_ERROR,
                           fmt::format("failed to abort multipart upload: {}, upload_id={}",
                                       opts.path.native(), upload_id)},
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId(),
        };
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse S3ObjStorageBackend::get_life_cycle(const std::string& bucket,
                                                          int64_t* expiration_days) {
    Aws::S3::Model::GetBucketLifecycleConfigurationRequest request;
    request.SetBucket(bucket);

    auto outcome = _client->GetBucketLifecycleConfiguration(request);
    bool has_lifecycle = false;
    if (outcome.IsSuccess()) {
        const auto& rules = outcome.GetResult().GetRules();
        for (const auto& rule : rules) {
            if (rule.NoncurrentVersionExpirationHasBeenSet()) {
                has_lifecycle = true;
                *expiration_days = rule.GetNoncurrentVersionExpiration().GetNoncurrentDays();
            }
        }
    } else {
        record_s3_request_failed(outcome.GetError());
        LOG(WARNING) << "Err for check interval: failed to get bucket lifecycle"
                     << " endpoint=" << _config.endpoint << " bucket=" << bucket
                     << " responseCode=" << static_cast<int>(outcome.GetError().GetResponseCode())
                     << " error=" << outcome.GetError().GetMessage()
                     << " request_id=" << outcome.GetError().GetRequestId();
        return ObjectStorageResponse {
                .status = s3fs_error(outcome.GetError(),
                                     fmt::format("failed to get lift cycle: {}", bucket)),
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId()};
    }

    if (!has_lifecycle) {
        LOG(WARNING) << "Err for check interval: bucket doesn't have lifecycle configuration"
                     << " endpoint=" << _config.endpoint << " bucket=" << bucket;
        return ObjectStorageResponse {.status = {-1}};
    }
    return ObjectStorageResponse::OK();
}

} // namespace doris
