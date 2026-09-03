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

#include "s3_obj_storage_client.h"

#include <cpp/obj-client/obj_storage_client.h>
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

std::string object_identity(const ObjStoragePath& opts) {
    return opts.path.empty() ? opts.key : opts.path.native();
}

std::string s3_error_message(const Aws::S3::S3Error& error, std::string_view message) {
    // A failure raised by the client itself carries no request id, and a dangling
    // `request_id=` has been read as a request id of the object storage.
    std::string request_id =
            error.GetRequestId().empty() ? "<empty>" : error.GetRequestId().c_str();
    return fmt::format("{}: {} {} code={}, type={}, request_id={}", message,
                       error.GetExceptionName(), error.GetMessage(),
                       static_cast<int>(error.GetResponseCode()),
                       static_cast<int>(error.GetErrorType()), request_id);
}

} // namespace

ObjStorageStatus s3fs_error(const Aws::S3::S3Error& err, std::string_view msg) {
    return obj_storage_status_from_http_code(static_cast<int>(err.GetResponseCode()),
                                             s3_error_message(err, msg));
}

ObjStorageResponse S3ObjStorageClient::_gcp_token_unavailable() {
    return {
            .status = {ObjStorageStatus::NETWORK_ERROR,
                       "GCP workload identity token is unavailable"},
            .http_code = 0,
    };
}

ObjStorageUploadResult S3ObjStorageClient::create_multipart_upload(const ObjStoragePath& opts) {
    CreateMultipartUploadRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    request.SetContentType("application/octet-stream");
    if (!_set_gcp_authorization_header(request)) {
        return {.resp = _gcp_token_unavailable()};
    }

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
        return ObjStorageUploadResult {
                .resp = {.status = st,
                         .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                         .request_id = outcome.GetError().GetRequestId()},
        };
    }

    return ObjStorageUploadResult {.resp = ObjStorageResponse::OK(),
                                   .upload_id {outcome.GetResult().GetUploadId()}};
}

ObjStorageResponse S3ObjStorageClient::put_object(const ObjStoragePath& opts,
                                                  std::string_view stream) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    auto string_view_stream = std::make_shared<StringViewStream>(stream.data(), stream.size());
    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*string_view_stream));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));
    request.SetBody(string_view_stream);
    request.SetContentLength(stream.size());
    request.SetContentType("application/octet-stream");
    if (!_set_gcp_authorization_header(request)) {
        return _gcp_token_unavailable();
    }

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
        return ObjStorageResponse {
                .status = st,
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId()};
    }

    LOG_IF(INFO, elapsed_ms > S3_REQUEST_THRESHOLD_MS)
            << "PutObject cost=" << elapsed_ms << "ms"
            << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key;
    return ObjStorageResponse::OK();
}

ObjStorageUploadResult S3ObjStorageClient::upload_part(const ObjStoragePath& opts,
                                                       const std::string& upload_id,
                                                       std::string_view stream, int part_num) {
    UploadPartRequest request;
    request.WithBucket(opts.bucket)
            .WithKey(opts.key)
            .WithPartNumber(part_num)
            .WithUploadId(upload_id);
    auto string_view_stream = std::make_shared<StringViewStream>(stream.data(), stream.size());

    request.SetBody(string_view_stream);

    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*string_view_stream));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    request.SetContentLength(stream.size());
    request.SetContentType("application/octet-stream");
    if (!_set_gcp_authorization_header(request)) {
        return {.resp = _gcp_token_unavailable()};
    }

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
                                         opts.path.native(), part_num, upload_id));

        LOG(WARNING) << st.code << ", request_id=" << request_id;
        return ObjStorageUploadResult {
                .resp = {.status = st,
                         .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                         .request_id = outcome.GetError().GetRequestId()}};
    }
    LOG_IF(INFO, elapsed_ms > S3_REQUEST_THRESHOLD_MS)
            << "UploadPart cost=" << elapsed_ms << "ms"
            << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key
            << ", part_num=" << part_num << ", upload_id=" << upload_id;
    return ObjStorageUploadResult {.resp = ObjStorageResponse::OK(),
                                   .etag = outcome.GetResult().GetETag()};
}

ObjStorageResponse S3ObjStorageClient::complete_multipart_upload(
        const ObjStoragePath& opts, const std::string& upload_id,
        const std::vector<ObjStorageCompletedPart>& completed_parts) {
    CompleteMultipartUploadRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key).WithUploadId(upload_id);

    CompletedMultipartUpload completed_upload;
    std::vector<CompletedPart> complete_parts;
    std::ranges::transform(completed_parts, std::back_inserter(complete_parts),
                           [](const ObjStorageCompletedPart& part_ptr) {
                               CompletedPart part;
                               part.SetPartNumber(part_ptr.part_num);
                               part.SetETag(part_ptr.etag);
                               return part;
                           });
    completed_upload.SetParts(std::move(complete_parts));
    request.WithMultipartUpload(completed_upload);
    if (!_set_gcp_authorization_header(request)) {
        return _gcp_token_unavailable();
    }

    TEST_SYNC_POINT_RETURN_WITH_VALUE("S3FileWriter::_complete:3", ObjStorageResponse(), this);

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
                                         opts.path.native(), upload_id));
        LOG(WARNING) << st.code << ", request_id=" << request_id;
        return {.status = st,
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId()};
    }

    LOG_IF(INFO, elapsed_ms > S3_REQUEST_THRESHOLD_MS)
            << "CompleteMultipartUpload cost=" << elapsed_ms << "ms"
            << ", request_id=" << request_id << ", bucket=" << opts.bucket << ", key=" << opts.key
            << ", upload_id=" << upload_id;
    return ObjStorageResponse::OK();
}

ObjStorageHeadResult S3ObjStorageClient::head_object(const ObjStoragePath& opts) {
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    if (!_set_gcp_authorization_header(request)) {
        return {.resp = _gcp_token_unavailable()};
    }

    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            [&]() {
                client_bvar::ScopedLatency scoped_latency(client_bvar::s3_head_latency);
                return _client->HeadObject(request);
            }(),
            "s3_file_system::head_object", std::ref(request).get());

    if (outcome.IsSuccess()) {
        return {.resp = ObjStorageResponse::OK(),
                .file_size = outcome.GetResult().GetContentLength()};
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return {.resp = {.status = ObjStorageStatus::NOT_FOUND}, .file_size = 0};
    } else {
        record_s3_request_failed(outcome.GetError());
        LOG(WARNING) << "failed to head object"
                     << "bucket " << opts.bucket << " key " << opts.key << " responseCode "
                     << outcome.GetError() << " error " << outcome.GetError().GetMessage()
                     << " request_id " << outcome.GetError().GetRequestId();
        return {.resp = {.status = s3fs_error(
                                 outcome.GetError(),
                                 fmt::format("failed to head object: {}", object_identity(opts))),
                         .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                         .request_id = outcome.GetError().GetRequestId()},
                .file_size = -1};
    }
}

ObjStorageResponse S3ObjStorageClient::get_object(const ObjStoragePath& opts, void* buffer,
                                                  size_t offset, size_t bytes_read,
                                                  size_t* size_return) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    request.SetRange(fmt::format("bytes={}-{}", offset, offset + bytes_read - 1));
    request.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer, bytes_read));
    if (!_set_gcp_authorization_header(request)) {
        return _gcp_token_unavailable();
    }

    auto outcome = [&]() {
        client_bvar::ScopedLatency scoped_latency(client_bvar::s3_get_latency);
        return _client->GetObject(request);
    }();
    if (!outcome.IsSuccess()) {
        record_s3_request_failed(outcome.GetError());
        return ObjStorageResponse {
                .status = s3fs_error(
                        outcome.GetError(),
                        fmt::format("failed to get object: bucket={} object={} offset={} size={}",
                                    opts.bucket, object_identity(opts), offset, bytes_read)),
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId(),
        };
    }
    *size_return = outcome.GetResult().GetContentLength();
    // Short read, or a server or a proxy answering a ranged read with the whole object.
    SYNC_POINT_CALLBACK("s3_obj_storage_client::get_object", size_return);
    if (*size_return != bytes_read) {
        const auto& request_id = outcome.GetResult().GetRequestId();
        return ObjStorageResponse {
                .status = {ObjStorageStatus::INTERNAL_ERROR,
                           fmt::format("incomplete read from bucket={} object={} offset={}, expect "
                                       "{}, got {}, request_id={}",
                                       opts.bucket, object_identity(opts), offset, bytes_read,
                                       *size_return, request_id)},
                .request_id = request_id};
    }
    return ObjStorageResponse::OK();
}

ObjStorageListPageResult S3ObjStorageClient::list_objects_page(
        const ObjStoragePath& opts, std::string_view continuation_token) {
    const auto& prefix = opts.prefix.empty() ? opts.key : opts.prefix;
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(opts.bucket)
            .WithPrefix(prefix)
            .WithMaxKeys(static_cast<int>(capabilities().max_list_page));
    if (!continuation_token.empty()) {
        request.SetContinuationToken(std::string(continuation_token));
    }
    if (!_set_gcp_authorization_header(request)) {
        return {.resp = _gcp_token_unavailable()};
    }
    TEST_SYNC_POINT_CALLBACK("S3ObjStorageClient::list_objects", &request);

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
            return {.resp = ObjStorageResponse::OK()};
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
                .resp = {.status = {ObjStorageStatus::INTERNAL_ERROR,
                                    fmt::format("failed to list objects: {}, prefix: {}",
                                                request.GetBucket(), request.GetPrefix())},
                         .http_code = 0,
                         .request_id = request_id},
        };
    }

    ObjStorageListPageResult page {
            .resp = ObjStorageResponse::OK(),
            .continuation_token = result.GetNextContinuationToken(),
            .has_more = result.GetIsTruncated(),
    };
    const auto& content = result.GetContents();
    page.objects.reserve(content.size());
    for (const auto& obj : content) {
        DCHECK(obj.GetKey().starts_with(request.GetPrefix()))
                << obj.GetKey() << ' ' << request.GetPrefix();
        page.objects.emplace_back(ObjectMeta {.key = obj.GetKey(),
                                              .size = obj.GetSize(),
                                              .mtime_s = obj.GetLastModified().Seconds()});
    }
    return page;
}

ObjStorageResponse S3ObjStorageClient::delete_objects(const ObjStoragePath& opts,
                                                      std::vector<std::string> objs) {
    size_t max_delete_batch = 1000;
    TEST_SYNC_POINT_CALLBACK("S3ObjClient::delete_objects", &max_delete_batch);
    TEST_SYNC_POINT_CALLBACK("S3ObjStorageClient::delete_objects", &max_delete_batch);
    max_delete_batch = std::max<size_t>(1, max_delete_batch);
    for (size_t begin = 0; begin < objs.size(); begin += max_delete_batch) {
        const size_t end = std::min(begin + max_delete_batch, objs.size());
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
        if (!_set_gcp_authorization_header(delete_request)) {
            return _gcp_token_unavailable();
        }

        auto delete_outcome = [&]() {
            client_bvar::ScopedLatency scoped_latency(client_bvar::s3_delete_objects_latency);
            return _client->DeleteObjects(delete_request);
        }();
        SYNC_POINT_CALLBACK("s3_obj_storage_client::delete_objects", &delete_outcome);
        SYNC_POINT_CALLBACK("s3_obj_storage_client::delete_objects_recursively", &delete_outcome);
        if (!delete_outcome.IsSuccess()) {
            record_s3_request_failed(delete_outcome.GetError());
            LOG(WARNING) << fmt::format(
                    "failed to delete objects, endpoint: {}, bucket: {}, key: {}, responseCode: "
                    "{}, error: {}, request_id: {}",
                    _config.endpoint, opts.bucket,
                    delete_request.GetDelete().GetObjects().front().GetKey(),
                    static_cast<int>(delete_outcome.GetError().GetResponseCode()),
                    delete_outcome.GetError().GetMessage(),
                    delete_outcome.GetError().GetRequestId());
            return ObjStorageResponse {
                    .status = s3fs_error(delete_outcome.GetError(),
                                         fmt::format("failed to delete dir {}", opts.key)),
                    .http_code = static_cast<int>(delete_outcome.GetError().GetResponseCode()),
                    .request_id = delete_outcome.GetError().GetRequestId()};
        }
        if (!delete_outcome.GetResult().GetErrors().empty()) {
            const auto& error = delete_outcome.GetResult().GetErrors().front();
            LOG(WARNING) << fmt::format(
                    "failed to delete object in batch, endpoint: {}, bucket: {}, key: {}, error "
                    "code: {}, error: {}, request_id: {}",
                    _config.endpoint, opts.bucket, error.GetKey(), error.GetCode(),
                    error.GetMessage(), delete_outcome.GetResult().GetRequestId());
            return ObjStorageResponse {
                    .status = {ObjStorageStatus::INTERNAL_ERROR,
                               fmt::format("failed to delete object {}: {}, request_id={}",
                                           error.GetKey(), error.GetMessage(),
                                           delete_outcome.GetResult().GetRequestId())},
                    .request_id = delete_outcome.GetResult().GetRequestId()};
        }
    }
    return ObjStorageResponse::OK();
}

ObjStorageResponse S3ObjStorageClient::delete_object(const ObjStoragePath& opts) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    if (!_set_gcp_authorization_header(request)) {
        return _gcp_token_unavailable();
    }

    auto outcome = [&]() {
        client_bvar::ScopedLatency scoped_latency(client_bvar::s3_delete_object_latency);

        return _client->DeleteObject(request);
    }();
    TEST_SYNC_POINT_CALLBACK("S3ObjClient::delete_object", &outcome);
    TEST_SYNC_POINT_CALLBACK("S3ObjStorageClient::delete_object", &outcome);
    if (outcome.IsSuccess()) {
        return ObjStorageResponse::OK();
    }
    ObjStorageResponse response {
            .status = s3fs_error(outcome.GetError(),
                                 fmt::format("failed to delete object {}", opts.key)),
            .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
            .request_id = outcome.GetError().GetRequestId()};
    if (response.status.code == ObjStorageStatus::NOT_FOUND) {
        return response;
    }
    record_s3_request_failed(outcome.GetError());
    LOG(WARNING) << fmt::format(
            "failed to delete object, endpoint: {}, bucket: {}, key: {}, responseCode: {}, "
            "error: {}, request_id: {}",
            _config.endpoint, opts.bucket, opts.key,
            static_cast<int>(outcome.GetError().GetResponseCode()), outcome.GetError().GetMessage(),
            outcome.GetError().GetRequestId());
    return response;
}

std::string S3ObjStorageClient::generate_presigned_url(const ObjStoragePath& opts,
                                                       int64_t expiration_secs) {
    if (_token_provider != nullptr) {
        return {};
    }
    return _client->GeneratePresignedUrl(opts.bucket, opts.key, Aws::Http::HttpMethod::HTTP_GET,
                                         expiration_secs);
}

ObjStorageResponse S3ObjStorageClient::check_versioning(const std::string& bucket) {
    Aws::S3::Model::GetBucketVersioningRequest request;
    request.SetBucket(bucket);
    if (!_set_gcp_authorization_header(request)) {
        return _gcp_token_unavailable();
    }

    auto outcome = _client->GetBucketVersioning(request);

    if (outcome.IsSuccess()) {
        const auto& versioning_configuration = outcome.GetResult().GetStatus();
        if (versioning_configuration != Aws::S3::Model::BucketVersioningStatus::Enabled) {
            LOG(WARNING) << "Err for check interval: bucket doesn't enable bucket versioning"
                         << " endpoint=" << _config.endpoint << " bucket=" << bucket;
            return ObjStorageResponse {
                    .status = {ObjStorageStatus::INTERNAL_ERROR,
                               fmt::format("bucket versioning is not enabled: {}", bucket)}};
        }
    } else {
        record_s3_request_failed(outcome.GetError());
        LOG(WARNING) << "Err for check interval: failed to get status of bucket versioning"
                     << " endpoint=" << _config.endpoint << " bucket=" << bucket
                     << " responseCode=" << static_cast<int>(outcome.GetError().GetResponseCode())
                     << " error=" << outcome.GetError().GetMessage()
                     << " request_id=" << outcome.GetError().GetRequestId();
        return ObjStorageResponse {
                .status = s3fs_error(outcome.GetError(),
                                     fmt::format("failed to get bucket versioning: {}", bucket)),
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId()};
    }
    return ObjStorageResponse::OK();
}

ObjStorageResponse S3ObjStorageClient::abort_multipart_upload(const ObjStoragePath& opts,
                                                              const std::string& upload_id) {
    Aws::S3::Model::AbortMultipartUploadRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key).WithUploadId(upload_id);
    if (!_set_gcp_authorization_header(request)) {
        return _gcp_token_unavailable();
    }

    auto outcome = _client->AbortMultipartUpload(request);
    if (!outcome.IsSuccess()) {
        LOG(WARNING) << "failed to abort multipart upload"
                     << " endpoint=" << _config.endpoint << " bucket=" << opts.bucket
                     << " key=" << opts.key << " upload_id=" << upload_id
                     << " responseCode=" << static_cast<int>(outcome.GetError().GetResponseCode())
                     << " error=" << outcome.GetError().GetMessage()
                     << " request_id=" << outcome.GetError().GetRequestId();
        if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
            return ObjStorageResponse::OK();
        }
        record_s3_request_failed(outcome.GetError());
        return ObjStorageResponse {
                .status =
                        s3fs_error(outcome.GetError(),
                                   fmt::format("failed to abort multipart upload: {}, upload_id={}",
                                               opts.path.native(), upload_id)),
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId(),
        };
    }
    return ObjStorageResponse::OK();
}

ObjStorageResponse S3ObjStorageClient::get_lifecycle(const std::string& bucket,
                                                     int64_t* expiration_days) {
    Aws::S3::Model::GetBucketLifecycleConfigurationRequest request;
    request.SetBucket(bucket);
    if (!_set_gcp_authorization_header(request)) {
        return _gcp_token_unavailable();
    }

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
        return ObjStorageResponse {
                .status = s3fs_error(outcome.GetError(),
                                     fmt::format("failed to get lift cycle: {}", bucket)),
                .http_code = static_cast<int>(outcome.GetError().GetResponseCode()),
                .request_id = outcome.GetError().GetRequestId()};
    }

    if (!has_lifecycle) {
        LOG(WARNING) << "Err for check interval: bucket doesn't have lifecycle configuration"
                     << " endpoint=" << _config.endpoint << " bucket=" << bucket;
        return ObjStorageResponse {
                .status = {ObjStorageStatus::NOT_FOUND,
                           fmt::format("bucket has no lifecycle configuration: {}", bucket)}};
    }
    return ObjStorageResponse::OK();
}

} // namespace doris
