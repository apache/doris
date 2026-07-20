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

#include "io/fs/rate_limited_obj_storage_client.h"

#include "common/logging.h"
#include "common/status.h"
#include "util/s3_rate_limiter_manager.h"

namespace doris::io {
namespace {

ObjectStorageResponse rate_limited_response(S3RateLimitType type, S3RateLimitRejectReason reason) {
    CHECK(reason != S3RateLimitRejectReason::NONE);
    const auto* limit_type = reason == S3RateLimitRejectReason::QPS ? "QPS" : "bytes";
    return {.status = convert_to_obj_response(Status::Error<ErrorCode::EXCEEDED_LIMIT, false>(
                    "s3 {} request exceeds {} limit, rejected by BE rate limiter", to_string(type),
                    limit_type)),
            .http_code = 429};
}

} // namespace

ObjectStorageUploadResponse RateLimitedObjStorageClient::create_multipart_upload(
        const ObjectStoragePathOptions& opts) {
    S3RateLimitGuard guard(S3RateLimitType::PUT, 0);
    if (!guard.ok()) {
        return {.resp = rate_limited_response(S3RateLimitType::PUT, guard.reject_reason())};
    }
    return _inner->create_multipart_upload(opts);
}

ObjectStorageResponse RateLimitedObjStorageClient::put_object(const ObjectStoragePathOptions& opts,
                                                              std::string_view stream) {
    S3RateLimitGuard guard(S3RateLimitType::PUT, stream.size());
    if (!guard.ok()) {
        return rate_limited_response(S3RateLimitType::PUT, guard.reject_reason());
    }
    return _inner->put_object(opts, stream);
}

ObjectStorageUploadResponse RateLimitedObjStorageClient::upload_part(
        const ObjectStoragePathOptions& opts, std::string_view stream, int part_num) {
    S3RateLimitGuard guard(S3RateLimitType::PUT, stream.size());
    if (!guard.ok()) {
        return {.resp = rate_limited_response(S3RateLimitType::PUT, guard.reject_reason())};
    }
    return _inner->upload_part(opts, stream, part_num);
}

ObjectStorageResponse RateLimitedObjStorageClient::complete_multipart_upload(
        const ObjectStoragePathOptions& opts,
        const std::vector<ObjectCompleteMultiPart>& completed_parts) {
    S3RateLimitGuard guard(S3RateLimitType::PUT, 0);
    if (!guard.ok()) {
        return rate_limited_response(S3RateLimitType::PUT, guard.reject_reason());
    }
    return _inner->complete_multipart_upload(opts, completed_parts);
}

ObjectStorageHeadResponse RateLimitedObjStorageClient::head_object(
        const ObjectStoragePathOptions& opts) {
    S3RateLimitGuard guard(S3RateLimitType::GET, 0);
    if (!guard.ok()) {
        return {.resp = rate_limited_response(S3RateLimitType::GET, guard.reject_reason())};
    }
    return _inner->head_object(opts);
}

ObjectStorageResponse RateLimitedObjStorageClient::get_object(const ObjectStoragePathOptions& opts,
                                                              void* buffer, size_t offset,
                                                              size_t bytes_read,
                                                              size_t* size_return) {
    S3RateLimitGuard guard(S3RateLimitType::GET, bytes_read);
    if (!guard.ok()) {
        return rate_limited_response(S3RateLimitType::GET, guard.reject_reason());
    }
    auto resp = _inner->get_object(opts, buffer, offset, bytes_read, size_return);
    if (resp.status.code == 0) {
        // Refund the difference for short reads (e.g. requested range crosses EOF).
        guard.settle(*size_return);
    }
    return resp;
}

ObjectStorageResponse RateLimitedObjStorageClient::list_objects(
        const ObjectStoragePathOptions& opts, std::vector<FileInfo>* files) {
    S3RateLimitGuard guard(S3RateLimitType::GET, 0);
    if (!guard.ok()) {
        return rate_limited_response(S3RateLimitType::GET, guard.reject_reason());
    }
    return _inner->list_objects(opts, files);
}

ObjectStorageResponse RateLimitedObjStorageClient::delete_objects(
        const ObjectStoragePathOptions& opts, std::vector<std::string> objs) {
    S3RateLimitGuard guard(S3RateLimitType::PUT, 0);
    if (!guard.ok()) {
        return rate_limited_response(S3RateLimitType::PUT, guard.reject_reason());
    }
    return _inner->delete_objects(opts, std::move(objs));
}

ObjectStorageResponse RateLimitedObjStorageClient::delete_object(
        const ObjectStoragePathOptions& opts) {
    S3RateLimitGuard guard(S3RateLimitType::PUT, 0);
    if (!guard.ok()) {
        return rate_limited_response(S3RateLimitType::PUT, guard.reject_reason());
    }
    return _inner->delete_object(opts);
}

ObjectStorageResponse RateLimitedObjStorageClient::delete_objects_recursively(
        const ObjectStoragePathOptions& opts) {
    S3RateLimitGuard guard(S3RateLimitType::PUT, 0);
    if (!guard.ok()) {
        return rate_limited_response(S3RateLimitType::PUT, guard.reject_reason());
    }
    return _inner->delete_objects_recursively(opts);
}

std::string RateLimitedObjStorageClient::generate_presigned_url(
        const ObjectStoragePathOptions& opts, int64_t expiration_secs, const S3ClientConf& conf) {
    // Generating a presigned URL is a local computation, no request goes out.
    return _inner->generate_presigned_url(opts, expiration_secs, conf);
}

} // namespace doris::io
