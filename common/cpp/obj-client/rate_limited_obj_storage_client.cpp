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

#include "rate_limited_obj_storage_client.h"

namespace doris {

ObjStorageAdmission RateLimitedObjStorageClient::acquire(S3RateLimitType type,
                                                         size_t estimated_bytes) const {
    return rate_limit_policy_->acquire(type, estimated_bytes);
}

ObjStorageUploadResult RateLimitedObjStorageClient::create_multipart_upload(
        const ObjStoragePath& opts) {
    auto rate_limit = acquire(S3RateLimitType::PUT);
    if (!rate_limit.resp.ok()) {
        return {.resp = std::move(rate_limit.resp)};
    }
    return inner_->create_multipart_upload(opts);
}

ObjStorageResponse RateLimitedObjStorageClient::put_object(const ObjStoragePath& opts,
                                                           std::string_view stream) {
    auto rate_limit = acquire(S3RateLimitType::PUT, stream.size());
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return inner_->put_object(opts, stream);
}

ObjStorageUploadResult RateLimitedObjStorageClient::upload_part(const ObjStoragePath& opts,
                                                                const std::string& upload_id,
                                                                std::string_view stream,
                                                                int part_num) {
    auto rate_limit = acquire(S3RateLimitType::PUT, stream.size());
    if (!rate_limit.resp.ok()) {
        return {.resp = std::move(rate_limit.resp)};
    }
    return inner_->upload_part(opts, upload_id, stream, part_num);
}

ObjStorageResponse RateLimitedObjStorageClient::complete_multipart_upload(
        const ObjStoragePath& opts, const std::string& upload_id,
        const std::vector<ObjStorageCompletedPart>& completed_parts) {
    auto rate_limit = acquire(S3RateLimitType::PUT);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return inner_->complete_multipart_upload(opts, upload_id, completed_parts);
}

ObjStorageHeadResult RateLimitedObjStorageClient::head_object(const ObjStoragePath& opts) {
    auto rate_limit = acquire(S3RateLimitType::GET);
    if (!rate_limit.resp.ok()) {
        return {.resp = std::move(rate_limit.resp)};
    }
    return inner_->head_object(opts);
}

ObjStorageResponse RateLimitedObjStorageClient::head_bucket(const std::string& bucket) {
    auto rate_limit = acquire(S3RateLimitType::GET);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return inner_->head_bucket(bucket);
}

ObjStorageResponse RateLimitedObjStorageClient::get_object(const ObjStoragePath& opts, void* buffer,
                                                           size_t offset, size_t bytes_read,
                                                           size_t* size_return) {
    auto rate_limit = acquire(S3RateLimitType::GET, bytes_read);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    auto response = inner_->get_object(opts, buffer, offset, bytes_read, size_return);
    if (response.ok()) {
        rate_limit.settle_bytes(*size_return);
    }
    return response;
}

ObjStorageListPageResult RateLimitedObjStorageClient::list_objects_page(
        const ObjStoragePath& opts, std::string_view continuation_token) {
    auto rate_limit = acquire(S3RateLimitType::GET);
    if (!rate_limit.resp.ok()) {
        return {.resp = std::move(rate_limit.resp)};
    }
    return inner_->list_objects_page(opts, continuation_token);
}

ObjStorageResponse RateLimitedObjStorageClient::delete_objects(const ObjStoragePath& opts,
                                                               std::vector<std::string> objs) {
    auto rate_limit = acquire(S3RateLimitType::PUT);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return inner_->delete_objects(opts, std::move(objs));
}

ObjStorageResponse RateLimitedObjStorageClient::delete_object(const ObjStoragePath& opts) {
    auto rate_limit = acquire(S3RateLimitType::PUT);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return inner_->delete_object(opts);
}

std::string RateLimitedObjStorageClient::generate_presigned_url(const ObjStoragePath& opts,
                                                                int64_t expiration_secs) {
    return inner_->generate_presigned_url(opts, expiration_secs);
}

ObjStorageResponse RateLimitedObjStorageClient::get_lifecycle(const std::string& bucket,
                                                              int64_t* expiration_days) {
    auto rate_limit = acquire(S3RateLimitType::GET);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return inner_->get_lifecycle(bucket, expiration_days);
}

ObjStorageResponse RateLimitedObjStorageClient::check_versioning(const std::string& bucket) {
    auto rate_limit = acquire(S3RateLimitType::GET);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return inner_->check_versioning(bucket);
}

ObjStorageResponse RateLimitedObjStorageClient::abort_multipart_upload(
        const ObjStoragePath& opts, const std::string& upload_id) {
    auto rate_limit = acquire(S3RateLimitType::PUT);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return inner_->abort_multipart_upload(opts, upload_id);
}

} // namespace doris
