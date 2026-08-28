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

#include "s3_express_obj_storage_client.h"

#include <aws/s3/model/ChecksumAlgorithm.h>

#include <algorithm>
#include <string>
#include <string_view>

namespace doris {
namespace {

std::string directory_bucket_list_prefix(std::string_view logical_prefix) {
    if (logical_prefix.empty() || logical_prefix.ends_with('/')) {
        return std::string(logical_prefix);
    }

    auto separator = logical_prefix.rfind('/');
    return separator == std::string_view::npos
                   ? std::string()
                   : std::string(logical_prefix.substr(0, separator + 1));
}

template <typename Request>
void set_crc32c_checksum(Request& request, Aws::IOStream& stream) {
    Aws::Utils::ByteBuffer crc32c(Aws::Utils::HashingUtils::CalculateCRC32C(stream));
    request.SetChecksumAlgorithm(Aws::S3::Model::ChecksumAlgorithm::CRC32C);
    request.SetChecksumCRC32C(Aws::Utils::HashingUtils::Base64Encode(crc32c));
}

} // namespace

ObjStorageListPageResult S3ExpressObjStorageClient::list_objects_page(
        const ObjStoragePath& opts, std::string_view continuation_token) {
    const std::string logical_prefix = opts.prefix.empty() ? opts.key : opts.prefix;
    const std::string list_prefix = directory_bucket_list_prefix(logical_prefix);

    auto list_opts = opts;
    list_opts.key.clear();
    list_opts.prefix = list_prefix;
    auto page = S3ObjStorageClient::list_objects_page(list_opts, continuation_token);
    if (!page.resp.ok() || logical_prefix == list_prefix) {
        return page;
    }

    std::erase_if(page.objects, [&logical_prefix](const ObjectMeta& object) {
        return !object.key.starts_with(logical_prefix);
    });
    return page;
}

std::string S3ExpressObjStorageClient::generate_presigned_url(const ObjStoragePath& opts,
                                                              int64_t expiration_secs) {
    // Session credentials expire after five minutes. Use the standard SigV4 client so a
    // presigned URL remains valid for the expiration requested by the caller (subject to the
    // lifetime of the configured IAM/STS credentials).
    return standard_auth_client_->GeneratePresignedUrl(
            opts.bucket, opts.key, Aws::Http::HttpMethod::HTTP_GET, expiration_secs);
}

ObjStorageResponse S3ExpressObjStorageClient::get_lifecycle(const std::string& /*bucket*/,
                                                            int64_t* expiration_days) {
    // Directory buckets do not support the noncurrent-version lifecycle rule checked by
    // InstanceChecker.
    *expiration_days = INT64_MAX;
    return ObjStorageResponse::OK();
}

ObjStorageResponse S3ExpressObjStorageClient::check_versioning(const std::string& /*bucket*/) {
    // Directory buckets do not support S3 Versioning.
    return ObjStorageResponse::OK();
}

ObjStorageResponse S3ExpressObjStorageClient::complete_multipart_upload(
        const ObjStoragePath& opts, const std::string& upload_id,
        const std::vector<ObjStorageCompletedPart>& completed_parts) {
    Aws::S3::Model::CompleteMultipartUploadRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key).WithUploadId(upload_id);

    Aws::S3::Model::CompletedMultipartUpload completed_upload;
    std::vector<Aws::S3::Model::CompletedPart> complete_parts;
    complete_parts.reserve(completed_parts.size());
    int expected_part_num = 1;
    for (const auto& completed_part : completed_parts) {
        if (completed_part.part_num != expected_part_num) {
            return {.status = {TStatusCode::INVALID_ARGUMENT,
                               fmt::format("S3 Express multipart upload parts must be consecutive, "
                                           "expected part_num={}, actual part_num={}",
                                           expected_part_num, completed_part.part_num)}};
        }
        if (!completed_part.checksum_crc32c.has_value() ||
            completed_part.checksum_crc32c->empty()) {
            return {.status = {TStatusCode::INVALID_ARGUMENT,
                               fmt::format("S3 Express multipart upload part {} is missing CRC32C",
                                           completed_part.part_num)}};
        }

        Aws::S3::Model::CompletedPart part;
        part.SetPartNumber(completed_part.part_num);
        part.SetETag(completed_part.etag);
        part.SetChecksumCRC32C(*completed_part.checksum_crc32c);
        complete_parts.emplace_back(std::move(part));
        ++expected_part_num;
    }
    completed_upload.SetParts(std::move(complete_parts));
    request.WithMultipartUpload(std::move(completed_upload));

    return complete_multipart_upload_impl(opts, upload_id, std::move(request));
}

void S3ExpressObjStorageClient::set_create_multipart_upload_checksum(
        Aws::S3::Model::CreateMultipartUploadRequest& request) const {
    request.SetChecksumAlgorithm(Aws::S3::Model::ChecksumAlgorithm::CRC32C);
}

void S3ExpressObjStorageClient::set_put_object_checksum(Aws::S3::Model::PutObjectRequest& request,
                                                        Aws::IOStream& stream) const {
    set_crc32c_checksum(request, stream);
}

void S3ExpressObjStorageClient::set_upload_part_checksum(Aws::S3::Model::UploadPartRequest& request,
                                                         Aws::IOStream& stream) const {
    set_crc32c_checksum(request, stream);
}

} // namespace doris
