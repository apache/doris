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

#include "io/fs/s3_obj_storage_client.h"

#include <aws/core/client/AWSError.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CopyObjectResult.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/Error.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>

#include <algorithm>
#include <ranges>

#include "common/logging.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/fs/err_utils.h"
#include "io/fs/s3_common.h"
#include "util/bvar_helper.h"

namespace {
inline ::Aws::Client::AWSError<::Aws::S3::S3Errors> s3_error_factory() {
    return {::Aws::S3::S3Errors::INTERNAL_FAILURE, "exceeds limit", "exceeds limit", false};
}

template <typename Func>
auto s3_rate_limit(doris::S3RateLimitType op, Func callback) -> decltype(callback()) {
    using T = decltype(callback());
    if (!doris::config::enable_s3_rate_limiter) {
        return callback();
    }
    auto sleep_duration = doris::S3ClientFactory::instance().rate_limiter(op)->add(1);
    if (sleep_duration < 0) {
        return T(s3_error_factory());
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
} // namespace

namespace Aws::S3::Model {
class DeleteObjectRequest;
} // namespace Aws::S3::Model

using Aws::S3::Model::CompletedPart;
using Aws::S3::Model::CompletedMultipartUpload;
using Aws::S3::Model::CompleteMultipartUploadRequest;
using Aws::S3::Model::CreateMultipartUploadRequest;
using Aws::S3::Model::UploadPartRequest;
using Aws::S3::Model::UploadPartOutcome;

namespace doris::io {
using namespace Aws::S3::Model;

ObjectStorageUploadResponse S3ObjStorageClient::create_multipart_upload(
        const ObjectStoragePathOptions& opts) {
    CreateMultipartUploadRequest create_request;
    create_request.WithBucket(opts.bucket).WithKey(opts.key);
    create_request.SetContentType("application/octet-stream");

    SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            s3_put_rate_limit([&]() { return _client->CreateMultipartUpload(create_request); }),
            "s3_file_writer::create_multi_part_upload", std::cref(create_request).get());
    SYNC_POINT_CALLBACK("s3_file_writer::_open", &outcome);

    if (outcome.IsSuccess()) {
        return ObjectStorageUploadResponse {.upload_id {outcome.GetResult().GetUploadId()}};
    }

    return ObjectStorageUploadResponse {
            .resp = {convert_to_obj_response(
                             s3fs_error(outcome.GetError(),
                                        fmt::format("failed to create multipart upload {} ",
                                                    opts.path.native()))),
                     static_cast<int>(outcome.GetError().GetResponseCode()),
                     outcome.GetError().GetRequestId()},
    };
}

ObjectStorageResponse S3ObjStorageClient::put_object(const ObjectStoragePathOptions& opts,
                                                     std::string_view stream) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    auto string_view_stream = std::make_shared<StringViewStream>(stream.data(), stream.size());
    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*string_view_stream));
    request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));
    request.SetBody(string_view_stream);
    request.SetContentLength(stream.size());
    request.SetContentType("application/octet-stream");
    SCOPED_BVAR_LATENCY(s3_bvar::s3_put_latency);
    auto response = SYNC_POINT_HOOK_RETURN_VALUE(
            s3_put_rate_limit([&]() { return _client->PutObject(request); }),
            "s3_file_writer::put_object", std::cref(request).get(), &stream);
    if (!response.IsSuccess()) {
        auto st = s3fs_error(response.GetError(),
                             fmt::format("failed to put object {}", opts.path.native()));
        LOG(WARNING) << st;
        return ObjectStorageResponse {convert_to_obj_response(std::move(st)),
                                      static_cast<int>(response.GetError().GetResponseCode()),
                                      response.GetError().GetRequestId()};
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageUploadResponse S3ObjStorageClient::upload_part(const ObjectStoragePathOptions& opts,
                                                            std::string_view stream, int part_num) {
    UploadPartRequest upload_request;
    upload_request.WithBucket(opts.bucket)
            .WithKey(opts.key)
            .WithPartNumber(part_num)
            .WithUploadId(*opts.upload_id);
    auto string_view_stream = std::make_shared<StringViewStream>(stream.data(), stream.size());

    upload_request.SetBody(string_view_stream);

    Aws::Utils::ByteBuffer part_md5(Aws::Utils::HashingUtils::CalculateMD5(*string_view_stream));
    upload_request.SetContentMD5(Aws::Utils::HashingUtils::Base64Encode(part_md5));

    upload_request.SetContentLength(stream.size());
    upload_request.SetContentType("application/octet-stream");

    UploadPartOutcome upload_part_outcome;
    {
        SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
        upload_part_outcome = SYNC_POINT_HOOK_RETURN_VALUE(
                s3_put_rate_limit([&]() { return _client->UploadPart(upload_request); }),
                "s3_file_writer::upload_part", std::cref(upload_request).get(), &stream);
    }
    TEST_SYNC_POINT_CALLBACK("S3FileWriter::_upload_one_part", &upload_part_outcome);
    if (!upload_part_outcome.IsSuccess()) {
        auto s = Status::IOError(
                "failed to upload part (bucket={}, key={}, part_num={}, up_load_id={}): {}, "
                "exception {}, error code {}",
                opts.bucket, opts.path.native(), part_num, *opts.upload_id,
                upload_part_outcome.GetError().GetMessage(),
                upload_part_outcome.GetError().GetExceptionName(),
                upload_part_outcome.GetError().GetResponseCode());
        LOG_WARNING(s.to_string());
        return ObjectStorageUploadResponse {
                .resp = {convert_to_obj_response(std::move(s)),
                         static_cast<int>(upload_part_outcome.GetError().GetResponseCode()),
                         upload_part_outcome.GetError().GetRequestId()}};
    }
    return ObjectStorageUploadResponse {.etag = upload_part_outcome.GetResult().GetETag()};
}

ObjectStorageResponse S3ObjStorageClient::complete_multipart_upload(
        const ObjectStoragePathOptions& opts,
        const std::vector<ObjectCompleteMultiPart>& completed_parts) {
    CompleteMultipartUploadRequest complete_request;
    complete_request.WithBucket(opts.bucket).WithKey(opts.key).WithUploadId(*opts.upload_id);

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
    complete_request.WithMultipartUpload(completed_upload);

    TEST_SYNC_POINT_RETURN_WITH_VALUE("S3FileWriter::_complete:3", ObjectStorageResponse(), this);
    SCOPED_BVAR_LATENCY(s3_bvar::s3_multi_part_upload_latency);
    auto complete_outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            s3_put_rate_limit([&]() { return _client->CompleteMultipartUpload(complete_request); }),
            "s3_file_writer::complete_multi_part", std::cref(complete_request).get());

    if (!complete_outcome.IsSuccess()) {
        auto st = s3fs_error(complete_outcome.GetError(),
                             fmt::format("failed to complete multi part upload {}, upload_id={}",
                                         opts.path.native(), *opts.upload_id));
        LOG(WARNING) << st;
        return {convert_to_obj_response(std::move(st)),
                static_cast<int>(complete_outcome.GetError().GetResponseCode()),
                complete_outcome.GetError().GetRequestId()};
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageHeadResponse S3ObjStorageClient::head_object(const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);

    SCOPED_BVAR_LATENCY(s3_bvar::s3_head_latency);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            s3_get_rate_limit([&]() { return _client->HeadObject(request); }),
            "s3_file_system::head_object", std::ref(request).get());
    if (outcome.IsSuccess()) {
        return {.resp = {convert_to_obj_response(Status::OK())},
                .file_size = outcome.GetResult().GetContentLength()};
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return {.resp = {convert_to_obj_response(Status::Error<ErrorCode::NOT_FOUND, false>(""))}};
    } else {
        return {.resp = {convert_to_obj_response(
                                 s3fs_error(outcome.GetError(),
                                            fmt::format("failed to check exists {}", opts.key))),
                         static_cast<int>(outcome.GetError().GetResponseCode()),
                         outcome.GetError().GetRequestId()}};
    }
    return ObjectStorageHeadResponse {
            .resp = ObjectStorageResponse::OK(),
    };
}

ObjectStorageResponse S3ObjStorageClient::get_object(const ObjectStoragePathOptions& opts,
                                                     void* buffer, size_t offset, size_t bytes_read,
                                                     size_t* size_return) {
    Aws::S3::Model::GetObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    request.SetRange(fmt::format("bytes={}-{}", offset, offset + bytes_read - 1));
    request.SetResponseStreamFactory(AwsWriteableStreamFactory(buffer, bytes_read));

    SCOPED_BVAR_LATENCY(s3_bvar::s3_get_latency);
    auto outcome = s3_get_rate_limit([&]() { return _client->GetObject(request); });
    if (!outcome.IsSuccess()) {
        return {convert_to_obj_response(
                        s3fs_error(outcome.GetError(),
                                   fmt::format("failed to read from {}", opts.path.native()))),
                static_cast<int>(outcome.GetError().GetResponseCode()),
                outcome.GetError().GetRequestId()};
    }
    *size_return = outcome.GetResult().GetContentLength();
    if (*size_return != bytes_read) {
        return {convert_to_obj_response(
                Status::InternalError("failed to read from {}(bytes read: {}, bytes req: {})",
                                      opts.path.native(), *size_return, bytes_read))};
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse S3ObjStorageClient::list_objects(const ObjectStoragePathOptions& opts,
                                                       std::vector<FileInfo>* files) {
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(opts.bucket).WithPrefix(opts.prefix);
    bool is_trucated = false;
    do {
        Aws::S3::Model::ListObjectsV2Outcome outcome;
        {
            SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
            outcome = s3_get_rate_limit([&]() { return _client->ListObjectsV2(request); });
        }
        if (!outcome.IsSuccess()) {
            files->clear();
            return {convert_to_obj_response(s3fs_error(
                            outcome.GetError(), fmt::format("failed to list {}", opts.prefix))),
                    static_cast<int>(outcome.GetError().GetResponseCode()),
                    outcome.GetError().GetRequestId()};
        }
        for (const auto& obj : outcome.GetResult().GetContents()) {
            std::string key = obj.GetKey();
            bool is_dir = (key.back() == '/');
            FileInfo file_info;
            file_info.file_name = obj.GetKey();
            file_info.file_size = obj.GetSize();
            file_info.is_file = !is_dir;
            files->push_back(std::move(file_info));
        }
        is_trucated = outcome.GetResult().GetIsTruncated();
        request.SetContinuationToken(outcome.GetResult().GetNextContinuationToken());
    } while (is_trucated);
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse S3ObjStorageClient::delete_objects(const ObjectStoragePathOptions& opts,
                                                         std::vector<std::string> objs) {
    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(opts.bucket);
    Aws::S3::Model::Delete del;
    Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
    std::ranges::transform(objs, std::back_inserter(objects), [](auto&& obj_key) {
        Aws::S3::Model::ObjectIdentifier obj_identifier;
        obj_identifier.SetKey(std::move(obj_key));
        return obj_identifier;
    });
    del.WithObjects(std::move(objects)).SetQuiet(true);
    delete_request.SetDelete(std::move(del));
    SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_objects_latency);
    auto delete_outcome =
            s3_put_rate_limit([&]() { return _client->DeleteObjects(delete_request); });
    if (!delete_outcome.IsSuccess()) {
        return {convert_to_obj_response(
                        s3fs_error(delete_outcome.GetError(),
                                   fmt::format("failed to delete dir {}", opts.key))),
                static_cast<int>(delete_outcome.GetError().GetResponseCode()),
                delete_outcome.GetError().GetRequestId()};
    }
    if (!delete_outcome.GetResult().GetErrors().empty()) {
        const auto& e = delete_outcome.GetResult().GetErrors().front();
        return {convert_to_obj_response(Status::InternalError("failed to delete object {}: {}",
                                                              e.GetKey(), e.GetMessage()))};
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse S3ObjStorageClient::delete_object(const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);

    SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_object_latency);
    auto outcome = s3_put_rate_limit([&]() { return _client->DeleteObject(request); });
    if (outcome.IsSuccess() ||
        outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return ObjectStorageResponse::OK();
    }
    return {convert_to_obj_response(s3fs_error(outcome.GetError(),
                                               fmt::format("failed to delete file {}", opts.key))),
            static_cast<int>(outcome.GetError().GetResponseCode()),
            outcome.GetError().GetRequestId()};
}

ObjectStorageResponse S3ObjStorageClient::delete_objects_recursively(
        const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(opts.bucket).WithPrefix(opts.prefix);
    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(opts.bucket);
    bool is_trucated = false;
    do {
        Aws::S3::Model::ListObjectsV2Outcome outcome;
        {
            SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
            outcome = s3_get_rate_limit([&]() { return _client->ListObjectsV2(request); });
        }
        if (!outcome.IsSuccess()) {
            return {convert_to_obj_response(s3fs_error(
                            outcome.GetError(),
                            fmt::format("failed to list objects when delete dir {}", opts.prefix))),
                    static_cast<int>(outcome.GetError().GetResponseCode()),
                    outcome.GetError().GetRequestId()};
        }
        const auto& result = outcome.GetResult();
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        objects.reserve(result.GetContents().size());
        for (const auto& obj : result.GetContents()) {
            objects.emplace_back().SetKey(obj.GetKey());
        }
        if (!objects.empty()) {
            Aws::S3::Model::Delete del;
            del.WithObjects(std::move(objects)).SetQuiet(true);
            delete_request.SetDelete(std::move(del));
            SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_objects_latency);
            auto delete_outcome =
                    s3_put_rate_limit([&]() { return _client->DeleteObjects(delete_request); });
            if (!delete_outcome.IsSuccess()) {
                return {convert_to_obj_response(
                                s3fs_error(delete_outcome.GetError(),
                                           fmt::format("failed to delete dir {}", opts.key))),
                        static_cast<int>(delete_outcome.GetError().GetResponseCode()),
                        delete_outcome.GetError().GetRequestId()};
            }
            if (!delete_outcome.GetResult().GetErrors().empty()) {
                const auto& e = delete_outcome.GetResult().GetErrors().front();
                return {convert_to_obj_response(Status::InternalError(
                        "failed to delete object {}: {}", opts.key, e.GetMessage()))};
            }
        }
        is_trucated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_trucated);
    return ObjectStorageResponse::OK();
}

std::string S3ObjStorageClient::generate_presigned_url(const ObjectStoragePathOptions& opts,
                                                       int64_t expiration_secs,
                                                       const S3ClientConf&) {
    return _client->GeneratePresignedUrl(opts.bucket, opts.key, Aws::Http::HttpMethod::HTTP_GET,
                                         expiration_secs);
}

} // namespace doris::io