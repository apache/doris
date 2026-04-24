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

#include "io/fs/oss_v2_obj_storage_client.h"

#ifdef USE_OSS

#include <alibabacloud/oss/OssClient.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <ctime>
#include <iostream>
#include <streambuf>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "io/fs/file_system.h"
#include "io/fs/obj_storage_client.h"
#include "util/bvar_helper.h"
#include "util/oss_util.h"
#include "util/s3_util.h"

namespace doris::io {

namespace {

// Read-only iostream wrapper around an existing buffer (no AWS SDK dependency).
class OSSStringViewStream : public std::streambuf, public std::iostream {
public:
    OSSStringViewStream(const char* data, size_t size) : std::streambuf(), std::iostream(this) {
        // const_cast is safe: read-only streambuf (get-area only, no put-area).
        char* p = const_cast<char*>(data);
        setg(p, p, p + size);
    }
};

// Rate limiting helpers — reuse shared S3RateLimiter infrastructure (parallel to s3_obj_storage_client.cpp).
inline AlibabaCloud::OSS::OssError oss_rate_limit_error() {
    return {"ExceedsRateLimit", "request rate exceeds configured limit"};
}

template <typename Func>
auto oss_rate_limit(doris::S3RateLimitType op, Func callback) -> decltype(callback()) {
    using T = decltype(callback());
    if (!doris::config::enable_s3_rate_limiter) {
        return callback();
    }
    auto sleep_duration = doris::S3ClientFactory::instance().rate_limiter(op)->add(1);
    if (sleep_duration < 0) {
        return T(oss_rate_limit_error());
    }
    return callback();
}

template <typename Func>
auto oss_get_rate_limit(Func callback) -> decltype(callback()) {
    return oss_rate_limit(doris::S3RateLimitType::GET, std::move(callback));
}

template <typename Func>
auto oss_put_rate_limit(Func callback) -> decltype(callback()) {
    return oss_rate_limit(doris::S3RateLimitType::PUT, std::move(callback));
}

// Build a failed ObjectStorageResponse from an OSS error
inline ObjectStorageResponse oss_error_response(const std::string& op, const std::string& bucket,
                                                const std::string& key, const std::string& code,
                                                const std::string& msg) {
    ObjectStorageResponse resp;
    resp.status.code = -1;
    resp.status.msg = fmt::format("OSS {} failed [{}/{}]: {} - {}", op, bucket, key, code, msg);
    return resp;
}

// OssError has no HTTP status code; Code() string is the only discriminator.
inline ObjectStorageHeadResponse oss_head_error_response(const std::string& bucket,
                                                         const std::string& key,
                                                         const std::string& code,
                                                         const std::string& msg) {
    if (code == "NoSuchKey") {
        return {.resp = {convert_to_obj_response(Status::Error<ErrorCode::NOT_FOUND, false>(""))}};
    }
    ObjectStorageHeadResponse r;
    r.resp.status.code = -1;
    r.resp.status.msg =
            fmt::format("OSS HeadObject failed [{}/{}]: {} - {}", bucket, key, code, msg);
    return r;
}

} // namespace

OSSv2ObjStorageClient::OSSv2ObjStorageClient(std::shared_ptr<AlibabaCloud::OSS::OssClient> client,
                                             std::string bucket)
        : _client(std::move(client)), _bucket(std::move(bucket)) {}

// ----------------------------------------------------------------------------
// create_multipart_upload
// ----------------------------------------------------------------------------
ObjectStorageUploadResponse OSSv2ObjStorageClient::create_multipart_upload(
        const ObjectStoragePathOptions& opts) {
    ObjectStorageUploadResponse resp;
    const std::string& bucket = resolve_bucket(opts);

    AlibabaCloud::OSS::InitiateMultipartUploadRequest request(bucket, opts.key);
    SCOPED_BVAR_LATENCY(oss_bvar::oss_multi_part_upload_latency);
    auto outcome = oss_put_rate_limit([&]() { return _client->InitiateMultipartUpload(request); });

    if (!outcome.isSuccess()) {
        resp.resp = oss_error_response("InitiateMultipartUpload", bucket, opts.key,
                                       outcome.error().Code(), outcome.error().Message());
        LOG(WARNING) << resp.resp.status.msg;
        return resp;
    }

    resp.upload_id = outcome.result().UploadId();
    VLOG(2) << "OSS InitiateMultipartUpload: " << bucket << "/" << opts.key
            << " upload_id=" << *resp.upload_id;
    return resp;
}

// ----------------------------------------------------------------------------
// put_object
// ----------------------------------------------------------------------------
ObjectStorageResponse OSSv2ObjStorageClient::put_object(const ObjectStoragePathOptions& opts,
                                                        std::string_view stream) {
    const std::string& bucket = resolve_bucket(opts);
    auto content = std::make_shared<OSSStringViewStream>(stream.data(), stream.size());

    AlibabaCloud::OSS::PutObjectRequest request(bucket, opts.key, content);
    request.MetaData().setContentLength(static_cast<int64_t>(stream.size()));
    SCOPED_BVAR_LATENCY(oss_bvar::oss_put_latency);
    auto outcome = oss_put_rate_limit([&]() { return _client->PutObject(request); });

    if (!outcome.isSuccess()) {
        return oss_error_response("PutObject", bucket, opts.key, outcome.error().Code(),
                                  outcome.error().Message());
    }

    VLOG(2) << "OSS PutObject: " << bucket << "/" << opts.key << " size=" << stream.size();
    return ObjectStorageResponse::OK();
}

// ----------------------------------------------------------------------------
// upload_part
// ----------------------------------------------------------------------------
ObjectStorageUploadResponse OSSv2ObjStorageClient::upload_part(const ObjectStoragePathOptions& opts,
                                                               std::string_view stream,
                                                               int part_num) {
    ObjectStorageUploadResponse resp;
    const std::string& bucket = resolve_bucket(opts);

    if (!opts.upload_id.has_value()) {
        resp.resp.status.code = -1;
        resp.resp.status.msg = "upload_id is required for OSS UploadPart";
        return resp;
    }

    auto content = std::make_shared<OSSStringViewStream>(stream.data(), stream.size());

    AlibabaCloud::OSS::UploadPartRequest request(bucket, opts.key, content);
    request.setPartNumber(part_num);
    request.setUploadId(*opts.upload_id);
    request.setContentLength(static_cast<uint64_t>(stream.size()));

    SCOPED_BVAR_LATENCY(oss_bvar::oss_multi_part_upload_latency);
    auto outcome = oss_put_rate_limit([&]() { return _client->UploadPart(request); });

    if (!outcome.isSuccess()) {
        resp.resp = oss_error_response("UploadPart", bucket, opts.key, outcome.error().Code(),
                                       outcome.error().Message());
        LOG(WARNING) << resp.resp.status.msg;
        return resp;
    }

    resp.etag = outcome.result().ETag();
    VLOG(2) << "OSS UploadPart " << part_num << ": " << bucket << "/" << opts.key
            << " etag=" << *resp.etag;
    return resp;
}

// ----------------------------------------------------------------------------
// complete_multipart_upload
// ----------------------------------------------------------------------------
ObjectStorageResponse OSSv2ObjStorageClient::complete_multipart_upload(
        const ObjectStoragePathOptions& opts,
        const std::vector<ObjectCompleteMultiPart>& completed_parts) {
    const std::string& bucket = resolve_bucket(opts);

    if (!opts.upload_id.has_value()) {
        return oss_error_response("CompleteMultipartUpload", bucket, opts.key, "InvalidArgument",
                                  "upload_id is required");
    }

    AlibabaCloud::OSS::PartList part_list;
    part_list.reserve(completed_parts.size());
    for (const auto& part : completed_parts) {
        part_list.emplace_back(part.part_num, part.etag);
    }
    // Server requires ascending part-number order; SDK does not enforce this locally.
    std::sort(part_list.begin(), part_list.end(),
              [](const AlibabaCloud::OSS::Part& a, const AlibabaCloud::OSS::Part& b) {
                  return a.PartNumber() < b.PartNumber();
              });

    AlibabaCloud::OSS::CompleteMultipartUploadRequest request(bucket, opts.key, part_list,
                                                              *opts.upload_id);
    SCOPED_BVAR_LATENCY(oss_bvar::oss_multi_part_upload_latency);
    auto outcome = oss_put_rate_limit([&]() { return _client->CompleteMultipartUpload(request); });

    if (!outcome.isSuccess()) {
        return oss_error_response("CompleteMultipartUpload", bucket, opts.key,
                                  outcome.error().Code(), outcome.error().Message());
    }

    VLOG(1) << "OSS CompleteMultipartUpload: " << bucket << "/" << opts.key
            << " parts=" << completed_parts.size();
    return ObjectStorageResponse::OK();
}

// ----------------------------------------------------------------------------
// head_object
// ----------------------------------------------------------------------------
ObjectStorageHeadResponse OSSv2ObjStorageClient::head_object(const ObjectStoragePathOptions& opts) {
    ObjectStorageHeadResponse resp;
    const std::string& bucket = resolve_bucket(opts);

    SCOPED_BVAR_LATENCY(oss_bvar::oss_head_latency);
    auto outcome = oss_get_rate_limit([&]() { return _client->HeadObject(bucket, opts.key); });

    if (!outcome.isSuccess()) {
        return oss_head_error_response(bucket, opts.key, outcome.error().Code(),
                                       outcome.error().Message());
    }

    resp.file_size = outcome.result().ContentLength();
    return resp;
}

// ----------------------------------------------------------------------------
// get_object
// ----------------------------------------------------------------------------
ObjectStorageResponse OSSv2ObjStorageClient::get_object(const ObjectStoragePathOptions& opts,
                                                        void* buffer, size_t offset,
                                                        size_t bytes_read, size_t* size_return) {
    const std::string& bucket = resolve_bucket(opts);

    if (bytes_read == 0) {
        *size_return = 0;
        return ObjectStorageResponse::OK();
    }

    AlibabaCloud::OSS::GetObjectRequest request(bucket, opts.key);
    request.setRange(static_cast<int64_t>(offset), static_cast<int64_t>(offset + bytes_read - 1));

    SCOPED_BVAR_LATENCY(oss_bvar::oss_get_latency);
    auto outcome = oss_get_rate_limit([&]() { return _client->GetObject(request); });

    if (!outcome.isSuccess()) {
        return oss_error_response("GetObject", bucket, opts.key, outcome.error().Code(),
                                  outcome.error().Message());
    }

    auto& content = outcome.result().Content();
    content->read(static_cast<char*>(buffer), static_cast<std::streamsize>(bytes_read));
    *size_return = static_cast<size_t>(content->gcount());
    return ObjectStorageResponse::OK();
}

// ----------------------------------------------------------------------------
// list_objects
// ----------------------------------------------------------------------------
ObjectStorageResponse OSSv2ObjStorageClient::list_objects(const ObjectStoragePathOptions& opts,
                                                          std::vector<FileInfo>* files) {
    const std::string& bucket = resolve_bucket(opts);
    bool is_truncated = true;
    std::string marker;

    while (is_truncated) {
        AlibabaCloud::OSS::ListObjectsRequest request(bucket);
        request.setPrefix(opts.prefix);
        request.setMaxKeys(1000);
        if (!marker.empty()) {
            request.setMarker(marker);
        }

        SCOPED_BVAR_LATENCY(oss_bvar::oss_list_latency);
        auto outcome = oss_get_rate_limit([&]() { return _client->ListObjects(request); });
        if (!outcome.isSuccess()) {
            return oss_error_response("ListObjects", bucket, opts.prefix, outcome.error().Code(),
                                      outcome.error().Message());
        }

        const auto& result = outcome.result();
        for (const auto& obj : result.ObjectSummarys()) {
            FileInfo fi;
            fi.file_name = obj.Key();
            fi.file_size = obj.Size();
            fi.is_file = true;
            files->push_back(std::move(fi));
        }

        is_truncated = result.IsTruncated();
        if (is_truncated) {
            // OSS v1 ListObjects may omit NextMarker without a delimiter; fall back to last key.
            const auto& summaries = result.ObjectSummarys();
            marker = result.NextMarker();
            if (marker.empty() && !summaries.empty()) {
                marker = summaries.back().Key();
            }
            if (marker.empty()) {
                return oss_error_response("ListObjects", bucket, opts.prefix, "InvalidResponse",
                                          "IsTruncated=true but no continuation marker available");
            }
        }
    }

    return ObjectStorageResponse::OK();
}

// ----------------------------------------------------------------------------
// delete_objects  (batch, up to 1000 per OSS call)
// ----------------------------------------------------------------------------
ObjectStorageResponse OSSv2ObjStorageClient::delete_objects(const ObjectStoragePathOptions& opts,
                                                            std::vector<std::string> objs) {
    if (objs.empty()) {
        return ObjectStorageResponse::OK();
    }

    const std::string& bucket = resolve_bucket(opts);
    constexpr size_t OSS_BATCH_DELETE_LIMIT = 1000;

    for (size_t i = 0; i < objs.size(); i += OSS_BATCH_DELETE_LIMIT) {
        AlibabaCloud::OSS::DeletedKeyList keys;
        size_t end = std::min(i + OSS_BATCH_DELETE_LIMIT, objs.size());
        for (size_t j = i; j < end; ++j) {
            keys.push_back(objs[j]);
        }

        AlibabaCloud::OSS::DeleteObjectsRequest request(bucket);
        request.setKeyList(keys);
        SCOPED_BVAR_LATENCY(oss_bvar::oss_delete_objects_latency);
        auto outcome = oss_put_rate_limit([&]() { return _client->DeleteObjects(request); });

        if (!outcome.isSuccess()) {
            return oss_error_response("DeleteObjects", bucket, opts.prefix, outcome.error().Code(),
                                      outcome.error().Message());
        }
        // OSS has no per-key error objects in batch delete (unlike S3 GetErrors()).
        // Detect partial failure via count: keyList() returns confirmed-deleted keys in Verbose mode.
        const auto& confirmed = outcome.result().keyList();
        if (confirmed.size() < keys.size()) {
            LOG(WARNING) << "OSS BatchDelete partial failure: sent " << keys.size()
                         << " keys, only " << confirmed.size() << " confirmed deleted in bucket "
                         << bucket << "; some objects may still exist";
        }
    }

    return ObjectStorageResponse::OK();
}

// ----------------------------------------------------------------------------
// delete_object  (single object, idempotent on NoSuchKey)
// ----------------------------------------------------------------------------
ObjectStorageResponse OSSv2ObjStorageClient::delete_object(const ObjectStoragePathOptions& opts) {
    const std::string& bucket = resolve_bucket(opts);
    SCOPED_BVAR_LATENCY(oss_bvar::oss_delete_object_latency);
    auto outcome = oss_put_rate_limit([&]() { return _client->DeleteObject(bucket, opts.key); });

    if (!outcome.isSuccess()) {
        const std::string& code = outcome.error().Code();
        if (code == "NoSuchKey") {
            return ObjectStorageResponse::OK(); // idempotent
        }
        return oss_error_response("DeleteObject", bucket, opts.key, code,
                                  outcome.error().Message());
    }

    return ObjectStorageResponse::OK();
}

// ----------------------------------------------------------------------------
// delete_objects_recursively  (list all under prefix then batch delete)
// ----------------------------------------------------------------------------
ObjectStorageResponse OSSv2ObjStorageClient::delete_objects_recursively(
        const ObjectStoragePathOptions& opts) {
    std::vector<FileInfo> files;
    if (auto resp = list_objects(opts, &files); resp.status.code != 0) {
        return resp;
    }
    std::vector<std::string> keys;
    keys.reserve(files.size());
    for (auto& fi : files) {
        keys.push_back(std::move(fi.file_name));
    }
    return delete_objects(opts, std::move(keys));
}

// ----------------------------------------------------------------------------
// generate_presigned_url
// ----------------------------------------------------------------------------
std::string OSSv2ObjStorageClient::generate_presigned_url(const ObjectStoragePathOptions& opts,
                                                          int64_t expiration_secs,
                                                          const S3ClientConf& /*conf*/) {
    // `conf` unused: OSS SDK signs with credentials embedded at client construction time.
    // Unlike S3/Azure, GeneratePresignedUrl does not accept a per-call credential override.
    const std::string& bucket = resolve_bucket(opts);
    time_t expiration_time = std::time(nullptr) + static_cast<time_t>(expiration_secs);

    AlibabaCloud::OSS::GeneratePresignedUrlRequest request(bucket, opts.key,
                                                           AlibabaCloud::OSS::Http::Get);
    request.setExpires(expiration_time);

    auto outcome = _client->GeneratePresignedUrl(request);
    if (!outcome.isSuccess()) {
        LOG(WARNING) << "OSS GeneratePresignedUrl failed [" << bucket << "/" << opts.key
                     << "]: " << outcome.error().Code() << " - " << outcome.error().Message();
        return "";
    }

    return outcome.result();
}

} // namespace doris::io

#endif // USE_OSS
