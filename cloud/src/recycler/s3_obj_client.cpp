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

#include "recycler/s3_obj_client.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "common/logging.h"
#include "common/sync_point.h"

namespace doris::cloud {

#ifndef UNIT_TEST
#define HELPER_MACRO(ret, req, point_name)
#else
#define HELPER_MACRO(ret, req, point_name)                     \
    do {                                                       \
        std::pair p {&ret, &req};                              \
        [[maybe_unused]] auto ret_pair = [&p]() mutable {      \
            TEST_SYNC_POINT_RETURN_WITH_VALUE(point_name, &p); \
            return p;                                          \
        }();                                                   \
        return ret;                                            \
    } while (false);
#endif
#define SYNC_POINT_HOOK_RETURN_VALUE(expr, request, point_name) \
    [&]() -> decltype(auto) {                                   \
        using T = decltype((expr));                             \
        [[maybe_unused]] T t;                                   \
        HELPER_MACRO(t, request, point_name)                    \
        return (expr);                                          \
    }()

ObjectStorageResponse S3ObjClient::put_object(const ObjectStoragePathOptions& opts,
                                              std::string_view stream) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    auto input = Aws::MakeShared<Aws::StringStream>("S3Accessor");
    *input << stream;
    request.SetBody(input);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->PutObject(request),
                                                std::ref(request).get(), "s3_client::put_object");
    if (!outcome.IsSuccess()) {
        LOG_WARNING("failed to put object")
                .tag("endpoint", opts.endpoint)
                .tag("bucket", opts.bucket)
                .tag("key", opts.key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
    return 0;
}
ObjectStorageResponse S3ObjClient::head_object(const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->HeadObject(request),
                                                std::ref(request).get(), "s3_client::head_object");
    if (outcome.IsSuccess()) {
        return 0;
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return 1;
    } else {
        LOG_WARNING("failed to head object")
                .tag("endpoint", opts.endpoint)
                .tag("bucket", opts.bucket)
                .tag("key", opts.key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
}
ObjectStorageResponse S3ObjClient::list_objects(const ObjectStoragePathOptions& opts,
                                                std::vector<ObjectMeta>* files) {
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(opts.bucket).WithPrefix(opts.prefix);

    bool is_truncated = false;
    do {
        auto outcome =
                SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->ListObjectsV2(request),
                                             std::ref(request).get(), "s3_client::list_objects_v2");
        if (!outcome.IsSuccess()) {
            LOG_WARNING("failed to list objects")
                    .tag("endpoint", opts.endpoint)
                    .tag("bucket", opts.bucket)
                    .tag("prefix", opts.prefix)
                    .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                    .tag("error", outcome.GetError().GetMessage());
            return -1;
        }
        const auto& result = outcome.GetResult();
        VLOG_DEBUG << "get " << result.GetContents().size() << " objects";
        for (const auto& obj : result.GetContents()) {
            files->push_back({obj.GetKey(), obj.GetSize(), obj.GetLastModified().Seconds()});
        }
        is_truncated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_truncated);
    return 0;
}
ObjectStorageResponse S3ObjClient::delete_objects(const ObjectStoragePathOptions& opts,
                                                  std::vector<std::string> objs) {
    if (objs.empty()) {
        LOG_INFO("No objects to delete").tag("endpoint", opts.endpoint).tag("bucket", opts.bucket);
        return {0};
    }
    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(opts.bucket);
    Aws::S3::Model::Delete del;
    Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
    for (auto&& obj : objs) {
        objects.emplace_back().SetKey(std::move(obj));
    }
    del.WithObjects(std::move(objects)).SetQuiet(true);
    delete_request.SetDelete(std::move(del));
    auto delete_outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->DeleteObjects(delete_request),
                                                       std::ref(delete_request).get(),
                                                       "s3_client::delete_objects");
    if (!delete_outcome.IsSuccess()) {
        LOG_WARNING("failed to delete objects")
                .tag("endpoint", opts.endpoint)
                .tag("bucket", opts.bucket)
                .tag("key[0]", delete_request.GetDelete().GetObjects().front().GetKey())
                .tag("responseCode", static_cast<int>(delete_outcome.GetError().GetResponseCode()))
                .tag("error", delete_outcome.GetError().GetMessage());
        return {-1};
    }
    if (!delete_outcome.GetResult().GetErrors().empty()) {
        const auto& e = delete_outcome.GetResult().GetErrors().front();
        LOG_WARNING("failed to delete object")
                .tag("endpoint", opts.endpoint)
                .tag("bucket", opts.bucket)
                .tag("key", e.GetKey())
                .tag("responseCode", static_cast<int>(delete_outcome.GetError().GetResponseCode()))
                .tag("error", e.GetMessage());
        return {-2};
    }
    return {0};
}

ObjectStorageResponse S3ObjClient::delete_object(const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(opts.bucket).WithKey(opts.key);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            s3_client_->DeleteObject(request), std::ref(request).get(), "s3_client::delete_object");
    if (!outcome.IsSuccess()) {
        LOG_WARNING("failed to delete object")
                .tag("endpoint", opts.endpoint)
                .tag("bucket", opts.bucket)
                .tag("key", opts.key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage())
                .tag("exception", outcome.GetError().GetExceptionName());
        return -1;
    }
    return 0;
}

ObjectStorageResponse S3ObjClient::delete_objects_recursively(
        const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(opts.bucket).WithPrefix(opts.prefix);

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(opts.bucket);
    bool is_truncated = false;
    do {
        auto outcome =
                SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->ListObjectsV2(request),
                                             std::ref(request).get(), "s3_client::list_objects_v2");

        if (!outcome.IsSuccess()) {
            LOG_WARNING("failed to list objects")
                    .tag("endpoint", opts.endpoint)
                    .tag("bucket", opts.bucket)
                    .tag("prefix", opts.prefix)
                    .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                    .tag("error", outcome.GetError().GetMessage());
            if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN) {
                return {1};
            }
            return {-1};
        }
        const auto& result = outcome.GetResult();
        VLOG_DEBUG << "get " << result.GetContents().size() << " objects";
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        objects.reserve(result.GetContents().size());
        for (const auto& obj : result.GetContents()) {
            objects.emplace_back().SetKey(obj.GetKey());
            LOG_INFO("delete object")
                    .tag("endpoint", opts.endpoint)
                    .tag("bucket", opts.bucket)
                    .tag("key", obj.GetKey());
        }
        if (!objects.empty()) {
            Aws::S3::Model::Delete del;
            del.WithObjects(std::move(objects)).SetQuiet(true);
            delete_request.SetDelete(std::move(del));
            auto delete_outcome = SYNC_POINT_HOOK_RETURN_VALUE(
                    s3_client_->DeleteObjects(delete_request), std::ref(delete_request).get(),
                    "s3_client::delete_objects");
            if (!delete_outcome.IsSuccess()) {
                LOG_WARNING("failed to delete objects")
                        .tag("endpoint", opts.endpoint)
                        .tag("bucket", opts.bucket)
                        .tag("prefix", opts.prefix)
                        .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                        .tag("error", outcome.GetError().GetMessage());
                if (delete_outcome.GetError().GetResponseCode() ==
                    Aws::Http::HttpResponseCode::FORBIDDEN) {
                    return {1};
                }
                return {-2};
            }
            if (!delete_outcome.GetResult().GetErrors().empty()) {
                const auto& e = delete_outcome.GetResult().GetErrors().front();
                LOG_WARNING("failed to delete object")
                        .tag("endpoint", opts.endpoint)
                        .tag("bucket", opts.bucket)
                        .tag("key", e.GetKey())
                        .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                        .tag("error", e.GetMessage());
                return {-3};
            }
        }
        is_truncated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_truncated);
    return {0};
}
ObjectStorageResponse S3ObjClient::delete_expired(const ObjectStorageDeleteExpiredOptions& opts,
                                                  int64_t expired_time) {
    Aws::S3::Model::ListObjectsV2Request request;
    request.WithBucket(opts.path_opts.bucket).WithPrefix(opts.path_opts.prefix);

    bool is_truncated = false;
    do {
        auto outcome =
                SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->ListObjectsV2(request),
                                             std::ref(request).get(), "s3_client::list_objects_v2");
        if (!outcome.IsSuccess()) {
            LOG_WARNING("failed to list objects")
                    .tag("endpoint", opts.path_opts.endpoint)
                    .tag("bucket", opts.path_opts.bucket)
                    .tag("prefix", opts.path_opts.prefix)
                    .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                    .tag("error", outcome.GetError().GetMessage());
            return -1;
        }
        const auto& result = outcome.GetResult();
        std::vector<std::string> expired_keys;
        for (const auto& obj : result.GetContents()) {
            if (obj.GetLastModified().Seconds() < expired_time) {
                auto relative_key = opts.relative_path_factory(obj.GetKey());
                if (relative_key.empty()) {
                    LOG_WARNING("failed get relative path")
                            .tag("prefix", opts.path_opts.prefix)
                            .tag("key", obj.GetKey());
                } else {
                    expired_keys.push_back(obj.GetKey());
                    LOG_INFO("delete expired object")
                            .tag("prefix", opts.path_opts.prefix)
                            .tag("key", obj.GetKey())
                            .tag("relative_key", relative_key)
                            .tag("lastModifiedTime", obj.GetLastModified().Seconds())
                            .tag("expiredTime", expired_time);
                }
            }
        }

        if (!expired_keys.empty()) {
            if (auto ret = delete_objects(opts.path_opts, std::move(expired_keys)); ret.ret != 0) {
                return ret;
            }
        }
        LOG_INFO("delete expired objects")
                .tag("endpoint", opts.path_opts.endpoint)
                .tag("bucket", opts.path_opts.bucket)
                .tag("prefix", opts.path_opts.prefix)
                .tag("num_scanned", result.GetContents().size())
                .tag("num_recycled", expired_keys.size());
        is_truncated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_truncated);
    return 0;
}
ObjectStorageResponse S3ObjClient::get_life_cycle(const ObjectStoragePathOptions& opts,
                                                  int64_t* expiration_days) {
    Aws::S3::Model::GetBucketLifecycleConfigurationRequest request;
    request.SetBucket(opts.bucket);

    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(
            s3_client_->GetBucketLifecycleConfiguration(request), std::ref(request).get(),
            "s3_client::get_bucket_lifecycle_configuration");
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
        LOG_WARNING("Err for check interval: failed to get bucket lifecycle")
                .tag("endpoint", opts.endpoint)
                .tag("bucket", opts.bucket)
                .tag("prefix", opts.prefix)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }

    if (!has_lifecycle) {
        LOG_WARNING("Err for check interval: bucket doesn't have lifecycle configuration")
                .tag("endpoint", opts.endpoint)
                .tag("bucket", opts.bucket)
                .tag("prefix", opts.prefix);
        return -1;
    }
    return 0;
}

ObjectStorageResponse S3ObjClient::check_versioning(const ObjectStoragePathOptions& opts) {
    Aws::S3::Model::GetBucketVersioningRequest request;
    request.SetBucket(opts.bucket);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->GetBucketVersioning(request),
                                                std::ref(request).get(),
                                                "s3_client::get_bucket_versioning");

    if (outcome.IsSuccess()) {
        const auto& versioning_configuration = outcome.GetResult().GetStatus();
        if (versioning_configuration != Aws::S3::Model::BucketVersioningStatus::Enabled) {
            LOG_WARNING("Err for check interval: bucket doesn't enable bucket versioning")
                    .tag("endpoint", opts.endpoint)
                    .tag("bucket", opts.bucket)
                    .tag("prefix", opts.prefix);
            return -1;
        }
    } else {
        LOG_WARNING("Err for check interval: failed to get status of bucket versioning")
                .tag("endpoint", opts.endpoint)
                .tag("bucket", opts.bucket)
                .tag("prefix", opts.prefix)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
    return 0;
}

#undef SYNC_POINT_HOOK_RETURN_VALUE
#undef HELPER_MACRO
} // namespace doris::cloud