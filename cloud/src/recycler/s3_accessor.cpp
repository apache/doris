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

#include "recycler/s3_accessor.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/LifecycleRule.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <algorithm>
#include <execution>
#include <utility>

#include "common/logging.h"
#include "common/sync_point.h"
#include "recycler/obj_store_accessor.h"

namespace doris::cloud {
#ifndef UNIT_TEST
#define HELP_MACRO(ret, req, point_name)
#else
#define HELP_MACRO(ret, req, point_name)                       \
    do {                                                       \
        std::pair p {&ret, &req};                              \
        [[maybe_unused]] auto ret_pair = [&p]() mutable {      \
            TEST_SYNC_POINT_RETURN_WITH_VALUE(point_name, &p); \
            return p;                                          \
        }();                                                   \
        return ret;                                            \
    } while (false)
#endif
#define SYNC_POINT_HOOK_RETURN_VALUE(expr, point_name, req) \
    [&]() mutable {                                         \
        [[maybe_unused]] decltype((expr)) t;                \
        HELP_MACRO(t, req, point_name);                     \
        return (expr);                                      \
    }()

class S3Environment {
public:
    S3Environment() { Aws::InitAPI(aws_options_); }

    ~S3Environment() { Aws::ShutdownAPI(aws_options_); }

private:
    Aws::SDKOptions aws_options_;
};

S3Accessor::S3Accessor(S3Conf conf) : ObjStoreAccessor(AccessorType::S3), conf_(std::move(conf)) {
    path_ = conf_.endpoint + '/' + conf_.bucket + '/' + conf_.prefix;
}

S3Accessor::~S3Accessor() = default;

std::string S3Accessor::get_key(const std::string& relative_path) const {
    return conf_.prefix + '/' + relative_path;
}

std::string S3Accessor::get_relative_path(const std::string& key) const {
    return key.find(conf_.prefix + "/") != 0 ? "" : key.substr(conf_.prefix.length() + 1);
}

int S3Accessor::init() {
    static S3Environment s3_env;
    Aws::Auth::AWSCredentials aws_cred(conf_.ak, conf_.sk);
    Aws::Client::ClientConfiguration aws_config;
    aws_config.endpointOverride = conf_.endpoint;
    aws_config.region = conf_.region;
    aws_config.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(
            /*maxRetries = 10, scaleFactor = 25*/);
    s3_client_ = std::make_shared<Aws::S3::S3Client>(
            std::move(aws_cred), std::move(aws_config),
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            true /* useVirtualAddressing */);
    return 0;
}

int S3Accessor::delete_objects_by_prefix(const std::string& relative_path) {
    Aws::S3::Model::ListObjectsV2Request request;
    auto prefix = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithPrefix(prefix);

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(conf_.bucket);
    bool is_truncated = false;
    do {
        auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->ListObjectsV2(request),
                                                    "s3_client::list_objects_v2", request);
        if (!outcome.IsSuccess()) {
            LOG_WARNING("failed to list objects")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("prefix", prefix)
                    .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                    .tag("error", outcome.GetError().GetMessage());
            if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::FORBIDDEN) {
                return 1;
            }
            return -1;
        }
        const auto& result = outcome.GetResult();
        VLOG_DEBUG << "get " << result.GetContents().size() << " objects";
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        objects.reserve(result.GetContents().size());
        for (const auto& obj : result.GetContents()) {
            objects.emplace_back().SetKey(obj.GetKey());
            LOG_INFO("delete object")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("key", obj.GetKey());
        }
        if (!objects.empty()) {
            Aws::S3::Model::Delete del;
            del.WithObjects(std::move(objects)).SetQuiet(true);
            delete_request.SetDelete(std::move(del));
            auto delete_outcome =
                    SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->DeleteObjects(delete_request),
                                                 "s3_client::delete_objects", delete_request);
            if (!delete_outcome.IsSuccess()) {
                LOG_WARNING("failed to delete objects")
                        .tag("endpoint", conf_.endpoint)
                        .tag("bucket", conf_.bucket)
                        .tag("prefix", prefix)
                        .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                        .tag("error", outcome.GetError().GetMessage());
                if (delete_outcome.GetError().GetResponseCode() ==
                    Aws::Http::HttpResponseCode::FORBIDDEN) {
                    return 1;
                }
                return -2;
            }
            if (!delete_outcome.GetResult().GetErrors().empty()) {
                const auto& e = delete_outcome.GetResult().GetErrors().front();
                LOG_WARNING("failed to delete object")
                        .tag("endpoint", conf_.endpoint)
                        .tag("bucket", conf_.bucket)
                        .tag("key", e.GetKey())
                        .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                        .tag("error", e.GetMessage());
                return -3;
            }
        }
        is_truncated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_truncated);
    return 0;
}

int S3Accessor::delete_objects(const std::vector<std::string>& relative_paths) {
    if (relative_paths.empty()) {
        return 0;
    }
    // `DeleteObjectsRequest` can only contain 1000 keys at most.
    constexpr size_t max_delete_batch = 1000;
    auto path_iter = relative_paths.begin();

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(conf_.bucket);
    do {
        Aws::S3::Model::Delete del;
        Aws::Vector<Aws::S3::Model::ObjectIdentifier> objects;
        auto path_begin = path_iter;
        for (; path_iter != relative_paths.end() && (path_iter - path_begin < max_delete_batch);
             ++path_iter) {
            auto key = get_key(*path_iter);
            LOG_INFO("delete object")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("key", key)
                    .tag("size", objects.size());
            objects.emplace_back().SetKey(std::move(key));
        }
        if (objects.empty()) {
            return 0;
        }
        del.WithObjects(std::move(objects)).SetQuiet(true);
        delete_request.SetDelete(std::move(del));
        auto delete_outcome =
                SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->DeleteObjects(delete_request),
                                             "s3_client::delete_objects", delete_request);
        if (!delete_outcome.IsSuccess()) {
            LOG_WARNING("failed to delete objects")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("key[0]", delete_request.GetDelete().GetObjects().front().GetKey())
                    .tag("responseCode",
                         static_cast<int>(delete_outcome.GetError().GetResponseCode()))
                    .tag("error", delete_outcome.GetError().GetMessage());
            return -1;
        }
        if (!delete_outcome.GetResult().GetErrors().empty()) {
            const auto& e = delete_outcome.GetResult().GetErrors().front();
            LOG_WARNING("failed to delete object")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("key", e.GetKey())
                    .tag("responseCode",
                         static_cast<int>(delete_outcome.GetError().GetResponseCode()))
                    .tag("error", e.GetMessage());
            return -2;
        }
    } while (path_iter != relative_paths.end());

    return 0;
}

int S3Accessor::delete_object(const std::string& relative_path) {
    Aws::S3::Model::DeleteObjectRequest request;
    auto key = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithKey(key);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->DeleteObject(request),
                                                "s3_client::delete_object", request);
    if (!outcome.IsSuccess()) {
        LOG_WARNING("failed to delete object")
                .tag("endpoint", conf_.endpoint)
                .tag("bucket", conf_.bucket)
                .tag("key", key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage())
                .tag("exception", outcome.GetError().GetExceptionName());
        return -1;
    }
    return 0;
}

int S3Accessor::put_object(const std::string& relative_path, const std::string& content) {
    Aws::S3::Model::PutObjectRequest request;
    auto key = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithKey(key);
    auto input = Aws::MakeShared<Aws::StringStream>("S3Accessor");
    *input << content;
    request.SetBody(input);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->PutObject(request),
                                                "s3_client::put_object", request);
    if (!outcome.IsSuccess()) {
        LOG_WARNING("failed to put object")
                .tag("endpoint", conf_.endpoint)
                .tag("bucket", conf_.bucket)
                .tag("key", key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
    return 0;
}

int S3Accessor::list(const std::string& relative_path, std::vector<ObjectMeta>* files) {
    Aws::S3::Model::ListObjectsV2Request request;
    auto prefix = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithPrefix(prefix);

    bool is_truncated = false;
    do {
        auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->ListObjectsV2(request),
                                                    "s3_client::list_objects_v2", request);
        ;
        if (!outcome.IsSuccess()) {
            LOG_WARNING("failed to list objects")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("prefix", prefix)
                    .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                    .tag("error", outcome.GetError().GetMessage());
            return -1;
        }
        const auto& result = outcome.GetResult();
        VLOG_DEBUG << "get " << result.GetContents().size() << " objects";
        for (const auto& obj : result.GetContents()) {
            files->push_back({obj.GetKey().substr(conf_.prefix.size() + 1), obj.GetSize()});
        }
        is_truncated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_truncated);
    return 0;
}

int S3Accessor::exist(const std::string& relative_path) {
    Aws::S3::Model::HeadObjectRequest request;
    auto key = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithKey(key);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->HeadObject(request),
                                                "s3_client::head_object", request);
    ;
    if (outcome.IsSuccess()) {
        return 0;
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return 1;
    } else {
        LOG_WARNING("failed to head object")
                .tag("endpoint", conf_.endpoint)
                .tag("bucket", conf_.bucket)
                .tag("key", key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
}

int S3Accessor::delete_expired_objects(const std::string& relative_path, int64_t expired_time) {
    Aws::S3::Model::ListObjectsV2Request request;
    auto prefix = get_key(relative_path);
    request.WithBucket(conf_.bucket).WithPrefix(prefix);

    bool is_truncated = false;
    do {
        auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->ListObjectsV2(request),
                                                    "s3_client::list_objects_v2", request);
        ;
        if (!outcome.IsSuccess()) {
            LOG_WARNING("failed to list objects")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("prefix", prefix)
                    .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                    .tag("error", outcome.GetError().GetMessage());
            return -1;
        }
        const auto& result = outcome.GetResult();
        std::vector<std::string> expired_keys;
        for (const auto& obj : result.GetContents()) {
            if (obj.GetLastModified().Seconds() < expired_time) {
                auto relative_key = get_relative_path(obj.GetKey());
                if (relative_key.empty()) {
                    LOG_WARNING("failed get relative path")
                            .tag("prefix", conf_.prefix)
                            .tag("key", obj.GetKey());
                } else {
                    expired_keys.push_back(relative_key);
                    LOG_INFO("delete expired object")
                            .tag("prefix", conf_.prefix)
                            .tag("key", obj.GetKey())
                            .tag("relative_key", relative_key)
                            .tag("lastModifiedTime", obj.GetLastModified().Seconds())
                            .tag("expiredTime", expired_time);
                }
            }
        }

        auto ret = delete_objects(expired_keys);
        if (ret != 0) {
            return ret;
        }
        LOG_INFO("delete expired objects")
                .tag("endpoint", conf_.endpoint)
                .tag("bucket", conf_.bucket)
                .tag("prefix", conf_.prefix)
                .tag("num_scanned", result.GetContents().size())
                .tag("num_recycled", expired_keys.size());
        is_truncated = result.GetIsTruncated();
        request.SetContinuationToken(result.GetNextContinuationToken());
    } while (is_truncated);
    return 0;
}

int S3Accessor::get_bucket_lifecycle(int64_t* expiration_days) {
    Aws::S3::Model::GetBucketLifecycleConfigurationRequest request;
    request.SetBucket(conf_.bucket);

    auto outcome =
            SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->GetBucketLifecycleConfiguration(request),
                                         "s3_client::get_bucket_lifecycle_configuration", request);
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
                .tag("endpoint", conf_.endpoint)
                .tag("bucket", conf_.bucket)
                .tag("prefix", conf_.prefix)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }

    if (!has_lifecycle) {
        LOG_WARNING("Err for check interval: bucket doesn't have lifecycle configuration")
                .tag("endpoint", conf_.endpoint)
                .tag("bucket", conf_.bucket)
                .tag("prefix", conf_.prefix);
        return -1;
    }
    return 0;
}

int S3Accessor::check_bucket_versioning() {
    Aws::S3::Model::GetBucketVersioningRequest request;
    request.SetBucket(conf_.bucket);
    auto outcome = SYNC_POINT_HOOK_RETURN_VALUE(s3_client_->GetBucketVersioning(request),
                                                "s3_client::get_bucket_versioning", request);

    if (outcome.IsSuccess()) {
        const auto& versioning_configuration = outcome.GetResult().GetStatus();
        if (versioning_configuration != Aws::S3::Model::BucketVersioningStatus::Enabled) {
            LOG_WARNING("Err for check interval: bucket doesn't enable bucket versioning")
                    .tag("endpoint", conf_.endpoint)
                    .tag("bucket", conf_.bucket)
                    .tag("prefix", conf_.prefix);
            return -1;
        }
    } else {
        LOG_WARNING("Err for check interval: failed to get status of bucket versioning")
                .tag("endpoint", conf_.endpoint)
                .tag("bucket", conf_.bucket)
                .tag("prefix", conf_.prefix)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
    return 0;
}

int GcsAccessor::delete_objects(const std::vector<std::string>& relative_paths) {
    std::vector<int> delete_rets(relative_paths.size());
    std::transform(std::execution::par, relative_paths.begin(), relative_paths.end(),
                   delete_rets.begin(),
                   [this](const std::string& path) { return delete_object(path); });
    int ret = 0;
    for (int delete_ret : delete_rets) {
        if (delete_ret != 0) {
            ret = delete_ret;
            break;
        }
    }
    return ret;
}

#undef HELP_MACRO
} // namespace doris::cloud
