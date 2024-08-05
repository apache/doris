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

#include <ranges>

#include "common/config.h"
#include "common/logging.h"
#include "cpp/s3_rate_limiter.h"
#include "cpp/sync_point.h"
#include "recycler/s3_accessor.h"

namespace doris::cloud {

[[maybe_unused]] static Aws::Client::AWSError<Aws::S3::S3Errors> s3_error_factory() {
    return {Aws::S3::S3Errors::INTERNAL_FAILURE, "exceeds limit", "exceeds limit", false};
}

template <typename Func>
auto s3_rate_limit(S3RateLimitType op, Func callback) -> decltype(callback()) {
    using T = decltype(callback());
    if (!config::enable_s3_rate_limiter) {
        return callback();
    }
    auto sleep_duration = AccessorRateLimiter::instance().rate_limiter(op)->add(1);
    if (sleep_duration < 0) {
        return T(s3_error_factory());
    }
    return callback();
}

template <typename Func>
auto s3_get_rate_limit(Func callback) -> decltype(callback()) {
    return s3_rate_limit(S3RateLimitType::GET, std::move(callback));
}

template <typename Func>
auto s3_put_rate_limit(Func callback) -> decltype(callback()) {
    return s3_rate_limit(S3RateLimitType::PUT, std::move(callback));
}

class S3ObjListIterator final : public ObjectListIterator {
public:
    S3ObjListIterator(std::shared_ptr<Aws::S3::S3Client> client, std::string bucket,
                      std::string prefix, std::string endpoint)
            : client_(std::move(client)), endpoint_(std::move(endpoint)) {
        req_.WithBucket(std::move(bucket)).WithPrefix(std::move(prefix));
        TEST_SYNC_POINT_CALLBACK("S3ObjListIterator", &req_);
    }

    ~S3ObjListIterator() override = default;

    bool is_valid() override { return is_valid_; }

    bool has_next() override {
        if (!is_valid_) {
            return false;
        }

        if (!results_.empty()) {
            return true;
        }

        if (!has_more_) {
            return false;
        }

        auto outcome = s3_get_rate_limit([&]() {
            SCOPED_BVAR_LATENCY(s3_bvar::s3_list_latency);
            return client_->ListObjectsV2(req_);
        });

        if (!outcome.IsSuccess()) {
            LOG_WARNING("failed to list objects")
                    .tag("endpoint", endpoint_)
                    .tag("bucket", req_.GetBucket())
                    .tag("prefix", req_.GetPrefix())
                    .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                    .tag("error", outcome.GetError().GetMessage());
            is_valid_ = false;
            return false;
        }

        has_more_ = outcome.GetResult().GetIsTruncated();
        req_.SetContinuationToken(std::move(
                const_cast<std::string&&>(outcome.GetResult().GetNextContinuationToken())));

        auto&& content = outcome.GetResult().GetContents();
        DCHECK(!(has_more_ && content.empty())) << has_more_ << ' ' << content.empty();

        results_.reserve(content.size());
        for (auto&& obj : std::ranges::reverse_view(content)) {
            DCHECK(obj.GetKey().starts_with(req_.GetPrefix()))
                    << obj.GetKey() << ' ' << req_.GetPrefix();
            results_.emplace_back(
                    ObjectMeta {.key = std::move(const_cast<std::string&&>(obj.GetKey())),
                                .size = obj.GetSize(),
                                .mtime_s = obj.GetLastModified().Seconds()});
        }

        return !results_.empty();
    }

    std::optional<ObjectMeta> next() override {
        std::optional<ObjectMeta> res;
        if (!has_next()) {
            return res;
        }

        res = std::move(results_.back());
        results_.pop_back();
        return res;
    }

private:
    std::shared_ptr<Aws::S3::S3Client> client_;
    Aws::S3::Model::ListObjectsV2Request req_;
    std::vector<ObjectMeta> results_;
    std::string endpoint_;
    bool is_valid_ {true};
    bool has_more_ {true};
};

static constexpr size_t MaxDeleteBatch = 1000;

S3ObjClient::~S3ObjClient() = default;

ObjectStorageResponse S3ObjClient::put_object(ObjectStoragePathRef path, std::string_view stream) {
    Aws::S3::Model::PutObjectRequest request;
    request.WithBucket(path.bucket).WithKey(path.key);
    auto input = Aws::MakeShared<Aws::StringStream>("S3Accessor");
    *input << stream;
    request.SetBody(input);
    auto outcome = s3_put_rate_limit([&]() {
        SCOPED_BVAR_LATENCY(s3_bvar::s3_put_latency);
        return s3_client_->PutObject(request);
    });
    if (!outcome.IsSuccess()) {
        LOG_WARNING("failed to put object")
                .tag("endpoint", endpoint_)
                .tag("bucket", path.bucket)
                .tag("key", path.key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
    return 0;
}

ObjectStorageResponse S3ObjClient::head_object(ObjectStoragePathRef path, ObjectMeta* res) {
    Aws::S3::Model::HeadObjectRequest request;
    request.WithBucket(path.bucket).WithKey(path.key);
    auto outcome = s3_get_rate_limit([&]() {
        SCOPED_BVAR_LATENCY(s3_bvar::s3_head_latency);
        return s3_client_->HeadObject(request);
    });
    if (outcome.IsSuccess()) {
        res->key = path.key;
        res->size = outcome.GetResult().GetContentLength();
        res->mtime_s = outcome.GetResult().GetLastModified().Seconds();
        return 0;
    } else if (outcome.GetError().GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND) {
        return 1;
    } else {
        LOG_WARNING("failed to head object")
                .tag("endpoint", endpoint_)
                .tag("bucket", path.bucket)
                .tag("key", path.key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
}

std::unique_ptr<ObjectListIterator> S3ObjClient::list_objects(ObjectStoragePathRef path) {
    return std::make_unique<S3ObjListIterator>(s3_client_, path.bucket, path.key, endpoint_);
}

ObjectStorageResponse S3ObjClient::delete_objects(const std::string& bucket,
                                                  std::vector<std::string> keys,
                                                  ObjClientOptions option) {
    if (keys.empty()) {
        return {0};
    }

    Aws::S3::Model::DeleteObjectsRequest delete_request;
    delete_request.SetBucket(bucket);

    auto issue_delete = [&bucket, &delete_request,
                         this](std::vector<Aws::S3::Model::ObjectIdentifier> objects) -> int {
        if (objects.size() == 1) {
            return delete_object({.bucket = bucket, .key = objects[0].GetKey()}).ret;
        }

        Aws::S3::Model::Delete del;
        del.WithObjects(std::move(objects)).SetQuiet(true);
        delete_request.SetDelete(std::move(del));
        auto delete_outcome = s3_put_rate_limit([&]() {
            SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_objects_latency);
            return s3_client_->DeleteObjects(delete_request);
        });
        if (!delete_outcome.IsSuccess()) {
            LOG_WARNING("failed to delete objects")
                    .tag("endpoint", endpoint_)
                    .tag("bucket", bucket)
                    .tag("key[0]", delete_request.GetDelete().GetObjects().front().GetKey())
                    .tag("responseCode",
                         static_cast<int>(delete_outcome.GetError().GetResponseCode()))
                    .tag("error", delete_outcome.GetError().GetMessage());
            return -1;
        }

        if (!delete_outcome.IsSuccess()) {
            LOG_WARNING("failed to delete objects")
                    .tag("endpoint", endpoint_)
                    .tag("bucket", bucket)
                    .tag("key[0]", delete_request.GetDelete().GetObjects().front().GetKey())
                    .tag("responseCode",
                         static_cast<int>(delete_outcome.GetError().GetResponseCode()))
                    .tag("error", delete_outcome.GetError().GetMessage());
            return -1;
        }

        return 0;
    };

    int ret = 0;
    // `DeleteObjectsRequest` can only contain 1000 keys at most.
    std::vector<Aws::S3::Model::ObjectIdentifier> objects;

    size_t delete_batch_size = MaxDeleteBatch;
    TEST_INJECTION_POINT_CALLBACK("S3ObjClient::delete_objects", &delete_batch_size);

    // std::views::chunk(1000)
    for (auto&& key : keys) {
        objects.emplace_back().SetKey(std::move(key));
        if (objects.size() < delete_batch_size) {
            continue;
        }

        ret = issue_delete(std::move(objects));
        if (ret != 0) {
            return {ret};
        }
    }

    if (!objects.empty()) {
        ret = issue_delete(std::move(objects));
    }

    return {ret};
}

ObjectStorageResponse S3ObjClient::delete_object(ObjectStoragePathRef path) {
    Aws::S3::Model::DeleteObjectRequest request;
    request.WithBucket(path.bucket).WithKey(path.key);
    auto outcome = s3_put_rate_limit([&]() {
        SCOPED_BVAR_LATENCY(s3_bvar::s3_delete_object_latency);
        return s3_client_->DeleteObject(request);
    });
    if (!outcome.IsSuccess()) {
        LOG_WARNING("failed to delete object")
                .tag("endpoint", endpoint_)
                .tag("bucket", path.bucket)
                .tag("key", path.key)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage())
                .tag("exception", outcome.GetError().GetExceptionName());
        return -1;
    }
    return 0;
}

ObjectStorageResponse S3ObjClient::delete_objects_recursively(ObjectStoragePathRef path,
                                                              ObjClientOptions option,
                                                              int64_t expiration_time) {
    return delete_objects_recursively_(path, option, expiration_time, MaxDeleteBatch);
}

ObjectStorageResponse S3ObjClient::get_life_cycle(const std::string& bucket,
                                                  int64_t* expiration_days) {
    Aws::S3::Model::GetBucketLifecycleConfigurationRequest request;
    request.SetBucket(bucket);

    auto outcome = s3_get_rate_limit(
            [&]() { return s3_client_->GetBucketLifecycleConfiguration(request); });
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
                .tag("endpoint", endpoint_)
                .tag("bucket", bucket)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }

    if (!has_lifecycle) {
        LOG_WARNING("Err for check interval: bucket doesn't have lifecycle configuration")
                .tag("endpoint", endpoint_)
                .tag("bucket", bucket);
        return -1;
    }
    return 0;
}

ObjectStorageResponse S3ObjClient::check_versioning(const std::string& bucket) {
    Aws::S3::Model::GetBucketVersioningRequest request;
    request.SetBucket(bucket);
    auto outcome = s3_get_rate_limit([&]() { return s3_client_->GetBucketVersioning(request); });

    if (outcome.IsSuccess()) {
        const auto& versioning_configuration = outcome.GetResult().GetStatus();
        if (versioning_configuration != Aws::S3::Model::BucketVersioningStatus::Enabled) {
            LOG_WARNING("Err for check interval: bucket doesn't enable bucket versioning")
                    .tag("endpoint", endpoint_)
                    .tag("bucket", bucket);
            return -1;
        }
    } else {
        LOG_WARNING("Err for check interval: failed to get status of bucket versioning")
                .tag("endpoint", endpoint_)
                .tag("bucket", bucket)
                .tag("responseCode", static_cast<int>(outcome.GetError().GetResponseCode()))
                .tag("error", outcome.GetError().GetMessage());
        return -1;
    }
    return 0;
}

} // namespace doris::cloud
