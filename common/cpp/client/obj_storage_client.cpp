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

#include "obj_storage_client.h"

#include <cpp/sync_point.h>

#include <algorithm>
#include <iterator>

namespace doris {
namespace {

ObjStorageRateLimitToken acquire_rate_limit(
        const std::shared_ptr<const ObjStorageRateLimitPolicy>& policy, ObjStorageRequestType type,
        size_t estimated_bytes = 0) {
    if (!policy) {
        return {};
    }
    return policy->acquire(type, estimated_bytes);
}

} // namespace

ObjStorageRateLimitToken ObjStorageClient::acquire(ObjStorageRequestType type,
                                                   size_t estimated_bytes) const {
    return acquire_rate_limit(rate_limit_policy_, type, estimated_bytes);
}

ObjectStorageUploadResponse ObjStorageClient::create_multipart_upload(
        const ObjectStoragePathOptions& opts) {
    auto rate_limit = acquire(ObjStorageRequestType::PUT);
    if (!rate_limit.resp.ok()) {
        return {.resp = std::move(rate_limit.resp)};
    }
    return backend_->create_multipart_upload(opts);
}

ObjectStorageResponse ObjStorageClient::put_object(const ObjectStoragePathOptions& opts,
                                                   std::string_view stream) {
    auto rate_limit = acquire(ObjStorageRequestType::PUT, stream.size());
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return backend_->put_object(opts, stream);
}

ObjectStorageUploadResponse ObjStorageClient::upload_part(const ObjectStoragePathOptions& opts,
                                                          std::string_view stream, int part_num) {
    auto rate_limit = acquire(ObjStorageRequestType::PUT, stream.size());
    if (!rate_limit.resp.ok()) {
        return {.resp = std::move(rate_limit.resp)};
    }
    return backend_->upload_part(opts, stream, part_num);
}

ObjectStorageResponse ObjStorageClient::complete_multipart_upload(
        const ObjectStoragePathOptions& opts,
        const std::vector<ObjectCompleteMultiPart>& completed_parts) {
    auto rate_limit = acquire(ObjStorageRequestType::PUT);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return backend_->complete_multipart_upload(opts, completed_parts);
}

ObjectStorageHeadResponse ObjStorageClient::head_object(const ObjectStoragePathOptions& opts) {
    auto rate_limit = acquire(ObjStorageRequestType::GET);
    if (!rate_limit.resp.ok()) {
        return {.resp = std::move(rate_limit.resp)};
    }
    return backend_->head_object(opts);
}

ObjectStorageResponse ObjStorageClient::get_object(const ObjectStoragePathOptions& opts,
                                                   void* buffer, size_t offset, size_t bytes_read,
                                                   size_t* size_return) {
    auto rate_limit = acquire(ObjStorageRequestType::GET, bytes_read);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    auto response = backend_->get_object(opts, buffer, offset, bytes_read, size_return);
    if (response.ok()) {
        rate_limit.settle_bytes(*size_return);
    }
    return response;
}

ObjectStorageListPage ObjStorageClient::list_objects(const ObjectStoragePathOptions& opts,
                                                     std::string_view continuation_token) {
    auto rate_limit = acquire(ObjStorageRequestType::GET);
    if (!rate_limit.resp.ok()) {
        return {.resp = std::move(rate_limit.resp)};
    }
    return backend_->list_objects(opts, continuation_token);
}

ObjectStorageResponse ObjectListIterator::has_next() {
    if (!is_valid_) {
        return {
                .status = {TStatusCode::INTERNAL_ERROR, "Iterator is invalid"},
                .http_code = 0,
        };
    }
    while (next_index_ == objects_.size()) {
        if (!has_more_) {
            return {
                    .status = {TStatusCode::END_OF_FILE, "No more results"},
                    .http_code = 200,
            };
        }
        auto page = client_->list_objects(opts_, continuation_token_);
        if (!page.resp.ok()) {
            is_valid_ = false;
            return page.resp;
        }
        objects_ = std::move(page.objects);
        next_index_ = 0;
        continuation_token_ = std::move(page.continuation_token);
        has_more_ = page.has_more;
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageListResponse ObjectListIterator::next() {
    auto response = has_next();
    if (response.status.code == ObjectStorageStatus::END_OF_FILE) {
        return {.resp = ObjectStorageResponse::OK(), .results_ = {}};
    }
    if (!response.ok()) {
        return {.resp = std::move(response), .results_ = {}};
    }
    return {
            .resp = ObjectStorageResponse::OK(),
            .results_ = std::move(objects_[next_index_++]),
    };
}

ObjectStorageResponse ObjStorageClient::delete_objects(const ObjectStoragePathOptions& opts,
                                                       std::vector<std::string> objs) {
    const auto max_batch_size = std::max<size_t>(1, backend_->capabilities().max_delete_batch);
    for (size_t begin = 0; begin < objs.size(); begin += max_batch_size) {
        const auto end = std::min(begin + max_batch_size, objs.size());
        auto rate_limit = acquire(ObjStorageRequestType::PUT);
        if (!rate_limit.resp.ok()) {
            return rate_limit.resp;
        }
        std::vector<std::string> batch(std::make_move_iterator(objs.begin() + begin),
                                       std::make_move_iterator(objs.begin() + end));
        auto response = backend_->delete_objects(opts, std::move(batch));
        if (!response.ok()) {
            return response;
        }
    }
    return ObjectStorageResponse::OK();
}

ObjectStorageResponse ObjStorageClient::delete_object(const ObjectStoragePathOptions& opts) {
    auto rate_limit = acquire(ObjStorageRequestType::PUT);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return backend_->delete_object(opts);
}

ObjectStorageResponse ObjStorageClient::delete_objects_recursively(
        const ObjectStoragePathOptions& opts, const RecursiveDeleteOptions& options) {
    auto list_opts = opts;
    if (list_opts.prefix.empty()) {
        list_opts.prefix = list_opts.key;
    }
    auto delete_batch_size = std::max<size_t>(1, backend_->capabilities().max_delete_batch);
    TEST_SYNC_POINT_CALLBACK("ObjStorageClient::delete_objects_recursively_", &delete_batch_size);
    delete_batch_size = std::max<size_t>(1, delete_batch_size);
    const auto max_tasks_per_batch = std::max<size_t>(1, options.max_tasks_per_batch);
    std::vector<std::string> keys;
    keys.reserve(delete_batch_size);
    size_t pending_tasks = 0;

    auto wait_for_tasks = [&]() {
        if (pending_tasks == 0) {
            return ObjectStorageResponse::OK();
        }
        pending_tasks = 0;
        return options.executor ? options.executor.wait() : ObjectStorageResponse::OK();
    };
    auto submit_delete_task = [&]() {
        ObjStorageDeleteTask task = [backend = backend_, rate_limit_policy = rate_limit_policy_,
                                     bucket = opts.bucket, batch = std::move(keys)]() mutable {
            auto rate_limit = acquire_rate_limit(rate_limit_policy, ObjStorageRequestType::PUT);
            if (!rate_limit.resp.ok()) {
                return rate_limit.resp;
            }
            return backend->delete_objects(ObjectStoragePathOptions {.bucket = std::move(bucket)},
                                           std::move(batch));
        };
        keys.clear();
        keys.reserve(delete_batch_size);

        ObjectStorageResponse response;
        if (options.executor) {
            response = options.executor.submit(std::move(task));
        } else {
            response = task();
        }
        if (!response.ok()) {
            auto wait_response = wait_for_tasks();
            if (!wait_response.ok()) {
                return wait_response;
            }
            return response;
        }
        ++pending_tasks;
        return pending_tasks == max_tasks_per_batch ? wait_for_tasks()
                                                    : ObjectStorageResponse::OK();
    };

    std::string continuation_token;
    bool has_more = true;
    while (has_more) {
        auto page = list_objects(list_opts, continuation_token);
        if (!page.resp.ok()) {
            if (!keys.empty()) {
                auto submit_response = submit_delete_task();
                if (!submit_response.ok()) {
                    return submit_response;
                }
            }
            auto delete_response = wait_for_tasks();
            if (!delete_response.ok()) {
                return delete_response;
            }
            return page.resp;
        }
        continuation_token = std::move(page.continuation_token);
        has_more = page.has_more;
        for (auto& object : page.objects) {
            if (options.expiration_time > 0 && object.mtime_s > options.expiration_time) {
                continue;
            }
            keys.emplace_back(std::move(object.file_path));
            if (keys.size() == delete_batch_size) {
                auto response = submit_delete_task();
                if (!response.ok()) {
                    return response;
                }
            }
        }
    }
    if (!keys.empty()) {
        auto response = submit_delete_task();
        if (!response.ok()) {
            return response;
        }
    }
    return wait_for_tasks();
}

std::string ObjStorageClient::generate_presigned_url(const ObjectStoragePathOptions& opts,
                                                     int64_t expiration_secs) {
    return backend_->generate_presigned_url(opts, expiration_secs);
}

ObjectStorageResponse ObjStorageClient::get_life_cycle(const std::string& bucket,
                                                       int64_t* expiration_days) {
    auto rate_limit = acquire(ObjStorageRequestType::GET);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return backend_->get_life_cycle(bucket, expiration_days);
}

ObjectStorageResponse ObjStorageClient::check_versioning(const std::string& bucket) {
    auto rate_limit = acquire(ObjStorageRequestType::GET);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return backend_->check_versioning(bucket);
}

ObjectStorageResponse ObjStorageClient::abort_multipart_upload(const ObjectStoragePathOptions& opts,
                                                               const std::string& upload_id) {
    auto rate_limit = acquire(ObjStorageRequestType::PUT);
    if (!rate_limit.resp.ok()) {
        return rate_limit.resp;
    }
    return backend_->abort_multipart_upload(opts, upload_id);
}

} // namespace doris
