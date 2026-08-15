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
#include <glog/logging.h>

#include <algorithm>
#include <chrono>

namespace doris {
ObjStorageStatus obj_storage_status_from_http_code(int http_code, std::string message) {
    switch (http_code) {
    case 401:
    case 403:
        return {ObjStorageStatus::PERMISSION_DENIED, std::move(message)};
    case 404:
        return {ObjStorageStatus::NOT_FOUND, std::move(message)};
    case 429:
        return {ObjStorageStatus::LIMIT_REACH, std::move(message)};
    default:
        return {http_code <= 0 ? ObjStorageStatus::NETWORK_ERROR : ObjStorageStatus::INTERNAL_ERROR,
                std::move(message)};
    }
}

std::unique_ptr<ObjStorageListIterator> ObjStorageClient::list_objects(const ObjStoragePath& opts) {
    return std::make_unique<ObjStorageListIterator>(shared_from_this(), opts);
}

ObjStorageResponse ObjStorageClient::list_objects(const ObjStoragePath& opts,
                                                  std::vector<ObjectMeta>* objects) {
    objects->clear();
    auto iter = list_objects(opts);
    for (;;) {
        auto result = iter->next();
        if (!result.object.has_value()) {
            if (!result.resp.ok()) {
                objects->clear();
            }
            return result.resp;
        }
        objects->emplace_back(std::move(*result.object));
    }
}

ObjStorageResponse ObjStorageListIterator::has_next() {
    if (!is_valid_) {
        return {
                .status = {ObjStorageStatus::INTERNAL_ERROR, "Iterator is invalid"},
                .http_code = 0,
        };
    }
    while (next_index_ == objects_.size()) {
        if (!has_more_) {
            return {
                    .status = {ObjStorageStatus::END_OF_FILE, "No more results"},
                    .http_code = 200,
            };
        }
        auto page = client_->list_objects_page(opts_, continuation_token_);
        if (!page.resp.ok()) {
            is_valid_ = false;
            return page.resp;
        }
        objects_ = std::move(page.objects);
        next_index_ = 0;
        continuation_token_ = std::move(page.continuation_token);
        has_more_ = page.has_more;
    }
    return ObjStorageResponse::OK();
}

ObjStorageListResult ObjStorageListIterator::next() {
    auto response = has_next();
    if (response.status.code == ObjStorageStatus::END_OF_FILE) {
        return {.resp = ObjStorageResponse::OK(), .object = {}};
    }
    if (!response.ok()) {
        return {.resp = std::move(response), .object = {}};
    }
    return {
            .resp = ObjStorageResponse::OK(),
            .object = std::move(objects_[next_index_++]),
    };
}

ObjStorageResponse delete_objects_recursively(
        std::shared_ptr<ObjStorageClient> client, const ObjStoragePath& path,
        const ObjStorageRecursiveDeleteOptions& delete_options) {
    const auto start_time = std::chrono::steady_clock::now();
    auto list_path = path;
    if (list_path.prefix.empty()) {
        list_path.prefix = list_path.key;
    }
    auto delete_batch_size = std::max<size_t>(1, client->capabilities().max_delete_batch);
    TEST_SYNC_POINT_CALLBACK("ObjStorageClient::delete_objects_recursively_", &delete_batch_size);
    delete_batch_size = std::max<size_t>(1, delete_batch_size);
    const auto max_tasks_per_batch = std::max<size_t>(1, delete_options.max_tasks_per_batch);
    std::vector<std::string> keys;
    keys.reserve(delete_batch_size);
    size_t pending_tasks = 0;
    size_t total_batches = 0;
    size_t num_deleted = 0;
    size_t error_count = 0;
    auto first_error = ObjStorageResponse::OK();

    auto elapsed_milliseconds = [&]() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - start_time)
                .count();
    };
    auto finish = [&](ObjStorageResponse response) {
        LOG(INFO) << "delete objects under " << list_path.bucket << "/" << list_path.prefix
                  << " finished, ret=" << response.status.code
                  << ", total_batches=" << total_batches << ", num_deleted=" << num_deleted
                  << ", error_count=" << error_count << ", cost=" << elapsed_milliseconds()
                  << " ms";
        return response;
    };
    auto record_error = [&](ObjStorageResponse response) {
        if (response.ok()) {
            return;
        }
        ++error_count;
        if (first_error.ok()) {
            first_error = std::move(response);
        }
    };

    auto wait_for_tasks = [&]() {
        if (pending_tasks == 0) {
            return ObjStorageResponse::OK();
        }
        const auto tasks_in_batch = pending_tasks;
        pending_tasks = 0;
        auto response = delete_options.executor ? delete_options.executor->wait()
                                                : ObjStorageResponse::OK();
        ++total_batches;
        LOG(INFO) << "delete objects under " << list_path.bucket << "/" << list_path.prefix
                  << " batch " << total_batches << " completed"
                  << ", tasks_in_batch=" << tasks_in_batch << ", total_deleted=" << num_deleted
                  << ", elapsed=" << elapsed_milliseconds() << " ms";
        return response;
    };
    auto submit_delete_task = [&]() -> bool {
        ObjStorageDeleteTask task = [client, bucket = path.bucket,
                                     batch = std::move(keys)]() mutable {
            return client->delete_objects(ObjStoragePath {.bucket = std::move(bucket)},
                                          std::move(batch));
        };
        keys.clear();
        keys.reserve(delete_batch_size);

        ObjStorageResponse response;
        if (delete_options.executor) {
            response = delete_options.executor->submit(std::move(task));
        } else {
            response = task();
        }
        ++pending_tasks;
        if (!response.ok()) {
            record_error(std::move(response));
            record_error(wait_for_tasks());
            return false;
        }
        if (pending_tasks == max_tasks_per_batch) {
            record_error(wait_for_tasks());
        }
        // Match the pre-refactor Recycler behavior: do not scan the next task batch after the
        // current batch reports a submit, delete, or wait failure.
        return first_error.ok();
    };

    auto iter = client->list_objects(list_path);
    for (;;) {
        auto result = iter->next();
        if (!result.object.has_value()) {
            if (result.resp.ok()) {
                break;
            }
            if (!keys.empty()) {
                submit_delete_task();
            }
            record_error(wait_for_tasks());
            record_error(std::move(result.resp));
            return finish(std::move(first_error));
        }
        auto& object = *result.object;
        if (delete_options.expiration_time > 0 && object.mtime_s > delete_options.expiration_time) {
            continue;
        }
        ++num_deleted;
        keys.emplace_back(std::move(object.key));
        if (keys.size() == delete_batch_size && !submit_delete_task()) {
            return finish(std::move(first_error));
        }
    }
    if (!keys.empty() && !submit_delete_task()) {
        return finish(std::move(first_error));
    }
    record_error(wait_for_tasks());
    return finish(std::move(first_error));
}

} // namespace doris
