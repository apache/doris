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

#include "recycler/obj_storage_client.h"

#include <chrono>

#include "common/config.h"
#include "cpp/sync_point.h"
#include "recycler/sync_executor.h"

using namespace std::chrono;

namespace doris::cloud {

ObjectStorageResponse ObjStorageClient::delete_objects_recursively_(ObjectStoragePathRef path,
                                                                    const ObjClientOptions& option,
                                                                    int64_t expired_time,
                                                                    size_t batch_size) {
    TEST_SYNC_POINT_CALLBACK("ObjStorageClient::delete_objects_recursively_", &batch_size);
    auto list_iter = list_objects(path);
    ObjectStorageResponse ret;
    size_t num_deleted = 0;
    int error_count = 0;
    size_t batch_count = 0;
    auto start_time = steady_clock::now();

    // Read max tasks per batch from config, validate to prevent overflow
    int32_t config_val = config::recycler_max_tasks_per_batch;
    size_t max_tasks_per_batch = 1000; // default value
    if (config_val > 0) {
        max_tasks_per_batch = static_cast<size_t>(config_val);
    } else {
        LOG(WARNING) << "recycler_max_tasks_per_batch=" << config_val
                     << " is not positive, using default 1000";
    }

    while (true) {
        // Create a new SyncExecutor for each batch
        // Note: cancel lambda only takes effect within the current batch
        SyncExecutor<int> batch_executor(
                option.executor, fmt::format("delete batch under {}/{}", path.bucket, path.key),
                [](const int& r) { return r != 0; });

        std::vector<std::string> keys;
        size_t tasks_in_batch = 0;
        bool has_more = true;

        // Collect tasks until reaching batch limit or no more files
        while (tasks_in_batch < max_tasks_per_batch && has_more) {
            auto obj = list_iter->next();
            if (!obj.has_value()) {
                has_more = false;
                break;
            }
            if (expired_time > 0 && obj->mtime_s > expired_time) {
                continue;
            }

            num_deleted++;
            keys.emplace_back(std::move(obj->key));

            // Submit a delete task when we have batch_size keys
            if (keys.size() >= batch_size) {
                batch_executor.add([this, &path, k = std::move(keys), option]() mutable {
                    return delete_objects(path.bucket, std::move(k), option).ret;
                });
                keys.clear();
                tasks_in_batch++;
            }
        }

        // Handle remaining keys (less than batch_size)
        if (!keys.empty()) {
            batch_executor.add([this, &path, k = std::move(keys), option]() mutable {
                return delete_objects(path.bucket, std::move(k), option).ret;
            });
            tasks_in_batch++;
        }

        // Before exiting on empty batch, check if listing is valid
        // Avoid silently treating listing failure as success
        if (tasks_in_batch == 0) {
            if (!list_iter->is_valid()) {
                LOG(WARNING) << "list_iter invalid with no tasks collected";
                ret = {-1};
            }
            break;
        }

        // Wait for current batch to complete
        bool finished = true;
        std::vector<int> rets = batch_executor.when_all(&finished);
        batch_count++;

        for (int r : rets) {
            if (r != 0) {
                error_count++;
            }
        }

        // Log batch progress for monitoring long-running delete tasks
        auto batch_elapsed = duration_cast<milliseconds>(steady_clock::now() - start_time).count();
        LOG(INFO) << "delete objects under " << path.bucket << "/" << path.key << " batch "
                  << batch_count << " completed"
                  << ", tasks_in_batch=" << tasks_in_batch << ", total_deleted=" << num_deleted
                  << ", elapsed=" << batch_elapsed << " ms";

        // Check finished status: false means stop_token triggered, task timeout, or task invalid
        if (!finished) {
            LOG(WARNING) << "batch execution did not finish normally, stopping";
            ret = {-1};
            break;
        }

        // Check if list_iter is still valid (network errors, etc.)
        if (!list_iter->is_valid()) {
            LOG(WARNING) << "list_iter became invalid during iteration";
            ret = {-1};
            break;
        }

        // batch_executor goes out of scope, resources are automatically released
    }

    if (error_count > 0) {
        LOG(WARNING) << "delete_objects_recursively completed with " << error_count << " errors";
        ret = {-1};
    }

    auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start_time).count();
    LOG(INFO) << "delete objects under " << path.bucket << "/" << path.key
              << " finished, ret=" << ret.ret << ", total_batches=" << batch_count
              << ", num_deleted=" << num_deleted << ", error_count=" << error_count
              << ", cost=" << elapsed << " ms";

    return ret;
}

} // namespace doris::cloud
