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

#include "cpp/sync_point.h"
#include "recycler/sync_executor.h"

using namespace std::chrono;

namespace doris::cloud {

ObjectStorageResponse ObjStorageClient::delete_objects_recursively_(ObjectStoragePathRef path,
                                                                    const ObjClientOptions& option,
                                                                    int64_t expired_time,
                                                                    size_t batch_size) {
    TEST_SYNC_POINT_CALLBACK("ObjStorageClient::delete_objects_recursively_", &batch_size);
    size_t num_deleted_objects = 0;
    auto start_time = steady_clock::now();

    auto list_iter = list_objects(path);

    ObjectStorageResponse ret;
    std::vector<std::string> keys;
    SyncExecutor<int> concurrent_delete_executor(
            option.executor,
            fmt::format("delete objects under bucket {}, path {}", path.bucket, path.key),
            [](const int& ret) { return ret != 0; });

    for (auto obj = list_iter->next(); obj.has_value(); obj = list_iter->next()) {
        if (expired_time > 0 && obj->mtime_s > expired_time) {
            continue;
        }

        num_deleted_objects++;
        keys.emplace_back(std::move(obj->key));
        if (keys.size() < batch_size) {
            continue;
        }
        concurrent_delete_executor.add([this, &path, k = std::move(keys), option]() mutable {
            return delete_objects(path.bucket, std::move(k), option).ret;
        });
    }

    if (!list_iter->is_valid()) {
        bool finished;
        concurrent_delete_executor.when_all(&finished);
        return {-1};
    }

    if (!keys.empty()) {
        concurrent_delete_executor.add([this, &path, k = std::move(keys), option]() mutable {
            return delete_objects(path.bucket, std::move(k), option).ret;
        });
    }
    bool finished = true;
    std::vector<int> rets = concurrent_delete_executor.when_all(&finished);
    for (int r : rets) {
        if (r != 0) {
            ret = -1;
        }
    }

    auto elapsed = duration_cast<milliseconds>(steady_clock::now() - start_time).count();
    LOG(INFO) << "delete objects under " << path.bucket << "/" << path.key
              << " finished, ret=" << ret.ret << ", finished=" << finished
              << ", num_deleted_objects=" << num_deleted_objects << ", cost=" << elapsed << " ms";

    ret = finished ? ret : -1;

    return ret;
}

} // namespace doris::cloud
