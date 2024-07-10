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

#include "cpp/sync_point.h"

namespace doris::cloud {

ObjectStorageResponse ObjStorageClient::delete_objects_recursively_(ObjectStoragePathRef path,
                                                                    int64_t expired_time,
                                                                    size_t batch_size) {
    TEST_SYNC_POINT_CALLBACK("ObjStorageClient::delete_objects_recursively_", &batch_size);
    auto list_iter = list_objects(path);

    ObjectStorageResponse ret;
    std::vector<std::string> keys;

    for (auto obj = list_iter->next(); obj.has_value(); obj = list_iter->next()) {
        if (expired_time > 0 && obj->mtime_s > expired_time) {
            continue;
        }

        keys.emplace_back(std::move(obj->key));
        if (keys.size() < batch_size) {
            continue;
        }

        ret = delete_objects(path.bucket, std::move(keys));
        if (ret.ret != 0) {
            return ret;
        }
    }

    if (!list_iter->is_valid()) {
        return {-1};
    }

    if (!keys.empty()) {
        return delete_objects(path.bucket, std::move(keys));
    }

    return ret;
}

} // namespace doris::cloud
