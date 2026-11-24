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

#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "cpp/obj-storage/client/obj_storage_client.h"
#include "cpp/obj-storage/parser/obj_storage_config.h"

namespace doris {

class ObjStorageClientFactory {
public:
    static ObjStorageClientFactory& instance();

    // BE reuses clients by effective configuration within each bucket. Equal configurations
    // in different buckets still create separate clients. Recycler sets use_cache=false.
    ObjStorageClientBuildResult create(const ObjStorageClientConfig& config, bool use_cache = true,
                                       const std::string& bucket = {});

#ifdef BE_TEST
    void set_client_creator_for_test(
            std::function<std::shared_ptr<ObjStorageClient>(const ObjStorageClientConfig&)>
                    creator);
    void clear_client_creator_for_test();
#endif

private:
    ObjStorageClientFactory() = default;

    using ClientCache = std::unordered_map<ObjStorageClientConfig,
                                           std::shared_ptr<ObjStorageClient>, ObjStorageClientHash>;
    std::mutex _lock;
    std::unordered_map<std::string, ClientCache> _cache;
#ifdef BE_TEST
    std::function<std::shared_ptr<ObjStorageClient>(const ObjStorageClientConfig&)>
            _test_client_creator;
#endif
};

} // namespace doris
