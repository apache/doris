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

#include "io/object_storage/obj_storage_client_factory.h"

#include "common/logging.h"
#include "cpp/obj-storage/client/obj_storage_client_builder.h"

namespace doris {

ObjStorageClientFactory& ObjStorageClientFactory::instance() {
    static ObjStorageClientFactory factory;
    return factory;
}

Result<std::shared_ptr<io::ObjStorageClient>> ObjStorageClientFactory::create(
        const ObjStorageClientConfig& storage_config) {
    if (auto error = validate_obj_storage_config(storage_config)) {
        return ResultError(Status::InvalidArgument<false>(*error));
    }

#ifdef BE_TEST
    {
        std::lock_guard l(_lock);
        if (_test_client_creator) {
            return _test_client_creator(storage_config);
        }
    }
#endif

    {
        std::lock_guard l(_lock);
        auto it = _cache.find(storage_config);
        if (it != _cache.end()) {
            return it->second;
        }
    }

    auto built = build_obj_storage_client(storage_config);
    if (!built.client) {
        if (built.unsupported) {
            return ResultError(Status::NotSupported("{}", built.error));
        }
        return ResultError(
                Status::InvalidArgument("failed to create object storage client: {}", built.error));
    }
    LOG_INFO("create object storage client with {}", obj_storage_config_to_string(storage_config));

    {
        std::lock_guard l(_lock);
        auto [it, _] = _cache.emplace(storage_config, std::move(built.client));
        return it->second;
    }
}

#ifdef BE_TEST
void ObjStorageClientFactory::set_client_creator_for_test(
        std::function<std::shared_ptr<io::ObjStorageClient>(const ObjStorageClientConfig&)>
                creator) {
    std::lock_guard l(_lock);
    _test_client_creator = std::move(creator);
}

void ObjStorageClientFactory::clear_client_creator_for_test() {
    std::lock_guard l(_lock);
    _test_client_creator = nullptr;
}
#endif

} // namespace doris
