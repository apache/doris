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

#include "cpp/obj-storage/client/obj_storage_client_factory.h"

#include <glog/logging.h>

#include <exception>
#include <type_traits>
#include <utility>

#include "cpp/obj-storage/client/bos_obj_storage_client.h"
#include "cpp/obj-storage/client/cos_obj_storage_client.h"
#include "cpp/obj-storage/client/gcp_obj_storage_client.h"
#include "cpp/obj-storage/client/obs_obj_storage_client.h"
#include "cpp/obj-storage/client/oss_obj_storage_client.h"
#include "cpp/obj-storage/client/s3_obj_storage_client.h"
#include "cpp/obj-storage/client/tos_obj_storage_client.h"
#ifdef USE_AZURE
#include "cpp/obj-storage/client/azure_obj_storage_client.h"
#endif

namespace doris {

ObjStorageClientFactory& ObjStorageClientFactory::instance() {
    static ObjStorageClientFactory factory;
    return factory;
}

ObjStorageClientBuildResult ObjStorageClientFactory::create(const ObjStorageClientConfig& config,
                                                            bool use_cache,
                                                            const std::string& bucket) {
    if (auto error = validate_obj_storage_config(config)) {
        return {.error = std::move(*error)};
    }

#ifdef BE_TEST
    {
        std::lock_guard l(_lock);
        if (_test_client_creator) {
            return {.client = _test_client_creator(config)};
        }
    }
#endif

    std::shared_ptr<ClientCacheCallback> cache;
    if (use_cache) {
        std::lock_guard l(_lock);
        cache = _client_cache;
    }
    if (cache) {
        return (*cache)(config, bucket);
    }

    if (use_cache) {
        std::lock_guard l(_lock);
        auto bucket_cache = _cache.find(bucket);
        if (bucket_cache != _cache.end()) {
            auto it = bucket_cache->second.find(config);
            if (it != bucket_cache->second.end()) {
                return {.client = it->second};
            }
        }
    }

    ObjStorageClientBuildResult result;
    try {
        result = std::visit(
                []<class C>(const C& settings) -> ObjStorageClientBuildResult {
                    if constexpr (std::is_same_v<C, AwsObjStorageConfig>) {
                        return S3ObjStorageClient::create(settings);
                    } else if constexpr (std::is_same_v<C, BosObjStorageConfig>) {
                        return BosObjStorageClient::create(settings);
                    } else if constexpr (std::is_same_v<C, CosObjStorageConfig>) {
                        return CosObjStorageClient::create(settings);
                    } else if constexpr (std::is_same_v<C, GcpObjStorageConfig>) {
                        return GcpObjStorageClient::create(settings);
                    } else if constexpr (std::is_same_v<C, ObsObjStorageConfig>) {
                        return ObsObjStorageClient::create(settings);
                    } else if constexpr (std::is_same_v<C, OssObjStorageConfig>) {
                        return OssObjStorageClient::create(settings);
                    } else if constexpr (std::is_same_v<C, TosObjStorageConfig>) {
                        return TosObjStorageClient::create(settings);
                    } else if constexpr (std::is_same_v<C, AzureObjStorageConfig>) {
#ifdef USE_AZURE
                        return AzureObjStorageClient::create(settings);
#else
                        return {.error = "Azure support is disabled in this build",
                                .unsupported = true};
#endif
                    }
                },
                config);
    } catch (const std::exception& ex) {
        result.error = ex.what();
    }
    if (!result.client) {
        if (!result.unsupported) {
            result.error = "failed to create object storage client: " + result.error;
        }
        return result;
    }
    LOG(INFO) << "create object storage client with " << obj_storage_config_to_string(config);

    if (use_cache) {
        std::lock_guard l(_lock);
        auto [it, _] = _cache[bucket].emplace(config, std::move(result.client));
        return {.client = it->second};
    }
    return result;
}

void ObjStorageClientFactory::set_client_cache(ClientCacheCallback cache) {
    // Release old clients outside the factory lock; SDK destruction may re-enter it.
    decltype(_cache) old_cache;
    auto callback = std::make_shared<ClientCacheCallback>(std::move(cache));
    std::shared_ptr<ClientCacheCallback> old_callback;
    {
        std::lock_guard l(_lock);
        old_cache.swap(_cache);
        old_callback.swap(_client_cache);
        _client_cache = std::move(callback);
    }
}

#ifdef BE_TEST
void ObjStorageClientFactory::set_client_creator_for_test(
        std::function<std::shared_ptr<ObjStorageClient>(const ObjStorageClientConfig&)> creator) {
    std::lock_guard l(_lock);
    _test_client_creator = std::move(creator);
}

void ObjStorageClientFactory::clear_client_creator_for_test() {
    std::lock_guard l(_lock);
    _test_client_creator = nullptr;
}
#endif

} // namespace doris
