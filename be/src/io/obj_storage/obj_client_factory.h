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

#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>

#include "common/status.h"
#include "io/obj_storage/azure_client_provider.h"
#include "io/obj_storage/obj_storage_config.h"
#include "io/obj_storage/s3_client_provider.h"

namespace doris::io {

// Entry for getting a BE native object storage client. It validates the configuration with
// the rules of its backend, then dispatches to the provider that builds and caches the client.
class ObjClientFactory {
public:
    static ObjClientFactory& instance();

    Result<std::shared_ptr<ObjStorageClient>> get_or_create(const ObjClientConfig& config);

#ifdef BE_TEST
    // Replaces client construction (after validation) for tests; nullptr restores it.
    void set_client_creator_for_test(
            std::function<std::shared_ptr<ObjStorageClient>(const ObjClientConfig&)> creator);
#endif

    // Called when azure_client_cache_capacity changes; see AzureClientProvider::set_cache_capacity.
    void set_azure_client_cache_capacity(size_t capacity) { _azure.set_cache_capacity(capacity); }

private:
    ObjClientFactory();

    // One overload per backend alternative. get_or_create() reaches them through
    // std::visit, so adding an alternative to ObjClientConfig::backend without a matching
    // overload fails to compile instead of throwing bad_variant_access at runtime.
    Result<std::shared_ptr<ObjStorageClient>> _dispatch(const ObjClientConfig& config,
                                                        const S3ClientConfig& s3);
    Result<std::shared_ptr<ObjStorageClient>> _dispatch(const ObjClientConfig& config,
                                                        const AzureClientConfig& azure);

    S3ClientProvider _s3;
    AzureClientProvider _azure;
#ifdef BE_TEST
    std::mutex _test_lock;
    std::function<std::shared_ptr<ObjStorageClient>(const ObjClientConfig&)> _test_client_creator;
#endif
};

// Wraps a newly built client with the BE shared rate limiter when its usage requires it.
// Every provider calls this before publishing to its cache, so all backends follow one
// policy: in cloud mode only internal storage (storage vaults) is limited, otherwise all
// clients are.
std::shared_ptr<ObjStorageClient> apply_rate_limit_policy(std::shared_ptr<ObjStorageClient> client,
                                                          ClientUsage usage);

} // namespace doris::io
