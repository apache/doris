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

#include "io/obj_storage/obj_client_factory.h"

#include <variant>

#include "common/config.h"
#include "util/s3_util.h"

namespace doris::io {

ObjClientFactory::ObjClientFactory() {
    // Complete AWS SDK initialization before this singleton finishes construction. Function
    // local statics are destroyed in reverse order of construction, so the cached AWS
    // clients held by the providers are released before AwsSdkManager shuts the SDK down.
    AwsSdkManager::instance();
}

ObjClientFactory& ObjClientFactory::instance() {
    static ObjClientFactory ret;
    return ret;
}

Result<std::shared_ptr<ObjStorageClient>> ObjClientFactory::get_or_create(
        const ObjClientConfig& config) {
    RETURN_IF_ERROR_RESULT(validate_obj_client_config(config));
#ifdef BE_TEST
    {
        std::lock_guard lock(_test_lock);
        if (_test_client_creator) {
            return _test_client_creator(config);
        }
    }
#endif
    return std::visit([&](const auto& backend) { return _dispatch(config, backend); },
                      config.backend);
}

#ifdef BE_TEST
void ObjClientFactory::set_client_creator_for_test(
        std::function<std::shared_ptr<ObjStorageClient>(const ObjClientConfig&)> creator) {
    std::lock_guard lock(_test_lock);
    _test_client_creator = std::move(creator);
}
#endif

Result<std::shared_ptr<ObjStorageClient>> ObjClientFactory::_dispatch(const ObjClientConfig& config,
                                                                      const S3ClientConfig& s3) {
    return _s3.get_or_create(config, s3);
}

Result<std::shared_ptr<ObjStorageClient>> ObjClientFactory::_dispatch(
        const ObjClientConfig& config, const AzureClientConfig& azure) {
    return _azure.get_or_create(config, azure);
}

} // namespace doris::io

namespace doris::config {

// The cache takes the new capacity at once, so a lowered one evicts immediately even if only
// hits follow, and a publish that read the old value from the config cannot exceed it. The
// callback runs after the new value passed its validator, so it is always positive.
DEFINE_ON_UPDATE(azure_client_cache_capacity, [](int32_t /*old_value*/, int32_t new_value) {
    io::ObjClientFactory::instance().set_azure_client_cache_capacity(
            static_cast<size_t>(new_value));
});

} // namespace doris::config
