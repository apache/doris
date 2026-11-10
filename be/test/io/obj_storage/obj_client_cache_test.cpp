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

#include "io/obj_storage/obj_client_cache.h"

#include <aws/s3/S3Client.h>
#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <string>

#include "cpp/obj-client/s3_obj_storage_client.h"
#include "io/obj_storage/obj_storage_config.h"
#include "util/s3_util.h"

namespace doris::io {
namespace {

std::shared_ptr<ObjStorageClient> cache_test_client() {
    // The AWS SDK must be initialized before an S3Client is constructed.
    AwsSdkManager::instance();
    return std::make_shared<S3ObjStorageClient>(std::make_shared<Aws::S3::S3Client>(),
                                                ObjStorageEndpointInfo {});
}

// One identity per SAS token, as a catalog vending a fresh SAS per query produces.
ObjClientConfig cache_test_sas_config(std::string token) {
    return ObjClientConfig {
            .connection = {},
            .usage = ClientUsage::EXTERNAL_ACCESS,
            .backend =
                    AzureClientConfig {
                            .endpoint = "https://acct.blob.core.windows.net",
                            .account = "acct",
                            .container = "container",
                            .auth = AzureSasConfig {.token = std::move(token),
                                                    .expires_at_ms = std::nullopt},
                    },
    };
}

} // namespace

TEST(ObjClientCacheTest, FirstPublishWins) {
    ObjClientCache cache;
    auto first = cache_test_client();
    auto second = cache_test_client();
    auto config = cache_test_sas_config("same-token");
    EXPECT_EQ(cache.publish(config, first), first);
    EXPECT_EQ(cache.publish(config, second), first);
    EXPECT_EQ(cache.find(config), first);
}

TEST(ObjClientCacheTest, OtherTokensDoNotDisplaceAnExistingClient) {
    ObjClientCache cache;
    auto first = cache_test_client();
    auto other = cache_test_client();
    auto config = cache_test_sas_config("first");
    cache.publish(config, first);
    for (int i = 0; i < 256; ++i) {
        cache.publish(cache_test_sas_config(std::to_string(i)), other);
    }
    EXPECT_EQ(cache.find(config), first);
}

} // namespace doris::io
