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
    S3ClientFactory::instance();
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

TEST(ObjClientCacheTest, BoundedPublishEvictsLeastRecentlyUsed) {
    ObjClientCache cache;
    auto a = cache_test_client();
    auto b = cache_test_client();
    auto c = cache_test_client();
    cache.publish(cache_test_sas_config("a"), a, std::nullopt, 0, 2);
    cache.publish(cache_test_sas_config("b"), b, std::nullopt, 0, 2);
    // Touching a makes b the least recently used.
    EXPECT_EQ(cache.find(cache_test_sas_config("a")), a);

    cache.publish(cache_test_sas_config("c"), c, std::nullopt, 0, 2);

    EXPECT_EQ(cache.size(), 2U);
    EXPECT_EQ(cache.find(cache_test_sas_config("b")), nullptr);
    EXPECT_EQ(cache.find(cache_test_sas_config("a")), a);
    EXPECT_EQ(cache.find(cache_test_sas_config("c")), c);
}

TEST(ObjClientCacheTest, ExpiredEntriesAreEvictedFirst) {
    ObjClientCache cache;
    cache.publish(cache_test_sas_config("expires-at-100"), cache_test_client(), 100, 0, 3);
    cache.publish(cache_test_sas_config("no-expiry"), cache_test_client(), std::nullopt, 0, 3);
    cache.publish(cache_test_sas_config("expires-at-1000"), cache_test_client(), 1000, 0, 3);

    // At 500 the first entry has expired; it goes even though the cache is not full.
    cache.publish(cache_test_sas_config("new"), cache_test_client(), std::nullopt, 500, 5);

    EXPECT_EQ(cache.size(), 3U);
    EXPECT_EQ(cache.find(cache_test_sas_config("expires-at-100")), nullptr);
    EXPECT_NE(cache.find(cache_test_sas_config("no-expiry")), nullptr);
    EXPECT_NE(cache.find(cache_test_sas_config("expires-at-1000")), nullptr);
    EXPECT_NE(cache.find(cache_test_sas_config("new")), nullptr);
}

TEST(ObjClientCacheTest, EvictionKeepsClientsInUseAlive) {
    ObjClientCache cache;
    auto in_use = cache_test_client();
    cache.publish(cache_test_sas_config("a"), in_use, std::nullopt, 0, 1);
    cache.publish(cache_test_sas_config("b"), cache_test_client(), std::nullopt, 0, 1);

    EXPECT_EQ(cache.find(cache_test_sas_config("a")), nullptr);
    // Only the cache's reference was dropped; the reader still owns the client.
    EXPECT_EQ(in_use.use_count(), 1);
}

TEST(ObjClientCacheTest, FirstPublishWins) {
    ObjClientCache cache;
    auto first = cache_test_client();
    auto second = cache_test_client();
    EXPECT_EQ(cache.publish(cache_test_sas_config("a"), first, std::nullopt, 0, 4), first);
    EXPECT_EQ(cache.publish(cache_test_sas_config("a"), second, std::nullopt, 0, 4), first);
    EXPECT_EQ(cache.publish(cache_test_sas_config("a"), second), first);
    EXPECT_EQ(cache.size(), 1U);
}

TEST(ObjClientCacheTest, UnboundedPublishNeverEvicts) {
    ObjClientCache cache;
    for (int i = 0; i < 5; ++i) {
        cache.publish(cache_test_sas_config(std::to_string(i)), cache_test_client());
    }
    EXPECT_EQ(cache.size(), 5U);
}

} // namespace doris::io
