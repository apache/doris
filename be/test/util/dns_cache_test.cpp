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

#include "util/dns_cache.h"

#include <gtest/gtest.h>

namespace doris {

TEST(DNSCacheTest, RestartInvalidationPreservesFallback) {
    std::string answer = "10.0.0.1";
    int lookups = 0;
    DNSCache cache([&](const std::string&, std::string& ip, bool, int*) {
        ++lookups;
        ip = answer;
        return ip.empty() ? Status::InternalError("temporary resolver failure") : Status::OK();
    });
    std::string ip;
    ASSERT_TRUE(cache.get("be.test", &ip).ok());
    EXPECT_EQ(ip, answer);
    answer = "10.0.0.2";
    ASSERT_TRUE(cache.get("be.test", &ip).ok());
    EXPECT_EQ(ip, "10.0.0.1");
    EXPECT_EQ(lookups, 1);
    cache.invalidate("be.test");
    ASSERT_TRUE(cache.get("be.test", &ip).ok());
    EXPECT_EQ(ip, "10.0.0.2");
    answer.clear();
    cache.invalidate("be.test");
    ASSERT_TRUE(cache.get("be.test", &ip).ok());
    EXPECT_EQ(ip, "10.0.0.2");
    EXPECT_EQ(lookups, 3);
}

TEST(DNSCacheTest, InvalidationDuringLookupRequiresAnotherLookup) {
    int lookups = 0;
    DNSCache* instance = nullptr;
    DNSCache cache([&](const std::string&, std::string& ip, bool, int*) {
        ++lookups;
        if (lookups == 1) {
            instance->invalidate("be.test");
        }
        ip = lookups == 1 ? "10.0.0.1" : "10.0.0.2";
        return Status::OK();
    });
    instance = &cache;
    std::string ip;
    ASSERT_TRUE(cache.get("be.test", &ip).ok());
    ASSERT_TRUE(cache.get("be.test", &ip).ok());
    EXPECT_EQ(lookups, 2);
    EXPECT_EQ(ip, "10.0.0.2");
}

} // namespace doris
