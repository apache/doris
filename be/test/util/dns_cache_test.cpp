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

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "common/config.h"
#include "gtest/gtest_pred_impl.h"

namespace doris {

class DNSCacheTest : public testing::Test {
public:
    DNSCacheTest() = default;
    ~DNSCacheTest() override = default;

protected:
    void SetUp() override {
        _saved_threshold = config::dns_cache_max_consecutive_failures;
        _saved_log_every = config::dns_cache_log_every_n_failures;
    }

    void TearDown() override {
        config::dns_cache_max_consecutive_failures = _saved_threshold;
        config::dns_cache_log_every_n_failures = _saved_log_every;
    }

private:
    int32_t _saved_threshold = 0;
    int32_t _saved_log_every = 0;
};

// Sanity: localhost resolves successfully and is cached.
TEST_F(DNSCacheTest, resolve_localhost) {
    DNSCache cache;
    std::string ip;
    EXPECT_TRUE(cache.get("localhost", &ip).ok());
    EXPECT_FALSE(ip.empty());
    // Second call hits the cache fast path and returns the same IP.
    std::string ip2;
    EXPECT_TRUE(cache.get("localhost", &ip2).ok());
    EXPECT_EQ(ip, ip2);
    EXPECT_EQ(1u, cache.size_for_test());
}

// Unresolvable hostname on first access returns InternalError and is NOT cached.
TEST_F(DNSCacheTest, first_miss_does_not_cache) {
    DNSCache cache;
    std::string ip;
    Status st = cache.get("this-host-does-not-exist.invalid", &ip);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(0u, cache.size_for_test());
}

// Repeated successful resolution does not grow the cache and does not accumulate
// any failure state.
TEST_F(DNSCacheTest, success_keeps_cache_stable) {
    DNSCache cache;
    std::string ip;
    for (int i = 0; i < 8; ++i) {
        EXPECT_TRUE(cache.get("localhost", &ip).ok());
    }
    EXPECT_EQ(1u, cache.size_for_test());
}

// The eviction config can be disabled by setting threshold <= 0 (legacy behavior).
// In legacy mode, an unresolvable hostname on first access is still not cached,
// matching the pre-fix semantics for callers.
TEST_F(DNSCacheTest, eviction_disabled_when_threshold_zero) {
    config::dns_cache_max_consecutive_failures = 0;
    DNSCache cache;
    std::string ip;
    Status st = cache.get("another-non-existent.invalid", &ip);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(0u, cache.size_for_test());
}

} // end of namespace doris
