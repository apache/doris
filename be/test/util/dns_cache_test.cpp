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

    // Build a resolver whose failure behaviour can be flipped at runtime.
    static DNSCache::Resolver make_resolver(bool* should_fail, const std::string& ip = "1.2.3.4") {
        return [should_fail, ip](const std::string&, std::string& out, bool) -> Status {
            if (*should_fail) {
                return Status::InternalError("mock failure");
            }
            out = ip;
            return Status::OK();
        };
    }

private:
    int32_t _saved_threshold = 0;
    int32_t _saved_log_every = 0;
};

// ── existing tests ────────────────────────────────────────────────────────────

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

// ── new tests for eviction logic ─────────────────────────────────────────────

// A hostname that was once successfully resolved is evicted from the cache after
// dns_cache_max_consecutive_failures refresh cycles of continuous DNS failure.
TEST_F(DNSCacheTest, evicts_after_threshold) {
    config::dns_cache_max_consecutive_failures = 3;

    bool should_fail = false;
    DNSCache cache(make_resolver(&should_fail));

    // Populate the cache with one successful resolution.
    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());
    ASSERT_EQ("1.2.3.4", ip);
    ASSERT_EQ(1u, cache.size_for_test());

    // Now make DNS fail.
    should_fail = true;

    // Each refresh_for_test() call is one _refresh_cache iteration.
    // The entry must survive the first threshold-1 cycles and disappear on the
    // threshold-th cycle.
    for (int i = 0; i < 2; ++i) {
        cache.refresh_for_test();
        EXPECT_EQ(1u, cache.size_for_test()) << "should not be evicted yet (i=" << i << ")";
    }
    cache.refresh_for_test(); // third failure → threshold reached → eviction
    EXPECT_EQ(0u, cache.size_for_test()) << "host should have been evicted after threshold";
}

// One successful resolution resets the failure counter, so a full threshold of
// additional failures is required before the next eviction.
TEST_F(DNSCacheTest, success_resets_failure_count) {
    config::dns_cache_max_consecutive_failures = 3;

    bool should_fail = false;
    DNSCache cache(make_resolver(&should_fail));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());

    // Accumulate threshold-1 failures — must NOT evict.
    should_fail = true;
    cache.refresh_for_test();
    cache.refresh_for_test();
    EXPECT_EQ(1u, cache.size_for_test()) << "should not be evicted yet";

    // One success clears the counter.
    should_fail = false;
    cache.refresh_for_test();
    EXPECT_EQ(1u, cache.size_for_test()) << "success should keep the cache entry";

    // A full new round of threshold failures is needed before eviction.
    should_fail = true;
    cache.refresh_for_test();
    cache.refresh_for_test();
    EXPECT_EQ(1u, cache.size_for_test()) << "still not enough failures after counter reset";
    cache.refresh_for_test(); // third failure post-reset → eviction
    EXPECT_EQ(0u, cache.size_for_test()) << "evicted after second run of threshold failures";
}

// A hostname that was never successfully cached must not accumulate entries in
// failure_count, regardless of how many times get() is called (fix for 7.2).
TEST_F(DNSCacheTest, failure_count_does_not_grow_for_never_cached_host) {
    auto always_fail = [](const std::string&, std::string&, bool) -> Status {
        return Status::InternalError("always fails");
    };
    DNSCache cache(always_fail);

    std::string ip;
    for (int i = 0; i < 5; ++i) {
        EXPECT_FALSE(cache.get("never-cached.test", &ip).ok());
    }

    EXPECT_EQ(0u, cache.size_for_test());
    EXPECT_EQ(0u, cache.failure_count_for_test("never-cached.test"))
            << "failure_count must not grow for a host that was never successfully resolved";
}

// After a hostname is evicted, subsequent get() calls must not re-accumulate
// entries in failure_count (fix for 7.2).
TEST_F(DNSCacheTest, failure_count_does_not_grow_after_eviction) {
    config::dns_cache_max_consecutive_failures = 2;

    bool should_fail = false;
    DNSCache cache(make_resolver(&should_fail));

    // Populate cache, then evict.
    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());
    should_fail = true;
    cache.refresh_for_test();
    cache.refresh_for_test();
    ASSERT_EQ(0u, cache.size_for_test()) << "prerequisite: host must be evicted";
    EXPECT_EQ(0u, cache.failure_count_for_test("fake-host.test"))
            << "failure_count must be cleared on eviction";

    // Further get() calls on the evicted host must not re-grow failure_count.
    for (int i = 0; i < 5; ++i) {
        EXPECT_FALSE(cache.get("fake-host.test", &ip).ok());
    }
    EXPECT_EQ(0u, cache.failure_count_for_test("fake-host.test"))
            << "failure_count must not grow for an evicted host";
}

// Race defense: simulate concurrent eviction happening between _resolve_hostname's
// shared_lock read of cached_ip and the unique_lock used to ++failure_count.
// The injected resolver erases the host while DNS resolution is "in flight",
// mimicking what the refresh thread would do.  Under the cache.find() re-check
// added to the unique_lock section, failure_count must NOT be re-introduced.
TEST_F(DNSCacheTest, failure_count_not_reintroduced_on_eviction_race) {
    config::dns_cache_max_consecutive_failures = 1000; // disable auto-eviction

    DNSCache* cache_ptr = nullptr;
    auto racing_resolver = [&cache_ptr](const std::string& host, std::string&,
                                        bool) -> Status {
        // Simulate the refresh thread's _erase() landing during the
        // small window when _resolve_hostname holds no lock.
        if (cache_ptr != nullptr) {
            cache_ptr->_erase(host);
        }
        return Status::InternalError("mock DNS failure during eviction race");
    };

    DNSCache cache(racing_resolver);
    cache_ptr = &cache;

    // Pre-populate so cached_ip is non-empty at the shared_lock read,
    // bypassing _update (which would re-insert after the racing erase).
    {
        std::unique_lock<std::shared_mutex> lock(cache.mutex);
        cache.cache["racing.test"] = "1.2.3.4";
    }
    ASSERT_EQ(1u, cache.size_for_test());

    // Call _resolve_hostname directly (via friend access) so the caller-side
    // re-insert in _update does not mask the behavior we want to verify.
    std::string returned = cache._resolve_hostname("racing.test");

    // _resolve_hostname returns the cached_ip captured before the race.
    EXPECT_EQ("1.2.3.4", returned);
    // The racing erase removed the host from cache.
    EXPECT_EQ(0u, cache.size_for_test());
    // The re-check under unique_lock prevented re-creating a failure_count entry.
    EXPECT_EQ(0u, cache.failure_count_for_test("racing.test"))
            << "failure_count must not be re-introduced for a host evicted mid-resolution";
}

} // end of namespace doris
