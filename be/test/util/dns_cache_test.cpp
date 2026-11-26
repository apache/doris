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
#include <netdb.h>

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
        _saved_negative_ttl = config::dns_cache_negative_ttl_seconds;
    }

    void TearDown() override {
        config::dns_cache_max_consecutive_failures = _saved_threshold;
        config::dns_cache_log_every_n_failures = _saved_log_every;
        config::dns_cache_negative_ttl_seconds = _saved_negative_ttl;
    }

    // Build a resolver whose failure behaviour can be flipped at runtime.  Failures are
    // reported as authoritative NXDOMAIN by default, which is what eviction requires.
    static DNSCache::Resolver make_resolver(bool* should_fail, const std::string& ip = "1.2.3.4",
                                            int fail_gai_err = EAI_NONAME) {
        return [should_fail, ip, fail_gai_err](const std::string&, std::string& out, bool,
                                               int* gai_err) -> Status {
            if (*should_fail) {
                if (gai_err != nullptr) {
                    *gai_err = fail_gai_err;
                }
                return Status::InternalError("mock failure");
            }
            if (gai_err != nullptr) {
                *gai_err = 0;
            }
            out = ip;
            return Status::OK();
        };
    }

    // Build a resolver that counts its invocations; failures are authoritative NXDOMAIN
    // unless `fail_gai_err` says otherwise.
    static DNSCache::Resolver make_counting_resolver(bool* should_fail, int* calls,
                                                     const std::string& ip = "1.2.3.4",
                                                     int fail_gai_err = EAI_NONAME) {
        return [should_fail, calls, ip, fail_gai_err](const std::string&, std::string& out, bool,
                                                      int* gai_err) -> Status {
            ++(*calls);
            if (*should_fail) {
                if (gai_err != nullptr) {
                    *gai_err = fail_gai_err;
                }
                return Status::InternalError("mock failure");
            }
            if (gai_err != nullptr) {
                *gai_err = 0;
            }
            out = ip;
            return Status::OK();
        };
    }

private:
    int32_t _saved_threshold = 0;
    int32_t _saved_log_every = 0;
    int32_t _saved_negative_ttl = 0;
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
    config::dns_cache_negative_ttl_seconds = 3600;

    DNSCache cache;
    std::string ip;
    Status st = cache.get("this-host-does-not-exist.invalid", &ip);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(0u, cache.size_for_test());
    // It is tombstoned instead, so the next caller does not pay another getaddrinfo.
    EXPECT_EQ(1u, cache.negative_cache_size_for_test());
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

// The eviction config can be disabled by setting threshold <= 0 (legacy behavior):
// a cached host whose DNS record disappears keeps serving its stale IP forever,
// exactly as it did before this fix.  Drive many refresh cycles (well past any
// plausible threshold) and assert the entry is never dropped.
TEST_F(DNSCacheTest, eviction_disabled_when_threshold_zero) {
    config::dns_cache_max_consecutive_failures = 0;

    bool should_fail = false;
    DNSCache cache(make_resolver(&should_fail));

    // Populate the cache with one successful resolution.
    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());
    ASSERT_EQ("1.2.3.4", ip);
    ASSERT_EQ(1u, cache.size_for_test());

    // DNS now fails permanently.
    should_fail = true;

    // Far more cycles than the default threshold (30) would need to evict.
    for (int i = 0; i < 50; ++i) {
        cache.refresh_for_test();
        ASSERT_EQ(1u, cache.size_for_test())
                << "eviction must be disabled when threshold <= 0 (cycle " << i << ")";
    }

    // No tombstone was written, and callers still get the stale IP.
    EXPECT_EQ(0u, cache.negative_cache_size_for_test());
    ip.clear();
    EXPECT_TRUE(cache.get("fake-host.test", &ip).ok())
            << "legacy mode must keep serving the cached ip";
    EXPECT_EQ("1.2.3.4", ip);

    // The failure counter still accumulates; only the eviction action is disabled.
    EXPECT_GT(cache.failure_count_for_test("fake-host.test"), 0u);
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
    config::dns_cache_negative_ttl_seconds = 0; // no tombstone, so every call reaches the resolver

    int resolver_calls = 0;
    auto always_fail = [&resolver_calls](const std::string&, std::string&, bool,
                                         int* gai_err) -> Status {
        ++resolver_calls;
        if (gai_err != nullptr) {
            *gai_err = EAI_NONAME;
        }
        return Status::InternalError("always fails");
    };
    DNSCache cache(always_fail);

    std::string ip;
    for (int i = 0; i < 5; ++i) {
        EXPECT_FALSE(cache.get("never-cached.test", &ip).ok());
    }

    EXPECT_EQ(5, resolver_calls) << "with the negative cache disabled every call resolves";
    EXPECT_EQ(0u, cache.size_for_test());
    EXPECT_EQ(0u, cache.failure_count_for_test("never-cached.test"))
            << "failure_count must not grow for a host that was never successfully resolved";
}

// After a hostname is evicted, subsequent get() calls must not re-accumulate
// entries in failure_count (fix for 7.2).
TEST_F(DNSCacheTest, failure_count_does_not_grow_after_eviction) {
    config::dns_cache_max_consecutive_failures = 2;
    config::dns_cache_negative_ttl_seconds = 3600; // keep negative cache active

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
    EXPECT_EQ(1u, cache.negative_cache_size_for_test())
            << "evicted host must be in the negative cache";

    // Further get() calls on the evicted host are served from the negative cache
    // and must not reach the resolver, so failure_count stays zero.
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
    auto racing_resolver = [&cache_ptr](const std::string& host, std::string&, bool,
                                        int* gai_err) -> Status {
        // Simulate the refresh thread's _erase() landing during the
        // small window when _resolve_hostname holds no lock.
        if (cache_ptr != nullptr) {
            cache_ptr->_erase(host);
        }
        if (gai_err != nullptr) {
            *gai_err = EAI_NONAME;
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

// ── negative cache tests ──────────────────────────────────────────────────────

// After a hostname is evicted, get() must return an error immediately without
// invoking the resolver, as long as the negative-cache TTL has not expired.
TEST_F(DNSCacheTest, negative_cache_blocks_resolver_after_eviction) {
    config::dns_cache_max_consecutive_failures = 2;
    config::dns_cache_negative_ttl_seconds = 3600; // will not expire during test

    int resolver_calls = 0;
    bool should_fail = false;
    DNSCache cache(make_counting_resolver(&should_fail, &resolver_calls));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());
    ASSERT_EQ(1, resolver_calls);

    // Evict via failures.
    should_fail = true;
    cache.refresh_for_test();
    cache.refresh_for_test();
    ASSERT_EQ(0u, cache.size_for_test()) << "prerequisite: host must be evicted";
    ASSERT_EQ(1u, cache.negative_cache_size_for_test());

    int calls_at_eviction = resolver_calls;
    for (int i = 0; i < 5; ++i) {
        EXPECT_FALSE(cache.get("fake-host.test", &ip).ok());
    }
    EXPECT_EQ(calls_at_eviction, resolver_calls)
            << "resolver must not be called while host is in the negative cache";
}

// Once the negative-cache TTL expires (simulated by clearing the map), get()
// must attempt a fresh resolve; on success the host re-enters the main cache
// and the negative-cache entry is removed.
TEST_F(DNSCacheTest, negative_cache_retries_after_ttl_expiry) {
    config::dns_cache_max_consecutive_failures = 1;
    config::dns_cache_negative_ttl_seconds = 3600;

    int resolver_calls = 0;
    bool should_fail = false;
    DNSCache cache(make_counting_resolver(&should_fail, &resolver_calls));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());

    // Evict.
    should_fail = true;
    cache.refresh_for_test();
    ASSERT_EQ(0u, cache.size_for_test());
    ASSERT_EQ(1u, cache.negative_cache_size_for_test());

    // Negative cache blocks the resolver.
    EXPECT_FALSE(cache.get("fake-host.test", &ip).ok());
    int calls_while_blocked = resolver_calls;

    // Simulate TTL expiry by backdating the entry.
    cache._expire_negative_cache_for_test();
    EXPECT_EQ(1u, cache.negative_cache_size_for_test()); // entry exists but expired

    // DNS recovers.
    should_fail = false;
    EXPECT_TRUE(cache.get("fake-host.test", &ip).ok());
    EXPECT_GT(resolver_calls, calls_while_blocked) << "resolver must be called after TTL expiry";
    EXPECT_EQ(1u, cache.size_for_test()) << "host must be re-cached after successful re-resolve";
    EXPECT_EQ(0u, cache.negative_cache_size_for_test())
            << "negative cache entry must be removed on successful re-resolve";
}

// A concurrent successful resolution between _update() returning and the
// threshold check must prevent eviction: _erase_if_still_failing() re-reads
// the live failure_count under its own lock so a reset counter is not lost.
TEST_F(DNSCacheTest, concurrent_success_prevents_stale_eviction) {
    config::dns_cache_max_consecutive_failures = 1;
    config::dns_cache_negative_ttl_seconds = 3600;

    bool should_fail = false;
    DNSCache cache(make_resolver(&should_fail));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());
    ASSERT_EQ(1u, cache.size_for_test());

    // Simulate: _update() returned failures == threshold, but before _erase()
    // was called a concurrent success cleared failure_count.
    {
        std::unique_lock<std::shared_mutex> lock(cache.mutex);
        cache.failure_count["fake-host.test"] = {1, true}; // at threshold, authoritative
        cache.failure_count.erase("fake-host.test");       // concurrent success clears it
    }

    bool erased = cache._erase_if_still_failing("fake-host.test", 1u);
    EXPECT_FALSE(erased) << "must not erase when failure_count was concurrently reset to zero";
    EXPECT_EQ(1u, cache.size_for_test()) << "host must survive after concurrent success";
    EXPECT_EQ(0u, cache.negative_cache_size_for_test());
}

// When _resolve_hostname returns a stale cached IP after a concurrent eviction,
// _update must not reinsert it (which would undo the eviction and clear the
// negative-cache tombstone).
TEST_F(DNSCacheTest, stale_fallback_not_reinserted_after_concurrent_eviction) {
    config::dns_cache_max_consecutive_failures = 1000; // disable threshold-based eviction
    config::dns_cache_negative_ttl_seconds = 3600;

    DNSCache* cache_ptr = nullptr;
    auto racing_resolver = [&cache_ptr](const std::string& host, std::string&, bool,
                                        int* gai_err) -> Status {
        if (cache_ptr) {
            cache_ptr->_erase(host); // evict mid-DNS-call
        }
        if (gai_err != nullptr) {
            *gai_err = EAI_NONAME;
        }
        return Status::InternalError("mock DNS failure during concurrent eviction");
    };

    DNSCache cache(racing_resolver);
    cache_ptr = &cache;

    // Pre-populate so _resolve_hostname reads a non-empty cached_ip before the
    // DNS call, then the racing erase fires during the call.
    {
        std::unique_lock<std::shared_mutex> lock(cache.mutex);
        cache.cache["racing.test"] = "1.2.3.4";
    }
    ASSERT_EQ(1u, cache.size_for_test());

    // Drive one refresh cycle: resolver evicts mid-DNS, returns failure.
    // _resolve_hostname returns the stale "1.2.3.4".  Without the guard _update
    // would reinsert it; with the guard it must not.
    cache.refresh_for_test();

    EXPECT_EQ(0u, cache.size_for_test()) << "stale IP must not be reinserted after eviction";
    EXPECT_EQ(1u, cache.negative_cache_size_for_test()) << "host must be in negative cache";
    EXPECT_EQ(0u, cache.failure_count_for_test("racing.test"));
}

// After negative-cache TTL expiry and DNS still failing, get() must re-arm the
// negative cache so the retry rate stays bounded at one attempt per TTL period.
TEST_F(DNSCacheTest, negative_cache_rearms_on_continued_failure_after_ttl_expiry) {
    config::dns_cache_max_consecutive_failures = 1;
    config::dns_cache_negative_ttl_seconds = 3600;

    int resolver_calls = 0;
    bool should_fail = false;
    DNSCache cache(make_counting_resolver(&should_fail, &resolver_calls));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());

    // Evict.
    should_fail = true;
    cache.refresh_for_test();
    ASSERT_EQ(0u, cache.size_for_test());
    ASSERT_EQ(1u, cache.negative_cache_size_for_test());

    // Simulate TTL expiry by backdating the entry (entry still exists, time is past).
    // Use _expire (not _clear) so the entry is visible to get()'s TTL check,
    // which sets expired_negative=true and triggers the re-arm path on failure.
    cache._expire_negative_cache_for_test();
    ASSERT_EQ(1u, cache.negative_cache_size_for_test()); // entry exists but expired

    // DNS still failing: one retry is allowed, then negative cache re-arms.
    int calls_before = resolver_calls;
    EXPECT_FALSE(cache.get("fake-host.test", &ip).ok());
    EXPECT_GT(resolver_calls, calls_before) << "resolver called after TTL expiry";
    EXPECT_EQ(1u, cache.negative_cache_size_for_test())
            << "negative cache re-armed on continued failure";

    // Subsequent calls are now blocked without hitting the resolver.
    calls_before = resolver_calls;
    EXPECT_FALSE(cache.get("fake-host.test", &ip).ok());
    EXPECT_EQ(calls_before, resolver_calls) << "resolver NOT called while re-armed";
}

// ── Finding 1: mutable TTL is honored by existing tombstones ─────────────────

// Setting dns_cache_negative_ttl_seconds to 0 after eviction must immediately
// disable the negative cache for existing entries.
TEST_F(DNSCacheTest, negative_cache_disabled_when_ttl_set_to_zero) {
    config::dns_cache_max_consecutive_failures = 1;
    config::dns_cache_negative_ttl_seconds = 3600;

    int resolver_calls = 0;
    bool should_fail = false;
    DNSCache cache(make_counting_resolver(&should_fail, &resolver_calls));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());

    // Evict with TTL=3600.
    should_fail = true;
    cache.refresh_for_test();
    ASSERT_EQ(0u, cache.size_for_test());
    ASSERT_EQ(1u, cache.negative_cache_size_for_test());

    // Negative cache blocks.
    int calls_at_eviction = resolver_calls;
    EXPECT_FALSE(cache.get("fake-host.test", &ip).ok());
    EXPECT_EQ(calls_at_eviction, resolver_calls);

    // Now disable negative cache by setting TTL to 0 — simulates config change.
    config::dns_cache_negative_ttl_seconds = 0;
    should_fail = false;

    // get() must now retry immediately (TTL disabled) and succeed.
    EXPECT_TRUE(cache.get("fake-host.test", &ip).ok());
    EXPECT_GT(resolver_calls, calls_at_eviction) << "resolver must be called when TTL is disabled";
    EXPECT_EQ(1u, cache.size_for_test());
}

// Decreasing dns_cache_negative_ttl_seconds applies to existing tombstones:
// a tombstone created with TTL=3600 must honor the new smaller TTL.
TEST_F(DNSCacheTest, negative_cache_honors_decreased_ttl) {
    config::dns_cache_max_consecutive_failures = 1;
    config::dns_cache_negative_ttl_seconds = 3600;

    bool should_fail = false;
    DNSCache cache(make_resolver(&should_fail));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());

    // Evict — tombstone stores eviction_time = now.
    should_fail = true;
    cache.refresh_for_test();
    ASSERT_EQ(1u, cache.negative_cache_size_for_test());

    // Decrease TTL to 1 second and backdate the entry to make it look older.
    config::dns_cache_negative_ttl_seconds = 1;
    cache._expire_negative_cache_for_test(); // backdate far → past any 1s deadline

    should_fail = false;
    EXPECT_TRUE(cache.get("fake-host.test", &ip).ok())
            << "reduced TTL must be honored for existing tombstones";
}

// ── Finding 2: single-flight retry ──────────────────────────────────────────

// Only one thread claims the expired negative-cache retry; a second concurrent
// caller sees the re-armed entry and gets the cheap error.
TEST_F(DNSCacheTest, single_flight_retry_after_negative_ttl_expiry) {
    config::dns_cache_max_consecutive_failures = 1;
    config::dns_cache_negative_ttl_seconds = 3600;

    int resolver_calls = 0;
    bool should_fail = false;
    DNSCache cache(make_counting_resolver(&should_fail, &resolver_calls));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());

    // Evict and expire the tombstone.
    should_fail = true;
    cache.refresh_for_test();
    cache._expire_negative_cache_for_test();

    // First get() claims the retry — DNS still fails, re-arm happens.
    int calls_before = resolver_calls;
    EXPECT_FALSE(cache.get("fake-host.test", &ip).ok());
    EXPECT_EQ(calls_before + 1, resolver_calls) << "first caller invokes DNS";

    // Second get() must NOT invoke DNS — the entry was re-armed by the first call.
    calls_before = resolver_calls;
    EXPECT_FALSE(cache.get("fake-host.test", &ip).ok());
    EXPECT_EQ(calls_before, resolver_calls)
            << "second caller must be blocked by the re-armed entry";
}

// ── Finding 4: refresh cleanup doesn't break re-arm ─────────────────────────

// After _refresh_once() runs, the negative-cache tombstone must still be present
// so that get() can recognize the host as evicted and bound retry rate.
TEST_F(DNSCacheTest, refresh_does_not_erase_negative_cache_tombstone) {
    config::dns_cache_max_consecutive_failures = 1;
    config::dns_cache_negative_ttl_seconds = 3600;

    int resolver_calls = 0;
    bool should_fail = false;
    DNSCache cache(make_counting_resolver(&should_fail, &resolver_calls));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());

    // Evict and expire the tombstone to simulate passage of time.
    should_fail = true;
    cache.refresh_for_test();
    ASSERT_EQ(1u, cache.negative_cache_size_for_test());
    cache._expire_negative_cache_for_test();

    // Run another refresh cycle — must NOT erase the expired tombstone.  The
    // tombstone is what keeps get() from issuing a blocking getaddrinfo on every
    // call, so it must outlive refresh cycles regardless of age.
    cache.refresh_for_test();
    EXPECT_EQ(1u, cache.negative_cache_size_for_test())
            << "refresh must not erase negative-cache tombstones";

    // get() still recognizes this as an evicted host: one retry then re-arm.
    int calls_before = resolver_calls;
    EXPECT_FALSE(cache.get("fake-host.test", &ip).ok());
    EXPECT_EQ(calls_before + 1, resolver_calls) << "one retry after expiry";

    // Subsequent call is blocked.
    calls_before = resolver_calls;
    EXPECT_FALSE(cache.get("fake-host.test", &ip).ok());
    EXPECT_EQ(calls_before, resolver_calls) << "blocked after re-arm";
}

// ── only authoritative NXDOMAIN may evict ────────────────────────────────────

// A resolver outage reports transient errors (EAI_AGAIN) for every hostname at once.
// Those must never evict: the backends are still alive at their last known IP, and
// dropping the cache would turn a DNS incident into a cluster-wide RPC outage.
TEST_F(DNSCacheTest, transient_failure_never_evicts) {
    config::dns_cache_max_consecutive_failures = 3;
    config::dns_cache_negative_ttl_seconds = 3600;

    bool should_fail = false;
    DNSCache cache(make_resolver(&should_fail, "1.2.3.4", EAI_AGAIN));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());
    ASSERT_EQ(1u, cache.size_for_test());

    // The resolver is now unreachable, but the host itself is fine.
    should_fail = true;

    // Far more cycles than the threshold would need if the failures counted.
    for (int i = 0; i < 20; ++i) {
        cache.refresh_for_test();
        ASSERT_EQ(1u, cache.size_for_test())
                << "transient DNS failures must not evict (cycle " << i << ")";
    }
    EXPECT_EQ(0u, cache.negative_cache_size_for_test()) << "no tombstone for a transient failure";
    // The failure counter still advances so the throttled warning stays informative.
    EXPECT_EQ(20u, cache.failure_count_for_test("fake-host.test"));

    // Callers keep getting the last known good IP - this is the graceful degradation
    // that lets the cluster ride out a resolver outage.
    ip.clear();
    EXPECT_TRUE(cache.get("fake-host.test", &ip).ok());
    EXPECT_EQ("1.2.3.4", ip);
}

// Once the resolver comes back and authoritatively answers NXDOMAIN, the host is
// evicted on that cycle - the failures accumulated during the outage still count
// toward the threshold, only the eviction action was withheld.
TEST_F(DNSCacheTest, authoritative_failure_after_transient_evicts) {
    config::dns_cache_max_consecutive_failures = 3;
    config::dns_cache_negative_ttl_seconds = 3600;

    bool should_fail = false;
    int fail_gai_err = EAI_AGAIN;
    DNSCache cache([&should_fail, &fail_gai_err](const std::string&, std::string& out, bool,
                                                 int* gai_err) -> Status {
        if (should_fail) {
            *gai_err = fail_gai_err;
            return Status::InternalError("mock failure");
        }
        *gai_err = 0;
        out = "1.2.3.4";
        return Status::OK();
    });

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());

    // Resolver outage: well past the threshold, still no eviction.
    should_fail = true;
    for (int i = 0; i < 5; ++i) {
        cache.refresh_for_test();
    }
    ASSERT_EQ(1u, cache.size_for_test()) << "prerequisite: transient failures did not evict";

    // The resolver recovers and states the name does not exist.
    fail_gai_err = EAI_NONAME;
    cache.refresh_for_test();

    EXPECT_EQ(0u, cache.size_for_test())
            << "an authoritative NXDOMAIN past the threshold must evict";
    EXPECT_EQ(1u, cache.negative_cache_size_for_test());
}

// _erase_if_still_failing() re-reads the state under the erase lock, so a host whose
// most recent failure turned transient is spared even if the count is past threshold.
TEST_F(DNSCacheTest, erase_skipped_when_last_failure_is_transient) {
    config::dns_cache_max_consecutive_failures = 1;
    config::dns_cache_negative_ttl_seconds = 3600;

    bool should_fail = false;
    DNSCache cache(make_resolver(&should_fail));

    std::string ip;
    ASSERT_TRUE(cache.get("fake-host.test", &ip).ok());
    ASSERT_EQ(1u, cache.size_for_test());

    {
        std::unique_lock<std::shared_mutex> lock(cache.mutex);
        // Past the threshold, but the latest failure was a resolver problem.
        cache.failure_count["fake-host.test"] = {5, false};
    }

    EXPECT_FALSE(cache._erase_if_still_failing("fake-host.test", 1u))
            << "must not erase when the most recent failure was not authoritative";
    EXPECT_EQ(1u, cache.size_for_test());
    EXPECT_EQ(0u, cache.negative_cache_size_for_test());
}

// ── negative cache covers hosts that never resolved ──────────────────────────

// A hostname that has never resolved never enters `cache`, so it never reaches the
// eviction path. It must still be tombstoned, otherwise every get() pays a full
// blocking getaddrinfo - and many of those calls run on bthreads.
TEST_F(DNSCacheTest, first_miss_is_negative_cached) {
    config::dns_cache_negative_ttl_seconds = 3600;

    int resolver_calls = 0;
    bool should_fail = true;
    DNSCache cache(make_counting_resolver(&should_fail, &resolver_calls));

    std::string ip;
    EXPECT_FALSE(cache.get("never-resolved.test", &ip).ok());
    EXPECT_EQ(1, resolver_calls) << "the first call must actually try to resolve";
    EXPECT_EQ(0u, cache.size_for_test());
    EXPECT_EQ(1u, cache.negative_cache_size_for_test())
            << "a host that never resolved must still be tombstoned";

    // Every later call is served from the negative cache without touching DNS.
    for (int i = 0; i < 5; ++i) {
        EXPECT_FALSE(cache.get("never-resolved.test", &ip).ok());
    }
    EXPECT_EQ(1, resolver_calls) << "resolver must not be called again within the TTL";
    EXPECT_EQ(0u, cache.failure_count_for_test("never-resolved.test"))
            << "failure_count must stay empty for a host that was never cached";

    // Once the TTL lapses and DNS starts working, the host is picked up normally.
    cache._expire_negative_cache_for_test();
    should_fail = false;
    EXPECT_TRUE(cache.get("never-resolved.test", &ip).ok());
    EXPECT_EQ("1.2.3.4", ip);
    EXPECT_EQ(1u, cache.size_for_test());
    EXPECT_EQ(0u, cache.negative_cache_size_for_test());
}

// Setting the TTL to 0 keeps the legacy behavior: no tombstone for a first miss.
TEST_F(DNSCacheTest, first_miss_not_negative_cached_when_ttl_disabled) {
    config::dns_cache_negative_ttl_seconds = 0;

    int resolver_calls = 0;
    bool should_fail = true;
    DNSCache cache(make_counting_resolver(&should_fail, &resolver_calls));

    std::string ip;
    for (int i = 0; i < 3; ++i) {
        EXPECT_FALSE(cache.get("never-resolved.test", &ip).ok());
    }
    EXPECT_EQ(0u, cache.negative_cache_size_for_test());
    EXPECT_EQ(3, resolver_calls) << "every call resolves when the negative cache is disabled";
}

// ── shutdown responsiveness ──────────────────────────────────────────────────

// _refresh_once() must bail out as soon as the stop flag is set. Each _update() inside
// it blocks in getaddrinfo, so a cycle over a cluster with dead DNS can run for minutes
// and would otherwise hold up ~DNSCache()'s join() for exactly that long.
TEST_F(DNSCacheTest, refresh_stops_early_when_stopping) {
    config::dns_cache_max_consecutive_failures = 0; // isolate: no eviction in play

    int resolver_calls = 0;
    bool should_fail = false;
    DNSCache cache(make_counting_resolver(&should_fail, &resolver_calls));

    std::string ip;
    ASSERT_TRUE(cache.get("host-a.test", &ip).ok());
    ASSERT_EQ(1u, cache.size_for_test());

    int calls_before = resolver_calls;
    cache.stop_refresh = true;
    cache.refresh_for_test();

    EXPECT_EQ(calls_before, resolver_calls)
            << "a refresh cycle must not start resolving once the stop flag is set";
    EXPECT_EQ(1u, cache.size_for_test()) << "bailing out must leave the cache untouched";
}

} // end of namespace doris
