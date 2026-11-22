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

package org.apache.doris.common;

import org.apache.doris.common.util.NetUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Forced re-resolution of a hostname after a backend restart or an RPC failure.
 */
public class DNSCacheInvalidationTest {
    private static final String HOST = "backend.example.com";

    private DNSCache dnsCache;
    private MockedStatic<NetUtils> resolver;

    @BeforeEach
    public void setUp() {
        dnsCache = new DNSCache();
        resolver = Mockito.mockStatic(NetUtils.class);
    }

    @AfterEach
    public void tearDown() {
        resolver.close();
    }

    @Test
    public void testInvalidateForcesNewLookup() {
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1", "10.0.0.2");
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));

        dnsCache.invalidate(HOST);
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
        resolver.verify(() -> NetUtils.getIpByHost(HOST, 0), Mockito.times(2));
        // The forced lookup bypasses the JVM's InetAddress cache; the first lookup has nothing to bypass.
        resolver.verify(() -> NetUtils.evictJvmDnsCache(HOST), Mockito.times(1));
    }

    @Test
    public void testForcedLookupKeepsLastIpWhenDnsFails() {
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0))
                .thenReturn("10.0.0.1")
                .thenThrow(new UnknownHostException("temporary resolver outage"))
                .thenReturn("")
                .thenReturn("10.0.0.2");
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));
        dnsCache.invalidate(HOST);
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));
        dnsCache.invalidate(HOST);
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));
        // A failed forced lookup is not retried on every get(); only a new invalidation retries.
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));
        dnsCache.invalidate(HOST);
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
        resolver.verify(() -> NetUtils.getIpByHost(HOST, 0), Mockito.times(4));
    }

    @Test
    public void testStaleRefreshDoesNotOverwriteForcedLookup() throws Exception {
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1");
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));

        // The periodic refresh resolves the old address, then stalls before publishing it.
        CountDownLatch refreshResolving = new CountDownLatch(1);
        CountDownLatch finishRefresh = new CountDownLatch(1);
        FutureTask<Void> refresh = onThread("dns-refresh", invocation -> {
            refreshResolving.countDown();
            Assertions.assertTrue(finishRefresh.await(10, TimeUnit.SECONDS));
            return "10.0.0.1";
        }, () -> {
            Method method = DNSCache.class.getDeclaredMethod("refresh");
            method.setAccessible(true);
            method.invoke(dnsCache);
            return null;
        });
        try {
            Assertions.assertTrue(refreshResolving.await(10, TimeUnit.SECONDS));
            // The backend restarts at a new address while the refresh is in flight. The refresh
            // resolves outside the map's lock, so the forced lookup does not wait for it.
            dnsCache.invalidate(HOST);
            FutureTask<String> forced = onThread("dns-forced", invocation -> "10.0.0.2", () -> dnsCache.get(HOST));
            Assertions.assertEquals("10.0.0.2", forced.get(10, TimeUnit.SECONDS));
        } finally {
            finishRefresh.countDown();
        }
        refresh.get(10, TimeUnit.SECONDS);
        // The stalled refresh must not publish its older answer over the forced lookup's.
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
    }

    @Test
    public void testRefreshStartedDuringForcedLookupDoesNotOverwriteIt() throws Exception {
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1");
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));

        // A forced lookup is still resolving the backend's new address.
        CountDownLatch forcedResolving = new CountDownLatch(1);
        CountDownLatch finishForced = new CountDownLatch(1);
        dnsCache.invalidate(HOST);
        FutureTask<String> forced = onThread("dns-forced", invocation -> {
            forcedResolving.countDown();
            Assertions.assertTrue(finishForced.await(10, TimeUnit.SECONDS));
            return "10.0.0.2";
        }, () -> dnsCache.get(HOST));
        FutureTask<Void> refresh;
        try {
            Assertions.assertTrue(forcedResolving.await(10, TimeUnit.SECONDS));
            // The periodic refresh starts meanwhile. Its own lookup fails, so it falls back to the
            // cached address, which the forced lookup has not replaced yet.
            CountDownLatch refreshResolved = new CountDownLatch(1);
            refresh = onThread("dns-refresh", invocation -> {
                refreshResolved.countDown();
                throw new UnknownHostException("temporary resolver outage");
            }, () -> {
                invokeRefresh(dnsCache);
                return null;
            });
            Assertions.assertTrue(refreshResolved.await(10, TimeUnit.SECONDS));
        } finally {
            finishForced.countDown();
        }
        Assertions.assertEquals("10.0.0.2", forced.get(10, TimeUnit.SECONDS));
        refresh.get(10, TimeUnit.SECONDS);
        // The refresh started before the forced lookup published, so its answer is the older one.
        Assertions.assertEquals("10.0.0.2", cachedIp(dnsCache));
    }

    @Test
    public void testStaleRefreshDoesNotOverwriteSameAddressLookup() throws Exception {
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1");
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));

        // The refresh gets an answer that is already out of date, then stalls before publishing it.
        CountDownLatch refreshResolving = new CountDownLatch(1);
        CountDownLatch finishRefresh = new CountDownLatch(1);
        FutureTask<Void> refresh = onThread("dns-refresh", invocation -> {
            refreshResolving.countDown();
            Assertions.assertTrue(finishRefresh.await(10, TimeUnit.SECONDS));
            return "10.0.0.9";
        }, () -> {
            invokeRefresh(dnsCache);
            return null;
        });
        try {
            Assertions.assertTrue(refreshResolving.await(10, TimeUnit.SECONDS));
            // A newer forced lookup confirms the address the refresh started from.
            dnsCache.invalidate(HOST);
            FutureTask<String> forced = onThread("dns-forced", invocation -> "10.0.0.1", () -> dnsCache.get(HOST));
            Assertions.assertEquals("10.0.0.1", forced.get(10, TimeUnit.SECONDS));
        } finally {
            finishRefresh.countDown();
        }
        refresh.get(10, TimeUnit.SECONDS);
        Assertions.assertEquals("10.0.0.1", cachedIp(dnsCache));
    }

    @Test
    public void testRefreshKeepsPendingInvalidation() throws Exception {
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0))
                .thenReturn("10.0.0.1", "10.0.0.2")
                .thenThrow(new UnknownHostException("temporary resolver outage"));
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));
        dnsCache.invalidate(HOST);
        invokeRefresh(dnsCache);
        // The refresh publishes its newer answer, but does not consume the invalidation.
        Assertions.assertEquals("10.0.0.2", cachedIp(dnsCache));
        // get() still forces a lookup; when that lookup fails, the refresh's answer is the fallback.
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
        resolver.verify(() -> NetUtils.getIpByHost(HOST, 0), Mockito.times(3));
        resolver.verify(() -> NetUtils.evictJvmDnsCache(HOST), Mockito.times(1));
    }

    @Test
    public void testInvalidationBeforeFirstLookupEvictsJvmCache() {
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.2");
        // The heartbeat saw the backend restart before this FE ever resolved the host; the JVM may
        // still hold an older answer for it.
        dnsCache.invalidate(HOST);
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
        resolver.verify(() -> NetUtils.evictJvmDnsCache(HOST), Mockito.times(1));
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
        resolver.verify(() -> NetUtils.getIpByHost(HOST, 0), Mockito.times(1));
    }

    @Test
    public void testInvalidationDuringFirstLookupForcesNextLookup() throws Exception {
        CountDownLatch resolving = new CountDownLatch(1);
        CountDownLatch finishLookup = new CountDownLatch(1);
        FutureTask<String> firstLookup = onThread("dns-first-lookup", invocation -> {
            resolving.countDown();
            Assertions.assertTrue(finishLookup.await(10, TimeUnit.SECONDS));
            return "10.0.0.1";
        }, () -> dnsCache.get(HOST));
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.2");
        try {
            Assertions.assertTrue(resolving.await(10, TimeUnit.SECONDS));
            // No entry is published yet when the restart is observed.
            dnsCache.invalidate(HOST);
        } finally {
            finishLookup.countDown();
        }
        Assertions.assertEquals("10.0.0.1", firstLookup.get(10, TimeUnit.SECONDS));
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
        resolver.verify(() -> NetUtils.getIpByHost(HOST, 0), Mockito.times(1));
    }

    @Test
    public void testWarmReaderWaitsForForcedLookup() throws Exception {
        for (boolean failLookup : new boolean[] {false, true}) {
            DNSCache cache = new DNSCache();
            resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1");
            Assertions.assertEquals("10.0.0.1", cache.get(HOST));
            cache.invalidate(HOST);

            CountDownLatch resolving = new CountDownLatch(1);
            CountDownLatch finishLookup = new CountDownLatch(1);
            FutureTask<String> forced = onThread("dns-forced", invocation -> {
                resolving.countDown();
                Assertions.assertTrue(finishLookup.await(10, TimeUnit.SECONDS));
                if (failLookup) {
                    throw new UnknownHostException("temporary resolver failure");
                }
                return "10.0.0.2";
            }, () -> cache.get(HOST));
            FutureTask<String> warm = null;
            try {
                Assertions.assertTrue(resolving.await(10, TimeUnit.SECONDS));
                // This reader must not return the address the forced lookup is replacing,
                // and must not start a lookup of its own.
                warm = onThread("dns-warm", invocation -> {
                    throw new AssertionError("warm reader must not resolve");
                }, () -> cache.get(HOST));
                assertStillRunning(warm);
            } finally {
                finishLookup.countDown();
            }
            String expectedIp = failLookup ? "10.0.0.1" : "10.0.0.2";
            Assertions.assertEquals(expectedIp, forced.get(10, TimeUnit.SECONDS));
            Assertions.assertEquals(expectedIp, warm.get(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testRpcFailureInvalidationIsScopedAndThrottled() {
        resolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenReturn("10.0.0.1", "10.0.0.2");
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));

        // A failure from a channel built for an address the cache no longer holds forces nothing.
        Assertions.assertFalse(dnsCache.invalidateAfterRpcFailure(HOST, "10.0.0.9"));
        Assertions.assertEquals("10.0.0.1", dnsCache.get(HOST));

        Assertions.assertTrue(dnsCache.invalidateAfterRpcFailure(HOST, "10.0.0.1"));
        // A burst of failures from the same address forces a single lookup.
        Assertions.assertFalse(dnsCache.invalidateAfterRpcFailure(HOST, "10.0.0.1"));
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
        Assertions.assertFalse(dnsCache.invalidateAfterRpcFailure(HOST, "10.0.0.2"));
        Assertions.assertEquals("10.0.0.2", dnsCache.get(HOST));
        resolver.verify(() -> NetUtils.getIpByHost(HOST, 0), Mockito.times(2));
    }

    @Test
    public void testEvictJvmDnsCacheRemovesEntry() throws Exception {
        resolver.close();
        try {
            InetAddress.getByName("localhost");
            Field field = InetAddress.class.getDeclaredField("cache");
            field.setAccessible(true);
            Map<?, ?> jvmCache = (Map<?, ?>) field.get(null);
            Assertions.assertTrue(jvmCache.containsKey("localhost"), "the lookup was not cached by the JVM");

            Assertions.assertTrue(NetUtils.evictJvmDnsCache("localhost"));
            Assertions.assertFalse(jvmCache.containsKey("localhost"));
        } finally {
            resolver = Mockito.mockStatic(NetUtils.class);
        }
    }

    private static void invokeRefresh(DNSCache cache) throws Exception {
        Method method = DNSCache.class.getDeclaredMethod("refresh");
        method.setAccessible(true);
        method.invoke(cache);
    }

    // The cached address, read without the side effects of get().
    private static String cachedIp(DNSCache cache) throws Exception {
        Field field = DNSCache.class.getDeclaredField("cache");
        field.setAccessible(true);
        return ((Map<?, ?>) field.get(cache)).get(HOST).toString();
    }

    private static void assertStillRunning(FutureTask<?> task) throws Exception {
        try {
            task.get(300, TimeUnit.MILLISECONDS);
            Assertions.fail("expected the lookup to wait");
        } catch (TimeoutException expected) {
            // The task is blocked as expected.
        }
    }

    /**
     * Runs body on a new thread whose DNS answers come from lookup. Static mocks are
     * thread-local, so each thread installs its own resolver.
     */
    private static <T> FutureTask<T> onThread(String name, Answer<String> lookup, Callable<T> body) {
        FutureTask<T> task = new FutureTask<>(() -> {
            try (MockedStatic<NetUtils> threadResolver = Mockito.mockStatic(NetUtils.class)) {
                threadResolver.when(() -> NetUtils.getIpByHost(HOST, 0)).thenAnswer(lookup);
                return body.call();
            }
        });
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        thread.start();
        return task;
    }
}
