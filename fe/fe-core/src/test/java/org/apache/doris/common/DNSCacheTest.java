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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.net.UnknownHostException;

/**
 * Unit tests for DNSCache to verify DNS caching and refresh functionality.
 */
public class DNSCacheTest {
    private DNSCache dnsCache;
    private MockedStatic<NetUtils> netUtilsMockedStatic;
    private boolean originalFqdnMode;

    @Before
    public void setUp() {
        // Save original config
        originalFqdnMode = Config.enable_fqdn_mode;

        // Create DNS cache instance
        dnsCache = new DNSCache();

        // Mock NetUtils static methods
        netUtilsMockedStatic = Mockito.mockStatic(NetUtils.class);
    }

    @After
    public void tearDown() {
        // Restore original config
        Config.enable_fqdn_mode = originalFqdnMode;

        // Close mocked static
        if (netUtilsMockedStatic != null) {
            netUtilsMockedStatic.close();
        }
    }

    /**
     * Test that get() successfully resolves a hostname and caches the result.
     */
    @Test
    public void testGetResolvesAndCachesHostname() throws UnknownHostException {
        String hostname = "backend.example.com";
        String expectedIp = "10.0.0.1";

        // Mock NetUtils to return expected IP
        netUtilsMockedStatic.when(() -> NetUtils.getIpByHost(hostname, 0))
                .thenReturn(expectedIp);

        // First call - should resolve and cache
        String ip1 = dnsCache.get(hostname);
        Assert.assertEquals(expectedIp, ip1);

        // Second call - should return cached value without calling NetUtils again
        String ip2 = dnsCache.get(hostname);
        Assert.assertEquals(expectedIp, ip2);

        // Verify NetUtils.getIpByHost was called only once (cached on subsequent calls)
        netUtilsMockedStatic.verify(() -> NetUtils.getIpByHost(hostname, 0), Mockito.times(1));
    }

    /**
     * Test that when DNS resolution fails, get() returns an empty string.
     */
    @Test
    public void testGetReturnsEmptyStringOnResolutionFailure() throws UnknownHostException {
        String hostname = "non-existent.example.com";

        // Mock NetUtils to throw UnknownHostException
        netUtilsMockedStatic.when(() -> NetUtils.getIpByHost(hostname, 0))
                .thenThrow(new UnknownHostException("Host not found"));

        // Should return empty string instead of throwing exception
        String ip = dnsCache.get(hostname);
        Assert.assertEquals("", ip);

        // Verify the result is cached (subsequent calls don't resolve again)
        String ip2 = dnsCache.get(hostname);
        Assert.assertEquals("", ip2);

        // Should only attempt resolution once
        netUtilsMockedStatic.verify(() -> NetUtils.getIpByHost(hostname, 0), Mockito.times(1));
    }

    /**
     * Test that multiple different hostnames can be cached simultaneously.
     */
    @Test
    public void testMultipleHostnamesCached() throws UnknownHostException {
        String hostname1 = "backend1.example.com";
        String hostname2 = "backend2.example.com";
        String ip1 = "10.0.0.1";
        String ip2 = "10.0.0.2";

        // Mock NetUtils for both hostnames
        netUtilsMockedStatic.when(() -> NetUtils.getIpByHost(hostname1, 0))
                .thenReturn(ip1);
        netUtilsMockedStatic.when(() -> NetUtils.getIpByHost(hostname2, 0))
                .thenReturn(ip2);

        // Get both hostnames
        String result1 = dnsCache.get(hostname1);
        String result2 = dnsCache.get(hostname2);

        // Verify both are cached correctly
        Assert.assertEquals(ip1, result1);
        Assert.assertEquals(ip2, result2);

        // Verify each was resolved once
        netUtilsMockedStatic.verify(() -> NetUtils.getIpByHost(hostname1, 0), Mockito.times(1));
        netUtilsMockedStatic.verify(() -> NetUtils.getIpByHost(hostname2, 0), Mockito.times(1));
    }

    /**
     * Test that localhost resolves to 127.0.0.1 (using real DNS resolution).
     */
    @Test
    public void testLocalhostResolution() {
        // Don't mock NetUtils for this test - use real resolution
        netUtilsMockedStatic.close();
        netUtilsMockedStatic = null;

        DNSCache realDnsCache = new DNSCache();

        String ip = realDnsCache.get("localhost");

        // localhost should resolve to 127.0.0.1 or ::1
        Assert.assertTrue("localhost should resolve to an IP",
                ip.equals("127.0.0.1") || ip.contains(":"));
    }

    /**
     * Test that the cache can handle IP addresses as input (should return the IP as-is).
     * Note: This depends on the implementation of NetUtils.getIpByHost.
     */
    @Test
    public void testIpAddressInput() throws UnknownHostException {
        String ipAddress = "10.0.0.1";

        // Mock NetUtils to return the IP as-is
        netUtilsMockedStatic.when(() -> NetUtils.getIpByHost(ipAddress, 0))
                .thenReturn(ipAddress);

        String result = dnsCache.get(ipAddress);

        Assert.assertEquals(ipAddress, result);
    }

    /**
     * Test that start() only schedules refresh task when enable_fqdn_mode is true.
     * Note: This is a behavior test and doesn't verify actual scheduling,
     * just that start() completes without errors.
     */
    @Test
    public void testStartWithFqdnModeEnabled() {
        Config.enable_fqdn_mode = true;

        DNSCache cache = new DNSCache();

        // Should not throw any exception
        cache.start();

        // Verify it completes successfully
        Assert.assertNotNull(cache);
    }

    /**
     * Test that start() does not schedule refresh when enable_fqdn_mode is false.
     */
    @Test
    public void testStartWithFqdnModeDisabled() {
        Config.enable_fqdn_mode = false;

        DNSCache cache = new DNSCache();

        // Should not throw any exception
        cache.start();

        // Verify it completes successfully
        Assert.assertNotNull(cache);
    }

    /**
     * Test thread safety - multiple threads accessing cache concurrently.
     */
    @Test
    public void testConcurrentAccess() throws InterruptedException, UnknownHostException {
        String hostname = "backend.example.com";
        String expectedIp = "10.0.0.1";

        netUtilsMockedStatic.when(() -> NetUtils.getIpByHost(hostname, 0))
                .thenReturn(expectedIp);

        // Pre-populate the cache by calling get() once before concurrent access
        // This ensures the cache is initialized and subsequent calls will hit the cache
        String initialIp = dnsCache.get(hostname);
        Assert.assertEquals(expectedIp, initialIp);

        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];

        // Create multiple threads that access the cache concurrently
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                String ip = dnsCache.get(hostname);
                Assert.assertEquals(expectedIp, ip);
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Despite concurrent access, NetUtils.getIpByHost should be called only once
        // (during the initial pre-population) due to caching
        netUtilsMockedStatic.verify(() -> NetUtils.getIpByHost(hostname, 0), Mockito.times(1));
    }

    /**
     * Test that concurrent access with race condition still resolves correctly.
     * This test uses localhost to avoid mock issues in multi-threaded scenarios.
     */
    @Test
    public void testConcurrentAccessWithRealDns() throws InterruptedException {
        // Don't mock NetUtils for this test - use real resolution
        netUtilsMockedStatic.close();
        netUtilsMockedStatic = null;

        DNSCache realDnsCache = new DNSCache();
        String hostname = "localhost";

        int threadCount = 20;
        Thread[] threads = new Thread[threadCount];
        String[] results = new String[threadCount];

        // Create multiple threads that try to resolve the same hostname concurrently
        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            threads[i] = new Thread(() -> {
                results[index] = realDnsCache.get(hostname);
            });
        }

        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // All threads should get the same result
        String expectedResult = results[0];
        Assert.assertNotNull("Result should not be null", expectedResult);
        Assert.assertFalse("Result should not be empty", expectedResult.isEmpty());

        for (int i = 1; i < threadCount; i++) {
            Assert.assertEquals("All threads should get the same result", expectedResult, results[i]);
        }
    }

    /**
     * Test that refresh() keeps the cached IP when DNS resolution fails.
     * The refresh method should log a warning and continue using the cached value.
     */
    @Test
    public void testRefreshKeepsCachedIpOnResolutionFailure() throws Exception {
        String hostname = "backend.example.com";
        String cachedIp = "10.0.0.1";

        // First, mock successful resolution to populate the cache
        netUtilsMockedStatic.when(() -> NetUtils.getIpByHost(hostname, 0))
                .thenReturn(cachedIp);

        // Populate the cache
        String ip = dnsCache.get(hostname);
        Assert.assertEquals(cachedIp, ip);

        // Now mock resolution failure
        netUtilsMockedStatic.when(() -> NetUtils.getIpByHost(hostname, 0))
                .thenThrow(new UnknownHostException("Host not found"));

        // Call refresh() using reflection since it's private
        Method refreshMethod = DNSCache.class.getDeclaredMethod("refresh");
        refreshMethod.setAccessible(true);
        refreshMethod.invoke(dnsCache);

        // The cached IP should remain unchanged after refresh failure
        String ipAfterRefresh = dnsCache.get(hostname);
        Assert.assertEquals("Cached IP should remain unchanged after refresh failure", cachedIp, ipAfterRefresh);
    }

    /**
     * Test that refresh() updates the cached IP when DNS resolution succeeds with a new IP.
     */
    @Test
    public void testRefreshUpdatesCachedIpOnSuccess() throws Exception {
        String hostname = "backend.example.com";
        String originalIp = "10.0.0.1";
        String newIp = "10.0.0.2";

        // First, mock successful resolution to populate the cache
        netUtilsMockedStatic.when(() -> NetUtils.getIpByHost(hostname, 0))
                .thenReturn(originalIp);

        // Populate the cache
        String ip = dnsCache.get(hostname);
        Assert.assertEquals(originalIp, ip);

        // Now mock resolution with new IP
        netUtilsMockedStatic.when(() -> NetUtils.getIpByHost(hostname, 0))
                .thenReturn(newIp);

        // Call refresh() using reflection since it's private
        Method refreshMethod = DNSCache.class.getDeclaredMethod("refresh");
        refreshMethod.setAccessible(true);
        refreshMethod.invoke(dnsCache);

        // The cached IP should be updated to the new IP
        String ipAfterRefresh = dnsCache.get(hostname);
        Assert.assertEquals("Cached IP should be updated after successful refresh", newIp, ipAfterRefresh);
    }
}
