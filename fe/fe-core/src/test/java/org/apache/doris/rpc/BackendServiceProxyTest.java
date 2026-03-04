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

package org.apache.doris.rpc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DNSCache;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TNetworkAddress;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.UnknownHostException;
import java.util.Map;

/**
 * Unit tests for BackendServiceProxy to verify DNS cache integration
 * and IP address change handling.
 */
public class BackendServiceProxyTest {
    private BackendServiceProxy proxy;
    private DNSCache mockDnsCache;
    private Env mockEnv;
    private MockedStatic<Env> envMockedStatic;
    private boolean originalFqdnMode;
    private int originalProxyNum;

    @Before
    public void setUp() {
        // Save original config values
        originalFqdnMode = Config.enable_fqdn_mode;
        originalProxyNum = Config.backend_proxy_num;

        // Set test config
        Config.enable_fqdn_mode = true;
        Config.backend_proxy_num = 1;

        // Create mock DNS cache
        mockDnsCache = Mockito.mock(DNSCache.class);

        // Create mock Env
        mockEnv = Mockito.mock(Env.class);
        Mockito.when(mockEnv.getDnsCache()).thenReturn(mockDnsCache);

        // Mock static Env.getCurrentEnv()
        envMockedStatic = Mockito.mockStatic(Env.class);
        envMockedStatic.when(Env::getCurrentEnv).thenReturn(mockEnv);

        // Create proxy instance
        proxy = new BackendServiceProxy();
    }

    @After
    public void tearDown() {
        // Restore original config
        Config.enable_fqdn_mode = originalFqdnMode;
        Config.backend_proxy_num = originalProxyNum;

        // Close mocked static
        if (envMockedStatic != null) {
            envMockedStatic.close();
        }
    }

    /**
     * Test that when DNS cache returns a valid IP, the client is created successfully
     * and uses the resolved IP address.
     */
    @Test
    public void testGetProxyWithValidIp() throws Exception {
        String hostname = "backend-host.example.com";
        String resolvedIp = "10.0.0.1";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Mock DNS cache to return valid IP
        Mockito.when(mockDnsCache.get(hostname)).thenReturn(resolvedIp);

        // Get proxy - should create client successfully
        BackendServiceClient client = Deencapsulation.invoke(proxy, "getProxy", address);

        // Verify client was created
        Assert.assertNotNull(client);

        // Verify DNS cache was called
        Mockito.verify(mockDnsCache, Mockito.times(1)).get(hostname);

        // Verify the client is stored in serviceMap
        Map<TNetworkAddress, Object> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        Assert.assertEquals(1, serviceMap.size());
        Assert.assertTrue(serviceMap.containsKey(address));
    }

    /**
     * Test that when DNS cache returns empty string (DNS resolution failed)
     * and FQDN mode is enabled, UnknownHostException is thrown.
     */
    @Test
    public void testGetProxyWithDnsResolutionFailure() {
        String hostname = "non-existent-host.example.com";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Mock DNS cache to return empty string (resolution failed)
        Mockito.when(mockDnsCache.get(hostname)).thenReturn("");

        // Should throw UnknownHostException
        try {
            Deencapsulation.invoke(proxy, "getProxy", address);
            Assert.fail("Expected UnknownHostException to be thrown");
        } catch (Exception e) {
            Assert.assertTrue("Expected UnknownHostException", e instanceof UnknownHostException);
            Assert.assertTrue("Exception message should contain hostname",
                    e.getMessage().contains(hostname));
            Assert.assertTrue("Exception message should mention DNS cache",
                    e.getMessage().contains("DNS cache returned empty IP address"));
        }

        // Verify DNS cache was called
        Mockito.verify(mockDnsCache, Mockito.times(1)).get(hostname);
    }

    /**
     * Test that when DNS cache returns empty string but FQDN mode is disabled,
     * client creation proceeds with hostname (fallback behavior).
     */
    @Test
    public void testGetProxyWithDnsFailureAndFqdnModeDisabled() throws Exception {
        Config.enable_fqdn_mode = false;

        String hostname = "backend-host.example.com";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Mock DNS cache to return empty string
        Mockito.when(mockDnsCache.get(hostname)).thenReturn("");

        // Should create client with hostname as fallback
        BackendServiceClient client = Deencapsulation.invoke(proxy, "getProxy", address);

        // Verify client was created
        Assert.assertNotNull(client);

        // Verify DNS cache was called
        Mockito.verify(mockDnsCache, Mockito.times(1)).get(hostname);
    }

    /**
     * Test that when IP address changes, the old client is shutdown and
     * a new client is created with the new IP.
     */
    @Test
    public void testGetProxyWithIpChange() throws Exception {
        String hostname = "backend-host.example.com";
        String oldIp = "10.0.0.1";
        String newIp = "10.0.0.2";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // First call - create client with old IP
        Mockito.when(mockDnsCache.get(hostname)).thenReturn(oldIp);
        BackendServiceClient client1 = Deencapsulation.invoke(proxy, "getProxy", address);
        Assert.assertNotNull(client1);

        // Verify serviceMap contains the client
        Map<TNetworkAddress, Object> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        Assert.assertEquals(1, serviceMap.size());

        // Second call - IP changed
        Mockito.when(mockDnsCache.get(hostname)).thenReturn(newIp);
        BackendServiceClient client2 = Deencapsulation.invoke(proxy, "getProxy", address);

        // Verify a new client was created
        Assert.assertNotNull(client2);

        // Verify DNS cache was called twice
        Mockito.verify(mockDnsCache, Mockito.times(2)).get(hostname);

        // Verify serviceMap still has one entry but with new client
        Assert.assertEquals(1, serviceMap.size());

        // Note: We cannot easily verify client1.shutdown() was called because
        // the client is created as a real object, not a mock. In a real test
        // environment, you would verify the channel state or use dependency injection.
    }

    /**
     * Test that when the same IP is returned, the existing client is reused
     * (no new client is created).
     */
    @Test
    public void testGetProxyReusesClientWithSameIp() throws Exception {
        String hostname = "backend-host.example.com";
        String resolvedIp = "10.0.0.1";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Mock DNS cache to return same IP
        Mockito.when(mockDnsCache.get(hostname)).thenReturn(resolvedIp);

        // First call
        BackendServiceClient client1 = Deencapsulation.invoke(proxy, "getProxy", address);
        Assert.assertNotNull(client1);

        // Second call with same IP
        BackendServiceClient client2 = Deencapsulation.invoke(proxy, "getProxy", address);

        // Should reuse the same client
        Assert.assertSame("Client should be reused when IP hasn't changed", client1, client2);

        // DNS cache should be called twice (once per getProxy call)
        Mockito.verify(mockDnsCache, Mockito.times(2)).get(hostname);
    }

    /**
     * Test that removeProxy properly removes and shuts down the client.
     */
    @Test
    public void testRemoveProxy() throws Exception {
        String hostname = "backend-host.example.com";
        String resolvedIp = "10.0.0.1";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Mock DNS cache
        Mockito.when(mockDnsCache.get(hostname)).thenReturn(resolvedIp);

        // Create client
        BackendServiceClient client = Deencapsulation.invoke(proxy, "getProxy", address);
        Assert.assertNotNull(client);

        // Verify serviceMap contains the client
        Map<TNetworkAddress, Object> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        Assert.assertEquals(1, serviceMap.size());

        // Remove proxy
        proxy.removeProxy(address);

        // Verify serviceMap is now empty
        Assert.assertEquals(0, serviceMap.size());

        // Note: In a real test, you would verify client.shutdown() was called
        // This would require mocking the client creation process
    }

    /**
     * Test that multiple different backends can coexist in the serviceMap.
     */
    @Test
    public void testMultipleBackends() throws Exception {
        String hostname1 = "backend1.example.com";
        String hostname2 = "backend2.example.com";
        String ip1 = "10.0.0.1";
        String ip2 = "10.0.0.2";
        int port = 9060;

        TNetworkAddress address1 = new TNetworkAddress(hostname1, port);
        TNetworkAddress address2 = new TNetworkAddress(hostname2, port);

        // Mock DNS cache for both hostnames
        Mockito.when(mockDnsCache.get(hostname1)).thenReturn(ip1);
        Mockito.when(mockDnsCache.get(hostname2)).thenReturn(ip2);

        // Create clients for both backends
        BackendServiceClient client1 = Deencapsulation.invoke(proxy, "getProxy", address1);
        BackendServiceClient client2 = Deencapsulation.invoke(proxy, "getProxy", address2);

        Assert.assertNotNull(client1);
        Assert.assertNotNull(client2);

        // Verify serviceMap contains both clients
        Map<TNetworkAddress, Object> serviceMap = Deencapsulation.getField(proxy, "serviceMap");
        Assert.assertEquals(2, serviceMap.size());
        Assert.assertTrue(serviceMap.containsKey(address1));
        Assert.assertTrue(serviceMap.containsKey(address2));
    }
}
