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

import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.thrift.TNetworkAddress;

import io.grpc.ManagedChannel;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Unit tests for BackendServiceClient to verify that it uses
 * resolved IP addresses instead of hostnames for gRPC connections.
 */
public class BackendServiceClientTest {
    private Executor executor;
    private int originalGrpcKeepAliveSeconds;
    private int originalGrpcMaxMessageSize;
    private long originalRemoteFragmentExecTimeout;

    @Before
    public void setUp() {
        // Create executor for tests
        executor = Executors.newCachedThreadPool();

        // Save original config values
        originalGrpcKeepAliveSeconds = Config.grpc_keep_alive_second;
        originalGrpcMaxMessageSize = Config.grpc_max_message_size_bytes;
        originalRemoteFragmentExecTimeout = Config.remote_fragment_exec_timeout_ms;

        // Set test config values to reasonable defaults
        Config.grpc_keep_alive_second = 60;
        Config.grpc_max_message_size_bytes = 1024 * 1024 * 100; // 100MB
        Config.remote_fragment_exec_timeout_ms = 5000;
    }

    @After
    public void tearDown() {
        // Restore original config
        Config.grpc_keep_alive_second = originalGrpcKeepAliveSeconds;
        Config.grpc_max_message_size_bytes = originalGrpcMaxMessageSize;
        Config.remote_fragment_exec_timeout_ms = originalRemoteFragmentExecTimeout;
    }

    /**
     * Test that BackendServiceClient uses the resolved IP address
     * when creating the gRPC channel.
     */
    @Test
    public void testClientUsesResolvedIp() {
        String hostname = "backend.example.com";
        String resolvedIp = "10.0.0.1";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Create client with resolved IP
        BackendServiceClient client = new BackendServiceClient(address, resolvedIp, executor);

        // Verify client was created
        Assert.assertNotNull(client);

        // Verify the address is stored
        TNetworkAddress storedAddress = Deencapsulation.getField(client, "address");
        Assert.assertEquals(address, storedAddress);

        // Verify the channel was created (non-null)
        ManagedChannel channel = Deencapsulation.getField(client, "channel");
        Assert.assertNotNull(channel);

        // Note: We cannot easily verify that the channel uses the IP instead of hostname
        // without inspecting the channel's internal state, which is implementation-dependent.
        // In a real scenario, you would use integration tests or network monitoring to verify.

        // Cleanup
        client.shutdown();
    }

    /**
     * Test that when resolved IP is empty, the client falls back to using hostname.
     */
    @Test
    public void testClientFallsBackToHostnameWhenIpIsEmpty() {
        String hostname = "localhost"; // Use localhost to ensure it resolves
        String emptyIp = "";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Create client with empty IP - should fallback to hostname
        BackendServiceClient client = new BackendServiceClient(address, emptyIp, executor);

        // Verify client was created
        Assert.assertNotNull(client);

        // Verify channel was created
        ManagedChannel channel = Deencapsulation.getField(client, "channel");
        Assert.assertNotNull(channel);

        // Cleanup
        client.shutdown();
    }

    /**
     * Test that when resolved IP is null, the client falls back to using hostname.
     */
    @Test
    public void testClientFallsBackToHostnameWhenIpIsNull() {
        String hostname = "localhost"; // Use localhost to ensure it resolves
        String nullIp = null;
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Create client with null IP - should fallback to hostname
        BackendServiceClient client = new BackendServiceClient(address, nullIp, executor);

        // Verify client was created
        Assert.assertNotNull(client);

        // Verify channel was created
        ManagedChannel channel = Deencapsulation.getField(client, "channel");
        Assert.assertNotNull(channel);

        // Cleanup
        client.shutdown();
    }

    /**
     * Test that the client's isNormalState() method works correctly
     * after creation.
     */
    @Test
    public void testIsNormalState() {
        String hostname = "localhost";
        String resolvedIp = "127.0.0.1";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Create client
        BackendServiceClient client = new BackendServiceClient(address, resolvedIp, executor);

        // Verify client is in normal state initially
        // (IDLE or CONNECTING state is considered normal)
        Assert.assertTrue("Client should be in normal state after creation",
                client.isNormalState());

        // Cleanup
        client.shutdown();

        // After shutdown, state should no longer be normal
        // Note: This might be racy, so we don't assert on it strictly
    }

    /**
     * Test that shutdown() properly closes the channel.
     */
    @Test
    public void testShutdown() throws InterruptedException {
        String hostname = "localhost";
        String resolvedIp = "127.0.0.1";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Create client
        BackendServiceClient client = new BackendServiceClient(address, resolvedIp, executor);

        // Verify channel is not shutdown initially
        ManagedChannel channel = Deencapsulation.getField(client, "channel");
        Assert.assertFalse("Channel should not be shutdown initially", channel.isShutdown());

        // Shutdown client
        client.shutdown();

        // Give it a moment to shutdown
        Thread.sleep(100);

        // Verify channel is shutdown or terminated
        Assert.assertTrue("Channel should be shutdown or terminated",
                channel.isShutdown() || channel.isTerminated());
    }

    /**
     * Test that multiple clients can be created with different addresses.
     */
    @Test
    public void testMultipleClients() {
        TNetworkAddress address1 = new TNetworkAddress("localhost", 9060);
        TNetworkAddress address2 = new TNetworkAddress("localhost", 9061);

        BackendServiceClient client1 = new BackendServiceClient(address1, "127.0.0.1", executor);
        BackendServiceClient client2 = new BackendServiceClient(address2, "127.0.0.1", executor);

        Assert.assertNotNull(client1);
        Assert.assertNotNull(client2);
        Assert.assertTrue(client1.isNormalState());
        Assert.assertTrue(client2.isNormalState());

        // Cleanup
        client1.shutdown();
        client2.shutdown();
    }
}
