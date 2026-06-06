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

package org.apache.doris.maxcompute;

import org.apache.doris.thrift.TMaxComputeBlockIdRequest;
import org.apache.doris.thrift.TMaxComputeBlockIdResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.protocol.TProtocol;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;

public class MaxComputeFeClientTest {
    @Test
    public void testRequestBlockIdSuccess() throws Exception {
        FakeExecutor executor = new FakeExecutor(okResult(42L));
        MaxComputeFeClient client = new MaxComputeFeClient(
                new TNetworkAddress("fe1", 9010), 1234, "THREAD_POOL", executor, 0);

        Assert.assertEquals(42L, client.requestBlockId(100L, "session-1"));
        Assert.assertEquals(1, executor.addresses.size());
        Assert.assertEquals("fe1", executor.addresses.get(0).getHostname());
        Assert.assertEquals(9010, executor.addresses.get(0).getPort());
        Assert.assertFalse(executor.framedTransports.get(0));
        Assert.assertEquals(1234, (int) executor.timeouts.get(0));
        Assert.assertEquals(100L, executor.requests.get(0).getTxnId());
        Assert.assertEquals("session-1", executor.requests.get(0).getWriteSessionId());
        Assert.assertEquals(1L, executor.requests.get(0).getLength());
    }

    @Test
    public void testRequestBlockIdRedirectsToMaster() throws Exception {
        FakeExecutor executor = new FakeExecutor(notMasterResult("master", 9020), okResult(7L));
        MaxComputeFeClient client = new MaxComputeFeClient(
                new TNetworkAddress("follower", 9010), 1234, "THREADED_SELECTOR", executor, 0);

        Assert.assertEquals(7L, client.requestBlockId(101L, "session-2"));
        Assert.assertEquals(2, executor.addresses.size());
        Assert.assertEquals("follower", executor.addresses.get(0).getHostname());
        Assert.assertEquals("master", executor.addresses.get(1).getHostname());
        Assert.assertTrue(executor.framedTransports.get(0));
        Assert.assertTrue(executor.framedTransports.get(1));
    }

    @Test
    public void testFeErrorFailsWithoutRetry() {
        FakeExecutor executor = new FakeExecutor(errorResult("allocation failed"));
        MaxComputeFeClient client = new MaxComputeFeClient(
                new TNetworkAddress("fe1", 9010), 1234, "THREAD_POOL", executor, 0);

        expectIOExceptionContains(() -> client.requestBlockId(102L, "session-3"), "allocation failed");
        Assert.assertEquals(1, executor.addresses.size());
    }

    @Test
    public void testRpcFailureRetries() throws Exception {
        FakeExecutor executor = new FakeExecutor(
                new IOException("connect failed"),
                new IOException("temporary failure"),
                okResult(9L));
        MaxComputeFeClient client = new MaxComputeFeClient(
                new TNetworkAddress("fe1", 9010), 1234, "THREAD_POOL", executor, 0);

        Assert.assertEquals(9L, client.requestBlockId(103L, "session-4"));
        Assert.assertEquals(3, executor.addresses.size());
    }

    private static TMaxComputeBlockIdResult okResult(long start) {
        TMaxComputeBlockIdResult result = new TMaxComputeBlockIdResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        result.setStart(start);
        result.setLength(1L);
        return result;
    }

    private static TMaxComputeBlockIdResult notMasterResult(String host, int port) {
        TMaxComputeBlockIdResult result = new TMaxComputeBlockIdResult();
        result.setStatus(new TStatus(TStatusCode.NOT_MASTER));
        result.setMasterAddress(new TNetworkAddress(host, port));
        return result;
    }

    private static TMaxComputeBlockIdResult errorResult(String errorMsg) {
        TStatus status = new TStatus(TStatusCode.ANALYSIS_ERROR);
        status.addToErrorMsgs(errorMsg);
        TMaxComputeBlockIdResult result = new TMaxComputeBlockIdResult();
        result.setStatus(status);
        return result;
    }

    private static void expectIOExceptionContains(IOAction action, String expectedMessage) {
        try {
            action.run();
            Assert.fail("expected IOException");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains(expectedMessage));
        }
    }

    private interface IOAction {
        void run() throws IOException;
    }

    private static class FakeExecutor implements MaxComputeFeClient.RpcExecutor {
        private final Queue<Object> responses;
        private final List<TNetworkAddress> addresses = new ArrayList<>();
        private final List<Integer> timeouts = new ArrayList<>();
        private final List<Boolean> framedTransports = new ArrayList<>();
        private final List<TMaxComputeBlockIdRequest> requests = new ArrayList<>();

        FakeExecutor(Object... responses) {
            this.responses = new ArrayDeque<>(Arrays.asList(responses));
        }

        @Override
        public <T> T call(TNetworkAddress address, int timeoutMs, boolean useFramedTransport,
                MaxComputeFeClient.FeCall<T> call) throws Exception {
            addresses.add(new TNetworkAddress(address.getHostname(), address.getPort()));
            timeouts.add(timeoutMs);
            framedTransports.add(useFramedTransport);

            FrontendServiceClient client = new FrontendServiceClient();
            return call.call(client);
        }

        private class FrontendServiceClient extends org.apache.doris.thrift.FrontendService.Client {
            FrontendServiceClient() {
                super((TProtocol) null);
            }

            @Override
            public TMaxComputeBlockIdResult getMaxComputeBlockIdRange(TMaxComputeBlockIdRequest request)
                    throws org.apache.thrift.TException {
                requests.add(request);

                Object response = responses.remove();
                if (response instanceof RuntimeException) {
                    throw (RuntimeException) response;
                }
                if (response instanceof IOException) {
                    throw new RuntimeException((IOException) response);
                }
                if (response instanceof org.apache.thrift.TException) {
                    throw (org.apache.thrift.TException) response;
                }
                if (response instanceof Exception) {
                    throw new RuntimeException((Exception) response);
                }
                return (TMaxComputeBlockIdResult) response;
            }
        }
    }
}
