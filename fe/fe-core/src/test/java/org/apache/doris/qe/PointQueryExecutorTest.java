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

package org.apache.doris.qe;

import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class PointQueryExecutorTest {
    @Test
    public void testCandidateBackendsShuffleDependsOnQuerySelectionOrder() {
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);

        Mockito.when(scanNode.isScanBackendOrderBySelection()).thenReturn(false);
        Assertions.assertTrue(PointQueryExecutor.shouldShuffleCandidateBackends(scanNode));

        Mockito.when(scanNode.isScanBackendOrderBySelection()).thenReturn(true);
        Assertions.assertFalse(PointQueryExecutor.shouldShuffleCandidateBackends(scanNode));
    }

    @Test
    public void testDeadlineAndCancellationDoNotBlacklistBackend() throws Exception {
        Backend backend = new Backend(1, "127.0.0.1", 9060);
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        ShortCircuitQueryContext context = Mockito.mock(ShortCircuitQueryContext.class);
        PointQueryExecutor executor = new PointQueryExecutor(context, 1024);
        InternalService.PTabletKeyLookupRequest request = InternalService.PTabletKeyLookupRequest.newBuilder()
                .setTabletId(1).build();
        try (MockedStatic<BackendServiceProxy> proxies = Mockito.mockStatic(BackendServiceProxy.class);
                MockedStatic<SimpleScheduler> scheduler = Mockito.mockStatic(SimpleScheduler.class)) {
            proxies.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            for (io.grpc.Status error : new io.grpc.Status[] {
                    io.grpc.Status.DEADLINE_EXCEEDED, io.grpc.Status.CANCELLED}) {
                Mockito.when(proxy.fetchTabletDataAsync(Mockito.any(), Mockito.any(), Mockito.anyLong()))
                        .thenReturn(Futures.immediateFailedFuture(error.asRuntimeException()));
                Status status = new Status();
                Assertions.assertNull(Deencapsulation.invoke(executor, "fetchTabletData", status,
                        backend, request, System.currentTimeMillis() + 10000));
                Assertions.assertEquals(error == io.grpc.Status.DEADLINE_EXCEEDED
                        ? TStatusCode.TIMEOUT : TStatusCode.CANCELLED, status.getErrorCode());
            }
            scheduler.verifyNoInteractions();
        }
    }

    @Test
    public void testExpiredBudgetCancelsUnfinishedFuture() throws Exception {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        SettableFuture<InternalService.PTabletKeyLookupResponse> future = SettableFuture.create();
        Mockito.when(proxy.fetchTabletDataAsync(Mockito.any(), Mockito.any(), Mockito.anyLong())).thenReturn(future);
        PointQueryExecutor executor = new PointQueryExecutor(Mockito.mock(ShortCircuitQueryContext.class), 1024);
        try (MockedStatic<BackendServiceProxy> proxies = Mockito.mockStatic(BackendServiceProxy.class)) {
            proxies.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Status status = new Status();
            Deencapsulation.invoke(executor, "fetchTabletData", status, new Backend(1, "127.0.0.1", 9060),
                    InternalService.PTabletKeyLookupRequest.newBuilder().setTabletId(1).build(), 0L);
            Assertions.assertTrue(future.isCancelled());
            Assertions.assertFalse(status.ok());
        }
    }

}
