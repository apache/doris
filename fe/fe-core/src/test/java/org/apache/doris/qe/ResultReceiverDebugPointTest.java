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

import org.apache.doris.common.Config;
import org.apache.doris.common.Status;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.common.util.DebugPointUtil.DebugPoint;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.Types;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.util.concurrent.FutureCallback;
import com.google.protobuf.ByteString;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * The test hook ResultReceiver.getNext.dropDataBatch is armed for a number of hits, and asking whether it is
 * enabled spends one. Only a session's own query may take a hit: an internal query fetching rows at the same
 * time (auto-analyze, say) has to leave it to the query it was armed for, or the suite that armed it passes
 * without the failure it injects, or fails on a query that never failed.
 */
public class ResultReceiverDebugPointTest {

    private static final String DEBUG_POINT = "ResultReceiver.getNext.dropDataBatch";

    private boolean debugPointsEnabled;

    @BeforeEach
    public void setUp() {
        debugPointsEnabled = Config.enable_debug_points;
        Config.enable_debug_points = true;
        DebugPoint oneHit = new DebugPoint();
        oneHit.executeLimit = 1;
        DebugPointUtil.addDebugPoint(DEBUG_POINT, oneHit);
    }

    @AfterEach
    public void tearDown() {
        DebugPointUtil.clearDebugPoints();
        Config.enable_debug_points = debugPointsEnabled;
        ConnectContext.remove();
    }

    @Test
    void resultFetchUsesTheWorkerEndpointPolicyAndPreservesTheLegacyDefault() throws Exception {
        BackendServiceProxy internal = Mockito.mock(BackendServiceProxy.class);
        BackendServiceProxy remote = Mockito.mock(BackendServiceProxy.class);
        FutureCallback<InternalService.PFetchDataResult> callback = new FutureCallback<>() {
            @Override
            public void onSuccess(InternalService.PFetchDataResult result) {
            }

            @Override
            public void onFailure(Throwable failure) {
            }
        };
        TNetworkAddress address = new TNetworkAddress("127.0.0.1", 8060);
        try (MockedStatic<BackendServiceProxy> factory = Mockito.mockStatic(BackendServiceProxy.class)) {
            factory.when(BackendServiceProxy::getInstance).thenReturn(internal);
            factory.when(() -> BackendServiceProxy.getInstance(false)).thenReturn(remote);
            new ResultReceiver(new TUniqueId(1, 2), new TUniqueId(3, 4), 1L, address,
                    Long.MAX_VALUE, 1 << 20, false, false).createFuture(callback);
            new ResultReceiver(new TUniqueId(1, 2), new TUniqueId(3, 4), 1L, address,
                    Long.MAX_VALUE, 1 << 20, false).createFuture(callback);
            Mockito.verify(remote).fetchDataAsyncWithCallback(
                    Mockito.eq(address), Mockito.any(), Mockito.eq(callback));
            Mockito.verify(internal).fetchDataAsyncWithCallback(
                    Mockito.eq(address), Mockito.any(), Mockito.eq(callback));
        }
    }

    @Test
    public void internalQueryLeavesTheHitToTheQueryUnderTest() throws Exception {
        runAs(true);
        Status status = new Status();
        RowBatch batch = receiverWithOneRow().getNext(status);

        Assertions.assertTrue(status.ok());
        Assertions.assertEquals(1, batch.getBatch().getRowsSize());
        // Still armed: the query the hit was meant for gets it.
        Assertions.assertTrue(DebugPointUtil.isEnable(DEBUG_POINT));
    }

    @Test
    public void sessionQueryTakesTheHit() throws Exception {
        runAs(false);
        Status status = new Status();
        RowBatch batch = receiverWithOneRow().getNext(status);

        Assertions.assertNull(batch);
        Assertions.assertEquals(TStatusCode.THRIFT_RPC_ERROR, status.getErrorCode());
        // Armed for one hit, and that hit is spent.
        Assertions.assertFalse(DebugPointUtil.isEnable(DEBUG_POINT));
    }

    private static void runAs(boolean internal) {
        ConnectContext ctx = new ConnectContext();
        ctx.getState().setInternal(internal);
        ctx.setThreadLocalInfo();
    }

    // A receiver whose fetch_data response has already arrived: the last packet, carrying one row.
    private static ResultReceiver receiverWithOneRow() throws Exception {
        TResultBatch rows = new TResultBatch(Collections.singletonList(ByteBuffer.wrap(new byte[] {1})), false, 0);
        InternalService.PFetchDataResult result = InternalService.PFetchDataResult.newBuilder()
                .setStatus(Types.PStatus.newBuilder().setStatusCode(0))
                .setPacketSeq(0)
                .setEos(true)
                .setRowBatch(ByteString.copyFrom(new TSerializer().serialize(rows)))
                .build();
        ResultReceiver receiver = new ResultReceiver(new TUniqueId(1, 2), new TUniqueId(3, 4), 1L,
                new TNetworkAddress("127.0.0.1", 8060), Long.MAX_VALUE, 1 << 20, false);
        Deencapsulation.setField(receiver, "fetchDataAsyncFuture", CompletableFuture.completedFuture(result));
        return receiver;
    }
}
