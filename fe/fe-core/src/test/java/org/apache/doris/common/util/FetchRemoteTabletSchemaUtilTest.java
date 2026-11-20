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

package org.apache.doris.common.util;

import org.apache.doris.catalog.Column;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.proto.InternalService.PFetchRemoteSchemaRequest;
import org.apache.doris.proto.InternalService.PFetchRemoteSchemaResponse;
import org.apache.doris.proto.OlapFile.ColumnPB;
import org.apache.doris.proto.OlapFile.TabletSchemaPB;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatusCode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.CompletableFuture;

class FetchRemoteTabletSchemaUtilTest {
    private static final PFetchRemoteSchemaRequest REQUEST =
            PFetchRemoteSchemaRequest.newBuilder().setIsCoordinator(true).build();

    private final BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);

    @Test
    void reportsMetadataErrorAfterAllCoordinatorsFail() throws Exception {
        Backend first = coordinator(8060, errorResponse("first metadata read failed"));
        Backend second = coordinator(8061, errorResponse("second metadata read failed"));
        FetchRemoteTabletSchemaUtil util = new FetchRemoteTabletSchemaUtil(List.of());
        try (MockedStatic<BackendServiceProxy> singleton = Mockito.mockStatic(BackendServiceProxy.class)) {
            singleton.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> util.fetchFromCoordinators(List.of(first, second), REQUEST, 1));
            Assertions.assertTrue(error.getMessage().contains("second metadata read failed"));
            Mockito.verify(proxy).fetchRemoteTabletSchemaAsync(first.getBrpcAddress(), REQUEST);
            Mockito.verify(proxy).fetchRemoteTabletSchemaAsync(second.getBrpcAddress(), REQUEST);
        }
    }

    @Test
    void usesCompleteSchemaFromSecondCoordinator() throws Exception {
        Backend first = coordinator(8060, errorResponse("temporary metadata read failure"));
        PFetchRemoteSchemaResponse complete = PFetchRemoteSchemaResponse.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()))
                .setMergedSchema(TabletSchemaPB.newBuilder()
                        .addColumn(ColumnPB.newBuilder().setName("v.child").setType("INT").setUniqueId(1)
                                .setAggregation("NONE")))
                .build();
        Backend second = coordinator(8061, complete);
        FetchRemoteTabletSchemaUtil util = new FetchRemoteTabletSchemaUtil(List.of());
        try (MockedStatic<BackendServiceProxy> singleton = Mockito.mockStatic(BackendServiceProxy.class)) {
            singleton.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            List<Column> columns = util.fetchFromCoordinators(List.of(first, second), REQUEST, 1);
            Assertions.assertEquals(1, columns.size());
            Assertions.assertEquals("v.child", columns.get(0).getName());
            Mockito.verify(proxy).fetchRemoteTabletSchemaAsync(first.getBrpcAddress(), REQUEST);
        }
    }

    private Backend coordinator(int port, PFetchRemoteSchemaResponse response) throws Exception {
        Backend backend = Mockito.mock(Backend.class);
        TNetworkAddress address = new TNetworkAddress("127.0.0.1", port);
        Mockito.when(backend.getBrpcAddress()).thenReturn(address);
        Mockito.when(proxy.fetchRemoteTabletSchemaAsync(address, REQUEST))
                .thenReturn(CompletableFuture.completedFuture(response));
        return backend;
    }

    private static PFetchRemoteSchemaResponse errorResponse(String message) {
        return PFetchRemoteSchemaResponse.newBuilder()
                .setStatus(PStatus.newBuilder()
                        .setStatusCode(TStatusCode.MEM_LIMIT_EXCEEDED.getValue())
                        .addErrorMsgs(message))
                .build();
    }
}
