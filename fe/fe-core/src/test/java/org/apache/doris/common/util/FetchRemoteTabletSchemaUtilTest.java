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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.proto.InternalService.PFetchRemoteSchemaResponse;
import org.apache.doris.proto.OlapFile.ColumnPB;
import org.apache.doris.proto.OlapFile.TabletSchemaPB;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.util.concurrent.Futures;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;

class FetchRemoteTabletSchemaUtilTest {
    private static final PFetchRemoteSchemaResponse FAILED = PFetchRemoteSchemaResponse.newBuilder()
            .setStatus(PStatus.newBuilder().setStatusCode(TStatusCode.INTERNAL_ERROR.getValue())
                    .addErrorMsgs("injected metadata read failure"))
            .build();
    private static final PStatus OK = PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()).build();

    @Test
    void failsWhenAllCoordinatorsFail() throws Exception {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any()))
                .thenReturn(Futures.immediateFuture(FAILED));
        Assertions.assertThrows(AnalysisException.class, () -> fetch(proxy, true, true));
        Mockito.verify(proxy, Mockito.times(2)).fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any());
    }

    @Test
    void failsWhenTabletHasNoLiveBackend() {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Assertions.assertThrows(AnalysisException.class, () -> fetch(proxy, false, false));
    }

    @Test
    void failsWhenSuccessfulRpcHasNoSchema() throws Exception {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any())).thenReturn(
                Futures.immediateFuture(PFetchRemoteSchemaResponse.newBuilder().setStatus(OK).build()));
        Assertions.assertThrows(AnalysisException.class, () -> fetch(proxy, true, false));
    }

    @Test
    void retriesAndReturnsOnlyTheCompleteResponse() throws Exception {
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        PFetchRemoteSchemaResponse recovered = PFetchRemoteSchemaResponse.newBuilder()
                .setStatus(OK)
                .setMergedSchema(TabletSchemaPB.newBuilder().addColumn(
                        ColumnPB.newBuilder().setUniqueId(-1).setName("v.key").setType("INT")))
                .build();
        Mockito.when(proxy.fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any())).thenReturn(
                Futures.immediateFuture(FAILED), Futures.immediateFuture(recovered));
        Assertions.assertEquals(List.of("v.key"), fetch(proxy, true, true).stream().map(Column::getName).toList());
        Mockito.verify(proxy, Mockito.times(2)).fetchRemoteTabletSchemaAsync(Mockito.any(), Mockito.any());
    }

    @Test
    void emptyTabletListIsValid() throws Exception {
        Assertions.assertEquals(List.of(), new FetchRemoteTabletSchemaUtil(List.of()).fetch());
    }

    private List<Column> fetch(BackendServiceProxy proxy, boolean firstAlive, boolean secondAlive) throws Exception {
        Tablet tablet = Mockito.mock(Tablet.class);
        Mockito.when(tablet.getId()).thenReturn(101L);
        Replica first = Mockito.mock(Replica.class);
        Replica second = Mockito.mock(Replica.class);
        Mockito.when(first.isAlive()).thenReturn(true);
        Mockito.when(second.isAlive()).thenReturn(true);
        Mockito.when(first.getBackendIdWithoutException()).thenReturn(1L);
        Mockito.when(second.getBackendIdWithoutException()).thenReturn(2L);
        Mockito.when(tablet.getReplicas()).thenReturn(List.of(first, second));

        SystemInfoService systemInfo = Mockito.mock(SystemInfoService.class);
        Backend firstBackend = new Backend(1L, "backend1", 9050);
        firstBackend.setAlive(firstAlive);
        Backend secondBackend = new Backend(2L, "backend2", 9050);
        secondBackend.setAlive(secondAlive);
        Mockito.when(systemInfo.getBackend(1L)).thenReturn(firstBackend);
        Mockito.when(systemInfo.getBackend(2L)).thenReturn(secondBackend);

        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(context.getSessionVariable()).thenReturn(new SessionVariable());
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<BackendServiceProxy> proxyStatic = Mockito.mockStatic(BackendServiceProxy.class);
                MockedStatic<ConnectContext> contextStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentSystemInfo).thenReturn(systemInfo);
            proxyStatic.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            contextStatic.when(ConnectContext::get).thenReturn(context);
            return new FetchRemoteTabletSchemaUtil(List.of(tablet)).fetch();
        }
    }
}
