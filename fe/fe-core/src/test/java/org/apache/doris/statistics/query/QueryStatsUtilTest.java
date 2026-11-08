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

package org.apache.doris.statistics.query;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.GenericPool;
import org.apache.doris.system.Frontend;
import org.apache.doris.system.SystemInfoService.HostInfo;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryStatsResult;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryStatsUtilTest {
    private GenericPool<FrontendService.Client> originalPool;
    private GenericPool<FrontendService.Client> pool;
    private FrontendService.Client client;
    private MockedStatic<Env> envMock;
    private TNetworkAddress address;

    @BeforeEach
    public void setUp() throws Exception {
        originalPool = ClientPool.frontendPool;
        pool = Mockito.mock(GenericPool.class);
        ClientPool.frontendPool = pool;
        client = Mockito.mock(FrontendService.Client.class);
        address = new TNetworkAddress("remote-fe", 9020);
        Mockito.when(pool.borrowObject(Mockito.eq(address), Mockito.anyInt())).thenReturn(client);

        Env env = Mockito.mock(Env.class);
        QueryStats queryStats = Mockito.mock(QueryStats.class);
        Frontend frontend = Mockito.mock(Frontend.class);
        Mockito.when(frontend.isAlive()).thenReturn(true);
        Mockito.when(frontend.getHost()).thenReturn(address.getHostname());
        Mockito.when(frontend.getRpcPort()).thenReturn(address.getPort());
        Mockito.when(env.getSelfNode()).thenReturn(new HostInfo("local-fe", 9010));
        Mockito.when(env.getFrontends(null)).thenReturn(List.of(frontend));
        Mockito.when(env.getQueryStats()).thenReturn(queryStats);
        Mockito.when(queryStats.getCatalogStats("catalog")).thenReturn(new HashMap<>());
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    public void tearDown() {
        envMock.close();
        ClientPool.frontendPool = originalPool;
    }

    @Test
    public void testReturnClientAfterSuccessfulRpc() throws Exception {
        TQueryStatsResult result = new TQueryStatsResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        result.setSimpleResult(Map.of("table", 3L));
        Mockito.when(client.getQueryStats(Mockito.any())).thenReturn(result);

        Assertions.assertEquals(Map.of("table", 3L), QueryStatsUtil.getMergedCatalogStats("catalog"));
        Mockito.verify(pool).returnObject(address, client);
        Mockito.verify(pool, Mockito.never()).invalidateObject(address, client);
    }

    @Test
    public void testReturnClientWhenResultIsSkipped() throws Exception {
        TQueryStatsResult result = new TQueryStatsResult();
        result.setStatus(new TStatus(TStatusCode.OK));
        result.setSimpleResult(Map.of());
        Mockito.when(client.getQueryStats(Mockito.any())).thenReturn(result);

        Assertions.assertTrue(QueryStatsUtil.getMergedCatalogStats("catalog").isEmpty());
        Mockito.verify(pool).returnObject(address, client);
    }

    @Test
    public void testReturnClientWhenStatusIsNotOk() throws Exception {
        TQueryStatsResult result = new TQueryStatsResult();
        result.setStatus(new TStatus(TStatusCode.INTERNAL_ERROR));
        Mockito.when(client.getQueryStats(Mockito.any())).thenReturn(result);

        Assertions.assertTrue(QueryStatsUtil.getMergedCatalogStats("catalog").isEmpty());
        Mockito.verify(pool).returnObject(address, client);
    }

    @Test
    public void testInvalidateClientAfterRpcFailure() throws Exception {
        Mockito.when(client.getQueryStats(Mockito.any())).thenThrow(new TException("RPC failed"));

        Assertions.assertTrue(QueryStatsUtil.getMergedCatalogStats("catalog").isEmpty());
        Mockito.verify(pool).invalidateObject(address, client);
        Mockito.verify(pool, Mockito.never()).returnObject(address, client);
    }

    @Test
    public void testBorrowFailureDoesNotReleaseClient() throws Exception {
        Mockito.when(pool.borrowObject(Mockito.eq(address), Mockito.anyInt()))
                .thenThrow(new Exception("borrow failed"));

        Assertions.assertTrue(QueryStatsUtil.getMergedCatalogStats("catalog").isEmpty());
        Mockito.verify(pool, Mockito.never()).returnObject(Mockito.any(), Mockito.any());
        Mockito.verify(pool, Mockito.never()).invalidateObject(Mockito.any(), Mockito.any());
    }
}
