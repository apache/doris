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

package org.apache.doris.tso;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.GenericPool;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.FrontendService;
import org.apache.doris.thrift.TGetCurrentTsoResult;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStatus;
import org.apache.doris.thrift.TStatusCode;

import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class MasterTsoProviderTest {
    @Test
    void testMasterReturnsCurrentTso() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        TSOService tsoService = Mockito.mock(TSOService.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
        Mockito.when(tsoService.getTSO()).thenReturn(123L);

        Assertions.assertEquals(123L, MasterTsoProvider.getCurrentTso(context));
    }

    @Test
    void testMasterPropagatesAllocationFailure() {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        TSOService tsoService = Mockito.mock(TSOService.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getTSOService()).thenReturn(tsoService);
        IllegalStateException failure = new IllegalStateException("TSO timestamp is not calibrated");
        Mockito.when(tsoService.getTSO()).thenThrow(failure);

        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class,
                () -> MasterTsoProvider.getCurrentTso(context)));
    }

    @SuppressWarnings("unchecked")
    @Test
    void testFollowerReusesSuccessfulRpcAndInvalidatesFailedRpc() throws Exception {
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Env env = Mockito.mock(Env.class);
        Mockito.when(context.getEnv()).thenReturn(env);
        Mockito.when(env.getMasterHost()).thenReturn("127.0.0.1");
        Mockito.when(env.getMasterRpcPort()).thenReturn(9020);
        Mockito.when(context.getExecTimeoutS()).thenReturn(10);
        TNetworkAddress address = new TNetworkAddress("127.0.0.1", 9020);
        FrontendService.Client client = Mockito.mock(FrontendService.Client.class);
        GenericPool<FrontendService.Client> pool = Mockito.mock(GenericPool.class);
        Mockito.when(pool.borrowObject(address, 10000)).thenReturn(client);
        GenericPool<FrontendService.Client> originalPool = ClientPool.frontendPool;
        ClientPool.frontendPool = pool;
        try {
            Mockito.when(client.getCurrentTso()).thenReturn(
                    new TGetCurrentTsoResult(new TStatus(TStatusCode.OK)).setTso(123L));
            Assertions.assertEquals(123L, MasterTsoProvider.getCurrentTso(context));
            Mockito.verify(pool).returnObject(address, client);

            Mockito.clearInvocations(pool);
            Mockito.when(client.getCurrentTso()).thenReturn(
                    new TGetCurrentTsoResult(new TStatus(TStatusCode.NOT_MASTER)));
            Assertions.assertThrows(AnalysisException.class, () -> MasterTsoProvider.getCurrentTso(context));
            Mockito.verify(pool).returnObject(address, client);

            Mockito.clearInvocations(pool);
            Mockito.when(client.getCurrentTso()).thenReturn(
                    new TGetCurrentTsoResult(new TStatus(TStatusCode.INTERNAL_ERROR)));
            Assertions.assertThrows(AnalysisException.class, () -> MasterTsoProvider.getCurrentTso(context));
            Mockito.verify(pool).returnObject(address, client);

            Mockito.clearInvocations(pool);
            Mockito.when(client.getCurrentTso()).thenThrow(new TException("transport failure"));
            Assertions.assertThrows(AnalysisException.class, () -> MasterTsoProvider.getCurrentTso(context));
            Mockito.verify(pool).invalidateObject(address, client);
            Mockito.verify(pool, Mockito.never()).returnObject(address, client);
        } finally {
            ClientPool.frontendPool = originalPool;
        }
    }

}
