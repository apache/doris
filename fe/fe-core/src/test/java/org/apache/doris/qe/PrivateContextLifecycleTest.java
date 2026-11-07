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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.protocol.MysqlProtocolAdapter;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.transaction.TransactionEntry;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.List;

class PrivateContextLifecycleTest {
    private boolean savedUnitTest;
    private MockedStatic<Env> environment;
    private Env env;
    private InternalCatalog catalog;
    private ConnectContext parent;

    @BeforeEach
    void setUp() {
        savedUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        env = Mockito.mock(Env.class);
        catalog = Mockito.mock(InternalCatalog.class);
        CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
        Mockito.when(catalogs.getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME)).thenReturn(catalog);
        environment = Mockito.mockStatic(Env.class);
        environment.when(Env::getCurrentEnv).thenReturn(env);
        parent = new ConnectContext();
        parent.setThreadLocalInfo();
    }

    @AfterEach
    void tearDown() {
        parent.cleanup();
        ConnectContext.remove();
        environment.close();
        FeConstants.runningUnitTest = savedUnitTest;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedFactoryClosesBeforeOwnershipCanReachCaller(boolean statistics) {
        RuntimeException failure = new IllegalStateException("database lookup failed");
        Mockito.when(catalog.getDb(Mockito.anyString())).thenThrow(failure);
        FeConstants.runningUnitTest = false;
        Assertions.assertSame(failure, Assertions.assertThrows(IllegalStateException.class, () -> {
            if (statistics) {
                StatisticsUtil.buildConnectContext(false);
            } else {
                ConnectContextUtil.getDummyCtx("db");
            }
        }));
        ArgumentCaptor<String> sessionId = ArgumentCaptor.forClass(String.class);
        Mockito.verify(env).registerSessionInfo(sessionId.capture());
        Mockito.verify(env).unregisterSessionInfo(sessionId.getValue());
        Assertions.assertSame(parent, ConnectContext.get());
        Assertions.assertFalse(parent.isClosingOrClosed());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void proxyCleanupReleasesLocalResourcesAndPreservesOriginSession(boolean proxy) throws Exception {
        FeConstants.runningUnitTest = false;
        ConnectContext child = proxy ? ConnectContext.forMysqlProxy("origin-session") : new ConnectContext();
        Mockito.verify(env, proxy ? Mockito.never() : Mockito.times(1)).registerSessionInfo(Mockito.anyString());
        StatementContext statement = Mockito.mock(StatementContext.class);
        child.setStatementContext(statement);
        TransactionEntry transaction = Mockito.mock(TransactionEntry.class);
        Mockito.when(transaction.isTxnModel()).thenReturn(true);
        child.setTxnEntry(transaction);
        List<ByteBuffer> response = null;
        ByteBuffer packet = ByteBuffer.wrap(new byte[] {1, 2, 3});
        if (proxy) {
            child.getMysqlChannel().sendOnePacket(packet);
            response = MysqlProtocolAdapter.of(child).proxyResultPackets();
        }
        try (AutoCloseConnectContext scope = new AutoCloseConnectContext(child)) {
            Assertions.assertSame(child, ConnectContext.get());
        }
        child.cleanup();
        Mockito.verify(statement).close();
        Mockito.verify(transaction, proxy ? Mockito.never() : Mockito.times(1)).abortTransaction();
        Mockito.verify(env, proxy ? Mockito.never() : Mockito.times(1)).unregisterSessionInfo(child.getSessionId());
        Assertions.assertTrue(child.isClosingOrClosed());
        Assertions.assertNull(child.getTxnEntry());
        Assertions.assertNull(child.getStatementContext());
        Assertions.assertSame(parent, ConnectContext.get());
        Assertions.assertFalse(parent.isClosingOrClosed());
        if (proxy) {
            Assertions.assertEquals(1, response.size());
            Assertions.assertSame(packet, response.get(0));
        }
    }
}
