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

package org.apache.doris.statistics.util;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class StatisticsUpdateContextLifecycleTest {
    @ParameterizedTest
    @CsvSource({"false, false", "true, false", "false, true", "true, true"})
    void auditReadsLiveContextBeforeScopeCloses(boolean statementFails, boolean auditFails) throws Exception {
        boolean savedUnitTest = FeConstants.runningUnitTest;
        ConnectContext previous = ConnectContext.get();
        FeConstants.runningUnitTest = true;
        String sql = "INSERT INTO statistics_table VALUES (1)";
        RuntimeException statementFailure = new IllegalStateException("statement failed");
        RuntimeException auditFailure = new IllegalStateException("audit failed");
        AtomicReference<ConnectContext> execution = new AtomicReference<>();
        AtomicReference<ConnectContext> audited = new AtomicReference<>();
        AtomicLong auditedRows = new AtomicLong(-1);
        AtomicBoolean auditedWhileOpen = new AtomicBoolean();
        AtomicBoolean auditedWhileBound = new AtomicBoolean();
        StatementContext statement = Mockito.mock(StatementContext.class);
        try (MockedStatic<Env> environment = Mockito.mockStatic(Env.class);
                MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                MockedStatic<AuditLogHelper> audit = Mockito.mockStatic(AuditLogHelper.class);
                MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                        (executor, constructor) -> {
                            ConnectContext context = (ConnectContext) constructor.arguments().get(0);
                            execution.set(context);
                            context.setStatementContext(statement);
                            Mockito.when(executor.getOriginStmt()).thenReturn(new OriginStatement(sql, 0));
                            Mockito.doAnswer(call -> {
                                context.updateReturnRows(17);
                                if (statementFails) {
                                    throw statementFailure;
                                }
                                return null;
                            }).when(executor).execute();
                        })) {
            Env env = Mockito.mock(Env.class);
            InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
            CatalogMgr catalogs = Mockito.mock(CatalogMgr.class);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogs);
            Mockito.when(catalogs.getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME)).thenReturn(catalog);
            environment.when(Env::getCurrentEnv).thenReturn(env);
            audit.when(() -> AuditLogHelper.logAuditLog(Mockito.any(ConnectContext.class), Mockito.eq(sql),
                    Mockito.isNull(), Mockito.isNull(), Mockito.eq(true))).thenAnswer(call -> {
                        ConnectContext context = call.getArgument(0);
                        audited.set(context);
                        auditedRows.set(context.getReturnRows());
                        auditedWhileOpen.set(!context.isClosingOrClosed());
                        auditedWhileBound.set(ConnectContext.get() == context);
                        if (auditFails) {
                            throw auditFailure;
                        }
                        return null;
                    });
            ConnectContext parent = new ConnectContext();
            parent.setThreadLocalInfo();
            try {
                if (statementFails || auditFails) {
                    RuntimeException caught = Assertions.assertThrows(RuntimeException.class,
                            () -> StatisticsUtil.execUpdate(sql));
                    Assertions.assertSame(auditFails ? auditFailure : statementFailure, caught);
                } else {
                    Assertions.assertNotNull(StatisticsUtil.execUpdate(sql));
                }
                Assertions.assertSame(execution.get(), audited.get());
                Assertions.assertEquals(17, auditedRows.get());
                Assertions.assertTrue(auditedWhileOpen.get());
                Assertions.assertTrue(auditedWhileBound.get());
                Assertions.assertTrue(execution.get().isClosingOrClosed());
                Assertions.assertNull(execution.get().getStatementContext());
                Mockito.verify(statement).close();
                Assertions.assertSame(parent, ConnectContext.get());
                Assertions.assertFalse(parent.isClosingOrClosed());
            } finally {
                parent.cleanup();
            }
        } finally {
            FeConstants.runningUnitTest = savedUnitTest;
            if (previous == null) {
                ConnectContext.remove();
            } else {
                previous.setThreadLocalInfo();
            }
        }
    }
}
