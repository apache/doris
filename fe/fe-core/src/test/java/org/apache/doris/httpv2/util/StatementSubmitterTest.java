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

package org.apache.doris.httpv2.util;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.NameSpaceContext;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

class StatementSubmitterTest {
    private boolean savedRunningUnitTest;
    private MockedStatic<Env> envMock;

    @BeforeEach
    void setUp() {
        savedRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        Env env = Mockito.mock(Env.class);
        envMock = Mockito.mockStatic(Env.class);
        envMock.when(Env::getCurrentEnv).thenReturn(env);
    }

    @AfterEach
    void tearDown() {
        ConnectContext.remove();
        envMock.close();
        FeConstants.runningUnitTest = savedRunningUnitTest;
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @SuppressWarnings("unchecked")
    void queuedWorkerUsesSubmittedDatabaseAfterRequestCloses(boolean blocking) throws Exception {
        ThreadPoolExecutor executor = Mockito.mock(ThreadPoolExecutor.class);
        Future<ExecutionResultSet> submitted = Mockito.mock(Future.class);
        Mockito.when(executor.submit(Mockito.<Callable<ExecutionResultSet>>any())).thenReturn(submitted);
        try (MockedStatic<ThreadPoolManager> pools = Mockito.mockStatic(ThreadPoolManager.class)) {
            pools.when(() -> ThreadPoolManager.newDaemonCacheThreadPoolThrowException(
                    Config.http_sql_submitter_max_worker_threads, "SQL submitter", true)).thenReturn(executor);
            pools.when(() -> ThreadPoolManager.newDaemonCacheThreadPoolUseBlockedPolicy(
                    Config.cloud_copy_into_statement_submitter_threads_num, "SQL submitter with block policy", true))
                    .thenReturn(executor);
            StatementSubmitter submitter = new StatementSubmitter();
            ConnectContext context = new ConnectContext();
            context.restoreNameSpaceContext(new NameSpaceContext(InternalCatalog.INTERNAL_CATALOG_NAME, "before", -1));
            context.setThreadLocalInfo();
            StatementSubmitter.StmtContext statement = new StatementSubmitter.StmtContext(
                    "SET x = 1", "user", "password", 1, false, null, "");
            Assertions.assertSame(submitted, blocking ? submitter.submitBlock(statement) : submitter.submit(statement));
            context.restoreNameSpaceContext(new NameSpaceContext(InternalCatalog.INTERNAL_CATALOG_NAME, "after", -1));
            context.cleanup();
            Assertions.assertNull(ConnectContext.get());

            ArgumentCaptor<Callable<ExecutionResultSet>> captured = ArgumentCaptor.forClass(Callable.class);
            Mockito.verify(executor).submit(captured.capture());
            Connection connection = Mockito.mock(Connection.class);
            Statement jdbcStatement = Mockito.mock(Statement.class);
            Mockito.when(connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY))
                    .thenReturn(jdbcStatement);
            // Load the JDBC implementation before mocking DriverManager's static entry point.
            Class.forName("org.mariadb.jdbc.Driver");
            try (MockedStatic<DriverManager> driver = Mockito.mockStatic(DriverManager.class)) {
                String expectedUrl = "jdbc:mariadb://127.0.0.1:" + Config.query_port + "/before";
                driver.when(() -> DriverManager.getConnection(expectedUrl, "user", "password"))
                        .thenReturn(connection);
                Assertions.assertNotNull(captured.getValue().call());
                driver.verify(() -> DriverManager.getConnection(expectedUrl, "user", "password"));
            }
            Mockito.verify(jdbcStatement).execute("SET x = 1");
            Mockito.verify(jdbcStatement).close();
            Mockito.verify(connection).close();
        }
    }
}
