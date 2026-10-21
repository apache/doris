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

import org.apache.doris.common.NereidsException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.ExceptionDiagnosticUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientException;
import org.apache.doris.job.base.JobExecutionConfiguration;
import org.apache.doris.job.base.TimerDefinition;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.extensions.insert.streaming.StreamingInsertJob;
import org.apache.doris.job.util.StreamingJobUtils;
import org.apache.doris.utframe.TestWithFeService;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

class JdbcFinalOutputDiagnosticsTest extends TestWithFeService {
    private static final String PASSWORD = "boundary-test-secret";
    private static final String URL = "jdbc:mysql://private-host:3306/source";

    private SQLException rawFailure() {
        return new SQLException("denied raw=" + PASSWORD + " endpoint=" + URL, "42000", 1142);
    }

    private void assertSafe(String text) {
        Assertions.assertTrue(text.contains("remote_sqlstate=42000"), text);
        Assertions.assertTrue(text.contains("remote_vendor_error_code=1142"), text);
        Assertions.assertFalse(text.contains(PASSWORD), text);
        Assertions.assertFalse(text.contains(URL), text);
    }

    @Test
    void showDatabasesFinalLogsApplyDiagnosticPolicy() throws Exception {
        ConnectContext context = Mockito.spy(connectContext);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.doReturn(catalog).when(context).getCatalog("jdbc_diag");
        Mockito.when(catalog.getDbNames()).thenThrow(JdbcClientException.diagnostic(
                rawFailure(), "list databases", PASSWORD, URL));
        try (CapturedLog capture = new CapturedLog(StmtExecutor.class)) {
            new StmtExecutor(context, "SHOW DATABASES FROM jdbc_diag").execute();
            Mockito.verify(catalog).getDbNames();
            String text = capture.text.toString();
            Assertions.assertTrue(text.contains("process failed"), text);
            Assertions.assertTrue(text.contains("Analyze failed"), text);
            Assertions.assertTrue(text.contains("Caused by:"), text);
            assertSafe(text);
        }
    }

    @Test
    void streamingInitLogsSanitizeDirectSqlException() throws Exception {
        StreamingInsertJob job = Deencapsulation.newInstance(StreamingInsertJob.class);
        Deencapsulation.setField(job, "properties", new HashMap<String, String>());
        Deencapsulation.setField(job, "dataSourceType", DataSourceType.OCEANBASE);
        Map<String, String> source = new HashMap<>();
        source.put(DataSourceConfigKeys.PASSWORD, PASSWORD);
        source.put(DataSourceConfigKeys.JDBC_URL, URL);
        source.put(DataSourceConfigKeys.DRIVER_URL, "file:///operator/driver.jar");
        source.put(DataSourceConfigKeys.DRIVER_CLASS, "com.mysql.cj.jdbc.Driver");
        source.put(DataSourceConfigKeys.USER, "test-user");
        source.put(DataSourceConfigKeys.DATABASE, "source");
        Deencapsulation.setField(job, "sourceProperties", source);
        JobExecutionConfiguration config = new JobExecutionConfiguration();
        TimerDefinition timer = new TimerDefinition();
        timer.setStartTimeMs(System.currentTimeMillis());
        config.setTimerDefinition(timer);
        Deencapsulation.setField(job, "jobConfig", config);
        JdbcClient client = Mockito.mock(JdbcClient.class);
        Connection connection = Mockito.mock(Connection.class);
        Mockito.when(client.getConnection()).thenReturn(connection);
        Mockito.when(connection.createStatement()).thenThrow(rawFailure());
        Method init = StreamingInsertJob.class.getDeclaredMethod("initSourceJob");
        init.setAccessible(true);
        try (MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class);
                CapturedLog capture = new CapturedLog(StreamingInsertJob.class)) {
            utils.when(() -> StreamingJobUtils.getJdbcClient(Mockito.eq(DataSourceType.OCEANBASE),
                    Mockito.anyMap())).thenReturn(client);
            InvocationTargetException failure = Assertions.assertThrows(InvocationTargetException.class,
                    () -> init.invoke(job));
            Mockito.verify(connection).createStatement();
            assertSafe(failure.getCause().getMessage());
            assertSafe(ExceptionDiagnosticUtils.sanitizedStackTrace(
                    new RuntimeException("outer job", failure.getCause())));
            String text = capture.text.toString();
            Assertions.assertTrue(text.contains("init streaming job for"), text);
            Assertions.assertTrue(text.contains("Caused by:"), text);
            assertSafe(text);
        }
        Mockito.verify(client).closeClient();
    }

    @Test
    void ordinaryErrorsKeepThrowableAndNereidsStoredExceptionKeepsPolicy() {
        RuntimeException ordinary = new RuntimeException("ordinary failure");
        try (CapturedLog capture = new CapturedLog(StmtExecutor.class)) {
            ExceptionDiagnosticUtils.log(LogManager.getLogger(StmtExecutor.class), Level.WARN,
                    "ordinary {}", "query", ordinary);
            Assertions.assertTrue(capture.text.toString().contains("at org.apache.doris.qe."),
                    capture.text.toString());
        }
        JdbcClientException diagnostic = JdbcClientException.diagnostic(rawFailure(), "remote query", PASSWORD, URL);
        assertSafe(ExceptionDiagnosticUtils.sanitizedStackTrace(new NereidsException(diagnostic)));
    }

    private static class CapturedLog implements AutoCloseable {
        private final StringBuilder text = new StringBuilder();
        private final Logger logger;
        private final Level originalLevel;
        private final AbstractAppender appender;

        CapturedLog(Class<?> type) {
            logger = (Logger) LogManager.getLogger(type);
            originalLevel = logger.getLevel();
            appender = new AbstractAppender("boundary-test", null,
                    PatternLayout.newBuilder().withPattern("%m%n%ex").build(), false, Property.EMPTY_ARRAY) {
                @Override
                public void append(LogEvent event) {
                    text.append(getLayout().toSerializable(event));
                }
            };
            appender.start();
            logger.addAppender(appender);
            logger.setLevel(Level.DEBUG);
        }

        @Override
        public void close() {
            logger.removeAppender(appender);
            logger.setLevel(originalLevel);
            appender.stop();
        }
    }
}
