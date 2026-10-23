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

package org.apache.doris.nereids.parser;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.commands.SetOptionsCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;

public class RoutineLoadAuditEncryptionTest {
    private String encrypt(String sql) {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof AlterRoutineLoadCommand);
        Assertions.assertTrue(plan instanceof NeedAuditEncryption);
        NeedAuditEncryption command = (NeedAuditEncryption) plan;
        Assertions.assertTrue(command.needAuditEncryption());
        return command.geneEncryptionSQL(sql);
    }

    @Test
    public void testMaskCredentialsAndPreserveOtherSql() {
        String sql = "ALTER ROUTINE LOAD FOR db.job PROPERTIES ('desired_concurrent_number' = '2') "
                + "FROM KAFKA ('kafka_topic' = 'orders', 'property.client.id' = 'routine-load-client', "
                + "'property.sasl.password' = 'synthetic-password', "
                + "'property.ssl.key.password' = 'synthetic-ssl-password', "
                + "'property.sasl.jaas.config' = 'synthetic-jaas-config', "
                + "'property.sasl.oauthbearer.config' = 'synthetic-oauth-config', "
                + "'property.sasl.oauthbearer.client.secret' = 'synthetic-client-secret', "
                + "'property.sasl.oauthbearer.token' = 'synthetic-token', "
                + "'aws.access_key' = 'synthetic-access-key', "
                + "'aws.secret_key' = 'synthetic-secret-key', "
                + "'property.ssl.key.pem' = 'synthetic-private-key')";
        String expected = sql;
        for (String secret : new String[] {"synthetic-password", "synthetic-ssl-password", "synthetic-jaas-config",
                "synthetic-oauth-config", "synthetic-client-secret", "synthetic-token", "synthetic-access-key",
                "synthetic-secret-key", "synthetic-private-key"}) {
            expected = expected.replace("'" + secret + "'", "'******'");
        }

        String masked = encrypt(sql);
        Assertions.assertEquals(expected, masked);
        Assertions.assertDoesNotThrow(() -> new NereidsParser().parseSingle(masked));
    }

    @Test
    public void testCaseInsensitiveAndWhitespacePaddedPropertyNames() {
        String sql = "ALTER ROUTINE LOAD FOR db.job FROM KAFKA ("
                + "' PrOpErTy.SaSl.PaSsWoRd ' = 'synthetic-password')";
        Assertions.assertEquals(sql.replace("'synthetic-password'", "'******'"), encrypt(sql));
    }

    @Test
    public void testMaskNativeNulCredentialAlias() {
        String sql = "ALTER ROUTINE LOAD FOR db.job FROM KAFKA ("
                + "'property.sasl.password\\0ignored' = 'synthetic-secret', 'property.client.id' = 'client-value')";
        SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT, () -> {
            AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) new NereidsParser().parseSingle(sql);
            Assertions.assertEquals("synthetic-secret",
                    command.getDataSourceMapProperties().get("property.sasl.password\0ignored"));
            Assertions.assertEquals(sql.replace("'synthetic-secret'", "'******'"), command.geneEncryptionSQL(sql));
            return null;
        });
    }

    @Test
    public void testCaptureAuditBeforeLoadExpressionChangesSqlMode() {
        String sql = "ALTER ROUTINE LOAD FOR db.job "
                + "COLUMNS (c = (SELECT /*+ SET_VAR(sql_mode='NO_BACKSLASH_ESCAPES') */ 1)) "
                + "FROM KAFKA ('property.sasl.\\password' = 'synthetic-secret')";
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().setSqlMode(SqlModeHelper.MODE_DEFAULT);
        context.setStatementContext(new StatementContext(context, new OriginStatement(sql, 0)));
        context.setThreadLocalInfo();
        try {
            AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) new NereidsParser().parseSingle(sql);
            Assertions.assertTrue(SqlModeHelper.hasNoBackSlashEscapes());
            Assertions.assertEquals("synthetic-secret", command.getDataSourceMapProperties().get("property.sasl.password"));
            Assertions.assertEquals(sql.replace("'synthetic-secret'", "'******'"), command.geneEncryptionSQL(sql));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testPreserveJobOnlyAlter() {
        String sql = "ALTER ROUTINE LOAD FOR db.job PROPERTIES ('desired_concurrent_number' = '2')";
        Assertions.assertEquals(sql, encrypt(sql));
    }

    @Test
    public void testPreserveNonSensitiveDataSourceProperties() {
        String sql = "ALTER ROUTINE LOAD FOR db.job FROM KAFKA ("
                + "'property.sasl.oauthbearer.token.endpoint.url' = 'https://auth.example/token', "
                + "'property.ssl.ca.location' = 'FILE:ca.pem')";
        Assertions.assertEquals(sql, encrypt(sql));
    }

    @Test
    public void testMaskEachOccurrenceOfCaseVariantSecretKeys() {
        String sql = "ALTER ROUTINE LOAD FOR db.job FROM KAFKA ("
                + "'property.sasl.password' = 'first-secret', 'property.SASL.PASSWORD' = 'second-secret')";
        Assertions.assertEquals(sql.replace("'first-secret'", "'******'")
                .replace("'second-secret'", "'******'"), encrypt(sql));
    }

    @Test
    public void testPreserveQuotedOperationalValues() {
        String sql = "ALTER ROUTINE LOAD FOR db.job FROM KAFKA ("
                + "\"property.client.id\" = \"client's-id\", "
                + "\"property.sasl.password\" = \"secret'with-quote\")";
        String masked = encrypt(sql);
        Assertions.assertEquals(sql.replace("\"secret'with-quote\"", "'******'"), masked);
        Assertions.assertDoesNotThrow(() -> new NereidsParser().parseSingle(masked));
    }

    @Test
    public void testSupplementaryCharactersBeforeAndInsideSecrets() {
        String sql = "/* 😀😀😀😀😀😀😀😀 */ ALTER ROUTINE LOAD FOR db.job FROM KAFKA ("
                + "'property.client.id' = 'client😀', 'property.sasl.password' = 'top😀secret', "
                + "'property.ssl.key.password' = 'second😀secret') /* trailing 😀 */;";
        Assertions.assertEquals(sql.replace("'top😀secret'", "'******'")
                .replace("'second😀secret'", "'******'"), encrypt(sql));
    }

    @Test
    public void testBatchSqlModeChangeUsesOriginalDecodedKeys() throws Exception {
        String alter = "ALTER ROUTINE LOAD FOR db.job FROM KAFKA "
                + "('property.sasl.\\password' = 'synthetic-secret')";
        String sql = "SET sql_mode='NO_BACKSLASH_ESCAPES'; " + alter;
        List<AuditEvent> events = executeBatchForAudit(sql);
        Assertions.assertEquals(2, events.size());
        Assertions.assertEquals(alter.replace("'synthetic-secret'", "'******'"), events.get(1).stmt);
        Assertions.assertEquals("OK", events.get(1).state);
    }

    @Test
    public void testOriginalNoBackslashModeMasksKeySensitiveOnMaster() {
        String sql = "ALTER ROUTINE LOAD FOR db.job FROM KAFKA "
                + "('property.sasl.\\password' = 'ordinary-value')";
        AlterRoutineLoadCommand command = SqlModeHelper.withSqlMode(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES,
                () -> (AlterRoutineLoadCommand) new NereidsParser().parseSingle(sql));
        Assertions.assertEquals("ordinary-value", command.getDataSourceMapProperties().get("property.sasl.\\password"));
        Assertions.assertEquals(sql.replace("'ordinary-value'", "'******'"),
                SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT,
                        () -> command.geneEncryptionSQL(sql)));
    }

    @Test
    public void testBothModesPreserveEscapedOperationalKey() {
        String sql = "ALTER ROUTINE LOAD FOR db.job FROM KAFKA "
                + "('property.client.\\id' = 'client-value')";
        for (long mode : new long[] {SqlModeHelper.MODE_DEFAULT, SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES}) {
            Assertions.assertEquals(sql, SqlModeHelper.withSqlMode(mode, () -> encrypt(sql)));
        }
    }

    @Test
    public void testChangedPropertyBoundariesMaskEntireStatementInBothModes() {
        String sql = "/* 😀 */ ALTER ROUTINE LOAD FOR db.job FROM KAFKA "
                + "('property.client.id' = 'client\\', \"property.sasl.password\" = \"synthetic-secret\" -- '\n)";
        for (long mode : new long[] {SqlModeHelper.MODE_DEFAULT, SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES}) {
            SqlModeHelper.withSqlMode(mode, () -> {
                AlterRoutineLoadCommand command = (AlterRoutineLoadCommand) new NereidsParser().parseSingle(sql);
                Assertions.assertEquals(mode == SqlModeHelper.MODE_DEFAULT ? 1 : 2,
                        command.getDataSourceMapProperties().size());
                String clientId = command.getDataSourceMapProperties().get("property.client.id");
                Assertions.assertEquals(mode == SqlModeHelper.MODE_DEFAULT, clientId.contains("synthetic-secret"));
                Assertions.assertEquals("/* masked ALTER ROUTINE LOAD: ambiguous SQL mode */",
                        command.geneEncryptionSQL(sql));
                Assertions.assertEquals(mode == SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES,
                        SqlModeHelper.hasNoBackSlashEscapes());
                Assertions.assertEquals(clientId, command.getDataSourceMapProperties().get("property.client.id"));
                return null;
            });
        }
    }

    @Test
    public void testInvalidAlternateModeDoesNotExposeEscapedValue() {
        String sql = "ALTER ROUTINE LOAD FOR db.job FROM KAFKA "
                + "('property.client.id' = 'client\\'id', 'property.sasl.password' = 'synthetic-secret')";
        Assertions.assertEquals("/* masked ALTER ROUTINE LOAD: ambiguous SQL mode */",
                SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT, () -> encrypt(sql)));
    }

    @Test
    public void testNestedCommentSplitMismatchStillSubmitsAlterAudit() throws Exception {
        String alter = "ALTER ROUTINE LOAD FOR db.job FROM KAFKA "
                + "('property.sasl.password' = 'synthetic-secret')";
        String sql = alter + "; /* outer /* inner */ x; */ SELECT 1";
        Assertions.assertEquals(3, SqlUtils.splitMultiStmts(sql).size());
        List<AuditEvent> events = executeBatchForAudit(sql);
        Assertions.assertEquals(2, events.size());
        Assertions.assertEquals(alter.replace("'synthetic-secret'", "'******'"), events.get(0).stmt);
        Assertions.assertEquals("OK", events.get(0).state);
    }

    @Test
    public void testEachAlterKeepsItsOwnSqlInBatch() throws Exception {
        String first = "ALTER ROUTINE LOAD FOR db.first FROM KAFKA "
                + "('property.sasl.password' = 'first-secret')";
        String second = "ALTER ROUTINE LOAD FOR db.second FROM KAFKA "
                + "('property.sasl.password' = 'second😀secret')";
        String sql = first + "; /* outer 😀 /* inner */ x; */ " + second;
        List<AuditEvent> events = executeBatchForAudit(sql);
        Assertions.assertEquals(2, events.size());
        Assertions.assertEquals(first.replace("'first-secret'", "'******'"), events.get(0).stmt);
        Assertions.assertEquals(second.replace("'second😀secret'", "'******'"), events.get(1).stmt);
    }

    private List<AuditEvent> executeBatchForAudit(String sql) throws Exception {
        ConnectContext context = Mockito.spy(new ConnectContext());
        context.setStartTime();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.getSessionVariable().setSqlMode(SqlModeHelper.MODE_DEFAULT);
        context.getSessionVariable().setWorkloadGroup("default");
        context.getSessionVariable().setEnableSqlCache(false);
        context.setThreadLocalInfo();
        Mockito.doReturn(null).when(context).getCurrentCatalog();
        Mockito.doReturn("127.0.0.1").when(context).getClientIP();
        Env env = Mockito.mock(Env.class);
        WorkloadRuntimeStatusMgr statusMgr = Mockito.mock(WorkloadRuntimeStatusMgr.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(statusMgr);
        PluginMgr pluginMgr = Mockito.mock(PluginMgr.class);
        Mockito.when(pluginMgr.getActiveDialectPluginList(Dialect.DORIS)).thenReturn(Collections.emptyList());
        Mockito.when(env.getPluginMgr()).thenReturn(pluginMgr);

        // Exercise the real parser, batch splitting, execution loop and audit submission.
        // Mock only statement execution so no Kafka job, backend or network is required.
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedConstruction<StmtExecutor> executors = Mockito.mockConstruction(StmtExecutor.class,
                        Mockito.withSettings().defaultAnswer(Mockito.RETURNS_DEEP_STUBS), (executor, construction) -> {
                            StatementBase statement = (StatementBase) construction.arguments().get(1);
                            LogicalPlan plan = ((LogicalPlanAdapter) statement).getLogicalPlan();
                            Mockito.when(executor.getParsedStmt()).thenReturn(statement);
                            Mockito.when(executor.getCoord()).thenReturn(null);
                            Mockito.when(executor.getQueryStatisticsForAuditLog()).thenReturn(null);
                            Mockito.when(executor.getAuditStatisticsBackendIds()).thenReturn(Collections.emptySet());
                            Mockito.doAnswer(invocation -> {
                                if (plan instanceof SetOptionsCommand) {
                                    context.getSessionVariable().setSqlMode(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
                                }
                                if (plan instanceof AlterRoutineLoadCommand && sql.startsWith("SET")) {
                                    Assertions.assertTrue(SqlModeHelper.hasNoBackSlashEscapes());
                                    Assertions.assertEquals("synthetic-secret", ((AlterRoutineLoadCommand) plan)
                                            .getDataSourceMapProperties().get("property.sasl.password"));
                                }
                                context.getState().setIsQuery(false);
                                context.getState().setOk();
                                return null;
                            }).when(executor).execute();
                        })) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            ConnectProcessor processor = new ConnectProcessor(context) {
                @Override
                public void processOnce() {
                }
            };
            processor.executeQuery(sql);
            Assertions.assertEquals(2, executors.constructed().size());
            ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
            Mockito.verify(statusMgr, Mockito.times(2)).submitFinishQueryToAudit(captor.capture());
            return captor.getAllValues();
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testSubmittedAuditEventContainsMaskedSql() {
        String sql = "/* 😀😀😀😀😀😀😀😀 */ ALTER ROUTINE LOAD FOR db.job "
                + "PROPERTIES ('desired_concurrent_number' = '2') "
                + "FROM KAFKA ('property.client.id' = 'routine-load-client', "
                + "'property.sasl.password' = 'synthetic-password')";
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        LogicalPlanAdapter statement = new LogicalPlanAdapter(plan, new StatementContext());
        ConnectContext context = Mockito.spy(new ConnectContext());
        context.setStartTime();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.getState().setIsQuery(false);
        context.getState().setOk();
        Mockito.doReturn(null).when(context).getCurrentCatalog();
        Mockito.doReturn("127.0.0.1").when(context).getClientIP();
        Env env = Mockito.mock(Env.class);
        WorkloadRuntimeStatusMgr statusMgr = Mockito.mock(WorkloadRuntimeStatusMgr.class);
        Mockito.when(env.isMaster()).thenReturn(true);
        Mockito.when(env.getWorkloadRuntimeStatusMgr()).thenReturn(statusMgr);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            AuditLogHelper.logAuditLog(context, sql, statement, null, false);

            ArgumentCaptor<AuditEvent> captor = ArgumentCaptor.forClass(AuditEvent.class);
            Mockito.verify(statusMgr).submitFinishQueryToAudit(captor.capture());
            AuditEvent event = captor.getValue();
            Assertions.assertEquals(sql.replace("'synthetic-password'", "'******'"), event.stmt);
            Assertions.assertEquals("root", event.user);
            Assertions.assertEquals("127.0.0.1", event.clientIp);
            Assertions.assertEquals("OK", event.state);
        }
    }

}
