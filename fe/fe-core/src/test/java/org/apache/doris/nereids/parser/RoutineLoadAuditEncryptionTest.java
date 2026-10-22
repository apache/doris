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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.workloadschedpolicy.WorkloadRuntimeStatusMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

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
    public void testSubmittedAuditEventContainsMaskedSql() {
        String sql = "ALTER ROUTINE LOAD FOR db.job PROPERTIES ('desired_concurrent_number' = '2') "
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
