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

package org.apache.doris.load.routineload;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.BackendServiceConstants;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class KafkaPropertyValidationTest {
    private KafkaDataSourceProperties properties(String key, String value, boolean alter) {
        Map<String, String> source = new HashMap<>();
        if (!alter) {
            source.put("kafka_broker_list", "127.0.0.1:9092");
            source.put("kafka_topic", "test_topic");
        }
        source.put("property." + key, value);
        KafkaDataSourceProperties properties = new KafkaDataSourceProperties(source);
        properties.setAlter(alter);
        properties.setTimezone("UTC");
        return properties;
    }

    @Test
    public void testCreateAndAlterRejectExecutableAndUnknownProperties() {
        for (boolean alter : new boolean[] {false, true}) {
            for (String key : List.of("sasl.kerberos.kinit.cmd", "sasl.kerberos.keytab",
                    "sasl.kerberos.min.time.before.relogin", "plugin.library.paths", "ssl.engine.location",
                    "ssl.engine.id", "ssl.providers", "unknown.future.property", "sasl.jaas.config",
                    "security.protocol\0suffix")) {
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> properties(key, "sensitive-value", alter).analyze());
                Assertions.assertTrue(error.getMessage().contains("is not allowed in Routine Load"), key);
                Assertions.assertFalse(error.getMessage().contains("sensitive-value"));
            }
        }
    }

    @Test
    public void testRejectKerberosRefreshReenable() {
        for (boolean alter : new boolean[] {false, true}) {
            for (String value : List.of("1", "60000", "00", "0\0suffix", "FILE:interval")) {
                Assertions.assertThrows(AnalysisException.class,
                        () -> properties("sasl.kerberos.min.time.before.relogin", value, alter).analyze());
            }
        }
    }

    @Test
    public void testCertificateFilesRequireManagedReferences() throws Exception {
        for (boolean alter : new boolean[] {false, true}) {
            for (String key : BackendServiceConstants.KAFKA_FILE_PROPERTIES) {
                for (String value : List.of("/etc/ssl/private/key.pem", "relative.pem", "", "FILE:")) {
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            () -> properties(key, value, alter).analyze());
                    Assertions.assertTrue(error.getMessage().contains("CREATE FILE"));
                }
                KafkaDataSourceProperties properties = properties(key, "FILE:client.pem", alter);
                properties.analyze();
                Assertions.assertEquals("FILE:client.pem", properties.getCustomKafkaProperties().get(key));
            }
        }
    }

    @Test
    public void testNormalConsumerPropertiesAndDefaultOffsets() throws Exception {
        Map<String, String> accepted = Map.of(
                "group.id", "routine-load-group",
                "client.id", "routine-load-client",
                "security.protocol", "SASL_SSL",
                "sasl.mechanisms", "SCRAM-SHA-256",
                "sasl.username", "user",
                "sasl.password", "secret",
                "isolation.level", "read_committed",
                "fetch.max.bytes", "1048576",
                "kafka_default_offsets", "OFFSET_BEGINNING");
        for (boolean alter : new boolean[] {false, true}) {
            for (Map.Entry<String, String> property : accepted.entrySet()) {
                properties(property.getKey(), property.getValue(), alter).analyze();
            }
        }
    }

    @Test
    public void testAwsPropertiesWithAndWithoutPrefix() throws Exception {
        for (String prefix : List.of("", "property.")) {
            Map<String, String> source = new HashMap<>();
            source.put("kafka_broker_list", "127.0.0.1:9098");
            source.put("kafka_topic", "test_topic");
            source.put("property.security.protocol", "SASL_SSL");
            source.put("property.sasl.mechanism", "OAUTHBEARER");
            source.put(prefix + "aws.region", "us-east-1");
            source.put(prefix + "aws.role_arn", "arn:aws:iam::123456789012:role/test");
            KafkaDataSourceProperties properties = new KafkaDataSourceProperties(source);
            properties.setTimezone("UTC");
            properties.analyze();
            Assertions.assertEquals("us-east-1", properties.getCustomKafkaProperties().get("aws.region"));
        }
    }

    @Test
    public void testDisableKerberosCommandForOlderBackends() throws Exception {
        properties("sasl.kerberos.min.time.before.relogin", "0", false).analyze();
        for (Map<String, String> custom : List.of(Map.<String, String>of(),
                Map.of("group.id", "test", "sasl.kerberos.principal", "user;unused"),
                Map.of("kafka_default_offsets", "OFFSET_BEGINNING",
                        "kafka_origin_default_offsets", "OFFSET_BEGINNING"))) {
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L,
                    "127.0.0.1:9092", "test_topic", UserIdentity.ADMIN);
            Deencapsulation.setField(job, "customProperties", custom);
            Deencapsulation.invoke(job, "convertCustomProperties", true);
            Assertions.assertEquals("0", job.getConvertedCustomProperties()
                    .get("sasl.kerberos.min.time.before.relogin"));
        }
    }

    @Test
    public void testPersistedPropertiesRejectedBeforeMetadataRequests() {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 1L,
                "127.0.0.1:9092", "test_topic", UserIdentity.ADMIN);
        Deencapsulation.setField(job, "customProperties", Map.of("plugin.library.paths", "unused.so"));
        // Even a previously populated conversion cache must not bypass validation.
        Deencapsulation.setField(job, "convertedCustomProperties", Map.of("group.id", "cached"));
        DdlException error = Assertions.assertThrows(DdlException.class,
                () -> Deencapsulation.invoke(job, "convertCustomProperties", false));
        Assertions.assertTrue(error.getMessage().contains("is not allowed in Routine Load"));
    }

    @Test
    public void testRecoverMigratedCertificateProperties() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        Table table = Mockito.mock(Table.class);
        SmallFileMgr smallFileMgr = Mockito.mock(SmallFileMgr.class);
        String originalSql = "CREATE ROUTINE LOAD test_db.test_job ON test_table COLUMNS(id) "
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"test_topic\", \"property.ssl.ca.location\" = \"/etc/ssl/ca.pem\")";

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME)).thenReturn(catalog);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(env.getSmallFileMgr()).thenReturn(smallFileMgr);
            Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
            Mockito.doReturn(Optional.of(db)).when(catalog).getDb(1L);
            Mockito.doReturn(Optional.of(db)).when(catalog).getDb("test_db");
            Mockito.doReturn(db).when(catalog).getDbOrAnalysisException("test_db");
            Mockito.when(db.getId()).thenReturn(1L);
            Mockito.when(db.getName()).thenReturn("test_db");
            Mockito.doReturn(Optional.of(table)).when(db).getTable(2L);
            Mockito.doReturn(table).when(db).getTableOrAnalysisException("test_table");
            Mockito.when(table.getName()).thenReturn("test_table");
            SmallFile ca = new SmallFile(1L, "kafka", "ca.pem", 3L, "", 0L, "checksum", true);
            Mockito.when(smallFileMgr.getSmallFile(1L, "kafka", "ca.pem", true)).thenReturn(ca);

            // ALTER changes the effective map, but the persisted CREATE SQL still contains the old path.
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 2L,
                    "127.0.0.1:9092", "test_topic", UserIdentity.ADMIN);
            job.setOrigStmt(new OriginStatement(originalSql, 0));
            Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.PAUSED);
            Deencapsulation.setField(job, "customProperties", Map.of("ssl.ca.location", "FILE:ca.pem"));
            KafkaRoutineLoadJob restored = (KafkaRoutineLoadJob) GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(job), RoutineLoadJob.class);

            Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, restored.getState());
            Assertions.assertEquals(-1L, restored.getEndTimestamp());
            Assertions.assertEquals("id", restored.getColumnExprDescs().descs.get(0).getColumnName());
            restored.prepare();
            Assertions.assertEquals("FILE:3:checksum", restored.getConvertedCustomProperties().get("ssl.ca.location"));

            // Recovery also preserves an unmigrated job, while runtime validation still rejects its current path.
            Deencapsulation.setField(job, "customProperties", Map.of("ssl.ca.location", "/etc/ssl/ca.pem"));
            KafkaRoutineLoadJob unmigrated = (KafkaRoutineLoadJob) GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(job), RoutineLoadJob.class);
            Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, unmigrated.getState());
            DdlException error = Assertions.assertThrows(DdlException.class, unmigrated::prepare);
            Assertions.assertTrue(error.getMessage().contains("CREATE FILE"));

            // The recovery path must not weaken CREATE admission for the same SQL.
            ConnectContext ctx = new ConnectContext();
            try {
                ctx.setThreadLocalInfo();
                CreateRoutineLoadCommand command = (CreateRoutineLoadCommand) new NereidsParser()
                        .parseSingle(originalSql);
                AnalysisException createError = Assertions.assertThrows(AnalysisException.class,
                        () -> command.getCreateRoutineLoadInfo().validate(ctx));
                Assertions.assertTrue(createError.getMessage().contains("CREATE FILE"));
            } finally {
                ctx.cleanup();
            }
        }
    }
}
