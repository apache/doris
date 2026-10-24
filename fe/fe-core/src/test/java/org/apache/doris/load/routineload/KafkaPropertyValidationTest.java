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
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.SmallFileMgr;
import org.apache.doris.common.util.SmallFileMgr.SmallFile;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.kafka.KafkaUtil;
import org.apache.doris.load.routineload.kafka.KafkaDataSourceProperties;
import org.apache.doris.load.routineload.kafka.KafkaProgress;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.AlterRoutineLoadCommand;
import org.apache.doris.nereids.trees.plans.commands.load.CreateRoutineLoadCommand;
import org.apache.doris.persist.AlterRoutineLoadJobOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.RoutineLoadOperation;
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
    public void testOidcCredentialAliasesCreateAndAlter() throws Exception {
        for (boolean alter : new boolean[] {false, true}) {
            Map<String, String> source = new HashMap<>();
            source.put("kafka_broker_list", "127.0.0.1:9092");
            source.put("kafka_topic", "test_topic");
            oidcAliasProperties().forEach((key, value) -> source.put("property." + key, value));
            KafkaDataSourceProperties properties = new KafkaDataSourceProperties(source);
            properties.setAlter(alter);
            properties.setTimezone("UTC");
            properties.analyze();
            oidcAliasProperties().forEach((key, value) ->
                    Assertions.assertEquals(value, properties.getCustomKafkaProperties().get(key)));
        }
    }

    private Map<String, String> oidcAliasProperties() {
        return Map.of("security.protocol", "SASL_SSL",
                "sasl.mechanism", "OAUTHBEARER",
                "sasl.oauthbearer.method", "oidc",
                "sasl.oauthbearer.client.credentials.client.id", "test-client",
                "sasl.oauthbearer.client.credentials.client.secret", "test-secret",
                "sasl.oauthbearer.token.endpoint.url", "https://issuer.example/token");
    }

    @Test
    public void testRecoverAndAlterJobWithOidcCredentialAliases() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database db = Mockito.mock(Database.class);
        Table table = Mockito.mock(Table.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        String originalSql = "CREATE ROUTINE LOAD test_db.test_job ON test_table COLUMNS(id) "
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", \"kafka_topic\" = \"test_topic\", "
                + "\"property.security.protocol\" = \"SASL_SSL\", \"property.sasl.mechanism\" = \"OAUTHBEARER\", "
                + "\"property.sasl.oauthbearer.method\" = \"oidc\", "
                + "\"property.sasl.oauthbearer.client.credentials.client.id\" = \"test-client\", "
                + "\"property.sasl.oauthbearer.client.credentials.client.secret\" = \"test-secret\", "
                + "\"property.sasl.oauthbearer.token.endpoint.url\" = \"https://issuer.example/token\")";

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
            Mockito.when(catalogMgr.getCatalog(InternalCatalog.INTERNAL_CATALOG_NAME)).thenReturn(catalog);
            Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(catalog.getName()).thenReturn(InternalCatalog.INTERNAL_CATALOG_NAME);
            Mockito.doReturn(Optional.of(db)).when(catalog).getDb(1L);
            Mockito.doReturn(Optional.of(db)).when(catalog).getDb("test_db");
            Mockito.doReturn(db).when(catalog).getDbOrAnalysisException("test_db");
            Mockito.when(db.getId()).thenReturn(1L);
            Mockito.when(db.getName()).thenReturn("test_db");
            Mockito.doReturn(Optional.of(table)).when(db).getTable(2L);
            Mockito.doReturn(table).when(db).getTableOrAnalysisException("test_table");
            Mockito.when(table.getName()).thenReturn("test_table");

            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 2L,
                    "127.0.0.1:9092", "test_topic", UserIdentity.ADMIN);
            job.setOrigStmt(new OriginStatement(originalSql, 0));
            Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.PAUSED);
            Deencapsulation.setField(job, "customProperties", new HashMap<>(oidcAliasProperties()));
            KafkaRoutineLoadJob restored = (KafkaRoutineLoadJob) GsonUtils.GSON.fromJson(
                    GsonUtils.GSON.toJson(job), RoutineLoadJob.class);
            Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, restored.getState());
            Assertions.assertEquals(-1L, restored.getEndTimestamp());
            restored.prepare();
            oidcAliasProperties().forEach((key, value) ->
                    Assertions.assertEquals(value, restored.getConvertedCustomProperties().get(key)));

            // Adding short names must not make the retained historical aliases fail validation.
            KafkaDataSourceProperties update = properties("sasl.oauthbearer.client.secret", "test-secret", true);
            update.analyze();
            AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
            Mockito.when(command.getAnalyzedJobProperties()).thenReturn(Map.of());
            Mockito.when(command.getDataSourceProperties()).thenReturn(update);
            restored.modifyProperties(command);
            Assertions.assertEquals("test-secret", restored.getConvertedCustomProperties()
                    .get("sasl.oauthbearer.client.credentials.client.secret"));
            Assertions.assertEquals("test-secret", restored.getConvertedCustomProperties()
                    .get("sasl.oauthbearer.client.secret"));
            Assertions.assertEquals("******", restored.getCustomProperties()
                    .get("property.sasl.oauthbearer.client.credentials.client.secret"));
            Assertions.assertFalse(restored.customPropertiesJsonToString().contains("test-secret"));
            Mockito.verify(editLog).logAlterRoutineLoadJob(Mockito.any(AlterRoutineLoadJobOperationLog.class));
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

    // Build an already analyzed, persisted ALTER from an older FE without applying
    // today's CREATE/ALTER admission rules to the historical input.
    private AlterRoutineLoadJobOperationLog historicalAlter(Map<String, String> custom,
            String topic, String brokers, List<Pair<Integer, Long>> offsets) {
        Map<String, String> source = new HashMap<>();
        custom.forEach((key, value) -> source.put("property." + key, value));
        source.put("kafka_topic", topic);
        source.put("kafka_broker_list", brokers);
        KafkaDataSourceProperties properties = new KafkaDataSourceProperties(source);
        properties.setAlter(true);
        Deencapsulation.setField(properties, "dataSourceType", "KAFKA");
        Deencapsulation.setField(properties, "customKafkaProperties", custom);
        Deencapsulation.setField(properties, "topic", topic);
        Deencapsulation.setField(properties, "brokerList", brokers);
        properties.setKafkaPartitionOffsets(offsets);
        AlterRoutineLoadJobOperationLog log = new AlterRoutineLoadJobOperationLog(1L,
                Map.of("max_batch_rows", "2000000"), properties);
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(log), AlterRoutineLoadJobOperationLog.class);
    }

    @Test
    public void testReplayHistoricalAlterRestoresCompleteState() throws Exception {
        for (boolean cloud : new boolean[] {false, true}) {
            try (MockedStatic<Config> config = Mockito.mockStatic(Config.class);
                    MockedStatic<Env> env = Mockito.mockStatic(Env.class);
                    MockedStatic<MetaServiceProxy> metaService = Mockito.mockStatic(MetaServiceProxy.class);
                    MockedStatic<KafkaUtil> kafka = Mockito.mockStatic(KafkaUtil.class)) {
                config.when(Config::isCloudMode).thenReturn(cloud);
                KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 2L,
                        "127.0.0.1:9092", "topic_a", UserIdentity.ADMIN);
                Deencapsulation.setField(job, "progress", new KafkaProgress(Map.of(0, 5L, 1, 6L)));
                job.prepare();

                AlterRoutineLoadJobOperationLog log = historicalAlter(
                        Map.of("sasl.kerberos.min.time.before.relogin", "60000"),
                        "topic_b", "127.0.0.2:9092", List.of(Pair.of(0, 100L)));
                job.replayModifyProperties(log);

                Assertions.assertEquals("topic_b", job.getTopic());
                Assertions.assertEquals("127.0.0.2:9092", job.getBrokerList());
                Assertions.assertEquals(Map.of(0, 100L),
                        ((KafkaProgress) job.getProgress()).getOffsetByPartition());
                Assertions.assertEquals(2000000L, job.getMaxBatchRows());
                Assertions.assertEquals("60000", job.getCustomProperties()
                        .get("property.sasl.kerberos.min.time.before.relogin"));
                Assertions.assertTrue(job.getConvertedCustomProperties().isEmpty());

                DdlException error = Assertions.assertThrows(DdlException.class, job::prepare);
                Assertions.assertTrue(error.getMessage().contains("Kerberos tickets must be managed externally"));
                Assertions.assertThrows(DdlException.class,
                        () -> Deencapsulation.invoke(job, "getAllKafkaPartitions"));
                Assertions.assertThrows(DdlException.class,
                        () -> Deencapsulation.invoke(job, "convertOffset", log.getDataSourceProperties()));

                // A later committed repair must retain topic B and its offsets.
                job.replayModifyProperties(historicalAlter(
                        Map.of("sasl.kerberos.min.time.before.relogin", "0"), "", "", List.of()));
                job.prepare();
                Assertions.assertEquals("topic_b", job.getTopic());
                Assertions.assertEquals("127.0.0.2:9092", job.getBrokerList());
                Assertions.assertEquals(Map.of(0, 100L),
                        ((KafkaProgress) job.getProgress()).getOffsetByPartition());
                Assertions.assertEquals("0", job.getConvertedCustomProperties()
                        .get("sasl.kerberos.min.time.before.relogin"));
                env.verifyNoInteractions();
                metaService.verifyNoInteractions();
                kafka.verifyNoInteractions();
            }
        }
    }

    @Test
    public void testReplayDefersManagedFileResolution() throws Exception {
        Env env = Mockito.mock(Env.class);
        SmallFileMgr smallFileMgr = Mockito.mock(SmallFileMgr.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getSmallFileMgr()).thenReturn(smallFileMgr);
            Mockito.when(smallFileMgr.getSmallFile(1L, "kafka", "missing.pem", true))
                    .thenThrow(new DdlException("missing managed file"));
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 2L,
                    "127.0.0.1:9092", "topic_a", UserIdentity.ADMIN);

            // Partition 3 need not exist in the pre-replay progress. These offsets
            // were checked when the ALTER was committed, not during recovery.
            job.replayModifyProperties(historicalAlter(Map.of("ssl.ca.location", "FILE:missing.pem"),
                    "topic_b", "127.0.0.2:9092", List.of(Pair.of(3, 100L))));
            Assertions.assertEquals("topic_b", job.getTopic());
            Assertions.assertEquals(Map.of(3, 100L),
                    ((KafkaProgress) job.getProgress()).getOffsetByPartition());
            Assertions.assertEquals(2000000L, job.getMaxBatchRows());
            envStatic.verifyNoInteractions();
            Mockito.verifyNoInteractions(smallFileMgr);

            DdlException error = Assertions.assertThrows(DdlException.class, job::prepare);
            Assertions.assertTrue(error.getMessage().contains("missing managed file"));
            Assertions.assertTrue(job.getConvertedCustomProperties().isEmpty());
        }
    }

    @Test
    public void testSchedulerPausesReplayedJobWithRejectedProperties() throws Exception {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        RoutineLoadManager manager = Mockito.mock(RoutineLoadManager.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<KafkaUtil> kafka = Mockito.mockStatic(KafkaUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 2L,
                    "127.0.0.1:9092", "topic_a", UserIdentity.ADMIN);
            job.replayModifyProperties(historicalAlter(Map.of("plugin.library.paths", "unused.so"),
                    "topic_b", "127.0.0.2:9092", List.of()));
            Mockito.when(manager.getRoutineLoadJobByState(Mockito.anySet())).thenReturn(List.of(job));

            Deencapsulation.invoke(new RoutineLoadScheduler(manager), "process");

            Assertions.assertEquals(RoutineLoadJob.JobState.PAUSED, job.getState());
            Assertions.assertTrue(job.getPauseReason().getMsg().contains("plugin.library.paths"));
            Assertions.assertEquals("topic_b", job.getTopic());
            kafka.verifyNoInteractions();
            Mockito.verify(editLog).logOpRoutineLoadJob(Mockito.any(RoutineLoadOperation.class));
        }
    }

    @Test
    public void testLiveAlterPublishesOnlyPreparedProperties() throws Exception {
        Env env = Mockito.mock(Env.class);
        SmallFileMgr smallFileMgr = Mockito.mock(SmallFileMgr.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getSmallFileMgr()).thenReturn(smallFileMgr);
            Mockito.when(smallFileMgr.getSmallFile(1L, "kafka", "missing.pem", true))
                    .thenThrow(new DdlException("missing managed file"));
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 2L,
                    "127.0.0.1:9092", "topic_a", UserIdentity.ADMIN);
            Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.PAUSED);
            job.prepare();
            Map<String, String> converted = new HashMap<>(job.getConvertedCustomProperties());
            AlterRoutineLoadJobOperationLog log = historicalAlter(Map.of("ssl.ca.location", "FILE:missing.pem"),
                    "topic_b", "127.0.0.2:9092", List.of());
            AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
            Mockito.when(command.getAnalyzedJobProperties()).thenReturn(log.getJobProperties());
            Mockito.when(command.getDataSourceProperties()).thenReturn(log.getDataSourceProperties());

            Assertions.assertThrows(DdlException.class, () -> job.modifyProperties(command));
            Assertions.assertEquals("topic_a", job.getTopic());
            Assertions.assertEquals("127.0.0.1:9092", job.getBrokerList());
            Assertions.assertTrue(job.getCustomProperties().isEmpty());
            Assertions.assertEquals(converted, job.getConvertedCustomProperties());
            Mockito.verify(env, Mockito.never()).getEditLog();

            KafkaDataSourceProperties safe = properties("sasl.kerberos.min.time.before.relogin", "0", true);
            safe.analyze();
            Mockito.when(command.getDataSourceProperties()).thenReturn(safe);
            EditLog editLog = Mockito.mock(EditLog.class);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            job.modifyProperties(command);
            Assertions.assertEquals(Map.of("property.sasl.kerberos.min.time.before.relogin", "0",
                    "property.kafka_default_offsets", KafkaProgress.OFFSET_END),
                    job.getCustomProperties());
            Assertions.assertEquals("0", job.getConvertedCustomProperties()
                    .get("sasl.kerberos.min.time.before.relogin"));
            Assertions.assertEquals(2000000L, job.getMaxBatchRows());
            Mockito.verify(editLog).logAlterRoutineLoadJob(Mockito.any(AlterRoutineLoadJobOperationLog.class));
        }
    }

    @Test
    public void testJobOnlyAlterPreservesDatetimeDefaultForNewPartitions() throws Exception {
        Env env = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<KafkaUtil> kafka = Mockito.mockStatic(KafkaUtil.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 2L,
                    "127.0.0.1:9092", "topic_a", UserIdentity.ADMIN);
            String datetime = "2026-10-01 00:00:00";
            KafkaDataSourceProperties source = properties("kafka_default_offsets", datetime, false);
            source.analyze();
            Deencapsulation.setField(job, "customProperties", source.getCustomKafkaProperties());
            Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.PAUSED);
            job.prepare();

            AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
            Mockito.when(command.getAnalyzedJobProperties()).thenReturn(Map.of("max_batch_rows", "2000000"));
            job.modifyProperties(command);
            job.modifyProperties(command);
            Assertions.assertEquals(datetime, Deencapsulation.getField(job, "kafkaDefaultOffSet"));
            Assertions.assertFalse(job.getConvertedCustomProperties().containsKey("kafka_default_offsets"));
            Assertions.assertFalse(job.getConvertedCustomProperties().containsKey("kafka_origin_default_offsets"));

            // New partitions are initialized before prepare() rebuilds the cache on resume.
            long timestamp = TimeUtils.timeStringToLong(datetime, TimeUtils.getOrSystemTimeZone(job.getTimezone()));
            List<Pair<Integer, Long>> requested = List.of(Pair.of(1, timestamp));
            List<Pair<Integer, Long>> expected = List.of(Pair.of(1, 42L));
            kafka.when(() -> KafkaUtil.getOffsetsForTimes("127.0.0.1:9092", "topic_a",
                    job.getConvertedCustomProperties(), requested, null)).thenReturn(expected);
            List<Pair<Integer, Long>> offsets = Deencapsulation.invoke(job,
                    "getNewPartitionOffsetsFromDefaultOffset", List.of(1));
            Assertions.assertEquals(expected, offsets);
            kafka.verify(() -> KafkaUtil.getOffsetsForTimes("127.0.0.1:9092", "topic_a",
                    job.getConvertedCustomProperties(), requested, null));
            kafka.verifyNoMoreInteractions();
        }
    }

    @Test
    public void testRepairCertificateAndOffsetsInSameAlter() throws Exception {
        for (boolean offsetsForTimes : new boolean[] {false, true}) {
            Env env = Mockito.mock(Env.class);
            SmallFileMgr smallFileMgr = Mockito.mock(SmallFileMgr.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                    MockedStatic<KafkaUtil> kafka = Mockito.mockStatic(KafkaUtil.class)) {
                envStatic.when(Env::getCurrentEnv).thenReturn(env);
                Mockito.when(env.getSmallFileMgr()).thenReturn(smallFileMgr);
                Mockito.when(env.getEditLog()).thenReturn(editLog);
                SmallFile ca = new SmallFile(1L, "kafka", "ca.pem", 3L, "", 0L, "checksum", true);
                Mockito.when(smallFileMgr.getSmallFile(1L, "kafka", "ca.pem", true)).thenReturn(ca);
                KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1L, "test_job", 1L, 2L,
                        "127.0.0.1:9092", "topic_a", UserIdentity.ADMIN);
                Deencapsulation.setField(job, "customProperties",
                        new HashMap<>(Map.of("ssl.ca.location", "/etc/ca.pem")));
                Deencapsulation.setField(job, "progress", new KafkaProgress(Map.of(0, 5L)));
                Deencapsulation.setField(job, "state", RoutineLoadJob.JobState.PAUSED);

                Map<String, String> input = Map.of("property.ssl.ca.location", "FILE:ca.pem",
                        "kafka_partitions", "0", "kafka_offsets", offsetsForTimes ? "2026-10-01 00:00:00" : "100");
                KafkaDataSourceProperties source = new KafkaDataSourceProperties(input);
                source.setAlter(true);
                source.setTimezone("UTC");
                source.analyze();
                Map<String, String> converted = Map.of("ssl.ca.location", "FILE:3:checksum",
                        "sasl.kerberos.min.time.before.relogin", "0");
                List<Pair<Integer, Long>> requested = source.getKafkaPartitionOffsets();
                List<Pair<Integer, Long>> expected = List.of(Pair.of(0, 100L));
                if (offsetsForTimes) {
                    kafka.when(() -> KafkaUtil.getOffsetsForTimes("127.0.0.1:9092", "topic_a",
                            converted, requested, null)).thenThrow(new LoadException("metadata unavailable"))
                            .thenReturn(expected);
                } else {
                    kafka.when(() -> KafkaUtil.getRealOffsets("127.0.0.1:9092", "topic_a",
                            converted, requested, null)).thenThrow(new LoadException("metadata unavailable"))
                            .thenCallRealMethod();
                }
                AlterRoutineLoadCommand command = Mockito.mock(AlterRoutineLoadCommand.class);
                Mockito.when(command.getAnalyzedJobProperties()).thenReturn(Map.of());
                Mockito.when(command.getDataSourceProperties()).thenReturn(source);

                Assertions.assertThrows(LoadException.class, () -> job.modifyProperties(command));
                Assertions.assertEquals("/etc/ca.pem", job.getCustomProperties().get("property.ssl.ca.location"));
                Assertions.assertTrue(job.getConvertedCustomProperties().isEmpty());
                Assertions.assertEquals(Map.of(0, 5L), ((KafkaProgress) job.getProgress()).getOffsetByPartition());
                Mockito.verifyNoInteractions(editLog);

                job.modifyProperties(command);

                Assertions.assertEquals("FILE:ca.pem", job.getCustomProperties().get("property.ssl.ca.location"));
                Assertions.assertEquals(converted, job.getConvertedCustomProperties());
                Assertions.assertEquals(Map.of(0, 100L), ((KafkaProgress) job.getProgress()).getOffsetByPartition());
                if (offsetsForTimes) {
                    kafka.verify(() -> KafkaUtil.getOffsetsForTimes("127.0.0.1:9092", "topic_a",
                            converted, requested, null), Mockito.times(2));
                } else {
                    kafka.verify(() -> KafkaUtil.getRealOffsets("127.0.0.1:9092", "topic_a",
                            converted, requested, null), Mockito.times(2));
                }
                kafka.verifyNoMoreInteractions();
                Mockito.verify(editLog).logAlterRoutineLoadJob(Mockito.any(AlterRoutineLoadJobOperationLog.class));
            }
        }
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
