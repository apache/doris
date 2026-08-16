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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.fileformat.CsvFileFormatProperties;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaConfiguration;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.load.routineload.kinesis.KinesisRoutineLoadJob;
import org.apache.doris.nereids.load.NereidsRoutineLoadTaskInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateRoutineLoadInfo;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;
import org.apache.doris.thrift.TUniqueKeyUpdateMode;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class RoutineLoadJobPersistenceTest {
    private static final String LEGACY_IMAGE =
            "/upgrade/routine-load/a8928245/routine-load-kafka-image.b64";

    @Test
    public void testDirectStateImageRoundTripDoesNotParseOrigStmt() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1001L, "direct_job", 1002L,
                1003L, "127.0.0.1:9092", "direct_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("this is deliberately not valid SQL", 0);

        Separator columnSeparator = analyzedSeparator("\\x01");
        Separator lineDelimiter = analyzedSeparator("\\n");
        List<ImportColumnDesc> columns = Lists.newArrayList(
                new ImportColumnDesc("source_col"),
                new ImportColumnDesc("mapped_col", new IntLiteral(7L)));
        Expr precedingFilter = predicate(BinaryPredicate.Operator.GT, "source_col", 1L);
        Expr whereExpr = predicate(BinaryPredicate.Operator.LE, "mapped_col", 10L);
        Expr deleteCondition = predicate(BinaryPredicate.Operator.EQ, "delete_flag", 1L);
        PartitionNamesInfo partitions = new PartitionNamesInfo(false, Lists.newArrayList("p1", "p2"));
        job.setRoutineLoadDesc(new RoutineLoadDesc(columnSeparator, lineDelimiter, columns,
                precedingFilter, whereExpr, partitions, deleteCondition, LoadTask.MergeType.MERGE, "seq_col"));

        job.desireTaskConcurrentNum = 5;
        job.maxErrorNum = 17L;
        job.maxBatchIntervalS = 23L;
        job.maxBatchRows = 300001L;
        job.maxBatchSizeBytes = 104857601L;
        job.execMemLimit = 345678901L;
        job.maxFilterRatio = 0.99;
        job.sendBatchParallelism = 99;
        job.loadToSingleTablet = false;
        job.memtableOnSinkNode = true;

        Map<String, String> jobProperties = Maps.newHashMap();
        jobProperties.put(CreateRoutineLoadInfo.MAX_FILTER_RATIO_PROPERTY, "0.25");
        jobProperties.put(CreateRoutineLoadInfo.SEND_BATCH_PARALLELISM, "4");
        jobProperties.put(CreateRoutineLoadInfo.LOAD_TO_SINGLE_TABLET, "true");
        jobProperties.put(CreateRoutineLoadInfo.UNIQUE_KEY_UPDATE_MODE, "UPDATE_FIXED_COLUMNS");
        jobProperties.put(CreateRoutineLoadInfo.PARTIAL_COLUMNS, "true");
        jobProperties.put(CreateRoutineLoadInfo.PARTIAL_UPDATE_NEW_KEY_POLICY, "ERROR");
        jobProperties.put(CsvFileFormatProperties.PROP_ENCLOSE, "\"");
        jobProperties.put(CsvFileFormatProperties.PROP_ESCAPE, "\\");
        jobProperties.put(CsvFileFormatProperties.PROP_EMPTY_FIELD_AS_NULL, "true");
        job.jobProperties = jobProperties;

        JsonObject json = imageJson(job);
        Assert.assertTrue(json.has("ostmt"));
        Assert.assertEquals(LoadTask.MergeType.MERGE.name(), json.get("mt").getAsString());
        for (String key : Lists.newArrayList(
                "pni", "cds", "pf", "we", "cs", "lidel", "sc", "mt", "dc", "eml", "mosn")) {
            Assert.assertTrue("missing direct-state key " + key, json.has(key));
        }
        Assert.assertFalse(json.has("ld"));
        Assert.assertEquals("\\x01", json.getAsJsonObject("cs").get("os").getAsString());
        Assert.assertEquals("\u0001", json.getAsJsonObject("cs").get("s").getAsString());
        Assert.assertEquals("\\n", json.getAsJsonObject("lidel").get("os").getAsString());
        Assert.assertEquals("\n", json.getAsJsonObject("lidel").get("s").getAsString());
        Assert.assertEquals(2, json.getAsJsonObject("cds").getAsJsonArray("des").size());

        RoutineLoadJob restored;
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            restored = imageRoundTrip(job);
            envStatic.verifyNoInteractions();
        }

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, restored.getState());
        Assert.assertEquals(Lists.newArrayList("p1", "p2"), restored.getPartitionNamesInfo().getPartitionNames());
        Assert.assertEquals(2, restored.columnDescs.descs.size());
        Assert.assertEquals("source_col", restored.columnDescs.descs.get(0).getColumnName());
        Assert.assertEquals("mapped_col", restored.columnDescs.descs.get(1).getColumnName());
        Assert.assertNotNull(restored.columnDescs.descs.get(1).getExpr());
        Assert.assertNotNull(restored.getPrecedingFilter());
        Assert.assertNotNull(restored.getWhereExpr());
        Assert.assertEquals("\\x01", restored.getColumnSeparator().getOriSeparator());
        Assert.assertEquals("\u0001", restored.getColumnSeparator().getSeparator());
        Assert.assertEquals("\\n", restored.getLineDelimiter().getOriSeparator());
        Assert.assertEquals("\n", restored.getLineDelimiter().getSeparator());
        Assert.assertEquals("seq_col", restored.getSequenceCol());
        Assert.assertEquals(LoadTask.MergeType.MERGE, restored.getMergeType());
        Assert.assertNotNull(restored.getDeleteCondition());
        Assert.assertEquals(345678901L, restored.getMemLimit());
        Assert.assertTrue(restored.isMemtableOnSinkNode());
        Assert.assertEquals(5, restored.desireTaskConcurrentNum);
        Assert.assertEquals(17L, restored.maxErrorNum);
        Assert.assertEquals(23L, restored.getMaxBatchIntervalS());
        Assert.assertEquals(300001L, restored.getMaxBatchRows());
        Assert.assertEquals(104857601L, restored.getMaxBatchSizeBytes());

        NereidsRoutineLoadTaskInfo taskInfo = restored.toNereidsRoutineLoadTaskInfo();
        Assert.assertEquals(345678901L, taskInfo.getMemLimit());
        Assert.assertEquals(0.25, taskInfo.getMaxFilterRatio(), 0.0);
        Assert.assertEquals(4, taskInfo.getSendBatchParallelism());
        Assert.assertTrue(taskInfo.isLoadToSingleTablet());
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS, taskInfo.getUniqueKeyUpdateMode());
        Assert.assertTrue(taskInfo.isFixedPartialUpdate());
        Assert.assertEquals(TPartialUpdateNewRowPolicy.ERROR, taskInfo.getPartialUpdateNewRowPolicy());
        Assert.assertEquals((byte) '"', taskInfo.getEnclose());
        Assert.assertEquals((byte) '\\', taskInfo.getEscape());
        Assert.assertTrue(taskInfo.getEmptyFieldAsNull());
        Assert.assertTrue(taskInfo.isMemtableOnSinkNode());
        Assert.assertEquals(LoadTask.MergeType.MERGE, taskInfo.getMergeType());
        Assert.assertNotNull(taskInfo.getDeleteCondition());
        Assert.assertEquals("seq_col", taskInfo.getSequenceCol());
        Assert.assertEquals(Lists.newArrayList("p1", "p2"),
                taskInfo.getPartitionNamesInfo().getPartitionNames());
        Assert.assertEquals(2, taskInfo.getColumnExprDescs().descs.size());
        Assert.assertNotNull(taskInfo.getPrecedingFilter());
        Assert.assertNotNull(taskInfo.getWhereExpr());
        Assert.assertEquals("\u0001", taskInfo.getColumnSeparator().getSeparator());
        Assert.assertEquals("\n", taskInfo.getLineDelimiter().getSeparator());
    }

    @Test
    public void testDirectStateImageWithNoLoadClausesDoesNotFallback() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(2001L, "empty_job", 2002L,
                2003L, "127.0.0.1:9092", "empty_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("also not valid SQL", 0);

        JsonObject json = imageJson(job);
        Assert.assertTrue(json.has("ostmt"));
        Assert.assertEquals(LoadTask.MergeType.APPEND.name(), json.get("mt").getAsString());
        for (String key : Lists.newArrayList("pni", "cds", "pf", "we", "cs", "lidel", "sc", "dc")) {
            Assert.assertFalse("unexpected nullable direct-state key " + key, json.has(key));
        }

        RoutineLoadJob restored;
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            restored = imageRoundTrip(job);
            envStatic.verifyNoInteractions();
        }

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, restored.getState());
        Assert.assertNull(restored.getPartitionNamesInfo());
        Assert.assertNull(restored.columnDescs);
        Assert.assertNull(restored.getPrecedingFilter());
        Assert.assertNull(restored.getWhereExpr());
        Assert.assertNull(restored.getColumnSeparator());
        Assert.assertNull(restored.getLineDelimiter());
        Assert.assertNull(restored.getSequenceCol());
        Assert.assertNull(restored.getDeleteCondition());
        Assert.assertEquals(LoadTask.MergeType.APPEND, restored.getMergeType());
    }

    @Test
    public void testLegacyImageMigratesOnce() throws Exception {
        Env env = Mockito.mock(Env.class);
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(env.getInternalCatalog()).thenReturn(catalog);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        Mockito.when(catalogMgr.getCatalog(Mockito.anyString())).thenReturn(catalog);
        Mockito.when(catalog.getDb(8001L)).thenReturn(Optional.of(database));
        Mockito.when(catalog.getDb("legacy_db")).thenReturn(Optional.of(database));
        Mockito.when(catalog.getDbOrAnalysisException("legacy_db")).thenReturn(database);
        Mockito.when(database.getName()).thenReturn("legacy_db");
        Mockito.when(database.getTable(9001L)).thenReturn(Optional.of((Table) table));
        Mockito.when(database.getTableOrAnalysisException("current_table")).thenReturn(table);
        Mockito.when(table.getName()).thenReturn("current_table");
        Mockito.when(table.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(table.getEnableUniqueKeyMergeOnWrite()).thenReturn(true);

        byte[] legacyImage = loadBase64Fixture(LEGACY_IMAGE);
        JsonObject legacyJson = imageJson(legacyImage);
        Assert.assertFalse(legacyJson.has("mt"));
        Assert.assertTrue(legacyJson.has("ostmt"));

        RoutineLoadJob migrated;
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
            migrated = readImage(legacyImage);
        }

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, migrated.getState());
        Assert.assertEquals("|", migrated.getColumnSeparator().getOriSeparator());
        Assert.assertEquals("|", migrated.getColumnSeparator().getSeparator());
        Assert.assertNull(migrated.getSequenceCol());
        Assert.assertEquals(33554432L, migrated.getMemLimit());
        Assert.assertEquals(0.25, migrated.getMaxFilterRatio(), 0.0);
        Assert.assertEquals(3, migrated.getSendBatchParallelism());
        Assert.assertTrue(migrated.isLoadToSingleTablet());
        Assert.assertEquals(TUniqueKeyUpdateMode.UPDATE_FIXED_COLUMNS, migrated.getUniqueKeyUpdateMode());
        Assert.assertTrue(migrated.isFixedPartialUpdate());
        Assert.assertEquals(TPartialUpdateNewRowPolicy.ERROR, migrated.partialUpdateNewKeyPolicy);
        Assert.assertEquals((byte) '"', migrated.getEnclose());
        Assert.assertEquals((byte) '\\', migrated.getEscape());
        Assert.assertTrue(migrated.getEmptyFieldAsNull());
        Assert.assertFalse(migrated.isMemtableOnSinkNode());

        JsonObject migratedJson = imageJson(migrated);
        Assert.assertTrue(migratedJson.has("ostmt"));
        Assert.assertEquals(LoadTask.MergeType.APPEND.name(), migratedJson.get("mt").getAsString());
        Assert.assertTrue(migratedJson.has("cs"));
        Assert.assertTrue(migratedJson.has("eml"));
        Assert.assertTrue(migratedJson.has("mosn"));
        migrated.origStmt = new OriginStatement("invalid after successful migration", 0);

        RoutineLoadJob restoredAgain;
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            restoredAgain = imageRoundTrip(migrated);
            envStatic.verifyNoInteractions();
        }
        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, restoredAgain.getState());
        Assert.assertEquals("|", restoredAgain.getColumnSeparator().getSeparator());
        Assert.assertEquals(33554432L, restoredAgain.getMemLimit());
        Assert.assertFalse(restoredAgain.isMemtableOnSinkNode());
    }

    @Test
    public void testKafkaDerivedStateIsRebuiltFromDurableProperties() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(3001L, "kafka_derived", 3002L,
                3003L, "127.0.0.1:9092", "derived_topic", UserIdentity.ADMIN);
        job.origStmt = new OriginStatement("invalid SQL must stay unused", 0);
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put("client.id", "durable-client");
        customProperties.put(KafkaConfiguration.KAFKA_ORIGIN_DEFAULT_OFFSETS.getName(), "OFFSET_BEGINNING");
        Deencapsulation.setField(job, "customProperties", customProperties);
        Deencapsulation.setField(job, "customKafkaPartitions", Lists.newArrayList(9));
        Deencapsulation.setField(job, "currentKafkaPartitions", Lists.newArrayList(1, 2));
        Deencapsulation.setField(job, "convertedCustomProperties",
                Maps.newHashMap(ImmutableMap.of("stale", "value")));
        Deencapsulation.setField(job, "cachedPartitionWithLatestOffsets",
                Maps.newHashMap(ImmutableMap.of(1, 100L)));
        Deencapsulation.setField(job, "newCurrentKafkaPartition", Lists.newArrayList(3));
        Deencapsulation.setField(job, "kafkaDefaultOffSet", "OFFSET_END");

        JsonObject json = imageJson(job);
        Assert.assertEquals("127.0.0.1:9092", json.get("bl").getAsString());
        Assert.assertEquals("derived_topic", json.get("tp").getAsString());
        Assert.assertEquals("durable-client", json.getAsJsonObject("prop").get("client.id").getAsString());
        Assert.assertEquals(1, json.getAsJsonArray("cskp").size());
        assertNoJavaFieldNames(json, "currentKafkaPartitions", "convertedCustomProperties",
                "cachedPartitionWithLatestOffsets", "newCurrentKafkaPartition", "kafkaDefaultOffSet");

        KafkaRoutineLoadJob restored = (KafkaRoutineLoadJob) imageRoundTrip(job);
        Assert.assertEquals("127.0.0.1:9092", restored.getBrokerList());
        Assert.assertEquals("derived_topic", restored.getTopic());
        Assert.assertEquals(Lists.newArrayList(9), Deencapsulation.getField(restored, "customKafkaPartitions"));
        Assert.assertTrue(((List<?>) Deencapsulation.getField(restored, "currentKafkaPartitions")).isEmpty());
        Assert.assertTrue(restored.getConvertedCustomProperties().isEmpty());
        Assert.assertTrue(((Map<?, ?>) Deencapsulation.getField(
                restored, "cachedPartitionWithLatestOffsets")).isEmpty());
        Assert.assertEquals("", Deencapsulation.getField(restored, "kafkaDefaultOffSet"));

        Env env = Mockito.mock(Env.class);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            restored.prepare();
        }
        Assert.assertEquals("durable-client", restored.getConvertedCustomProperties().get("client.id"));
        Assert.assertFalse(restored.getConvertedCustomProperties().containsKey(
                KafkaConfiguration.KAFKA_ORIGIN_DEFAULT_OFFSETS.getName()));
        Assert.assertEquals("OFFSET_BEGINNING", Deencapsulation.getField(restored, "kafkaDefaultOffSet"));
    }

    @Test
    public void testKinesisDerivedStateIsRebuiltFromDurableProperties() throws Exception {
        KinesisRoutineLoadJob job = new KinesisRoutineLoadJob(4001L, "kinesis_derived", 4002L,
                4003L, "us-east-1", "derived_stream", UserIdentity.ADMIN);
        job.origStmt = new OriginStatement("invalid SQL must stay unused", 0);
        Deencapsulation.setField(job, "endpoint", "https://kinesis.example.test");
        Map<String, String> customProperties = Maps.newHashMap();
        customProperties.put("client.setting", "durable-value");
        customProperties.put("kinesis_default_pos", "TRIM_HORIZON");
        Deencapsulation.setField(job, "customProperties", customProperties);
        Deencapsulation.setField(job, "customKinesisShards", Lists.newArrayList("custom-shard"));
        Deencapsulation.setField(job, "openKinesisShards", Lists.newArrayList("open-shard"));
        Deencapsulation.setField(job, "closedKinesisShards", Lists.newArrayList("closed-shard"));
        Deencapsulation.setField(job, "convertedCustomProperties",
                Maps.newHashMap(ImmutableMap.of("stale", "value")));
        Deencapsulation.setField(job, "cachedShardWithMillsBehindLatest",
                Maps.newHashMap(ImmutableMap.of("open-shard", 99L)));
        Deencapsulation.setField(job, "newCurrentKinesisShards", Lists.newArrayList("new-shard"));
        Deencapsulation.setField(job, "kinesisDefaultPosition", "LATEST");

        JsonObject json = imageJson(job);
        Assert.assertEquals("us-east-1", json.get("rg").getAsString());
        Assert.assertEquals("derived_stream", json.get("stm").getAsString());
        Assert.assertEquals("https://kinesis.example.test", json.get("ep").getAsString());
        Assert.assertEquals("durable-value",
                json.getAsJsonObject("prop").get("client.setting").getAsString());
        Assert.assertEquals("custom-shard", json.getAsJsonArray("csks").get(0).getAsString());
        Assert.assertEquals("open-shard", json.getAsJsonArray("opks").get(0).getAsString());
        Assert.assertEquals("closed-shard", json.getAsJsonArray("clks").get(0).getAsString());
        assertNoJavaFieldNames(json, "convertedCustomProperties", "cachedShardWithMillsBehindLatest",
                "newCurrentKinesisShards", "kinesisDefaultPosition");

        KinesisRoutineLoadJob restored = (KinesisRoutineLoadJob) imageRoundTrip(job);
        Assert.assertEquals("us-east-1", restored.getRegion());
        Assert.assertEquals("derived_stream", restored.getStream());
        Assert.assertEquals("https://kinesis.example.test", restored.getEndpoint());
        Assert.assertEquals(Lists.newArrayList("custom-shard"),
                Deencapsulation.getField(restored, "customKinesisShards"));
        Assert.assertEquals(Lists.newArrayList("open-shard"),
                Deencapsulation.getField(restored, "openKinesisShards"));
        Assert.assertEquals(Lists.newArrayList("closed-shard"),
                Deencapsulation.getField(restored, "closedKinesisShards"));
        Assert.assertTrue(restored.getConvertedCustomProperties().isEmpty());
        Assert.assertTrue(((Map<?, ?>) Deencapsulation.getField(
                restored, "cachedShardWithMillsBehindLatest")).isEmpty());
        Assert.assertTrue(((List<?>) Deencapsulation.getField(restored, "newCurrentKinesisShards")).isEmpty());
        Assert.assertEquals("", Deencapsulation.getField(restored, "kinesisDefaultPosition"));

        restored.prepare();
        Assert.assertEquals("durable-value", restored.getConvertedCustomProperties().get("client.setting"));
        Assert.assertEquals("TRIM_HORIZON",
                restored.getConvertedCustomProperties().get("kinesis_default_pos"));
        Assert.assertEquals("TRIM_HORIZON", Deencapsulation.getField(restored, "kinesisDefaultPosition"));
    }

    private static Separator analyzedSeparator(String value) throws Exception {
        Separator separator = new Separator(value);
        separator.analyze();
        return separator;
    }

    private static Expr predicate(BinaryPredicate.Operator operator, String column, long value) {
        return new BinaryPredicate(operator, new SlotRef(null, column), new IntLiteral(value));
    }

    private static JsonObject imageJson(RoutineLoadJob job) throws IOException {
        return imageJson(writeImage(job));
    }

    private static JsonObject imageJson(byte[] image) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(image))) {
            return JsonParser.parseString(Text.readString(in)).getAsJsonObject();
        }
    }

    private static RoutineLoadJob imageRoundTrip(RoutineLoadJob job) throws IOException {
        return readImage(writeImage(job));
    }

    private static byte[] writeImage(RoutineLoadJob job) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            job.write(out);
        }
        return bytes.toByteArray();
    }

    private static RoutineLoadJob readImage(byte[] image) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(image))) {
            return RoutineLoadJob.read(in);
        }
    }

    private static byte[] loadBase64Fixture(String resource) throws IOException {
        try (InputStream in = RoutineLoadJobPersistenceTest.class.getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("missing fixture " + resource);
            }
            String base64 = new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
            return Base64.getDecoder().decode(base64);
        }
    }

    private static void assertNoJavaFieldNames(JsonObject json, String... fieldNames) {
        for (String fieldName : fieldNames) {
            Assert.assertFalse("derived field leaked into image: " + fieldName, json.has(fieldName));
        }
    }
}
