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

import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.io.Text;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Lists;
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
import java.util.Optional;

public class RoutineLoadJobPersistenceTest {
    private static final String LEGACY_IMAGE =
            "/upgrade/routine-load/a8928245/routine-load-kafka-image.b64";

    @Test
    public void testImageRestoresLoadDefinitionFromOrigStmt() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1001L, "image_job", 8001L,
                9001L, "127.0.0.1:9092", "image_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("CREATE ROUTINE LOAD legacy_db.image_job ON stale_table "
                + "COLUMNS TERMINATED BY '|', "
                + "COLUMNS(source_col, mapped_col = source_col + 1), "
                + "PRECEDING FILTER source_col > 1, WHERE mapped_col <= 10 "
                + "PROPERTIES (\"exec_mem_limit\" = \"345678901\") "
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"image_topic\")", 0);

        job.setRoutineLoadDesc(new RoutineLoadDesc(new Separator(",", ","), analyzedSeparator("\\n"),
                Lists.newArrayList(new ImportColumnDesc("wrong_column")),
                null, null, null, null, LoadTask.MergeType.APPEND, null));
        job.memtableOnSinkNode = true;

        JsonObject json = imageJson(job);
        Assert.assertTrue(json.has("ostmt"));
        Assert.assertTrue(json.has("mosn"));
        Assert.assertTrue(json.has("lidel"));
        for (String key : Lists.newArrayList("pni", "cds", "pf", "we", "cs", "sc", "mt", "dc", "eml")) {
            Assert.assertFalse("load definition must only be persisted through origStmt: " + key, json.has(key));
        }

        RoutineLoadJob restored;
        try (MockedStatic<Env> ignored = mockCatalog()) {
            restored = imageRoundTrip(job);
        }

        Assert.assertEquals("|", restored.getColumnSeparator().getSeparator());
        Assert.assertEquals(2, restored.getColumnExprDescs().descs.size());
        Assert.assertEquals("source_col", restored.getColumnExprDescs().descs.get(0).getColumnName());
        Assert.assertEquals("mapped_col", restored.getColumnExprDescs().descs.get(1).getColumnName());
        Assert.assertNotNull(restored.getPrecedingFilter());
        Assert.assertNotNull(restored.getWhereExpr());
        Assert.assertEquals(345678901L, restored.getMemLimit());
        Assert.assertEquals("\n", restored.getLineDelimiter().getSeparator());
        Assert.assertTrue(restored.isMemtableOnSinkNode());
    }

    @Test
    public void testAlterReplayMergesCurrentDefinitionIntoOrigStmt() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(2001L, "alter_job", 8001L,
                9001L, "127.0.0.1:9092", "alter_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("CREATE ROUTINE LOAD legacy_db.alter_job ON current_table WITH MERGE "
                + "COLUMNS TERMINATED BY ',', "
                + "COLUMNS(source_col, mapped_col = source_col + 1), "
                + "PRECEDING FILTER source_col > 1, WHERE mapped_col < 100, "
                + "PARTITION(p1), DELETE ON delete_flag = 1, ORDER BY seq_col "
                + "PROPERTIES (\"exec_mem_limit\" = \"268435456\") "
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"alter_topic\")", 0);
        job.execMemLimit = 268435456L;

        try (MockedStatic<Env> ignored = mockCatalog()) {
            job = (KafkaRoutineLoadJob) imageRoundTrip(job);
            job.replayLoadDefinition(new OriginStatement(
                    "ALTER ROUTINE LOAD FOR alter_job COLUMNS TERMINATED BY '|', WHERE mapped_col < 50", 0));
            job.replayLoadDefinition(new OriginStatement(
                    "ALTER ROUTINE LOAD FOR alter_job "
                            + "PRECEDING FILTER content MATCH_ANY 'hello' USING ANALYZER 'english'", 0));
        }

        Assert.assertTrue(job.origStmt.originStmt.startsWith("CREATE ROUTINE LOAD"));
        Assert.assertTrue(job.origStmt.originStmt.contains("COLUMNS TERMINATED BY \"|\""));
        Assert.assertTrue(job.origStmt.originStmt.contains("COLUMNS("));
        Assert.assertTrue(job.origStmt.originStmt.contains("WHERE"));
        Assert.assertTrue(job.origStmt.originStmt.contains("PRECEDING FILTER"));
        Assert.assertTrue(job.origStmt.originStmt.contains("USING ANALYZER"));
        Assert.assertTrue(job.origStmt.originStmt.contains("PARTITION(`p1`)"));
        Assert.assertTrue(job.origStmt.originStmt.contains("DELETE ON"));
        Assert.assertTrue(job.origStmt.originStmt.contains("ORDER BY `seq_col`"));
        Assert.assertTrue(job.origStmt.originStmt.contains("WITH MERGE"));

        JsonObject expectedProperties = JsonParser.parseString(job.jobPropertiesToJsonString()).getAsJsonObject();
        RoutineLoadJob restored;
        try (MockedStatic<Env> ignored = mockCatalog()) {
            restored = imageRoundTrip(job);
        }
        JsonObject restoredProperties = JsonParser.parseString(restored.jobPropertiesToJsonString()).getAsJsonObject();
        for (String key : Lists.newArrayList("column_separator", "precedingFilter",
                "whereExpr", "partitions", "delete", "sequence_col", "merge_type", "exec_mem_limit")) {
            Assert.assertEquals(key, expectedProperties.get(key), restoredProperties.get(key));
        }
        Assert.assertTrue(restoredProperties.get("columnToColumnExpr").getAsString().contains("mapped_col="));
        Assert.assertEquals(job.origStmt.originStmt, restored.origStmt.originStmt);
    }

    @Test
    public void testLegacyImageContinuesToRestoreFromOrigStmt() throws Exception {
        byte[] legacyImage = loadBase64Fixture(LEGACY_IMAGE);
        JsonObject legacyJson = imageJson(legacyImage);
        Assert.assertTrue(legacyJson.has("ostmt"));
        Assert.assertFalse(legacyJson.has("mt"));

        RoutineLoadJob restored;
        try (MockedStatic<Env> ignored = mockCatalog()) {
            restored = readImage(legacyImage);
        }

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, restored.getState());
        Assert.assertEquals("|", restored.getColumnSeparator().getSeparator());
        Assert.assertEquals(33554432L, restored.getMemLimit());
        Assert.assertFalse(restored.isMemtableOnSinkNode());

        JsonObject newImage = imageJson(restored);
        Assert.assertTrue(newImage.has("ostmt"));
        Assert.assertFalse(newImage.has("mt"));
        Assert.assertFalse(newImage.has("cs"));
    }

    private static Separator analyzedSeparator(String value) throws Exception {
        Separator separator = new Separator(value);
        separator.analyze();
        return separator;
    }

    private static MockedStatic<Env> mockCatalog() throws Exception {
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
        Mockito.when(catalog.getDbOrMetaException(8001L)).thenReturn(database);
        Mockito.when(catalog.getDbOrAnalysisException("legacy_db")).thenReturn(database);
        Mockito.when(database.getName()).thenReturn("legacy_db");
        Mockito.when(database.getFullName()).thenReturn("legacy_db");
        Mockito.when(database.getTableOrMetaException(9001L)).thenReturn(table);
        Mockito.when(database.getTableOrAnalysisException("current_table")).thenReturn(table);
        Mockito.when(table.getName()).thenReturn("current_table");
        Mockito.when(table.getFullSchema()).thenReturn(Lists.newArrayList());

        MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return envStatic;
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
}
