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
import org.apache.doris.analysis.ExprToSqlVisitor;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.Separator;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TimeV2Literal;
import org.apache.doris.analysis.ToSqlParams;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.info.PartitionNamesInfo;
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
    public void testDirectStateImageRoundTripDoesNotParseOrigStmt() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1001L, "direct_job", 8001L,
                9001L, "127.0.0.1:9092", "image_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("deliberately invalid SQL", 0);
        Expr columnExpr = new TimeV2Literal(12, 34, 56, 123456, 6, true);
        Expr precedingFilter = new MatchPredicate(MatchPredicate.Operator.MATCH_ANY,
                namedSlot("content"), new StringLiteral("hello world"), Type.BOOLEAN,
                NullableMode.DEPEND_ON_ARGUMENT, null, false, "english");
        Expr whereExpr = new BinaryPredicate(
                BinaryPredicate.Operator.GT, namedSlot("a`b"), new IntLiteral(10));
        Expr deleteCondition = new BinaryPredicate(
                BinaryPredicate.Operator.EQ, new IntLiteral(1), new IntLiteral(1));
        job.setRoutineLoadDesc(new RoutineLoadDesc(
                new Separator("|", "|"), new Separator("\n", "\\n"),
                Lists.newArrayList(new ImportColumnDesc("source_col"),
                        new ImportColumnDesc("mapped_col", columnExpr)),
                precedingFilter, whereExpr,
                new PartitionNamesInfo(false, Lists.newArrayList("p1", "p2")),
                deleteCondition, LoadTask.MergeType.MERGE, "seq_col"));

        JsonObject json = imageJson(job);
        Assert.assertTrue(json.has("ostmt"));
        for (String key : Lists.newArrayList(
                "pni", "cds", "pf", "we", "cs", "lidel", "sc", "mt", "dc")) {
            Assert.assertTrue("missing direct-state key " + key, json.has(key));
        }

        RoutineLoadJob restored;
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            restored = imageRoundTrip(job);
            envStatic.verifyNoInteractions();
        }

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, restored.getState());
        Assert.assertEquals(Lists.newArrayList("p1", "p2"),
                restored.getPartitionNamesInfo().getPartitionNames());
        Assert.assertEquals(2, restored.getColumnExprDescs().descs.size());
        Assert.assertEquals(exprToSql(columnExpr),
                exprToSql(restored.getColumnExprDescs().descs.get(1).getExpr()));
        Assert.assertEquals(exprToSql(precedingFilter), exprToSql(restored.getPrecedingFilter()));
        Assert.assertEquals(exprToSql(whereExpr), exprToSql(restored.getWhereExpr()));
        Assert.assertEquals(exprToSql(deleteCondition), exprToSql(restored.getDeleteCondition()));
        Assert.assertEquals("|", restored.getColumnSeparator().getSeparator());
        Assert.assertEquals("\n", restored.getLineDelimiter().getSeparator());
        Assert.assertEquals("seq_col", restored.getSequenceCol());
        Assert.assertEquals(LoadTask.MergeType.MERGE, restored.getMergeType());
    }

    @Test
    public void testEmptyDirectStateSafelyFallsBackToOrigStmt() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(2001L, "empty_job", 8001L,
                9001L, "127.0.0.1:9092", "empty_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("CREATE ROUTINE LOAD legacy_db.empty_job ON current_table "
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"empty_topic\")", 0);

        RoutineLoadJob restored;
        try (MockedStatic<Env> ignored = mockCatalog()) {
            restored = imageRoundTrip(job);
        }

        Assert.assertEquals(RoutineLoadJob.JobState.PAUSED, restored.getState());
        Assert.assertNull(restored.getColumnSeparator());
        Assert.assertNull(restored.getWhereExpr());
        Assert.assertEquals(LoadTask.MergeType.APPEND, restored.getMergeType());
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

        JsonObject newImage = imageJson(restored);
        Assert.assertTrue(newImage.has("ostmt"));
        Assert.assertTrue(newImage.has("mt"));
        Assert.assertTrue(newImage.has("cs"));
        restored.origStmt = new OriginStatement("invalid after legacy recovery", 0);
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            RoutineLoadJob restoredAgain = imageRoundTrip(restored);
            envStatic.verifyNoInteractions();
            Assert.assertEquals("|", restoredAgain.getColumnSeparator().getSeparator());
        }
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
        Mockito.when(database.getTable(9001L)).thenReturn(Optional.of((Table) table));
        Mockito.when(database.getTableOrMetaException(9001L)).thenReturn(table);
        Mockito.when(database.getTableOrAnalysisException("current_table")).thenReturn(table);
        Mockito.when(table.getName()).thenReturn("current_table");
        Mockito.when(table.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.hasDeleteSign()).thenReturn(true);
        Mockito.when(table.getFullSchema()).thenReturn(Lists.newArrayList());

        MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return envStatic;
    }

    private static String exprToSql(Expr expr) {
        return expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);
    }

    private static SlotRef namedSlot(String column) {
        SlotRef slotRef = new SlotRef(null, column);
        slotRef.setLabel("`" + column.replace("`", "``") + "`");
        slotRef.setType(Type.VARCHAR);
        return slotRef;
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
