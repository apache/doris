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
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.RoutineLoadDesc;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.load.routineload.kafka.KafkaRoutineLoadJob;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;

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
import java.util.Optional;

public class RoutineLoadJobPersistenceTest {

    @Test
    public void testImageRestoresLoadDefinitionFromOrigStmt() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(1001L, "image_job", 8001L,
                9001L, "127.0.0.1:9092", "image_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("CREATE ROUTINE LOAD legacy_db.image_job ON stale_table "
                + "COLUMNS TERMINATED BY '|', "
                + "COLUMNS(source_col, mapped_col = source_col + 1), "
                + "PRECEDING FILTER source_col > 1, WHERE mapped_col <= 10 "
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"image_topic\")", 0);
        job.setRoutineLoadDesc(new RoutineLoadDesc(new Separator(",", ","), null,
                Lists.newArrayList(new ImportColumnDesc("wrong_column")),
                null, null, null, null, LoadTask.MergeType.APPEND, null));

        try (MockedStatic<Env> ignored = mockCatalog("current_table")) {
            job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        }

        Assert.assertEquals("|", job.getColumnSeparator().getSeparator());
        Assert.assertEquals(2, job.getColumnExprDescs().descs.size());
        Assert.assertEquals("source_col", job.getColumnExprDescs().descs.get(0).getColumnName());
        Assert.assertEquals("mapped_col", job.getColumnExprDescs().descs.get(1).getColumnName());
        Assert.assertNotNull(job.getPrecedingFilter());
        Assert.assertNotNull(job.getWhereExpr());
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
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"alter_topic\")", 0);

        try (MockedStatic<Env> ignored = mockCatalog("current_table")) {
            job = (KafkaRoutineLoadJob) imageRoundTrip(job);
            job.replayLoadDefinition(new OriginStatement(
                    "ALTER ROUTINE LOAD FOR alter_job COLUMNS TERMINATED BY '|', WHERE mapped_col < 50", 0));
            job.replayLoadDefinition(new OriginStatement(
                    "ALTER ROUTINE LOAD FOR alter_job "
                            + "PRECEDING FILTER content MATCH_ANY 'hello' USING ANALYZER 'english'", 0));
            String propertyOnlyOrigin = job.origStmt.originStmt;
            job.replayLoadDefinition(new OriginStatement(
                    "ALTER ROUTINE LOAD FOR alter_job PROPERTIES (\"max_error_number\" = \"10\")", 0));
            Assert.assertEquals(propertyOnlyOrigin, job.origStmt.originStmt);
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
        try (MockedStatic<Env> ignored = mockCatalog("current_table")) {
            restored = imageRoundTrip(job);
        }
        JsonObject restoredProperties = JsonParser.parseString(restored.jobPropertiesToJsonString()).getAsJsonObject();
        for (String key : Lists.newArrayList("column_separator", "precedingFilter",
                "whereExpr", "partitions", "delete", "sequence_col", "merge_type")) {
            Assert.assertEquals(key, expectedProperties.get(key), restoredProperties.get(key));
        }
        Assert.assertTrue(restoredProperties.get("columnToColumnExpr").getAsString().contains("mapped_col="));
        Assert.assertEquals(job.origStmt.originStmt, restored.origStmt.originStmt);
    }

    @Test
    public void testReplayUsesStatementIndexAndQuotesReservedTableName() throws Exception {
        KafkaRoutineLoadJob job = new KafkaRoutineLoadJob(3001L, "order_job", 8001L,
                9001L, "127.0.0.1:9092", "order_topic", UserIdentity.ADMIN);
        job.state = RoutineLoadJob.JobState.PAUSED;
        job.origStmt = new OriginStatement("CREATE ROUTINE LOAD legacy_db.order_job ON `order` "
                + "COLUMNS TERMINATED BY ',' "
                + "FROM KAFKA (\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"order_topic\")", 0);

        OriginStatement multiStatementAlter = new OriginStatement(
                "ALTER ROUTINE LOAD FOR order_job PROPERTIES (\"max_error_number\" = \"10\");"
                        + "ALTER ROUTINE LOAD FOR order_job COLUMNS TERMINATED BY '|'", 1);
        try (MockedStatic<Env> ignored = mockCatalog("order")) {
            job = (KafkaRoutineLoadJob) imageRoundTrip(job);
            job.replayLoadDefinition(multiStatementAlter, SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
            job = (KafkaRoutineLoadJob) imageRoundTrip(job);
        }

        Assert.assertEquals("|", job.getColumnSeparator().getSeparator());
        Assert.assertTrue(job.origStmt.originStmt.contains(" ON `order` "));
        Assert.assertEquals(Long.toString(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES),
                job.sessionVariables.get(SessionVariable.SQL_MODE));
    }

    private static MockedStatic<Env> mockCatalog(String tableName) throws Exception {
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
        Mockito.when(database.getTableOrAnalysisException(tableName)).thenReturn(table);
        Mockito.when(table.getName()).thenReturn(tableName);
        Mockito.when(table.getType()).thenReturn(Table.TableType.OLAP);
        Mockito.when(table.getKeysType()).thenReturn(KeysType.UNIQUE_KEYS);
        Mockito.when(table.hasDeleteSign()).thenReturn(true);
        Mockito.when(table.getFullSchema()).thenReturn(Lists.newArrayList());

        MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
        envStatic.when(Env::getCurrentEnv).thenReturn(env);
        envStatic.when(Env::getCurrentInternalCatalog).thenReturn(catalog);
        return envStatic;
    }

    private static RoutineLoadJob imageRoundTrip(RoutineLoadJob job) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            job.write(out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return RoutineLoadJob.read(in);
        }
    }
}
