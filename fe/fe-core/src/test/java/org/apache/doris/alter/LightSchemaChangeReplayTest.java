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

package org.apache.doris.alter;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.info.IndexType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.TableAddOrDropColumnsInfo;
import org.apache.doris.persist.TableAddOrDropInvertedIndicesInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LightSchemaChangeReplayTest extends TestWithFeService {
    private static final String DB_NAME = "light_schema_change_replay";

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase(DB_NAME);
    }

    @Test
    public void testReplayAddDropAndModifyColumnsPreservesJobTimestamps() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTable table = createLightSchemaChangeTable("column_replay");
        SchemaChangeHandler handler = Env.getCurrentEnv().getSchemaChangeHandler();

        LinkedList<Column> originalSchema = copyBaseSchema(table);
        LinkedList<Column> addedSchema = new LinkedList<>(originalSchema);
        Column addedColumn = new Column("v2", ScalarType.createType(PrimitiveType.INT),
                false, AggregateType.NONE, "0", "");
        addedColumn.setUniqueId(table.getBaseIndexMeta().getMaxColUniqueId() + 1);
        addedSchema.add(addedColumn);

        long addJobId = Env.getCurrentEnv().getNextId();
        TableAddOrDropColumnsInfo addInfo = executeAndCaptureColumnsInfo(
                handler, db, table, addedSchema, addJobId);
        long addCreateTimeMs = addInfo.getCreateTimeMs();
        long addFinishedTimeMs = addInfo.getFinishedTimeMs();
        assertJobAndShowAlterTimestamps(handler, db, addJobId, addCreateTimeMs, addFinishedTimeMs);
        handler.getAlterJobsV2().remove(addJobId);
        handler.runnableSchemaChangeJobV2.remove(addJobId);
        Map<Long, LinkedList<Column>> originalSchemaMap = Maps.newHashMap();
        originalSchemaMap.put(table.getBaseIndexId(), originalSchema);
        table.writeLock();
        try {
            handler.updateBaseIndexSchema(table, originalSchemaMap, table.getIndexes());
        } finally {
            table.writeUnlock();
        }
        Assertions.assertNull(table.getColumn("v2"));

        addInfo = roundTrip(addInfo);
        handler.replayModifyTableLightSchemaChange(addInfo);

        Assertions.assertNotNull(table.getColumn("v2"));
        assertJobAndShowAlterTimestamps(handler, db, addJobId, addCreateTimeMs, addFinishedTimeMs);

        LinkedList<Column> droppedSchema = copyBaseSchema(table);
        droppedSchema.removeIf(column -> column.getName().equals("v2"));
        long dropJobId = Env.getCurrentEnv().getNextId();
        long dropCreateTimeMs = System.currentTimeMillis() - 2000;
        long dropFinishedTimeMs = System.currentTimeMillis() - 1000;
        TableAddOrDropColumnsInfo dropInfo = roundTrip(createColumnsInfo(
                db, table, droppedSchema, dropJobId, dropCreateTimeMs, dropFinishedTimeMs));
        handler.replayModifyTableLightSchemaChange(dropInfo);

        Assertions.assertNull(table.getColumn("v2"));
        assertJobAndShowAlterTimestamps(handler, db, dropJobId, dropCreateTimeMs, dropFinishedTimeMs);

        LinkedList<Column> modifiedSchema = copyBaseSchema(table);
        Column modifiedColumn = modifiedSchema.stream()
                .filter(column -> column.getName().equals("v1"))
                .findFirst()
                .orElseThrow();
        modifiedColumn.setType(ScalarType.createVarchar(40));
        long modifyJobId = Env.getCurrentEnv().getNextId();
        long modifyCreateTimeMs = System.currentTimeMillis() - 500;
        long modifyFinishedTimeMs = System.currentTimeMillis();
        TableAddOrDropColumnsInfo modifyInfo = roundTrip(createColumnsInfo(
                db, table, modifiedSchema, modifyJobId, modifyCreateTimeMs, modifyFinishedTimeMs));
        handler.replayModifyTableLightSchemaChange(modifyInfo);

        Assertions.assertEquals(ScalarType.createVarchar(40), table.getColumn("v1").getType());
        assertJobAndShowAlterTimestamps(
                handler, db, modifyJobId, modifyCreateTimeMs, modifyFinishedTimeMs);
    }

    @Test
    public void testReplayAddAndDropInvertedIndexPreservesJobTimestamps() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTable table = createLightSchemaChangeTable("inverted_index_replay");
        SchemaChangeHandler handler = Env.getCurrentEnv().getSchemaChangeHandler();

        Index invertedIndex = new Index(Env.getCurrentEnv().getNextId(), "idx_v1",
                Lists.newArrayList("v1"), IndexType.INVERTED, null, "");
        List<Index> addedIndexes = Lists.newArrayList(table.getIndexes());
        addedIndexes.add(invertedIndex);
        long addJobId = Env.getCurrentEnv().getNextId();
        long baseTimeMs = System.currentTimeMillis() - 5000;
        long addCreateTimeMs = baseTimeMs;
        long addFinishedTimeMs = baseTimeMs + 1000;
        TableAddOrDropInvertedIndicesInfo persistedAddInfo = new TableAddOrDropInvertedIndicesInfo(
                "", db.getId(), table.getId(), createIndexSchemaMap(table), addedIndexes,
                Lists.newArrayList(invertedIndex), false, addJobId, addCreateTimeMs, addFinishedTimeMs);
        TableAddOrDropInvertedIndicesInfo addInfo = roundTrip(persistedAddInfo);
        handler.replayModifyTableAddOrDropInvertedIndices(addInfo);

        Index replayedIndex = table.getIndexes().stream()
                .filter(index -> index.getIndexName().equals("idx_v1"))
                .findFirst()
                .orElseThrow();
        assertJobAndShowAlterTimestamps(handler, db, addJobId, addCreateTimeMs, addFinishedTimeMs);

        List<Index> droppedIndexes = table.getIndexes().stream()
                .filter(index -> !index.getIndexName().equals("idx_v1"))
                .collect(Collectors.toList());
        long dropJobId = Env.getCurrentEnv().getNextId();
        long dropCreateTimeMs = baseTimeMs + 2000;
        long dropFinishedTimeMs = baseTimeMs + 3000;
        TableAddOrDropInvertedIndicesInfo dropInfo = roundTrip(new TableAddOrDropInvertedIndicesInfo(
                "", db.getId(), table.getId(), createIndexSchemaMap(table), droppedIndexes,
                Lists.newArrayList(replayedIndex), true, dropJobId, dropCreateTimeMs, dropFinishedTimeMs));
        handler.replayModifyTableAddOrDropInvertedIndices(dropInfo);

        Assertions.assertTrue(table.getIndexes().stream()
                .noneMatch(index -> index.getIndexName().equals("idx_v1")));
        assertJobAndShowAlterTimestamps(handler, db, dropJobId, dropCreateTimeMs, dropFinishedTimeMs);
    }

    @Test
    public void testDropInvertedIndexPostCommitFailureKeepsReplayTimestamps() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTable table = createLightSchemaChangeTable("inverted_index_post_commit_failure");
        SchemaChangeHandler handler = Env.getCurrentEnv().getSchemaChangeHandler();
        SchemaChangeHandler failingHandler = Mockito.spy(handler);

        Index invertedIndex = new Index(Env.getCurrentEnv().getNextId(), "idx_v1",
                Lists.newArrayList("v1"), IndexType.INVERTED, null, "");
        List<Index> indexesWithInvertedIndex = Lists.newArrayList(table.getIndexes());
        indexesWithInvertedIndex.add(invertedIndex);
        updateIndexes(handler, table, indexesWithInvertedIndex);

        Mockito.doThrow(new UserException("injected physical index cleanup failure"))
                .when(failingHandler)
                .buildOrDeleteTableInvertedIndices(Mockito.eq(db), Mockito.eq(table), Mockito.anyMap(),
                        Mockito.anyList(), Mockito.anyMap(), Mockito.eq(true));

        EditLog originalEditLog = Env.getCurrentEnv().getEditLog();
        EditLog capturedEditLog = Mockito.mock(EditLog.class);
        ArgumentCaptor<TableAddOrDropInvertedIndicesInfo> infoCaptor =
                ArgumentCaptor.forClass(TableAddOrDropInvertedIndicesInfo.class);
        Mockito.doNothing().when(capturedEditLog)
                .logModifyTableAddOrDropInvertedIndices(infoCaptor.capture());
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        Env.getCurrentEnv().setEditLog(capturedEditLog);
        FeConstants.runningUnitTest = false;
        long jobId = Env.getCurrentEnv().getNextId();
        List<Index> droppedIndexes = table.getIndexes().stream()
                .filter(index -> !index.getIndexName().equals("idx_v1"))
                .collect(Collectors.toList());
        try {
            table.writeLock();
            try {
                DdlException exception = Assertions.assertThrows(DdlException.class,
                        () -> failingHandler.modifyTableLightSchemaChange("", db, table,
                                createIndexSchemaMap(table), droppedIndexes, Lists.newArrayList(invertedIndex),
                                true, jobId, false, Maps.newHashMap(), null, null));
                Assertions.assertTrue(exception.getMessage().contains("injected physical index cleanup failure"));
            } finally {
                table.writeUnlock();
            }
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
            Env.getCurrentEnv().setEditLog(originalEditLog);
        }

        TableAddOrDropInvertedIndicesInfo committedInfo = infoCaptor.getValue();
        long createTimeMs = committedInfo.getCreateTimeMs();
        long finishedTimeMs = committedInfo.getFinishedTimeMs();
        assertJobAndShowAlterTimestamps(failingHandler, db, jobId, createTimeMs, finishedTimeMs);

        handler.getAlterJobsV2().remove(jobId);
        handler.runnableSchemaChangeJobV2.remove(jobId);
        updateIndexes(handler, table, indexesWithInvertedIndex);
        Assertions.assertTrue(table.getIndexes().stream()
                .anyMatch(index -> index.getIndexName().equals("idx_v1")));

        handler.replayModifyTableAddOrDropInvertedIndices(roundTrip(committedInfo));

        Assertions.assertTrue(table.getIndexes().stream()
                .noneMatch(index -> index.getIndexName().equals("idx_v1")));
        assertJobAndShowAlterTimestamps(handler, db, jobId, createTimeMs, finishedTimeMs);
    }

    @Test
    public void testReplayLegacyJournalUsesReplayTimestamps() throws Exception {
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        OlapTable table = createLightSchemaChangeTable("legacy_replay");
        LinkedList<Column> addedSchema = copyBaseSchema(table);
        Column addedColumn = new Column("v2", ScalarType.createType(PrimitiveType.INT),
                false, AggregateType.NONE, "0", "");
        addedColumn.setUniqueId(table.getBaseIndexMeta().getMaxColUniqueId() + 1);
        addedSchema.add(addedColumn);

        long jobId = Env.getCurrentEnv().getNextId();
        TableAddOrDropColumnsInfo currentInfo = createColumnsInfo(
                db, table, addedSchema, jobId, 1700000021000L, 1700000022000L);
        JsonObject legacyJson = GsonUtils.GSON.toJsonTree(currentInfo).getAsJsonObject();
        legacyJson.remove("createTimeMs");
        legacyJson.remove("finishedTimeMs");
        TableAddOrDropColumnsInfo legacyInfo = GsonUtils.GSON.fromJson(
                legacyJson, TableAddOrDropColumnsInfo.class);
        Assertions.assertNull(legacyInfo.getCreateTimeMs());
        Assertions.assertNull(legacyInfo.getFinishedTimeMs());

        long beforeReplayMs = System.currentTimeMillis();
        SchemaChangeHandler handler = Env.getCurrentEnv().getSchemaChangeHandler();
        handler.replayModifyTableLightSchemaChange(legacyInfo);
        long afterReplayMs = System.currentTimeMillis();

        AlterJobV2 replayedJob = handler.getAlterJobsV2().get(jobId);
        Assertions.assertNotNull(replayedJob);
        Assertions.assertTrue(replayedJob.getCreateTimeMs() >= beforeReplayMs);
        Assertions.assertTrue(replayedJob.getCreateTimeMs() <= afterReplayMs);
        Assertions.assertTrue(replayedJob.getFinishedTimeMs() >= beforeReplayMs);
        Assertions.assertTrue(replayedJob.getFinishedTimeMs() <= afterReplayMs);
        assertJobAndShowAlterTimestamps(handler, db, jobId,
                replayedJob.getCreateTimeMs(), replayedJob.getFinishedTimeMs());

        OlapTable invertedIndexTable = createLightSchemaChangeTable("legacy_inverted_index_replay");
        Index invertedIndex = new Index(Env.getCurrentEnv().getNextId(), "idx_v1",
                Lists.newArrayList("v1"), IndexType.INVERTED, null, "");
        List<Index> indexes = Lists.newArrayList(invertedIndexTable.getIndexes());
        indexes.add(invertedIndex);
        long invertedIndexJobId = Env.getCurrentEnv().getNextId();
        TableAddOrDropInvertedIndicesInfo currentIndexInfo = new TableAddOrDropInvertedIndicesInfo(
                "", db.getId(), invertedIndexTable.getId(), createIndexSchemaMap(invertedIndexTable), indexes,
                Lists.newArrayList(invertedIndex), false, invertedIndexJobId, 1700000023000L, 1700000024000L);
        JsonObject legacyIndexJson = GsonUtils.GSON.toJsonTree(currentIndexInfo).getAsJsonObject();
        legacyIndexJson.remove("createTimeMs");
        legacyIndexJson.remove("finishedTimeMs");
        TableAddOrDropInvertedIndicesInfo legacyIndexInfo = GsonUtils.GSON.fromJson(
                legacyIndexJson, TableAddOrDropInvertedIndicesInfo.class);
        Assertions.assertNull(legacyIndexInfo.getCreateTimeMs());
        Assertions.assertNull(legacyIndexInfo.getFinishedTimeMs());

        long beforeIndexReplayMs = System.currentTimeMillis();
        handler.replayModifyTableAddOrDropInvertedIndices(legacyIndexInfo);
        long afterIndexReplayMs = System.currentTimeMillis();

        AlterJobV2 replayedIndexJob = handler.getAlterJobsV2().get(invertedIndexJobId);
        Assertions.assertNotNull(replayedIndexJob);
        Assertions.assertTrue(replayedIndexJob.getCreateTimeMs() >= beforeIndexReplayMs);
        Assertions.assertTrue(replayedIndexJob.getCreateTimeMs() <= afterIndexReplayMs);
        Assertions.assertTrue(replayedIndexJob.getFinishedTimeMs() >= beforeIndexReplayMs);
        Assertions.assertTrue(replayedIndexJob.getFinishedTimeMs() <= afterIndexReplayMs);
        assertJobAndShowAlterTimestamps(handler, db, invertedIndexJobId,
                replayedIndexJob.getCreateTimeMs(), replayedIndexJob.getFinishedTimeMs());
    }

    private OlapTable createLightSchemaChangeTable(String tableName) throws Exception {
        createTable("CREATE TABLE " + DB_NAME + "." + tableName + " (\n"
                + "k1 INT NOT NULL,\n"
                + "v1 VARCHAR(20)\n"
                + ") ENGINE=OLAP\n"
                + "DUPLICATE KEY(k1)\n"
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1\n"
                + "PROPERTIES ('replication_num' = '1', 'light_schema_change' = 'true')");
        Database db = Env.getCurrentInternalCatalog().getDbOrMetaException(DB_NAME);
        return (OlapTable) db.getTableOrMetaException(tableName, Table.TableType.OLAP);
    }

    private TableAddOrDropColumnsInfo createColumnsInfo(Database db, OlapTable table,
            LinkedList<Column> schema, long jobId, Long createTimeMs, Long finishedTimeMs) {
        Map<Long, LinkedList<Column>> indexSchemaMap = Maps.newHashMap();
        indexSchemaMap.put(table.getBaseIndexId(), schema);
        return new TableAddOrDropColumnsInfo("", db.getId(), table.getId(), table.getBaseIndexId(),
                indexSchemaMap, table.getCopiedIndexIdToSchema(true, true),
                Maps.newHashMap(table.getIndexNameToId()), table.getIndexes(), jobId,
                createTimeMs, finishedTimeMs);
    }

    private TableAddOrDropColumnsInfo executeAndCaptureColumnsInfo(SchemaChangeHandler handler,
            Database db, OlapTable table, LinkedList<Column> schema, long jobId) throws Exception {
        EditLog originalEditLog = Env.getCurrentEnv().getEditLog();
        EditLog capturedEditLog = Mockito.mock(EditLog.class);
        ArgumentCaptor<TableAddOrDropColumnsInfo> infoCaptor =
                ArgumentCaptor.forClass(TableAddOrDropColumnsInfo.class);
        Mockito.doNothing().when(capturedEditLog).logModifyTableAddOrDropColumns(infoCaptor.capture());
        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        Env.getCurrentEnv().setEditLog(capturedEditLog);
        FeConstants.runningUnitTest = false;
        Map<Long, LinkedList<Column>> indexSchemaMap = Maps.newHashMap();
        indexSchemaMap.put(table.getBaseIndexId(), schema);
        try {
            table.writeLock();
            try {
                handler.modifyTableLightSchemaChange("", db, table, indexSchemaMap, table.getIndexes(),
                        null, false, jobId, false, Maps.newHashMap(), null, null);
            } finally {
                table.writeUnlock();
            }
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
            Env.getCurrentEnv().setEditLog(originalEditLog);
        }
        TableAddOrDropColumnsInfo info = infoCaptor.getValue();
        Assertions.assertNotNull(info.getCreateTimeMs());
        Assertions.assertNotNull(info.getFinishedTimeMs());
        return info;
    }

    private Map<Long, LinkedList<Column>> createIndexSchemaMap(OlapTable table) {
        Map<Long, LinkedList<Column>> indexSchemaMap = Maps.newHashMap();
        indexSchemaMap.put(table.getBaseIndexId(), copyBaseSchema(table));
        return indexSchemaMap;
    }

    private void updateIndexes(SchemaChangeHandler handler, OlapTable table, List<Index> indexes)
            throws IOException {
        table.writeLock();
        try {
            handler.updateBaseIndexSchema(table, createIndexSchemaMap(table), indexes);
        } finally {
            table.writeUnlock();
        }
    }

    private LinkedList<Column> copyBaseSchema(OlapTable table) {
        return table.getBaseSchema(true).stream()
                .map(Column::new)
                .collect(Collectors.toCollection(LinkedList::new));
    }

    private void assertJobAndShowAlterTimestamps(SchemaChangeHandler handler, Database db,
            long jobId, long createTimeMs, long finishedTimeMs) {
        AlterJobV2 job = handler.getAlterJobsV2().get(jobId);
        Assertions.assertNotNull(job);
        Assertions.assertEquals(createTimeMs, job.getCreateTimeMs());
        Assertions.assertEquals(finishedTimeMs, job.getFinishedTimeMs());

        List<Comparable> showAlterRow = handler.getAlterJobInfosByDb(db).stream()
                .filter(row -> ((Number) row.get(0)).longValue() == jobId)
                .findFirst()
                .orElseThrow();
        Assertions.assertEquals(TimeUtils.longToTimeStringWithms(createTimeMs), showAlterRow.get(2));
        Assertions.assertEquals(TimeUtils.longToTimeStringWithms(finishedTimeMs), showAlterRow.get(3));
    }

    private TableAddOrDropColumnsInfo roundTrip(TableAddOrDropColumnsInfo info) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            info.write(out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return TableAddOrDropColumnsInfo.read(in);
        }
    }

    private TableAddOrDropInvertedIndicesInfo roundTrip(TableAddOrDropInvertedIndicesInfo info)
            throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            info.write(out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return TableAddOrDropInvertedIndicesInfo.read(in);
        }
    }
}
