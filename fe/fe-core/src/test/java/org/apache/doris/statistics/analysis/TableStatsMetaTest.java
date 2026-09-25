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

package org.apache.doris.statistics.analysis;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.CatalogIf;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

class TableStatsMetaTest {

    private static final long BASE_INDEX_ID = 10001L;
    private static final long ROLLUP_INDEX_ID = 10002L;
    private static final String BASE_INDEX_NAME = "baseIndex";

    private OlapTable mockOlapTable() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.doReturn(Lists.newArrayList(BASE_INDEX_ID, ROLLUP_INDEX_ID)).when(table).getIndexIdList();
        Mockito.doReturn(BASE_INDEX_ID).when(table).getBaseIndexId();
        Mockito.doReturn(BASE_INDEX_NAME).when(table).getIndexNameById(BASE_INDEX_ID);
        DatabaseIf db = Mockito.mock(DatabaseIf.class);
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.doReturn(db).when(table).getDatabase();
        Mockito.doReturn(catalog).when(db).getCatalog();
        return table;
    }

    // 100 rows of the base index had been loaded when the job was built.
    private AnalysisInfo analyzeJob() {
        AnalysisInfo job = new AnalysisInfoBuilder().setRowCount(100)
                .setJobColumns(Sets.newHashSet(Pair.of(BASE_INDEX_NAME, "col1")))
                .setUpdateRows(100)
                .setTblUpdateTime(1000L)
                .setJobType(AnalysisInfo.JobType.MANUAL)
                .build();
        job.addIndexRowCount(BASE_INDEX_ID, 100);
        return job;
    }

    @Test
    void update() {
        OlapTable table = Mockito.mock(OlapTable.class);
        TableStatsMeta tableStatsMeta = new TableStatsMeta();
        AnalysisInfo jobInfo = new AnalysisInfoBuilder().setRowCount(4)
                .setJobColumns(new HashSet<>()).setColName("col1").build();
        tableStatsMeta.update(jobInfo, table);
        Assertions.assertEquals(4, tableStatsMeta.rowCount);
    }

    @Test
    void testClearStaleIndexRowCount() {
        TableStatsMeta meta = new TableStatsMeta();
        meta.addIndexRowForTest(1, 1);
        meta.addIndexRowForTest(2, 2);
        meta.addIndexRowForTest(3, 3);
        Assertions.assertEquals(1, meta.getRowCount(1));
        Assertions.assertEquals(2, meta.getRowCount(2));
        Assertions.assertEquals(3, meta.getRowCount(3));
        Assertions.assertEquals(-1, meta.getRowCount(4));

        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn(Lists.newArrayList(1L)).when(table).getIndexIdList();

        meta.clearStaleIndexRowCount(table);
        Assertions.assertEquals(1, meta.getRowCount(1));
        Assertions.assertEquals(-1, meta.getRowCount(2));
        Assertions.assertEquals(-1, meta.getRowCount(3));
        Assertions.assertEquals(-1, meta.getRowCount(4));
    }

    @Test
    void testResetByTruncateTable() {
        OlapTable table = mockOlapTable();
        TableStatsMeta meta = new TableStatsMeta();
        // 100 rows were collected by the last analysis, 50 more rows were loaded since then.
        meta.addIndexRowForTest(BASE_INDEX_ID, 100);
        meta.updatedRows.set(150);
        meta.partitionUpdateRows.put(1L, 100L);
        meta.update(analyzeJob(), table);
        Assertions.assertEquals(50, meta.getBaseIndexDeltaRowCount(table));
        Assertions.assertNotEquals(0, meta.updatedTime);
        Assertions.assertNotEquals(0, meta.lastAnalyzeTime);
        Assertions.assertNotNull(meta.jobType);

        meta.userInjected = true;
        meta.reset(table);

        Assertions.assertEquals(0, meta.rowCount);
        Assertions.assertEquals(0, meta.updatedRows.get());
        Assertions.assertTrue(meta.partitionUpdateRows.isEmpty());
        Assertions.assertFalse(meta.userInjected);
        Assertions.assertTrue(meta.isColumnsStatsEmpty());
        Assertions.assertTrue(meta.partitionChanged.get());
        // The emptied table has been analyzed by no job, and no job describes it any more.
        Assertions.assertEquals(0, meta.updatedTime);
        Assertions.assertEquals(0, meta.lastAnalyzeTime);
        Assertions.assertNull(meta.jobType);
        // The base index of the emptied table is known to be empty, the row count of the other indexes is
        // unknown, the rows loaded after the truncation are only counted for the base index.
        Assertions.assertEquals(0, meta.getRowCount(BASE_INDEX_ID));
        Assertions.assertEquals(-1, meta.getRowCount(10002L));

        // The 30 rows loaded after the truncation are the whole row count of the table now.
        meta.updatedRows.set(30);
        Assertions.assertEquals(30, meta.getBaseIndexDeltaRowCount(table));
        Assertions.assertEquals(30, meta.getRowCount(BASE_INDEX_ID) + meta.getBaseIndexDeltaRowCount(table));
    }

    @Test
    void testDeltaRowCountKeptAfterColumnStatsDropped() {
        OlapTable table = mockOlapTable();
        TableStatsMeta meta = new TableStatsMeta();
        meta.updatedRows.set(100);
        meta.update(analyzeJob(), table);
        // 50 rows were loaded after the analysis.
        meta.updatedRows.set(150);
        Assertions.assertEquals(50, meta.getBaseIndexDeltaRowCount(table));

        // The baseline is recorded in the table stats, dropping all the column statistics of the table
        // must not turn the 100 collected rows into delta rows again.
        meta.removeColumn(BASE_INDEX_NAME, "col1");
        Assertions.assertTrue(meta.isColumnsStatsEmpty());
        Assertions.assertEquals(50, meta.getBaseIndexDeltaRowCount(table));
    }

    @Test
    void testDeltaRowCountOfLegacyRecord() {
        OlapTable table = mockOlapTable();
        TableStatsMeta meta = new TableStatsMeta();
        meta.updatedRows.set(150);
        meta.update(analyzeJob(), table);
        // A record written before the baseline was recorded in the table stats has none of its own, it
        // is derived from the collected column statistics.
        meta.clearUpdatedRowsBaseForTest();
        Assertions.assertEquals(50, meta.getBaseIndexDeltaRowCount(table));

        // Once they are all dropped there is no baseline left, no row is reported as a delta row.
        meta.removeColumn(BASE_INDEX_NAME, "col1");
        Assertions.assertTrue(meta.isColumnsStatsEmpty());
        Assertions.assertEquals(0, meta.getBaseIndexDeltaRowCount(table));
    }

    @Test
    void testDeltaRowCountBaselineNotAdvancedWithoutBaseIndexRowCount() {
        OlapTable table = mockOlapTable();
        TableStatsMeta meta = new TableStatsMeta();
        meta.updatedRows.set(100);
        meta.update(analyzeJob(), table);
        meta.updatedRows.set(150);
        Assertions.assertEquals(50, meta.getBaseIndexDeltaRowCount(table));

        // An analyze job which collected the row count of another index only (a materialized view) does
        // not refresh the collected row count of the base index, so it must not advance its baseline,
        // otherwise the rows loaded in between are no longer reported as delta rows.
        AnalysisInfo mvJob = new AnalysisInfoBuilder().setRowCount(100)
                .setJobColumns(Sets.newHashSet(Pair.of("mvIndex", "col1")))
                .setUpdateRows(150)
                .build();
        mvJob.addIndexRowCount(10002L, 30);
        meta.update(mvJob, table);
        Assertions.assertEquals(50, meta.getBaseIndexDeltaRowCount(table));
    }

    @Test
    void testDeltaRowCountOfSnapshotTakenBeforeReset() {
        OlapTable table = mockOlapTable();
        TableStatsMeta meta = new TableStatsMeta();
        meta.updatedRows.set(100);
        meta.update(analyzeJob(), table);

        // An analyze job built before the truncation can deliver its snapshot after it. The 100 rows of
        // the snapshot are gone, only the 5 rows loaded after the truncation are there. The stale row
        // count and the stale baseline cancel each other out.
        meta.reset(table);
        meta.updatedRows.set(5);
        meta.update(analyzeJob(), table);
        Assertions.assertEquals(100, meta.getRowCount(BASE_INDEX_ID));
        Assertions.assertEquals(-95, meta.getBaseIndexDeltaRowCount(table));
        Assertions.assertEquals(5, meta.getRowCount(BASE_INDEX_ID) + meta.getBaseIndexDeltaRowCount(table));
    }

    @Test
    void testDeltaRowCountOfInjectedRowCountAfterDropStats() {
        OlapTable table = mockOlapTable();
        TableStatsMeta meta = new TableStatsMeta();
        // The table was truncated and 5 rows are loaded into it.
        meta.reset(table);
        meta.updatedRows.set(5);
        // The user supplies the row count of the table as of now.
        AnalysisInfo injected = new AnalysisInfoBuilder().setRowCount(5)
                .setJobColumns(new HashSet<>()).setUserInject(true).build();
        injected.addIndexRowCount(BASE_INDEX_ID, 5);
        meta.update(injected, table);
        Assertions.assertTrue(meta.userInjected);
        // The row count supplied by the user is reported as long as it applies.
        Assertions.assertEquals(0, meta.getBaseIndexDeltaRowCount(table));
        Assertions.assertEquals(5, meta.getRowCount(BASE_INDEX_ID));

        // Dropping the statistics of a column clears userInjected without clearing the row count. It and the
        // baseline describe the same snapshot, so the loaded rows are not counted twice.
        meta.userInjected = false;
        Assertions.assertEquals(0, meta.getBaseIndexDeltaRowCount(table));
        Assertions.assertEquals(5, meta.getRowCount(BASE_INDEX_ID) + meta.getBaseIndexDeltaRowCount(table));
    }


    @Test
    void testRowCountSnapshotIsCoherent() throws Exception {
        OlapTable table = mockOlapTable();
        TableStatsMeta meta = new TableStatsMeta();
        meta.addIndexRowForTest(BASE_INDEX_ID, 100);
        meta.updatedRows.set(100);
        meta.update(analyzeJob(), table);
        // 50 rows were loaded after the analysis, the table has 150 rows.
        meta.updatedRows.set(150);

        AtomicBoolean incoherent = new AtomicBoolean(false);
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread reader = new Thread(() -> {
            while (!stop.get()) {
                long rowCount = meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID);
                // Every state the writer goes through reports the 150 rows the table really holds, except the
                // state right after a truncation, which reports the empty table.
                if (rowCount != 0 && rowCount != 150) {
                    incoherent.set(true);
                }
            }
        });
        reader.start();
        // Only transitions which really happen are performed: TRUNCATE TABLE, then 150 rows are loaded into
        // the emptied table, then an analysis which collected 100 of those 150 rows completes. The table never
        // holds more than 150 rows, so a planner which reads the row count of the table without holding its
        // lock must never see more than 150 rows either.
        for (int i = 0; i < 5000; i++) {
            meta.reset(table);
            meta.updatedRows.set(150);
            meta.update(analyzeJob(), table);
        }
        stop.set(true);
        reader.join();
        // 250 is what an unsettled publication produces: the 100 rows collected by the analysis plus the 150
        // rows of the table counted as delta rows, because the reader paired the collected row count of one of
        // them with the baseline of the other.
        Assertions.assertFalse(incoherent.get(), "the reader saw a row count of neither state");
    }

    // The table has a rollup index of the given kind besides the base index. The schema is the one a real
    // index has: a key column, which carries no aggregation type at all, and a value column with the given
    // one.
    private void mockRollupIndex(OlapTable table, long indexId, KeysType keysType, AggregateType valueType) {
        MaterializedIndexMeta indexMeta = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.doReturn(keysType).when(indexMeta).getKeysType();
        Column keyColumn = new Column("col1", PrimitiveType.INT);
        keyColumn.setAggregationType(null, false);
        Column valueColumn = new Column("col2", PrimitiveType.INT);
        valueColumn.setAggregationType(valueType, false);
        Mockito.doReturn(Lists.newArrayList(keyColumn, valueColumn)).when(indexMeta).getSchema();
        Mockito.doReturn(indexMeta).when(table).getIndexMetaByIndexId(indexId);
    }

    @Test
    void testRollupKeepingOneRowPerBaseRowReportsTheRowsLoadedAfterTheTruncate() {
        OlapTable table = mockOlapTable();
        mockRollupIndex(table, ROLLUP_INDEX_ID, KeysType.DUP_KEYS, AggregateType.NONE);
        // The record a truncation creates: every index which follows the base index is known to be empty.
        TableStatsMeta meta = new TableStatsMeta(table);
        meta.updatedRows.set(150);

        Assertions.assertEquals(150, meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID));
        Assertions.assertEquals(150, meta.getRowCountWithDeltaRows(table, ROLLUP_INDEX_ID));
    }

    @Test
    void testAggregatingRollupDoesNotReportTheRowsLoadedAfterTheTruncate() {
        OlapTable table = mockOlapTable();
        mockRollupIndex(table, ROLLUP_INDEX_ID, KeysType.AGG_KEYS, AggregateType.SUM);
        TableStatsMeta meta = new TableStatsMeta(table);
        meta.updatedRows.set(150);

        Assertions.assertEquals(150, meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID));
        // The rollup aggregates the loaded rows, its row count is unknown until the backends report it, and
        // the rows of the base index must not be reported as its row count.
        Assertions.assertEquals(-1, meta.getRowCountWithDeltaRows(table, ROLLUP_INDEX_ID));
    }

}
