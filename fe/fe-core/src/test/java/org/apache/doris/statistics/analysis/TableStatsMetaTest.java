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

import org.apache.doris.analysis.Expr;
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

    // The table has a rollup index besides the base index. The schema is the one a real index has: a key
    // column, which carries no aggregation type at all, and a value column with the given one. A non null
    // where clause makes it a filtered index.
    private void mockRollupIndex(OlapTable table, long indexId, KeysType keysType, AggregateType valueType,
            Expr whereClause) {
        MaterializedIndexMeta indexMeta = Mockito.mock(MaterializedIndexMeta.class);
        Mockito.doReturn(keysType).when(indexMeta).getKeysType();
        Mockito.doReturn(whereClause).when(indexMeta).getWhereClause();
        Column keyColumn = new Column("col1", PrimitiveType.INT);
        keyColumn.setAggregationType(null, false);
        Column valueColumn = new Column("col2", PrimitiveType.INT);
        valueColumn.setAggregationType(valueType, false);
        Mockito.doReturn(Lists.newArrayList(keyColumn, valueColumn)).when(indexMeta).getSchema();
        Mockito.doReturn(indexMeta).when(table).getIndexMetaByIndexId(indexId);
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
    void testRecordCreatedByALoadReportsTheLoadedRows() {
        OlapTable table = mockOlapTable();
        // The record a load creates for a table whose statistics were dropped, e.g. by TRUNCATE TABLE: no row
        // count has ever been collected, so every loaded row is a delta row.
        TableStatsMeta meta = new TableStatsMeta(table);
        Assertions.assertTrue(meta.isColumnsStatsEmpty());
        Assertions.assertEquals(0, meta.updatedTime);

        meta.updatedRows.set(30);
        Assertions.assertEquals(30, meta.getBaseIndexDeltaRowCount(table));
        Assertions.assertEquals(0, meta.getRowCount(BASE_INDEX_ID));
        // The planner reads this as the row count of the table while the backends have not reported the new
        // tablets yet.
        Assertions.assertEquals(30, meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID));

        // The rows of the next loads add up.
        meta.updatedRows.addAndGet(12);
        Assertions.assertEquals(42, meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID));
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
        // A record written before the baseline was recorded in the table stats has none of its own, it is
        // derived from the collected column statistics.
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

        // An analyze job which collected the row count of another index only (a materialized view) does not
        // refresh the collected row count of the base index, so it must not advance its baseline, otherwise
        // the rows loaded in between are no longer reported as delta rows.
        AnalysisInfo mvJob = new AnalysisInfoBuilder().setRowCount(100)
                .setJobColumns(Sets.newHashSet(Pair.of("mvIndex", "col1")))
                .setUpdateRows(150)
                .build();
        mvJob.addIndexRowCount(ROLLUP_INDEX_ID, 30);
        meta.update(mvJob, table);
        Assertions.assertEquals(50, meta.getBaseIndexDeltaRowCount(table));
    }

    @Test
    void testDeltaRowCountOfStaleSnapshotDeliveredAfterTheRecordWasRecreated() {
        OlapTable table = mockOlapTable();
        // The record of a table whose statistics were dropped, with 5 rows loaded into the table since.
        TableStatsMeta meta = new TableStatsMeta(table);
        meta.updatedRows.set(5);

        // An analyze job built before the statistics were dropped delivers its snapshot afterwards: it still
        // describes the 100 rows it collected and the baseline it carries is the same 100 rows. The stale row
        // count and the stale baseline cancel each other out, the table is still estimated with its 5 rows.
        meta.update(analyzeJob(), table);
        Assertions.assertEquals(100, meta.getRowCount(BASE_INDEX_ID));
        Assertions.assertEquals(-95, meta.getBaseIndexDeltaRowCount(table));
        Assertions.assertEquals(5, meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID));
    }

    @Test
    void testDeltaRowCountOfInjectedRowCountAfterDropStats() {
        OlapTable table = mockOlapTable();
        // The statistics of the table were dropped and 5 rows are loaded into it.
        TableStatsMeta meta = new TableStatsMeta(table);
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
        Assertions.assertEquals(5, meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID));
    }

    @Test
    void testRowCountSnapshotIsCoherent() throws Exception {
        OlapTable table = mockOlapTable();
        TableStatsMeta meta = new TableStatsMeta(table);
        // An analysis collected the 100 rows the table had, then 50 more rows were loaded.
        meta.update(analyzeJob(), table);
        meta.updatedRows.set(150);

        AtomicBoolean incoherent = new AtomicBoolean(false);
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread reader = new Thread(() -> {
            while (!stop.get()) {
                // Every state the writer goes through reports the 150 rows the table holds.
                if (meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID) != 150) {
                    incoherent.set(true);
                }
            }
        });
        reader.start();
        // Only transitions which really happen are performed: an analysis collecting the 100 rows the table
        // had, while 150 rows are loaded. A reader which pairs the collected row count of one state with the
        // baseline of the other would compute 100 + (150 - 0) = 250.
        for (int i = 0; i < 5000; i++) {
            meta.update(analyzeJob(), table);
            meta.updatedRows.set(150);
        }
        stop.set(true);
        reader.join();
        Assertions.assertFalse(incoherent.get(), "the reader saw a row count of neither state");
    }

    @Test
    void testRollupKeepingOneRowPerBaseRowReportsTheLoadedRows() {
        OlapTable table = mockOlapTable();
        mockRollupIndex(table, ROLLUP_INDEX_ID, KeysType.DUP_KEYS, AggregateType.NONE, null);
        TableStatsMeta meta = new TableStatsMeta(table);
        meta.updatedRows.set(150);

        Assertions.assertEquals(150, meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID));
        Assertions.assertEquals(150, meta.getRowCountWithDeltaRows(table, ROLLUP_INDEX_ID));
    }

    @Test
    void testAggregatingRollupDoesNotReportTheLoadedRows() {
        OlapTable table = mockOlapTable();
        mockRollupIndex(table, ROLLUP_INDEX_ID, KeysType.AGG_KEYS, AggregateType.SUM, null);
        TableStatsMeta meta = new TableStatsMeta(table);
        meta.updatedRows.set(150);

        Assertions.assertEquals(150, meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID));
        // The rollup aggregates the loaded rows, its row count is unknown until the backends report it, and
        // the rows of the base index must not be reported as its row count.
        Assertions.assertEquals(-1, meta.getRowCountWithDeltaRows(table, ROLLUP_INDEX_ID));
    }

    @Test
    void testFilteredMaterializedIndexDoesNotReportTheLoadedRows() {
        OlapTable table = mockOlapTable();
        mockRollupIndex(table, ROLLUP_INDEX_ID, KeysType.DUP_KEYS, AggregateType.NONE,
                Mockito.mock(Expr.class));
        TableStatsMeta meta = new TableStatsMeta(table);
        meta.updatedRows.set(150);

        Assertions.assertEquals(150, meta.getRowCountWithDeltaRows(table, BASE_INDEX_ID));
        // A filtered index only holds the rows which satisfy its predicate, it must not be reported with the
        // rows of the whole table.
        Assertions.assertEquals(-1, meta.getRowCountWithDeltaRows(table, ROLLUP_INDEX_ID));
    }
}
