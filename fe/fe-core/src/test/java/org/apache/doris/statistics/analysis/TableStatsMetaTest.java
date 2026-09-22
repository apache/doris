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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;

class TableStatsMetaTest {

    private static final long BASE_INDEX_ID = 10001L;
    private static final String BASE_INDEX_NAME = "baseIndex";

    private OlapTable mockOlapTable() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.doReturn(Lists.newArrayList(BASE_INDEX_ID, 10002L)).when(table).getIndexIdList();
        Mockito.doReturn(BASE_INDEX_ID).when(table).getBaseIndexId();
        Mockito.doReturn(BASE_INDEX_NAME).when(table).getIndexNameById(BASE_INDEX_ID);
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
        meta.reset(table, 100);

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
        // Every index of the emptied table is 0 rows, not unknown.
        Assertions.assertEquals(0, meta.getRowCount(BASE_INDEX_ID));
        Assertions.assertEquals(0, meta.getRowCount(10002L));

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
        meta.reset(table, 500);
        meta.updatedRows.set(5);
        meta.update(analyzeJob(), table);
        Assertions.assertEquals(100, meta.getRowCount(BASE_INDEX_ID));
        Assertions.assertEquals(-95, meta.getBaseIndexDeltaRowCount(table));
        Assertions.assertEquals(5, meta.getRowCount(BASE_INDEX_ID) + meta.getBaseIndexDeltaRowCount(table));
    }

    @Test
    void testFenceOfTransactionsRemovedByTruncate() {
        OlapTable table = mockOlapTable();
        TableStatsMeta meta = new TableStatsMeta();
        meta.reset(table, 100);
        // Transactions which started not later than the watermark of the truncation were removed by it.
        Assertions.assertTrue(meta.isUpdateOfTruncatedRows(50));
        Assertions.assertTrue(meta.isUpdateOfTruncatedRows(100));
        // Transactions which started after the truncation loaded rows of the remaining data.
        Assertions.assertFalse(meta.isUpdateOfTruncatedRows(101));
        // An update which doesn't belong to a load transaction is not fenced.
        Assertions.assertFalse(meta.isUpdateOfTruncatedRows(-1));

        TableStatsMeta notTruncated = new TableStatsMeta();
        notTruncated.reset(table, -1);
        Assertions.assertFalse(notTruncated.isUpdateOfTruncatedRows(50));
    }
}
