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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Pair;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashSet;

class TableStatsMetaTest {

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
    void testBaseIndexDeltaRowCountUsesCollectedBaseline() {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.doReturn(0L).when(table).getBaseIndexId();
        Mockito.doReturn("index0").when(table).getIndexNameById(0L);
        Mockito.doReturn(Lists.newArrayList(0L)).when(table).getIndexIdList();
        Mockito.doReturn(new HashSet<Column>()).when(table).getSchemaAllIndexes(Mockito.anyBoolean());
        Mockito.doReturn(new HashSet<>()).when(table).getColumnIndexPairs(Mockito.any());

        TableStatsMeta meta = new TableStatsMeta();
        meta.updatedRows.set(150);

        // The job was built when the load counter was 100; 50 more rows became visible before the
        // analyze task ran its COUNT(1), which collected 150 rows.
        AnalysisInfo jobInfo = new AnalysisInfoBuilder().setRowCount(150).setUpdateRows(100)
                .setColName("k1")
                .setJobColumns(new HashSet<>(Collections.singletonList(Pair.of("index0", "k1"))))
                .build();
        jobInfo.addIndexRowCount(0, 150);
        // What BaseAnalysisTask#runQuery does right after collecting the count (#69281).
        jobInfo.updateRows = meta.updatedRows.get();

        meta.update(jobInfo, table);

        Assertions.assertEquals(150, meta.getRowCount(0));
        // The fallback estimate must not add the 50 rows that the collected count already includes.
        Assertions.assertEquals(0, meta.getBaseIndexDeltaRowCount(table));

        // Without the refresh at collection time the same load would be counted twice.
        jobInfo.updateRows = 100;
        meta.update(jobInfo, table);
        Assertions.assertEquals(50, meta.getBaseIndexDeltaRowCount(table));
    }
}
