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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.datasource.InternalCatalog;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
    void testNewBootstrapStatsSeedsBaseIndexRowCount() {
        InternalCatalog catalog = Mockito.mock(InternalCatalog.class);
        Database database = Mockito.mock(Database.class);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getId()).thenReturn(10L);
        Mockito.when(table.getName()).thenReturn("t1");
        Mockito.when(table.getBaseIndexId()).thenReturn(100L);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getId()).thenReturn(20L);
        Mockito.when(database.getFullName()).thenReturn("db1");
        Mockito.when(catalog.getId()).thenReturn(30L);
        Mockito.when(catalog.getName()).thenReturn("internal");

        TableStatsMeta meta = TableStatsMeta.newBootstrapStats(table, 123L, 123L);

        Assertions.assertEquals(123L, meta.rowCount);
        Assertions.assertEquals(123L, meta.updatedRows.get());
        Assertions.assertEquals(123L, meta.getRowCount(100L));
        Assertions.assertTrue(meta.isColumnsStatsEmpty());
        Assertions.assertFalse(meta.userInjected);
        // Bootstrap should record the current time as updatedTime but leave lastAnalyzeTime as 0.
        Assertions.assertTrue(meta.updatedTime > 0);
        Assertions.assertEquals(0L, meta.lastAnalyzeTime);
    }
}
