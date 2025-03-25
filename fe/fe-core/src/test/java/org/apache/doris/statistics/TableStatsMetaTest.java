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

import org.apache.doris.catalog.OlapTable;

import com.google.common.collect.Lists;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

class TableStatsMetaTest {

    @Test
    void update(@Mocked OlapTable table) {
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

        new MockUp<OlapTable>() {
            @Mock
            public List<Long> getIndexIdList() {
                List<Long> result = Lists.newArrayList();
                result.add(1L);
                return result;
            }
        };

        OlapTable table = new OlapTable();

        meta.clearStaleIndexRowCount(table);
        Assertions.assertEquals(1, meta.getRowCount(1));
        Assertions.assertEquals(-1, meta.getRowCount(2));
        Assertions.assertEquals(-1, meta.getRowCount(3));
        Assertions.assertEquals(-1, meta.getRowCount(4));
    }
}
