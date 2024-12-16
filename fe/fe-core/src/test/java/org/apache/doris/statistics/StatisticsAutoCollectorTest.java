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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.statistics.AnalysisInfo.AnalysisMethod;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

public class StatisticsAutoCollectorTest {

    @Test
    public void testCollect() throws DdlException, ExecutionException, InterruptedException {
        StatisticsAutoCollector collector = new StatisticsAutoCollector();
        final int[] count = {0, 0};
        new MockUp<StatisticsAutoCollector>() {
            @Mock
            protected Pair<TableIf, JobPriority> fetchOneJob() {
                count[0]++;
                return Pair.of(null, JobPriority.LOW);
            }
        };

        new MockUp<AnalysisManager>() {
            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                count[1]++;
                return null;
            }
        };
        collector.collect();
        Assertions.assertEquals(1, count[0]);
        Assertions.assertEquals(0, count[1]);

        OlapTable table = new OlapTable();
        new MockUp<StatisticsAutoCollector>() {
            @Mock
            protected Pair<TableIf, JobPriority> fetchOneJob() {
                if (count[0] == 0) {
                    count[0]++;
                    return Pair.of(table, JobPriority.LOW);
                }
                count[0]++;
                return Pair.of(null, JobPriority.LOW);
            }

            @Mock
            protected void processOneJob(TableIf table, JobPriority priority) {
                return;
            }
        };
        count[0] = 0;
        count[1] = 0;
        collector.collect();
        Assertions.assertEquals(2, count[0]);
        Assertions.assertEquals(1, count[1]);
    }

    @Test
    public void testFetchOneJob() throws InterruptedException {
        OlapTable table1 = new OlapTable();
        OlapTable table2 = new OlapTable();
        StatisticsAutoCollector collector = new StatisticsAutoCollector();
        collector.appendToHighPriorityJobs(table1);
        collector.appendToLowPriorityJobs(table2);
        Pair<TableIf, JobPriority> jobPair = collector.fetchOneJob();
        Assertions.assertSame(table1, jobPair.first);
        Assertions.assertEquals(JobPriority.HIGH, jobPair.second);
        jobPair = collector.fetchOneJob();
        Assertions.assertSame(table2, jobPair.first);
        Assertions.assertEquals(JobPriority.LOW, jobPair.second);
        jobPair = collector.fetchOneJob();
        Assertions.assertNull(jobPair.first);
    }

    @Test
    public void testTableRowCountReported() {
        StatisticsAutoCollector collector = new StatisticsAutoCollector();
        ExternalTable externalTable = new ExternalTable();
        Assertions.assertTrue(collector.tableRowCountReported(externalTable, AnalysisMethod.SAMPLE));
        OlapTable olapTable = new OlapTable();
        Assertions.assertTrue(collector.tableRowCountReported(olapTable, AnalysisMethod.FULL));
        Assertions.assertTrue(collector.tableRowCountReported(externalTable, AnalysisMethod.FULL));
        new MockUp<OlapTable>() {
            @Mock
            public long getRowCountForIndex(long indexId, boolean strict) {
                return TableIf.UNKNOWN_ROW_COUNT;
            }
        };
        Assertions.assertFalse(collector.tableRowCountReported(olapTable, AnalysisMethod.SAMPLE));
        new MockUp<OlapTable>() {
            @Mock
            public long getRowCountForIndex(long indexId, boolean strict) {
                return TableIf.UNKNOWN_ROW_COUNT + 1;
            }
        };
        Assertions.assertTrue(collector.tableRowCountReported(olapTable, AnalysisMethod.SAMPLE));
    }
}
