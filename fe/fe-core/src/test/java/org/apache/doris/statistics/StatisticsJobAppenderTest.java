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
import org.apache.doris.statistics.util.StatisticsUtil;

import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

public class StatisticsJobAppenderTest {
    @Test
    public void testSkip(@Mocked OlapTable olapTable, @Mocked TableStatsMeta stats, @Mocked TableIf anyOtherTable) {
        new MockUp<OlapTable>() {

            @Mock
            public long getDataSize(boolean singleReplica) {
                return StatisticsUtil.getHugeTableLowerBoundSizeInBytes() * 5 + 1000000000;
            }
        };

        new MockUp<AnalysisManager>() {

            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return stats;
            }
        };
        // A very huge table has been updated recently, so we should skip it this time
        stats.updatedTime = System.currentTimeMillis() - 1000;
        stats.newPartitionLoaded = new AtomicBoolean();
        stats.newPartitionLoaded.set(true);
        StatisticsJobAppender appender = new StatisticsJobAppender("appender");
        // Test new partition loaded data for the first time. Not skip.
        Assertions.assertFalse(appender.skip(olapTable));
        stats.newPartitionLoaded.set(false);
        // The update of this huge table is long time ago, so we shouldn't skip it this time
        stats.updatedTime = System.currentTimeMillis()
                - StatisticsUtil.getHugeTableAutoAnalyzeIntervalInMillis() - 10000;
        Assertions.assertFalse(appender.skip(olapTable));
        new MockUp<AnalysisManager>() {

            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return null;
            }
        };
        // can't find table stats meta, which means this table never get analyzed,  so we shouldn't skip it this time
        Assertions.assertFalse(appender.skip(olapTable));
        new MockUp<AnalysisManager>() {

            @Mock
            public TableStatsMeta findTableStatsStatus(long tblId) {
                return stats;
            }
        };
        stats.userInjected = true;
        Assertions.assertTrue(appender.skip(olapTable));

        // this is not olap table nor external table, so we should skip it this time
        Assertions.assertTrue(appender.skip(anyOtherTable));
    }
}
