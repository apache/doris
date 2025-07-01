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

package org.apache.doris.common.profile;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SummaryProfileTest {

    @Test
    public void testPlanSummary() {
        SummaryProfile profile = new SummaryProfile();
        profile.setQueryBeginTime(1);
        profile.setParseSqlStartTime(3);
        profile.setParseSqlFinishTime(6);
        profile.setNereidsLockTableFinishTime(10);
        profile.setNereidsAnalysisTime(15);
        profile.setNereidsRewriteTime(21);
        profile.setNereidsCollectTablePartitionFinishTime(28);
        profile.setNereidsOptimizeTime(36);
        profile.setNereidsTranslateTime(45);
        profile.setNereidsDistributeTime(55);
        profile.setQueryPlanFinishTime(66);
        profile.setQueryScheduleFinishTime(78);
        profile.setQueryFetchResultFinishTime(91);

        profile.addCollectTablePartitionTime(7);
        // update summary time
        profile.update(ImmutableMap.of());

        RuntimeProfile executionSummary = profile.getExecutionSummary();
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.PARSE_SQL_TIME), "3ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.PLAN_TIME), "60ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_LOCK_TABLE_TIME), "4ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_ANALYSIS_TIME), "5ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_REWRITE_TIME), "6ms");

        Assertions.assertEquals(executionSummary.getInfoString(
                SummaryProfile.NEREIDS_COLLECT_TABLE_PARTITION_TIME), "14ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_OPTIMIZE_TIME), "8ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_TRANSLATE_TIME), "9ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.NEREIDS_DISTRIBUTE_TIME), "10ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.SCHEDULE_TIME), "12ms");
        Assertions.assertEquals(executionSummary.getInfoString(SummaryProfile.WAIT_FETCH_RESULT_TIME), "13ms");
    }
}
