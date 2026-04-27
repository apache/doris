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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.thrift.TQueryStatistics;
import org.apache.doris.thrift.TQueryStatisticsResult;
import org.apache.doris.thrift.TReportWorkloadRuntimeStatusParams;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

/**
 * Unit test for WorkloadRuntimeStatusMgr.
 * Verifies that query progress statistics from multiple BEs are correctly
 * merged via the Thrift setter-based merge logic.
 */
public class WorkloadRuntimeStatusMgrTest {

    private WorkloadRuntimeStatusMgr mgr;

    @Before
    public void setUp() {
        mgr = new WorkloadRuntimeStatusMgr();
    }

    // ---- Merge: single BE ----

    @Test
    public void testSingleBeProgressMerge() {
        // Simulate one BE reporting progress for one query.
        long beId = 10001L;
        TQueryStatistics stats = new TQueryStatistics();
        stats.setTotalTasksNum(10);
        stats.setFinishedTasksNum(3);

        TReportWorkloadRuntimeStatusParams params = buildParams(beId, "q1", stats);
        mgr.updateBeQueryStats(params);

        Map<String, TQueryStatistics> merged = mgr.getQueryStatisticsMap();
        Assert.assertEquals(1, merged.size());

        TQueryStatistics result = merged.get("q1");
        Assert.assertNotNull(result);
        Assert.assertEquals(10, result.getTotalTasksNum());
        Assert.assertEquals(3, result.getFinishedTasksNum());
    }

    // ---- Merge: multiple BEs, same query (summing across BEs) ----

    @Test
    public void testMultiBeSummingAcrossQuery() {
        // BE1: total=10, finished=3
        // BE2: total=8,  finished=5
        // Merged: total=18, finished=8
        mgr.updateBeQueryStats(buildParams(10001L, "q1", buildStats(10, 3)));
        mgr.updateBeQueryStats(buildParams(10002L, "q1", buildStats(8, 5)));

        Map<String, TQueryStatistics> merged = mgr.getQueryStatisticsMap();
        Assert.assertEquals(1, merged.size());

        TQueryStatistics result = merged.get("q1");
        Assert.assertEquals(18, result.getTotalTasksNum());
        Assert.assertEquals(8, result.getFinishedTasksNum());
    }

    // ---- Merge: multiple BEs, multiple queries remain independent ----

    @Test
    public void testMultiQueryIndependence() {
        mgr.updateBeQueryStats(buildParams(10001L, "q1", buildStats(10, 2)));
        mgr.updateBeQueryStats(buildParams(10001L, "q2", buildStats(20, 15)));

        Map<String, TQueryStatistics> merged = mgr.getQueryStatisticsMap();
        Assert.assertEquals(2, merged.size());

        Assert.assertEquals(10, merged.get("q1").getTotalTasksNum());
        Assert.assertEquals(2, merged.get("q1").getFinishedTasksNum());
        Assert.assertEquals(20, merged.get("q2").getTotalTasksNum());
        Assert.assertEquals(15, merged.get("q2").getFinishedTasksNum());
    }

    // ---- isSet flag: unset fields should not override previous values ----

    @Test
    public void testIsSetPreservesPreviousValues() {
        // BE1 reports total=10, finished=3 with isSet properly set
        mgr.updateBeQueryStats(buildParams(10001L, "q1", buildStats(10, 3)));

        // BE2 reports same query but does NOT set total_tasks_num
        // This simulates an older BE version without progress support.
        TQueryStatistics stats2 = new TQueryStatistics();
        // Intentionally NOT calling setTotalTasksNum/setFinishedTasksNum
        // (isSet* returns false)
        stats2.setScanRows(100);  // set some other field to make it non-empty
        mgr.updateBeQueryStats(buildParams(10002L, "q1", stats2));

        Map<String, TQueryStatistics> merged = mgr.getQueryStatisticsMap();
        TQueryStatistics result = merged.get("q1");

        // BE2 didn't set total/finished, so original values from BE1 should be preserved
        Assert.assertEquals(10, result.getTotalTasksNum());
        Assert.assertEquals(3, result.getFinishedTasksNum());
    }

    // ---- Zero-reporting BE should not interfere ----

    @Test
    public void testBeWithZeroProgress() {
        mgr.updateBeQueryStats(buildParams(10001L, "q1", buildStats(10, 4)));

        // BE2 reports zero progress correctly
        mgr.updateBeQueryStats(buildParams(10002L, "q1", buildStats(0, 0)));

        Map<String, TQueryStatistics> merged = mgr.getQueryStatisticsMap();
        TQueryStatistics result = merged.get("q1");

        // total=10, finished=4 (from BE1); BE2's (0,0) is additive → still (10,4)
        Assert.assertEquals(10, result.getTotalTasksNum());
        Assert.assertEquals(4, result.getFinishedTasksNum());
    }

    // ---- getQueryStatistics returns per-BE map ----

    @Test
    public void testGetQueryStatisticsPerBe() {
        mgr.updateBeQueryStats(buildParams(10001L, "q1", buildStats(5, 2)));
        mgr.updateBeQueryStats(buildParams(10002L, "q1", buildStats(3, 1)));

        Map<Long, TQueryStatisticsResult> perBe = mgr.getQueryStatistics("q1");
        Assert.assertEquals(2, perBe.size());
        Assert.assertTrue(perBe.containsKey(10001L));
        Assert.assertTrue(perBe.containsKey(10002L));
        Assert.assertEquals(5, perBe.get(10001L).getStatistics().getTotalTasksNum());
        Assert.assertEquals(3, perBe.get(10002L).getStatistics().getTotalTasksNum());
    }

    // ---- Non-existent query returns empty map ----

    @Test
    public void testGetQueryStatisticsNonExistent() {
        Map<Long, TQueryStatisticsResult> perBe = mgr.getQueryStatistics("non-existent-query");
        Assert.assertTrue(perBe.isEmpty());
    }

    // ---- updateBeQueryStats with missing fields ----

    @Test
    public void testUpdateBeQueryStatsMissingBackendId() {
        TReportWorkloadRuntimeStatusParams params = new TReportWorkloadRuntimeStatusParams();
        // backend_id not set, updateBeQueryStats should log a warning and return early
        mgr.updateBeQueryStats(params);
        Assert.assertTrue(mgr.getQueryStatisticsMap().isEmpty());
    }

    // ---- updateBeQueryStats with missing query stats map ----

    @Test
    public void testUpdateBeQueryStatsMissingQueryStatsMap() {
        TReportWorkloadRuntimeStatusParams params = new TReportWorkloadRuntimeStatusParams();
        params.setBackendId(10001L);
        // query_statistics_result_map not set → should return early
        mgr.updateBeQueryStats(params);
        Assert.assertTrue(mgr.getQueryStatisticsMap().isEmpty());
    }

    // ---- isSet flag: verifying Thrift setter behavior inline ----

    @Test
    public void testThriftIsSetFlagRequired() {
        // Confirm that using setter (via __set*) sets the __isset flag,
        // whereas direct field assignment does not.
        // This documents the historical bug that was fixed in this feature.

        TQueryStatistics viaSetter = new TQueryStatistics();
        viaSetter.setTotalTasksNum(5);
        Assert.assertTrue("setTotalTasksNum via setter must set __isset flag",
                viaSetter.isSetTotalTasksNum());

        TQueryStatistics viaField = new TQueryStatistics();
        viaField.total_tasks_num = 5;  // direct field assignment
        Assert.assertFalse("direct field assignment must NOT set __isset flag",
                viaField.isSetTotalTasksNum());

        // Same for finished_tasks_num
        viaSetter.setFinishedTasksNum(3);
        Assert.assertTrue(viaSetter.isSetFinishedTasksNum());

        viaField.finished_tasks_num = 3;
        Assert.assertFalse(viaField.isSetFinishedTasksNum());
    }

    // ---- Merge without any progress fields ----

    @Test
    public void testMergeWithoutProgressFields() {
        // Regression test: when isSet is false for progress fields,
        // merge should not touch them, leaving them at default (0).
        TQueryStatistics stats = new TQueryStatistics();
        // Intentionally leave total/finished unset
        mgr.updateBeQueryStats(buildParams(10001L, "q1", stats));

        Map<String, TQueryStatistics> merged = mgr.getQueryStatisticsMap();
        TQueryStatistics result = merged.get("q1");

        // Fields should still be 0 and isSet should be false
        Assert.assertEquals(0, result.getTotalTasksNum());
        Assert.assertEquals(0, result.getFinishedTasksNum());
    }

    // ---- Merge: three BEs combined ----

    @Test
    public void testThreeBeMergeProgress() {
        mgr.updateBeQueryStats(buildParams(10001L, "q1", buildStats(4, 1)));
        mgr.updateBeQueryStats(buildParams(10002L, "q1", buildStats(3, 3)));
        mgr.updateBeQueryStats(buildParams(10003L, "q1", buildStats(5, 0)));

        Map<String, TQueryStatistics> merged = mgr.getQueryStatisticsMap();
        Assert.assertEquals(1, merged.size());

        TQueryStatistics result = merged.get("q1");
        // total = 4 + 3 + 5 = 12, finished = 1 + 3 + 0 = 4
        Assert.assertEquals(12, result.getTotalTasksNum());
        Assert.assertEquals(4, result.getFinishedTasksNum());
    }

    // ---- helper methods ----

    private TQueryStatistics buildStats(int totalTasks, int finishedTasks) {
        TQueryStatistics stats = new TQueryStatistics();
        stats.setTotalTasksNum(totalTasks);
        stats.setFinishedTasksNum(finishedTasks);
        return stats;
    }

    private TReportWorkloadRuntimeStatusParams buildParams(long beId, String queryId, TQueryStatistics stats) {
        TQueryStatisticsResult result = new TQueryStatisticsResult();
        result.setStatistics(stats);

        TReportWorkloadRuntimeStatusParams params = new TReportWorkloadRuntimeStatusParams();
        params.setBackendId(beId);
        Map<String, TQueryStatisticsResult> map = Maps.newHashMap();
        map.put(queryId, result);
        params.setQueryStatisticsResultMap(map);
        return params;
    }
}
