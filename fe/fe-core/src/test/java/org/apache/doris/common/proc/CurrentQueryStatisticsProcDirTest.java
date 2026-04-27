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

package org.apache.doris.common.proc;

import org.apache.doris.qe.QeProcessor;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.QueryStatisticsItem;
import org.apache.doris.thrift.TQueryStatistics;

import com.google.common.collect.Maps;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

/**
 * Unit test for CurrentQueryStatisticsProcDir.
 * Verifies that TotalTasks, FinishedTasks, and Progress are correctly
 * computed and displayed in SHOW PROC '/current_queries'.
 */
public class CurrentQueryStatisticsProcDirTest {

    private QeProcessor originalInstance;

    @Before
    public void setUp() throws Exception {
        // Save original INSTANCE
        originalInstance = getQeProcessorInstance();
    }

    @After
    public void tearDown() throws Exception {
        // Restore original INSTANCE
        setQeProcessorInstance(originalInstance);
    }

    @Test
    public void testProgressDisplayedCorrectly() throws Exception {
        // Build a TQueryStatistics with known progress values using Thrift setters
        TQueryStatistics stats = new TQueryStatistics();
        stats.setTotalTasksNum(20);
        stats.setFinishedTasksNum(7);

        QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .queryId("test-query-1")
                .queryStatistics(stats)
                .build();

        setQeProcessorInstance(createMockWithItems(item));

        CurrentQueryStatisticsProcDir dir = new CurrentQueryStatisticsProcDir();
        ProcResult result = dir.fetchResult();

        Assert.assertEquals(1, result.getRows().size());
        java.util.List<String> row = result.getRows().get(0);

        // Column indices: TotalTasks=22, FinishedTasks=23, Progress=24
        // (based on the TITLE_NAMES order in CurrentQueryStatisticsProcDir)
        Assert.assertEquals("20", row.get(22));
        Assert.assertEquals("7", row.get(23));
        Assert.assertEquals("35.0%", row.get(24));
    }

    @Test
    public void testProgressZeroTotal() throws Exception {
        // When total = 0, Progress should display "0.0%" to avoid division by zero
        TQueryStatistics stats = new TQueryStatistics();
        // total_tasks_num not set → isSet returns false

        QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .queryId("test-query-zero")
                .queryStatistics(stats)
                .build();

        setQeProcessorInstance(createMockWithItems(item));

        CurrentQueryStatisticsProcDir dir = new CurrentQueryStatisticsProcDir();
        ProcResult result = dir.fetchResult();

        java.util.List<String> row = result.getRows().get(0);
        Assert.assertEquals("0", row.get(22));  // TotalTasks defaults to 0
        Assert.assertEquals("0", row.get(23));  // FinishedTasks defaults to 0
        Assert.assertEquals("0.0%", row.get(24)); // Progress computed as 0.0%
    }

    @Test
    public void testProgressAllFinished() throws Exception {
        // When finished == total, Progress should show "100%"
        TQueryStatistics stats = new TQueryStatistics();
        stats.setTotalTasksNum(8);
        stats.setFinishedTasksNum(8);

        QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .queryId("test-query-done")
                .queryStatistics(stats)
                .build();

        setQeProcessorInstance(createMockWithItems(item));

        CurrentQueryStatisticsProcDir dir = new CurrentQueryStatisticsProcDir();
        ProcResult result = dir.fetchResult();

        java.util.List<String> row = result.getRows().get(0);
        Assert.assertEquals("8", row.get(22));
        Assert.assertEquals("8", row.get(23));
        Assert.assertEquals("100.0%", row.get(24));
    }

    @Test
    public void testProgressPartialFinished() throws Exception {
        TQueryStatistics stats = new TQueryStatistics();
        stats.setTotalTasksNum(3);
        stats.setFinishedTasksNum(1);

        QueryStatisticsItem item = new QueryStatisticsItem.Builder()
                .queryId("test-query-partial")
                .queryStatistics(stats)
                .build();

        setQeProcessorInstance(createMockWithItems(item));

        CurrentQueryStatisticsProcDir dir = new CurrentQueryStatisticsProcDir();
        ProcResult result = dir.fetchResult();

        java.util.List<String> row = result.getRows().get(0);
        Assert.assertEquals("3", row.get(22));
        Assert.assertEquals("1", row.get(23));
        Assert.assertEquals("33.3%", row.get(24));
    }

    @Test
    public void testMultipleQueriesSortedByExecTime() throws Exception {
        // Multiple queries should be sorted by exec time (descending).
        // Create items with different queryStartTime to control ordering.

        TQueryStatistics stats1 = new TQueryStatistics();
        stats1.setTotalTasksNum(10);
        stats1.setFinishedTasksNum(5);

        TQueryStatistics stats2 = new TQueryStatistics();
        stats2.setTotalTasksNum(20);
        stats2.setFinishedTasksNum(15);

        long now = System.currentTimeMillis();
        QueryStatisticsItem item1 = new QueryStatisticsItem.Builder()
                .queryId("query-new")
                .queryStartTime(now - 1000)  // started 1 second ago
                .queryStatistics(stats1)
                .build();
        QueryStatisticsItem item2 = new QueryStatisticsItem.Builder()
                .queryId("query-old")
                .queryStartTime(now - 5000)  // started 5 seconds ago
                .queryStatistics(stats2)
                .build();

        Map<String, QueryStatisticsItem> items = Maps.newHashMap();
        items.put("query-new", item1);
        items.put("query-old", item2);

        setQeProcessorInstance(createMock(items));

        CurrentQueryStatisticsProcDir dir = new CurrentQueryStatisticsProcDir();
        ProcResult result = dir.fetchResult();

        // Sorted by exec time descending → newer query first
        Assert.assertEquals(2, result.getRows().size());
        Assert.assertEquals("query-new", result.getRows().get(0).get(0));
        Assert.assertEquals("query-old", result.getRows().get(1).get(0));
    }

    @Test
    public void testEmptyQueries() throws Exception {
        // When there are no running queries, fetchResult should return only headers.
        setQeProcessorInstance(createMock(Maps.newHashMap()));

        CurrentQueryStatisticsProcDir dir = new CurrentQueryStatisticsProcDir();
        ProcResult result = dir.fetchResult();

        Assert.assertTrue("Should not throw when there are no running queries",
                result.getRows().isEmpty());
    }

    // ---- helper methods ----

    private QeProcessor createMockWithItems(QueryStatisticsItem... items) {
        Map<String, QueryStatisticsItem> map = Maps.newHashMap();
        for (QueryStatisticsItem item : items) {
            map.put(item.getQueryId(), item);
        }
        return createMock(map);
    }

    private QeProcessor createMock(Map<String, QueryStatisticsItem> items) {
        QeProcessor mock = Mockito.mock(QeProcessor.class);
        Mockito.when(mock.getQueryStatistics()).thenReturn(items);
        return mock;
    }

    private QeProcessor getQeProcessorInstance() throws Exception {
        Field field = QeProcessorImpl.class.getDeclaredField("INSTANCE");
        field.setAccessible(true);
        return (QeProcessor) field.get(null);
    }

    private void setQeProcessorInstance(QeProcessor instance) throws Exception {
        Field field = QeProcessorImpl.class.getDeclaredField("INSTANCE");
        field.setAccessible(true);

        // Remove final modifier so we can set the field in tests
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.set(null, instance);
    }
}
