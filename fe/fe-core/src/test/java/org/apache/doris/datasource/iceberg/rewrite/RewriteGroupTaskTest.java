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

package org.apache.doris.datasource.iceberg.rewrite;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.SystemInfoService;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

/**
 * Unit tests for RewriteGroupTask, specifically testing calculateRewriteStrategy logic
 */
public class RewriteGroupTaskTest {

    @Mock
    private RewriteDataGroup mockGroup;

    @Mock
    private IcebergExternalTable mockTable;

    @Mock
    private ConnectContext mockConnectContext;

    @Mock
    private SessionVariable mockSessionVariable;

    @Mock
    private FileScanTask mockFileScanTask;

    @Mock
    private DataFile mockDataFile;

    @Mock
    private Env mockEnv;

    @Mock
    private SystemInfoService mockSystemInfoService;

    private static final long MB = 1024 * 1024L;
    private static final long GB = 1024 * 1024 * 1024L;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup common mocks
        Mockito.when(mockConnectContext.getSessionVariable()).thenReturn(mockSessionVariable);
        Mockito.when(mockSessionVariable.getParallelExecInstanceNum()).thenReturn(8);

        // Mock Env and SystemInfoService
        Mockito.mockStatic(Env.class);
        Mockito.when(Env.getCurrentEnv()).thenReturn(mockEnv);
        Mockito.when(Env.getCurrentSystemInfo()).thenReturn(mockSystemInfoService);
    }

    /**
     * Test small data scenario - should use GATHER distribution
     * Data: 500MB, Target file size: 512MB
     * Expected: 1 file, useGather=true, parallelism=1
     */
    @Test
    public void testCalculateRewriteStrategy_SmallData_UseGather() throws Exception {
        // Setup: 500MB data, 512MB target file size -> expectedFileCount = 1
        long totalSize = 500 * MB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);

        // Create task and invoke private method via reflection
        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        // Verify strategy
        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        Assertions.assertEquals(1, parallelism, "Parallelism should be 1 for small data");
        Assertions.assertTrue(useGather, "Should use GATHER for small data (expected files <= 1)");
    }

    /**
     * Test very small data scenario - data smaller than target file size
     * Data: 100MB, Target file size: 512MB
     * Expected: 1 file, useGather=true, parallelism=1
     */
    @Test
    public void testCalculateRewriteStrategy_VerySmallData_UseGather() throws Exception {
        long totalSize = 100 * MB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        Assertions.assertEquals(1, parallelism);
        Assertions.assertTrue(useGather, "Should use GATHER for very small data");
    }

    /**
     * Test medium data scenario - should limit parallelism without GATHER
     * Data: 5GB, Target file size: 512MB
     * Expected: ~10 files, useGather=false, parallelism=min(10, 100, 8)=8
     */
    @Test
    public void testCalculateRewriteStrategy_MediumData_LimitParallelism() throws Exception {
        long totalSize = 5 * GB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(5GB / 512MB) = ceil(10.24) = 11
        // optimalParallelism = min(11, 100, 8) = 8
        Assertions.assertEquals(8, parallelism, "Parallelism should be limited by default parallelism");
        Assertions.assertFalse(useGather, "Should NOT use GATHER for medium data");
    }

    /**
     * Test large data scenario - should use full parallelism
     * Data: 50GB, Target file size: 512MB
     * Expected: ~100 files, useGather=false, parallelism=min(100, 100, 8)=8
     */
    @Test
    public void testCalculateRewriteStrategy_LargeData_FullParallelism() throws Exception {
        long totalSize = 50 * GB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(50GB / 512MB) = ceil(102.4) = 103
        // optimalParallelism = min(103, 100, 8) = 8
        Assertions.assertEquals(8, parallelism, "Parallelism should be limited by default parallelism");
        Assertions.assertFalse(useGather, "Should NOT use GATHER for large data");
    }

    /**
     * Test boundary case: exactly at threshold (1 file expected)
     * Data: 512MB, Target file size: 512MB
     * Expected: 1 file, useGather=true, parallelism=1
     */
    @Test
    public void testCalculateRewriteStrategy_BoundaryCase_ExactlyOneFile() throws Exception {
        long totalSize = 512 * MB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(512MB / 512MB) = 1
        Assertions.assertEquals(1, parallelism);
        Assertions.assertTrue(useGather, "Should use GATHER when exactly 1 file expected");
    }

    /**
     * Test boundary case: just over threshold (2 files expected)
     * Data: 513MB, Target file size: 512MB
     * Expected: 2 files, useGather=false, parallelism=2
     */
    @Test
    public void testCalculateRewriteStrategy_BoundaryCase_JustOverOneFile() throws Exception {
        long totalSize = 513 * MB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(513MB / 512MB) = 2
        // optimalParallelism = min(2, 100, 8) = 2
        Assertions.assertEquals(2, parallelism);
        Assertions.assertFalse(useGather, "Should NOT use GATHER when 2 files expected");
    }

    /**
     * Test with limited BE count - parallelism limited by available BEs
     * Data: 10GB, Target file size: 512MB, Available BEs: 5
     * Expected: ~20 files, useGather=false, parallelism=min(20, 5, 8)=5
     */
    @Test
    public void testCalculateRewriteStrategy_LimitedByBeCount() throws Exception {
        long totalSize = 10 * GB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 5; // Limited BE count

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(10GB / 512MB) = ceil(20.48) = 21
        // optimalParallelism = min(21, 5, 8) = 5
        Assertions.assertEquals(5, parallelism, "Parallelism should be limited by available BE count");
        Assertions.assertFalse(useGather);
    }

    /**
     * Test with high default parallelism - parallelism limited by expected files
     * Data: 2GB, Target file size: 512MB, Default parallelism: 100
     * Expected: 4 files, useGather=false, parallelism=min(4, 100, 100)=4
     */
    @Test
    public void testCalculateRewriteStrategy_LimitedByExpectedFileCount() throws Exception {
        long totalSize = 2 * GB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);
        Mockito.when(mockSessionVariable.getParallelExecInstanceNum()).thenReturn(100);

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(2GB / 512MB) = ceil(4.096) = 5
        // optimalParallelism = min(5, 100, 100) = 5
        Assertions.assertEquals(5, parallelism, "Parallelism should be limited by expected file count");
        Assertions.assertFalse(useGather);
    }

    /**
     * Test with very small target file size
     * Data: 1GB, Target file size: 100MB
     * Expected: 10 files, useGather=false, parallelism=min(10, 100, 8)=8
     */
    @Test
    public void testCalculateRewriteStrategy_SmallTargetFileSize() throws Exception {
        long totalSize = 1 * GB;
        long targetFileSizeBytes = 100 * MB; // Small target file size
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(1GB / 100MB) = ceil(10.24) = 11
        // optimalParallelism = min(11, 100, 8) = 8
        Assertions.assertEquals(8, parallelism);
        Assertions.assertFalse(useGather);
    }

    /**
     * Test minimum parallelism guarantee
     * Data: 0 bytes (edge case), Target file size: 512MB
     * Expected: parallelism=1 (guaranteed minimum)
     */
    @Test
    public void testCalculateRewriteStrategy_ZeroData_MinimumParallelism() throws Exception {
        long totalSize = 0L;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.emptyList());
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");

        // expectedFileCount = ceil(0 / 512MB) = 0
        // optimalParallelism = max(1, min(0, 100, 8)) = max(1, 0) = 1
        Assertions.assertEquals(1, parallelism, "Parallelism should be at least 1");
    }

    /**
     * Test realistic scenario with multiple partitions
     * Data: 3GB across multiple partitions, Target file size: 512MB
     * Expected: ~6 files, useGather=false, parallelism=min(6, 100, 8)=6
     */
    @Test
    public void testCalculateRewriteStrategy_MultiplePartitions() throws Exception {
        long totalSize = 3 * GB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        // Multiple tasks representing different partitions
        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Arrays.asList(
                mockFileScanTask, mockFileScanTask, mockFileScanTask));
        Mockito.when(mockSystemInfoService.getBackendsNumber(true)).thenReturn(availableBeCount);

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(3GB / 512MB) = ceil(6.144) = 7
        // optimalParallelism = min(7, 100, 8) = 7
        Assertions.assertEquals(7, parallelism);
        Assertions.assertFalse(useGather);
    }

    // ========== Helper Methods ==========

    /**
     * Invoke private method calculateRewriteStrategy using reflection
     */
    private Object invokeCalculateRewriteStrategy(RewriteGroupTask task) throws Exception {
        Method method = RewriteGroupTask.class.getDeclaredMethod("calculateRewriteStrategy");
        method.setAccessible(true);
        return method.invoke(task);
    }

    /**
     * Get field value from RewriteStrategy object using reflection
     */
    private Object getFieldValue(Object obj, String fieldName) throws Exception {
        Class<?> clazz = obj.getClass();
        java.lang.reflect.Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }
}
