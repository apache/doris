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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
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

    private SessionVariable sessionVariable;

    @Mock
    private FileScanTask mockFileScanTask;

    @Mock
    private DataFile mockDataFile;

    @Mock
    private Env mockEnv;

    @Mock
    private SystemInfoService mockSystemInfoService;

    private MockedStatic<Env> mockedStaticEnv;

    private static final long MB = 1024 * 1024L;
    private static final long GB = 1024 * 1024 * 1024L;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        // Setup common mocks
        sessionVariable = new SessionVariable();
        sessionVariable.parallelPipelineTaskNum = 8;
        sessionVariable.maxInstanceNum = 64;
        Mockito.when(mockConnectContext.getSessionVariable()).thenReturn(sessionVariable);

        // Mock Env and SystemInfoService
        mockedStaticEnv = Mockito.mockStatic(Env.class);
        mockedStaticEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
        mockedStaticEnv.when(Env::getCurrentSystemInfo).thenReturn(mockSystemInfoService);
    }

    @AfterEach
    public void tearDown() {
        // Clean up static mock to avoid "already registered" errors
        if (mockedStaticEnv != null) {
            mockedStaticEnv.close();
            mockedStaticEnv = null;
        }
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

        // Create task and invoke private method via reflection
        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

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

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        Assertions.assertEquals(1, parallelism);
        Assertions.assertTrue(useGather, "Should use GATHER for very small data");
    }

    /**
     * Test medium data scenario - should use GATHER when expected files < available BEs
     * Data: 5GB, Target file size: 512MB
     * Expected: expectedFileCount=11, useGather=true, parallelism=min(8, 11)=8
     */
    @Test
    public void testCalculateRewriteStrategy_MediumData_UseGatherWhenExpectedFilesNotExceedBeCount()
            throws Exception {
        long totalSize = 5 * GB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(5GB / 512MB) = ceil(10.24) = 11
        // expectedFileCount < availableBeCount, so use GATHER
        Assertions.assertEquals(8, parallelism, "Parallelism should be limited by default parallelism");
        Assertions.assertTrue(useGather, "Should use GATHER when expected files < available BEs");
    }

    /**
     * Test large data scenario - do NOT use GATHER when expected files == available BEs
     * Data: 50GB, Target file size: 512MB
     * Expected: expectedFileCount=100, useGather=false, parallelism=min(8, floor(100/100)=1)=1
     */
    @Test
    public void testCalculateRewriteStrategy_LargeData_NoGatherWhenExpectedFilesEqualBeCount()
            throws Exception {
        long totalSize = 50 * GB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(50GB / 512MB) = 100
        // expectedFileCount == availableBeCount, so do not use GATHER
        Assertions.assertEquals(1, parallelism, "Parallelism should be limited by expected files / BE count");
        Assertions.assertFalse(useGather, "Should NOT use GATHER when expected files == available BEs");
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

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(512MB / 512MB) = 1
        Assertions.assertEquals(1, parallelism);
        Assertions.assertTrue(useGather, "Should use GATHER when exactly 1 file expected");
    }

    /**
     * Test boundary case: just over 1 file
     * Data: 513MB, Target file size: 512MB
     * Expected: expectedFileCount=2, useGather=true, parallelism=min(8, 2)=2
     */
    @Test
    public void testCalculateRewriteStrategy_BoundaryCase_JustOverOneFile() throws Exception {
        long totalSize = 513 * MB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(513MB / 512MB) = 2
        // expectedFileCount < availableBeCount, so use GATHER
        Assertions.assertEquals(2, parallelism);
        Assertions.assertTrue(useGather, "Should use GATHER when expected files < available BEs");
    }

    /**
     * Test boundary case: expected files equal available BEs - should not use GATHER
     * Data: 512MB, Target file size: 64MB
     * Expected: expectedFileCount=8, useGather=false, parallelism=min(8, floor(8/8)=1)=1
     */
    @Test
    public void testCalculateRewriteStrategy_BoundaryCase_EqualToBeCount_NoGather() throws Exception {
        long totalSize = 512 * MB;
        long targetFileSizeBytes = 64 * MB;
        int availableBeCount = 8;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(512MB / 64MB) = 8
        // expectedFileCount == availableBeCount, so do not use GATHER
        Assertions.assertEquals(1, parallelism);
        Assertions.assertFalse(useGather);
    }

    /**
     * Test GATHER with high default parallelism - should cap at expected file count
     * Data: 513MB, Target file size: 512MB, Default parallelism: 100
     * Expected: expectedFileCount=2, useGather=true, parallelism=min(100, 2)=2
     */
    @Test
    public void testCalculateRewriteStrategy_GatherCapsAtExpectedFileCount() throws Exception {
        long totalSize = 513 * MB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        sessionVariable.parallelPipelineTaskNum = 100;

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(513MB / 512MB) = 2
        // expectedFileCount < availableBeCount, so use GATHER
        Assertions.assertEquals(2, parallelism);
        Assertions.assertTrue(useGather);
    }

    /**
     * Test with limited BE count - parallelism limited by available BEs
     * Data: 10GB, Target file size: 512MB, Available BEs: 5
     * Expected: ~20 files, useGather=false, parallelism=min(8, floor(21/5)=4)=4
     */
    @Test
    public void testCalculateRewriteStrategy_LimitedByBeCount() throws Exception {
        long totalSize = 10 * GB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 5; // Limited BE count

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(10GB / 512MB) = ceil(20.48) = 21
        // maxParallelismByFileCount = floor(21 / 5) = 4
        // optimalParallelism = min(8, 4) = 4
        Assertions.assertEquals(4, parallelism, "Parallelism should be limited by expected files / BE count");
        Assertions.assertFalse(useGather);
    }

    /**
     * Test with high default parallelism - still use GATHER if expected files < available BEs
     * Data: 2GB, Target file size: 512MB, Default parallelism: 100
     * Expected: expectedFileCount=4, useGather=true, parallelism=min(100, 4)=4
     */
    @Test
    public void testCalculateRewriteStrategy_UseGatherEvenWithHighDefaultParallelism() throws Exception {
        long totalSize = 2 * GB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));
        sessionVariable.parallelPipelineTaskNum = 100;

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(2GB / 512MB) = ceil(4.0) = 4
        // expectedFileCount < availableBeCount, so use GATHER
        Assertions.assertEquals(4, parallelism, "Parallelism should be limited by expected file count");
        Assertions.assertTrue(useGather);
    }

    /**
     * Test with very small target file size
     * Data: 1GB, Target file size: 100MB
     * Expected: 11 files, useGather=true, parallelism=min(11, 100, 8)=8
     */
    @Test
    public void testCalculateRewriteStrategy_SmallTargetFileSize() throws Exception {
        long totalSize = 1 * GB;
        long targetFileSizeBytes = 100 * MB; // Small target file size
        int availableBeCount = 100;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(1GB / 100MB) = ceil(10.24) = 11
        // expectedFileCount < availableBeCount, so use GATHER
        Assertions.assertEquals(8, parallelism);
        Assertions.assertTrue(useGather);
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

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");

        // expectedFileCount = ceil(0 / 512MB) = 0
        // optimalParallelism = max(1, min(0, 100, 8)) = max(1, 0) = 1
        Assertions.assertEquals(1, parallelism, "Parallelism should be at least 1");
    }

    /**
     * Test invalid BE count - should throw when available BE count is zero
     */
    @Test
    public void testCalculateRewriteStrategy_ZeroAvailableBe_Throws() throws Exception {
        long totalSize = 1 * GB;
        long targetFileSizeBytes = 512 * MB;
        int availableBeCount = 0;

        Mockito.when(mockGroup.getTotalSize()).thenReturn(totalSize);
        Mockito.when(mockGroup.getTasks()).thenReturn(Collections.singletonList(mockFileScanTask));

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        java.lang.reflect.InvocationTargetException exception = Assertions.assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> invokeCalculateRewriteStrategy(task));

        Assertions.assertTrue(exception.getCause() instanceof IllegalStateException);
        Assertions.assertTrue(exception.getCause().getMessage().contains("availableBeCount"),
                "Exception message should mention availableBeCount");
    }

    /**
     * Test realistic scenario with multiple partitions
     * Data: 3GB across multiple partitions, Target file size: 512MB
     * Expected: ~6 files, useGather=true, parallelism=min(6, 100, 8)=6
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

        RewriteGroupTask task = new RewriteGroupTask(
                mockGroup, 1L, mockTable, mockConnectContext,
                targetFileSizeBytes, availableBeCount, null);

        Object strategy = invokeCalculateRewriteStrategy(task);

        int parallelism = (int) getFieldValue(strategy, "parallelism");
        boolean useGather = (boolean) getFieldValue(strategy, "useGather");

        // expectedFileCount = ceil(3GB / 512MB) = ceil(6.0) = 6
        // expectedFileCount < availableBeCount, so use GATHER
        Assertions.assertEquals(6, parallelism);
        Assertions.assertTrue(useGather);
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
