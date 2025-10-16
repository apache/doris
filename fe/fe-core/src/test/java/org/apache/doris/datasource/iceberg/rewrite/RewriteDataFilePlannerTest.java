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

import org.apache.doris.common.UserException;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Unit tests for RewriteDataFilePlanner
 */
public class RewriteDataFilePlannerTest {

    @Mock
    private Table mockTable;

    @Mock
    private TableScan mockTableScan;

    @Mock
    private Schema mockSchema;

    @Mock
    private PartitionSpec mockPartitionSpec;

    @Mock
    private DataFile mockDataFile;

    @Mock
    private DeleteFile mockDeleteFile;

    @Mock
    private FileScanTask mockFileScanTask;

    @Mock
    private StructLike mockPartition;

    private RewriteDataFilePlanner.Parameters defaultParameters;
    private RewriteDataFilePlanner planner;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        // Create default parameters for testing
        defaultParameters = new RewriteDataFilePlanner.Parameters(
                128 * 1024 * 1024L, // targetFileSizeBytes: 128MB
                64 * 1024 * 1024L, // minFileSizeBytes: 64MB
                256 * 1024 * 1024L, // maxFileSizeBytes: 256MB
                2, // minInputFiles
                false, // rewriteAll
                512 * 1024 * 1024L, // maxFileGroupSizeBytes: 512MB
                3, // deleteFileThreshold
                0.1, // deleteRatioThreshold: 10%
                1L, // outputSpecId
                Optional.empty(), // partitionFilter
                Optional.empty() // whereCondition
        );

        planner = new RewriteDataFilePlanner(defaultParameters);
    }

    @Test
    public void testParametersGetters() {
        Assertions.assertEquals(128 * 1024 * 1024L, defaultParameters.getTargetFileSizeBytes());
        Assertions.assertEquals(64 * 1024 * 1024L, defaultParameters.getMinFileSizeBytes());
        Assertions.assertEquals(256 * 1024 * 1024L, defaultParameters.getMaxFileSizeBytes());
        Assertions.assertEquals(2, defaultParameters.getMinInputFiles());
        Assertions.assertFalse(defaultParameters.isRewriteAll());
        Assertions.assertEquals(512 * 1024 * 1024L, defaultParameters.getMaxFileGroupSizeBytes());
        Assertions.assertEquals(3, defaultParameters.getDeleteFileThreshold());
        Assertions.assertEquals(0.1, defaultParameters.getDeleteRatioThreshold(), 0.001);
        Assertions.assertFalse(defaultParameters.hasPartitionFilter());
        Assertions.assertFalse(defaultParameters.hasWhereCondition());
    }

    @Test
    public void testParametersToString() {
        String toString = defaultParameters.toString();
        Assertions.assertTrue(toString.contains("targetFileSizeBytes=134217728"));
        Assertions.assertTrue(toString.contains("minFileSizeBytes=67108864"));
        Assertions.assertTrue(toString.contains("maxFileSizeBytes=268435456"));
        Assertions.assertTrue(toString.contains("minInputFiles=2"));
        Assertions.assertTrue(toString.contains("rewriteAll=false"));
        Assertions.assertTrue(toString.contains("hasPartitionFilter=false"));
        Assertions.assertTrue(toString.contains("hasWhereCondition=false"));
    }

    @Test
    public void testPlanAndOrganizeTasksWithRewriteAll() throws UserException {
        // Test with rewriteAll = true
        RewriteDataFilePlanner.Parameters rewriteAllParams = new RewriteDataFilePlanner.Parameters(
                128 * 1024 * 1024L, 64 * 1024 * 1024L, 256 * 1024 * 1024L,
                2, true, 512 * 1024 * 1024L, 3, 0.1, 1L,
                Optional.empty(), Optional.empty());

        RewriteDataFilePlanner rewriteAllPlanner = new RewriteDataFilePlanner(rewriteAllParams);

        // Mock table scan
        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));

        // Mock file scan task
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockFileScanTask.deletes()).thenReturn(null);
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = rewriteAllPlanner.planAndOrganizeTasks(mockTable);

        Assertions.assertNotNull(result);
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).getTaskCount());
        Assertions.assertEquals(100 * 1024 * 1024L, result.get(0).getTotalSize());
    }

    @Test
    public void testFileSizeFiltering() throws UserException {
        // Test file size filtering logic
        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockFileScanTask.deletes()).thenReturn(null);
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        // Test file too small (should be filtered out)
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(32 * 1024 * 1024L); // 32MB < 64MB min

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty());

        // Test file too large (should be filtered out)
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(300 * 1024 * 1024L); // 300MB > 256MB max

        result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty());

        // Test file in acceptable range (should be included)
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L); // 100MB between 64MB-256MB

        result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertFalse(result.isEmpty());
    }

    @Test
    public void testDeleteFileThreshold() throws UserException {
        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        // Test with delete files below threshold (should be filtered out)
        Mockito.when(mockFileScanTask.deletes()).thenReturn(Arrays.asList(mockDeleteFile, mockDeleteFile)); // 2 < 3 threshold

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty());

        // Test with delete files at threshold (should be included)
        Mockito.when(mockFileScanTask.deletes()).thenReturn(Arrays.asList(mockDeleteFile, mockDeleteFile, mockDeleteFile)); // 3 = 3 threshold

        result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertFalse(result.isEmpty());
    }

    @Test
    public void testDeleteRatioThreshold() throws UserException {
        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        // Mock delete file with record count
        Mockito.when(mockDeleteFile.recordCount()).thenReturn(5L);
        Mockito.when(mockDataFile.recordCount()).thenReturn(100L); // 5/100 = 5% < 10% threshold

        Mockito.when(mockFileScanTask.deletes()).thenReturn(Collections.singletonList(mockDeleteFile));

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty());

        // Test with high delete ratio (should be included)
        Mockito.when(mockDeleteFile.recordCount()).thenReturn(15L); // 15/100 = 15% > 10% threshold

        result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertFalse(result.isEmpty());
    }

    @Test
    public void testGroupFilteringByInputFiles() throws UserException {
        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockFileScanTask.deletes()).thenReturn(null);
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        // Single file group (1 < 2 minInputFiles) should be filtered out
        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGroupFilteringByContentSize() throws UserException {
        // Create parameters with lower target file size for testing
        RewriteDataFilePlanner.Parameters params = new RewriteDataFilePlanner.Parameters(
                50 * 1024 * 1024L, // targetFileSizeBytes: 50MB
                64 * 1024 * 1024L, // minFileSizeBytes: 64MB
                256 * 1024 * 1024L, // maxFileSizeBytes: 256MB
                1, // minInputFiles
                false, // rewriteAll
                512 * 1024 * 1024L, // maxFileGroupSizeBytes: 512MB
                3, // deleteFileThreshold
                0.1, // deleteRatioThreshold: 10%
                1L, // outputSpecId
                Optional.empty(), // partitionFilter
                Optional.empty() // whereCondition
        );

        RewriteDataFilePlanner testPlanner = new RewriteDataFilePlanner(params);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockFileScanTask.deletes()).thenReturn(null);
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L); // 100MB > 50MB target
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = testPlanner.planAndOrganizeTasks(mockTable);
        Assertions.assertFalse(result.isEmpty());
        Assertions.assertEquals(1, result.get(0).getTaskCount());
        Assertions.assertEquals(100 * 1024 * 1024L, result.get(0).getTotalSize());
    }

    @Test
    public void testGroupFilteringByMaxFileGroupSize() throws UserException {
        // Create parameters with very small max file group size
        RewriteDataFilePlanner.Parameters params = new RewriteDataFilePlanner.Parameters(
                128 * 1024 * 1024L, // targetFileSizeBytes: 128MB
                64 * 1024 * 1024L, // minFileSizeBytes: 64MB
                256 * 1024 * 1024L, // maxFileSizeBytes: 256MB
                1, // minInputFiles
                false, // rewriteAll
                50 * 1024 * 1024L, // maxFileGroupSizeBytes: 50MB (very small)
                3, // deleteFileThreshold
                0.1, // deleteRatioThreshold: 10%
                1L, // outputSpecId
                Optional.empty(), // partitionFilter
                Optional.empty() // whereCondition
        );

        RewriteDataFilePlanner testPlanner = new RewriteDataFilePlanner(params);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockFileScanTask.deletes()).thenReturn(null);
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L); // 100MB > 50MB max group size
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = testPlanner.planAndOrganizeTasks(mockTable);
        Assertions.assertFalse(result.isEmpty()); // Should be included because it's too much content
    }

    @Test
    public void testPartitionGrouping() throws UserException {
        // Create two file scan tasks with different partitions
        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);
        DataFile dataFile1 = Mockito.mock(DataFile.class);
        DataFile dataFile2 = Mockito.mock(DataFile.class);
        StructLike partition1 = Mockito.mock(StructLike.class);
        StructLike partition2 = Mockito.mock(StructLike.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(Arrays.asList(task1, task2)));

        // Task 1
        Mockito.when(task1.file()).thenReturn(dataFile1);
        Mockito.when(task1.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task1.deletes()).thenReturn(null);
        Mockito.when(dataFile1.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(dataFile1.partition()).thenReturn(partition1);

        // Task 2
        Mockito.when(task2.file()).thenReturn(dataFile2);
        Mockito.when(task2.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task2.deletes()).thenReturn(null);
        Mockito.when(dataFile2.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(dataFile2.partition()).thenReturn(partition2);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        // Use rewriteAll to avoid filtering
        RewriteDataFilePlanner.Parameters rewriteAllParams = new RewriteDataFilePlanner.Parameters(
                128 * 1024 * 1024L, 64 * 1024 * 1024L, 256 * 1024 * 1024L,
                1, true, 512 * 1024 * 1024L, 3, 0.1, 1L,
                Optional.empty(), Optional.empty());

        RewriteDataFilePlanner rewriteAllPlanner = new RewriteDataFilePlanner(rewriteAllParams);
        List<RewriteDataGroup> result = rewriteAllPlanner.planAndOrganizeTasks(mockTable);

        // Should create two separate groups for different partitions
        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals(1, result.get(0).getTaskCount());
        Assertions.assertEquals(1, result.get(1).getTaskCount());
    }

    @Test
    public void testExceptionHandling() {
        Mockito.when(mockTable.newScan()).thenThrow(new RuntimeException("Table scan failed"));

        UserException exception = Assertions.assertThrows(UserException.class, () -> {
            planner.planAndOrganizeTasks(mockTable);
        });

        Assertions.assertTrue(exception.getMessage().contains("Failed to plan file scan tasks"));
        Assertions.assertTrue(exception.getCause() instanceof RuntimeException);
        Assertions.assertEquals("Table scan failed", exception.getCause().getMessage());
    }

    @Test
    public void testEmptyTableScan() throws UserException {
        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(Collections.emptyList()));

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testRewriteDataGroupOperations() {
        RewriteDataGroup group = new RewriteDataGroup();

        Assertions.assertTrue(group.isEmpty());
        Assertions.assertEquals(0, group.getTaskCount());
        Assertions.assertEquals(0, group.getTotalSize());
        Assertions.assertTrue(group.getDataFiles().isEmpty());

        // Add a task
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);

        group.addTask(mockFileScanTask);

        Assertions.assertFalse(group.isEmpty());
        Assertions.assertEquals(1, group.getTaskCount());
        Assertions.assertEquals(100 * 1024 * 1024L, group.getTotalSize());
        Assertions.assertEquals(1, group.getDataFiles().size());
        Assertions.assertTrue(group.getDataFiles().contains(mockDataFile));
    }

    @Test
    public void testParametersEdgeCases() {
        // Test with zero values
        RewriteDataFilePlanner.Parameters zeroParams = new RewriteDataFilePlanner.Parameters(
                0L, 0L, 0L, 0, true, 0L, 0, 0.0, 0L,
                Optional.empty(), Optional.empty());

        Assertions.assertEquals(0L, zeroParams.getTargetFileSizeBytes());
        Assertions.assertEquals(0L, zeroParams.getMinFileSizeBytes());
        Assertions.assertEquals(0L, zeroParams.getMaxFileSizeBytes());
        Assertions.assertEquals(0, zeroParams.getMinInputFiles());
        Assertions.assertTrue(zeroParams.isRewriteAll());
        Assertions.assertEquals(0L, zeroParams.getMaxFileGroupSizeBytes());
        Assertions.assertEquals(0, zeroParams.getDeleteFileThreshold());
        Assertions.assertEquals(0.0, zeroParams.getDeleteRatioThreshold(), 0.001);
    }

    @Test
    public void testFileSizeFilteringWithMultipleFiles() throws UserException {
        // Create multiple file scan tasks with different sizes
        FileScanTask smallFile = Mockito.mock(FileScanTask.class);
        FileScanTask mediumFile = Mockito.mock(FileScanTask.class);
        FileScanTask largeFile = Mockito.mock(FileScanTask.class);
        DataFile smallDataFile = Mockito.mock(DataFile.class);
        DataFile mediumDataFile = Mockito.mock(DataFile.class);
        DataFile largeDataFile = Mockito.mock(DataFile.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Arrays.asList(smallFile, mediumFile, largeFile)));

        // Small file (should be filtered out)
        Mockito.when(smallFile.file()).thenReturn(smallDataFile);
        Mockito.when(smallFile.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(smallFile.deletes()).thenReturn(null);
        Mockito.when(smallDataFile.fileSizeInBytes()).thenReturn(32 * 1024 * 1024L); // 32MB < 64MB min
        Mockito.when(smallDataFile.partition()).thenReturn(mockPartition);

        // Medium file (should be included)
        Mockito.when(mediumFile.file()).thenReturn(mediumDataFile);
        Mockito.when(mediumFile.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mediumFile.deletes()).thenReturn(null);
        Mockito.when(mediumDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L); // 100MB between 64MB-256MB
        Mockito.when(mediumDataFile.partition()).thenReturn(mockPartition);

        // Large file (should be filtered out)
        Mockito.when(largeFile.file()).thenReturn(largeDataFile);
        Mockito.when(largeFile.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(largeFile.deletes()).thenReturn(null);
        Mockito.when(largeDataFile.fileSizeInBytes()).thenReturn(300 * 1024 * 1024L); // 300MB > 256MB max
        Mockito.when(largeDataFile.partition()).thenReturn(mockPartition);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);

        // Only medium file should be included
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).getTaskCount());
        Assertions.assertEquals(100 * 1024 * 1024L, result.get(0).getTotalSize());
    }

    @Test
    public void testDeleteFileThresholdWithMultipleFiles() throws UserException {
        // Create multiple file scan tasks with different delete file counts
        FileScanTask lowDeletes = Mockito.mock(FileScanTask.class);
        FileScanTask highDeletes = Mockito.mock(FileScanTask.class);
        DataFile dataFile1 = Mockito.mock(DataFile.class);
        DataFile dataFile2 = Mockito.mock(DataFile.class);
        DeleteFile deleteFile1 = Mockito.mock(DeleteFile.class);
        DeleteFile deleteFile2 = Mockito.mock(DeleteFile.class);
        DeleteFile deleteFile3 = Mockito.mock(DeleteFile.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Arrays.asList(lowDeletes, highDeletes)));

        // File with low delete count (should be filtered out)
        Mockito.when(lowDeletes.file()).thenReturn(dataFile1);
        Mockito.when(lowDeletes.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(lowDeletes.deletes()).thenReturn(Arrays.asList(deleteFile1, deleteFile2)); // 2 < 3 threshold
        Mockito.when(dataFile1.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(dataFile1.partition()).thenReturn(mockPartition);

        // File with high delete count (should be included)
        Mockito.when(highDeletes.file()).thenReturn(dataFile2);
        Mockito.when(highDeletes.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(highDeletes.deletes()).thenReturn(Arrays.asList(deleteFile1, deleteFile2, deleteFile3)); // 3 = 3 threshold
        Mockito.when(dataFile2.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(dataFile2.partition()).thenReturn(mockPartition);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);

        // Only high delete count file should be included
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).getTaskCount());
        Assertions.assertEquals(100 * 1024 * 1024L, result.get(0).getTotalSize());
    }

    @Test
    public void testDeleteRatioThresholdWithMultipleFiles() throws UserException {
        // Create multiple file scan tasks with different delete ratios
        FileScanTask lowRatio = Mockito.mock(FileScanTask.class);
        FileScanTask highRatio = Mockito.mock(FileScanTask.class);
        DataFile dataFile1 = Mockito.mock(DataFile.class);
        DataFile dataFile2 = Mockito.mock(DataFile.class);
        DeleteFile deleteFile1 = Mockito.mock(DeleteFile.class);
        DeleteFile deleteFile2 = Mockito.mock(DeleteFile.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(Arrays.asList(lowRatio, highRatio)));

        // File with low delete ratio (should be filtered out)
        Mockito.when(lowRatio.file()).thenReturn(dataFile1);
        Mockito.when(lowRatio.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(lowRatio.deletes()).thenReturn(Collections.singletonList(deleteFile1));
        Mockito.when(dataFile1.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(dataFile1.partition()).thenReturn(mockPartition);
        Mockito.when(deleteFile1.recordCount()).thenReturn(5L);
        Mockito.when(dataFile1.recordCount()).thenReturn(100L); // 5/100 = 5% < 10% threshold

        // File with high delete ratio (should be included)
        Mockito.when(highRatio.file()).thenReturn(dataFile2);
        Mockito.when(highRatio.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(highRatio.deletes()).thenReturn(Collections.singletonList(deleteFile2));
        Mockito.when(dataFile2.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(dataFile2.partition()).thenReturn(mockPartition);
        Mockito.when(deleteFile2.recordCount()).thenReturn(15L);
        Mockito.when(dataFile2.recordCount()).thenReturn(100L); // 15/100 = 15% > 10% threshold

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);

        // Only high delete ratio file should be included
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).getTaskCount());
        Assertions.assertEquals(100 * 1024 * 1024L, result.get(0).getTotalSize());
    }

    @Test
    public void testGroupFilteringWithMultipleFiles() throws UserException {
        // Create parameters that require multiple files for grouping
        RewriteDataFilePlanner.Parameters params = new RewriteDataFilePlanner.Parameters(
                128 * 1024 * 1024L, // targetFileSizeBytes: 128MB
                64 * 1024 * 1024L, // minFileSizeBytes: 64MB
                256 * 1024 * 1024L, // maxFileSizeBytes: 256MB
                3, // minInputFiles: 3
                false, // rewriteAll
                512 * 1024 * 1024L, // maxFileGroupSizeBytes: 512MB
                3, // deleteFileThreshold
                0.1, // deleteRatioThreshold: 10%
                1L, // outputSpecId
                Optional.empty(), // partitionFilter
                Optional.empty() // whereCondition
        );

        RewriteDataFilePlanner testPlanner = new RewriteDataFilePlanner(params);

        // Create multiple file scan tasks
        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);
        FileScanTask task3 = Mockito.mock(FileScanTask.class);
        DataFile dataFile1 = Mockito.mock(DataFile.class);
        DataFile dataFile2 = Mockito.mock(DataFile.class);
        DataFile dataFile3 = Mockito.mock(DataFile.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(Arrays.asList(task1, task2, task3)));

        // All files have acceptable size and no delete issues
        Mockito.when(task1.file()).thenReturn(dataFile1);
        Mockito.when(task1.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task1.deletes()).thenReturn(null);
        Mockito.when(dataFile1.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(dataFile1.partition()).thenReturn(mockPartition);

        Mockito.when(task2.file()).thenReturn(dataFile2);
        Mockito.when(task2.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task2.deletes()).thenReturn(null);
        Mockito.when(dataFile2.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(dataFile2.partition()).thenReturn(mockPartition);

        Mockito.when(task3.file()).thenReturn(dataFile3);
        Mockito.when(task3.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task3.deletes()).thenReturn(null);
        Mockito.when(dataFile3.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(dataFile3.partition()).thenReturn(mockPartition);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = testPlanner.planAndOrganizeTasks(mockTable);

        // Should create one group with 3 files (meets minInputFiles requirement)
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(3, result.get(0).getTaskCount());
        Assertions.assertEquals(300 * 1024 * 1024L, result.get(0).getTotalSize());
    }

}
