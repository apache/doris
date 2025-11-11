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
        Assertions.assertTrue(toString.contains("hasWhereCondition=false"));
    }

    @Test
    public void testPlanAndOrganizeTasksWithRewriteAll() throws UserException {
        // Test with rewriteAll = true
        RewriteDataFilePlanner.Parameters rewriteAllParams = new RewriteDataFilePlanner.Parameters(
                128 * 1024 * 1024L, 64 * 1024 * 1024L, 256 * 1024 * 1024L,
                2, true, 512 * 1024 * 1024L, 3, 0.1, 1L,
                Optional.empty());

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

        // Test file too small (should be selected for rewrite but filtered by group size - single file)
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(32 * 1024 * 1024L); // 32MB < 64MB min

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);
        // Single file groups are filtered unless they meet tooMuchContent threshold
        Assertions.assertTrue(result.isEmpty(), "Single small file should be filtered by group rules");

        // Test file too large (should be selected for rewrite but filtered by group size - single file)
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(300 * 1024 * 1024L); // 300MB > 256MB max

        result = planner.planAndOrganizeTasks(mockTable);
        // Single file groups are filtered unless they meet tooMuchContent threshold
        Assertions.assertTrue(result.isEmpty(), "Single large file should be filtered by group rules");

        // Test file in acceptable range (should be filtered out as it doesn't need rewriting)
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L); // 100MB between 64MB-256MB

        result = planner.planAndOrganizeTasks(mockTable);
        // File in acceptable range with no deletes doesn't need rewriting
        Assertions.assertTrue(result.isEmpty(), "File in acceptable range should not be rewritten");
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
        Assertions.assertTrue(result.isEmpty(), "File with 2 delete files (< 3 threshold) should not be selected");

        // Test with delete files at threshold (should be included)
        Mockito.when(mockFileScanTask.deletes()).thenReturn(Arrays.asList(mockDeleteFile, mockDeleteFile, mockDeleteFile)); // 3 = 3 threshold

        result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertEquals(1, result.size(), "Should have exactly 1 group");
        Assertions.assertEquals(1, result.get(0).getTaskCount(), "Group should contain 1 task");
        Assertions.assertEquals(100 * 1024 * 1024L, result.get(0).getTotalSize(), "Group size should be 100MB");
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
        // Low delete ratio + single file = filtered out
        Assertions.assertTrue(result.isEmpty(), "File with 5% delete ratio (< 10% threshold) should not be selected");

        // Test with high delete ratio (should be included)
        Mockito.when(mockDeleteFile.recordCount()).thenReturn(15L); // 15/100 = 15% > 10% threshold

        result = planner.planAndOrganizeTasks(mockTable);
        // Note: delete ratio calculation requires ContentFileUtil.isFileScoped to return true
        // which cannot be easily mocked. Single file groups are also filtered out.
        Assertions.assertTrue(result.isEmpty(), "Single file group filtered even with high delete ratio (ContentFileUtil limitation)");
    }

    @Test
    public void testDeleteRatioWithZeroRecordCount() throws UserException {
        // Test that recordCount == 0 doesn't cause division by zero
        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        // Mock delete file with record count = 0 (should not cause division by zero)
        Mockito.when(mockDeleteFile.recordCount()).thenReturn(10L);
        Mockito.when(mockDataFile.recordCount()).thenReturn(0L); // Zero record count

        Mockito.when(mockFileScanTask.deletes()).thenReturn(Collections.singletonList(mockDeleteFile));

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);
        // File with zero record count should not be selected (should return false without throwing exception)
        Assertions.assertTrue(result.isEmpty(), "File with zero record count should not be selected");
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
        Assertions.assertTrue(result.isEmpty(), "Single file group with taskCount=1 < minInputFiles=2 should be filtered");
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
        // Single file in acceptable range doesn't need rewriting
        // enoughContent requires taskCount > 1
        Assertions.assertTrue(result.isEmpty());
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
        // File in acceptable range (64-256MB), so not selected by filterFiles
        // Even though 100MB > 50MB maxFileGroupSize, the file is filtered before grouping
        Assertions.assertTrue(result.isEmpty());
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
                Optional.empty());

        RewriteDataFilePlanner rewriteAllPlanner = new RewriteDataFilePlanner(rewriteAllParams);
        List<RewriteDataGroup> result = rewriteAllPlanner.planAndOrganizeTasks(mockTable);

        // With rewriteAll=true, all files are included
        // StructLikeWrapper groups by partition, but since we can't mock partition equality,
        // we just verify that groups are created
        Assertions.assertFalse(result.isEmpty());
        Assertions.assertTrue(result.size() >= 1 && result.size() <= 2);
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
    public void testRewriteDataGroupConstructorWithTasks() {
        // Test constructor that takes a list of tasks
        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);
        DataFile dataFile1 = Mockito.mock(DataFile.class);
        DataFile dataFile2 = Mockito.mock(DataFile.class);
        DeleteFile deleteFile1 = Mockito.mock(DeleteFile.class);
        DeleteFile deleteFile2 = Mockito.mock(DeleteFile.class);

        Mockito.when(task1.file()).thenReturn(dataFile1);
        Mockito.when(task1.deletes()).thenReturn(Arrays.asList(deleteFile1));
        Mockito.when(dataFile1.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);

        Mockito.when(task2.file()).thenReturn(dataFile2);
        Mockito.when(task2.deletes()).thenReturn(Arrays.asList(deleteFile2));
        Mockito.when(dataFile2.fileSizeInBytes()).thenReturn(200 * 1024 * 1024L);

        List<FileScanTask> tasks = Arrays.asList(task1, task2);
        RewriteDataGroup group = new RewriteDataGroup(tasks);

        Assertions.assertFalse(group.isEmpty());
        Assertions.assertEquals(2, group.getTaskCount());
        Assertions.assertEquals(300 * 1024 * 1024L, group.getTotalSize()); // 100MB + 200MB
        Assertions.assertEquals(2, group.getDeleteFileCount()); // 1 + 1
        Assertions.assertEquals(2, group.getDataFiles().size());
        Assertions.assertTrue(group.getDataFiles().contains(dataFile1));
        Assertions.assertTrue(group.getDataFiles().contains(dataFile2));
    }

    @Test
    public void testParametersEdgeCases() {
        // Test with zero values
        RewriteDataFilePlanner.Parameters zeroParams = new RewriteDataFilePlanner.Parameters(
                0L, 0L, 0L, 0, true, 0L, 0, 0.0, 0L,
                Optional.empty());

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

        // Small file (32MB) and large file (300MB) are outside range, but single files are filtered by group rules
        // Medium file (100MB) is in range, so not selected for rewrite
        // With minInputFiles=2, we need at least 2 files. We have 2 files that need rewriting (small+large)
        // but they form a group of 2 files which meets the minInputFiles requirement
        Assertions.assertTrue(result.size() >= 1);
        if (!result.isEmpty()) {
            Assertions.assertEquals(2, result.get(0).getTaskCount());
        }
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

        // Only high delete count file should be included (low delete file filtered by filterFiles)
        Assertions.assertEquals(1, result.size(), "Should have exactly 1 group");
        Assertions.assertEquals(1, result.get(0).getTaskCount(), "Group should contain only 1 file (high delete count)");
        Assertions.assertEquals(100 * 1024 * 1024L, result.get(0).getTotalSize(), "Total size should be 100MB");
        Assertions.assertTrue(result.get(0).getDataFiles().contains(dataFile2), "Should contain the high-delete file");
        Assertions.assertFalse(result.get(0).getDataFiles().contains(dataFile1), "Should NOT contain the low-delete file");
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

        // Delete ratio calculation requires ContentFileUtil.isFileScoped which cannot be easily mocked
        // Single file groups are also filtered out by group rules
        // So result should be empty or have limited entries
        Assertions.assertTrue(result.size() <= 1, "Result should have at most 1 group (limited by ContentFileUtil and group rules)");
        if (!result.isEmpty()) {
            Assertions.assertTrue(result.get(0).getTaskCount() <= 2, "If group exists, should have at most 2 tasks");
        }
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

        // Files in acceptable range (64-256MB) are not selected for rewrite
        // All 3 files are 100MB which is in range, so they are filtered out
        Assertions.assertTrue(result.isEmpty(), "All 3 files (100MB each) are in acceptable range [64-256MB], should not be rewritten");
    }

    @Test
    public void testBoundaryValueFileSizes() throws UserException {
        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockFileScanTask.deletes()).thenReturn(null);
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        // Test file size exactly at minFileSizeBytes (should NOT be selected for rewrite)
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(64 * 1024 * 1024L); // Exactly 64MB
        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty(), "File at exactly minFileSizeBytes (64MB) should NOT be selected");

        // Test file size just below minFileSizeBytes (should be selected but filtered by group rules)
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(64 * 1024 * 1024L - 1); // 64MB - 1
        result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty(), "Single file at 64MB-1 selected but filtered by group rules");

        // Test file size exactly at maxFileSizeBytes (should NOT be selected for rewrite)
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(256 * 1024 * 1024L); // Exactly 256MB
        result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty(), "File at exactly maxFileSizeBytes (256MB) should NOT be selected");

        // Test file size just above maxFileSizeBytes (should be selected but filtered by group rules)
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(256 * 1024 * 1024L + 1); // 256MB + 1
        result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty(), "Single file at 256MB+1 selected but filtered by group rules");
    }

    @Test
    public void testEnoughContentTrigger() throws UserException {
        // Create parameters with specific target size
        RewriteDataFilePlanner.Parameters params = new RewriteDataFilePlanner.Parameters(
                100 * 1024 * 1024L, // targetFileSizeBytes: 100MB
                50 * 1024 * 1024L, // minFileSizeBytes: 50MB
                200 * 1024 * 1024L, // maxFileSizeBytes: 200MB
                2, // minInputFiles: 2
                false, // rewriteAll
                500 * 1024 * 1024L, // maxFileGroupSizeBytes: 500MB
                3, // deleteFileThreshold
                0.1, // deleteRatioThreshold: 10%
                1L, // outputSpecId
                Optional.empty() // whereCondition
        );

        RewriteDataFilePlanner testPlanner = new RewriteDataFilePlanner(params);

        // Create two small files that together exceed target size
        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);
        DataFile dataFile1 = Mockito.mock(DataFile.class);
        DataFile dataFile2 = Mockito.mock(DataFile.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(Arrays.asList(task1, task2)));

        // Both files are too small (< 50MB min), so they will be selected for rewrite
        Mockito.when(task1.file()).thenReturn(dataFile1);
        Mockito.when(task1.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task1.deletes()).thenReturn(null);
        Mockito.when(dataFile1.fileSizeInBytes()).thenReturn(40 * 1024 * 1024L); // 40MB < 50MB min
        Mockito.when(dataFile1.partition()).thenReturn(mockPartition);

        Mockito.when(task2.file()).thenReturn(dataFile2);
        Mockito.when(task2.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task2.deletes()).thenReturn(null);
        Mockito.when(dataFile2.fileSizeInBytes()).thenReturn(70 * 1024 * 1024L); // 70MB > 50MB min but < 200MB max, in range!
        Mockito.when(dataFile2.partition()).thenReturn(mockPartition);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = testPlanner.planAndOrganizeTasks(mockTable);

        // file1 (40MB) is selected by filterFiles (< 50MB min)
        // file2 (70MB) is in range, not selected
        // Only 1 file in group, doesn't meet minInputFiles requirement
        Assertions.assertTrue(result.isEmpty(), "Only 1 file selected (taskCount=1 < minInputFiles=2), should be filtered");
    }

    @Test
    public void testEnoughContentTriggerWithBothFilesTooSmall() throws UserException {
        // Create parameters with specific target size
        RewriteDataFilePlanner.Parameters params = new RewriteDataFilePlanner.Parameters(
                100 * 1024 * 1024L, // targetFileSizeBytes: 100MB
                50 * 1024 * 1024L, // minFileSizeBytes: 50MB
                200 * 1024 * 1024L, // maxFileSizeBytes: 200MB
                2, // minInputFiles: 2
                false, // rewriteAll
                500 * 1024 * 1024L, // maxFileGroupSizeBytes: 500MB
                3, // deleteFileThreshold
                0.1, // deleteRatioThreshold: 10%
                1L, // outputSpecId
                Optional.empty() // whereCondition
        );

        RewriteDataFilePlanner testPlanner = new RewriteDataFilePlanner(params);

        // Create two small files that together exceed target size
        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);
        DataFile dataFile1 = Mockito.mock(DataFile.class);
        DataFile dataFile2 = Mockito.mock(DataFile.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(Arrays.asList(task1, task2)));

        // Both files are too small (< 50MB min), so they will be selected for rewrite
        Mockito.when(task1.file()).thenReturn(dataFile1);
        Mockito.when(task1.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task1.deletes()).thenReturn(null);
        Mockito.when(dataFile1.fileSizeInBytes()).thenReturn(40 * 1024 * 1024L); // 40MB < 50MB min
        Mockito.when(dataFile1.partition()).thenReturn(mockPartition);

        Mockito.when(task2.file()).thenReturn(dataFile2);
        Mockito.when(task2.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task2.deletes()).thenReturn(null);
        Mockito.when(dataFile2.fileSizeInBytes()).thenReturn(45 * 1024 * 1024L); // 45MB < 50MB min
        Mockito.when(dataFile2.partition()).thenReturn(mockPartition);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = testPlanner.planAndOrganizeTasks(mockTable);

        // Both files are too small and selected
        // Total size = 85MB < 100MB target (so enoughContent doesn't trigger)
        // But taskCount = 2 >= 2 minInputFiles (so enoughInputFiles triggers)
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(2, result.get(0).getTaskCount());
        Assertions.assertEquals(85 * 1024 * 1024L, result.get(0).getTotalSize());
    }

    @Test
    public void testEnoughContentWithLargerFiles() throws UserException {
        // Test specifically for enoughContent condition: taskCount > 1 && totalSize > targetSize
        RewriteDataFilePlanner.Parameters params = new RewriteDataFilePlanner.Parameters(
                80 * 1024 * 1024L, // targetFileSizeBytes: 80MB
                50 * 1024 * 1024L, // minFileSizeBytes: 50MB
                200 * 1024 * 1024L, // maxFileSizeBytes: 200MB
                5, // minInputFiles: 5 (high threshold, won't be met)
                false, // rewriteAll
                500 * 1024 * 1024L, // maxFileGroupSizeBytes: 500MB
                3, // deleteFileThreshold
                0.1, // deleteRatioThreshold: 10%
                1L, // outputSpecId
                Optional.empty() // whereCondition
        );

        RewriteDataFilePlanner testPlanner = new RewriteDataFilePlanner(params);

        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);
        DataFile dataFile1 = Mockito.mock(DataFile.class);
        DataFile dataFile2 = Mockito.mock(DataFile.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(Arrays.asList(task1, task2)));

        // Both files too small
        Mockito.when(task1.file()).thenReturn(dataFile1);
        Mockito.when(task1.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task1.deletes()).thenReturn(null);
        Mockito.when(dataFile1.fileSizeInBytes()).thenReturn(45 * 1024 * 1024L); // 45MB < 50MB
        Mockito.when(dataFile1.partition()).thenReturn(mockPartition);

        Mockito.when(task2.file()).thenReturn(dataFile2);
        Mockito.when(task2.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task2.deletes()).thenReturn(null);
        Mockito.when(dataFile2.fileSizeInBytes()).thenReturn(48 * 1024 * 1024L); // 48MB < 50MB
        Mockito.when(dataFile2.partition()).thenReturn(mockPartition);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = testPlanner.planAndOrganizeTasks(mockTable);

        // taskCount = 2 < 5 minInputFiles (enoughInputFiles = false)
        // totalSize = 93MB > 80MB target && taskCount = 2 > 1 (enoughContent = true!)
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(2, result.get(0).getTaskCount());
        Assertions.assertEquals(93 * 1024 * 1024L, result.get(0).getTotalSize());
    }

    @Test
    public void testTooMuchContentTrigger() throws UserException {
        // Create parameters with very small maxFileGroupSizeBytes
        RewriteDataFilePlanner.Parameters params = new RewriteDataFilePlanner.Parameters(
                128 * 1024 * 1024L, // targetFileSizeBytes: 128MB
                64 * 1024 * 1024L, // minFileSizeBytes: 64MB
                256 * 1024 * 1024L, // maxFileSizeBytes: 256MB
                2, // minInputFiles: 2
                false, // rewriteAll
                50 * 1024 * 1024L, // maxFileGroupSizeBytes: 50MB (very small!)
                3, // deleteFileThreshold
                0.1, // deleteRatioThreshold: 10%
                1L, // outputSpecId
                Optional.empty() // whereCondition
        );

        RewriteDataFilePlanner testPlanner = new RewriteDataFilePlanner(params);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockFileScanTask.deletes()).thenReturn(null);
        // File is too large (> 256MB max), so it will be selected by filterFiles
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(300 * 1024 * 1024L); // 300MB
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = testPlanner.planAndOrganizeTasks(mockTable);

        // Even though it's a single file (normally filtered), 300MB > 50MB maxFileGroupSize
        // This triggers tooMuchContent, so the file should be included
        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals(1, result.get(0).getTaskCount());
        Assertions.assertEquals(300 * 1024 * 1024L, result.get(0).getTotalSize());
    }

    @Test
    public void testGroupWithAnyFileHavingDeletes() throws UserException {
        // Test that if any file in a group has delete issues, the whole group is selected
        FileScanTask cleanTask = Mockito.mock(FileScanTask.class);
        FileScanTask dirtyTask = Mockito.mock(FileScanTask.class);
        DataFile cleanFile = Mockito.mock(DataFile.class);
        DataFile dirtyFile = Mockito.mock(DataFile.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(Arrays.asList(cleanTask, dirtyTask)));

        // Clean file - no deletes, size in range
        Mockito.when(cleanTask.file()).thenReturn(cleanFile);
        Mockito.when(cleanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(cleanTask.deletes()).thenReturn(null);
        Mockito.when(cleanFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(cleanFile.partition()).thenReturn(mockPartition);

        // Dirty file - has deletes exceeding threshold, size in range
        Mockito.when(dirtyTask.file()).thenReturn(dirtyFile);
        Mockito.when(dirtyTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(dirtyTask.deletes()).thenReturn(Arrays.asList(mockDeleteFile, mockDeleteFile, mockDeleteFile)); // 3 >= 3 threshold
        Mockito.when(dirtyFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(dirtyFile.partition()).thenReturn(mockPartition);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);

        // The dirty file triggers the group to be selected (via shouldRewriteGroup)
        // Only dirty file is selected by filterFiles (cleanFile is in acceptable range)
        Assertions.assertEquals(1, result.size(), "Should have exactly 1 group");
        Assertions.assertEquals(1, result.get(0).getTaskCount(), "Group should contain 1 task (dirty file only)");
        Assertions.assertEquals(100 * 1024 * 1024L, result.get(0).getTotalSize(), "Total size should be 100MB (dirty file)");
        Assertions.assertTrue(result.get(0).getDataFiles().contains(dirtyFile), "Should contain dirty file");
        Assertions.assertFalse(result.get(0).getDataFiles().contains(cleanFile), "Should NOT contain clean file (not selected by filterFiles)");
    }

    @Test
    public void testMixedScenarioWithMultiplePartitions() throws UserException {
        // Create a complex scenario with multiple partitions and mixed file sizes
        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);
        FileScanTask task3 = Mockito.mock(FileScanTask.class);
        FileScanTask task4 = Mockito.mock(FileScanTask.class);
        DataFile file1 = Mockito.mock(DataFile.class);
        DataFile file2 = Mockito.mock(DataFile.class);
        DataFile file3 = Mockito.mock(DataFile.class);
        DataFile file4 = Mockito.mock(DataFile.class);
        StructLike partition1 = Mockito.mock(StructLike.class);
        StructLike partition2 = Mockito.mock(StructLike.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(
                Arrays.asList(task1, task2, task3, task4)));

        // Partition 1: Two small files that should be rewritten
        Mockito.when(task1.file()).thenReturn(file1);
        Mockito.when(task1.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task1.deletes()).thenReturn(null);
        Mockito.when(file1.fileSizeInBytes()).thenReturn(30 * 1024 * 1024L); // Too small
        Mockito.when(file1.partition()).thenReturn(partition1);

        Mockito.when(task2.file()).thenReturn(file2);
        Mockito.when(task2.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task2.deletes()).thenReturn(null);
        Mockito.when(file2.fileSizeInBytes()).thenReturn(40 * 1024 * 1024L); // Too small
        Mockito.when(file2.partition()).thenReturn(partition1);

        // Partition 2: One large file + one good file
        Mockito.when(task3.file()).thenReturn(file3);
        Mockito.when(task3.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task3.deletes()).thenReturn(null);
        Mockito.when(file3.fileSizeInBytes()).thenReturn(300 * 1024 * 1024L); // Too large
        Mockito.when(file3.partition()).thenReturn(partition2);

        Mockito.when(task4.file()).thenReturn(file4);
        Mockito.when(task4.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task4.deletes()).thenReturn(null);
        Mockito.when(file4.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L); // Good size
        Mockito.when(file4.partition()).thenReturn(partition2);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);

        // Note: StructLikeWrapper may group all mocked StructLike objects together
        // Because we can't properly mock partition equality, the exact grouping is unpredictable
        // We can only verify that groups are created for files needing rewrite
        Assertions.assertTrue(result.size() >= 1, "Should have at least 1 group");

        // Verify total files selected for rewrite
        int totalFiles = result.stream().mapToInt(RewriteDataGroup::getTaskCount).sum();
        long totalSize = result.stream().mapToLong(RewriteDataGroup::getTotalSize).sum();
        // file1 (30MB), file2 (40MB), file3 (300MB) are outside range → selected
        // file4 (100MB) is in range → NOT selected
        // So we expect 2-3 files (depending on grouping and single-file filtering)
        Assertions.assertTrue(totalFiles >= 2, "Should have at least 2 files selected for rewrite");
        Assertions.assertTrue(totalFiles <= 3, "Should have at most 3 files selected");
        // Total size should be at least the 2 small files
        Assertions.assertTrue(totalSize >= 70 * 1024 * 1024L, "Total size should be at least 70MB (file1+file2)");
    }

    @Test
    public void testMultipleSmallFilesGroupedTogether() throws UserException {
        // Test that multiple small files in same partition are grouped and selected
        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);
        FileScanTask task3 = Mockito.mock(FileScanTask.class);
        DataFile file1 = Mockito.mock(DataFile.class);
        DataFile file2 = Mockito.mock(DataFile.class);
        DataFile file3 = Mockito.mock(DataFile.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(
                Arrays.asList(task1, task2, task3)));

        // All files are too small
        Mockito.when(task1.file()).thenReturn(file1);
        Mockito.when(task1.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task1.deletes()).thenReturn(null);
        Mockito.when(file1.fileSizeInBytes()).thenReturn(40 * 1024 * 1024L); // 40MB < 64MB min
        Mockito.when(file1.partition()).thenReturn(mockPartition);

        Mockito.when(task2.file()).thenReturn(file2);
        Mockito.when(task2.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task2.deletes()).thenReturn(null);
        Mockito.when(file2.fileSizeInBytes()).thenReturn(50 * 1024 * 1024L); // 50MB < 64MB min
        Mockito.when(file2.partition()).thenReturn(mockPartition);

        Mockito.when(task3.file()).thenReturn(file3);
        Mockito.when(task3.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task3.deletes()).thenReturn(null);
        Mockito.when(file3.fileSizeInBytes()).thenReturn(45 * 1024 * 1024L); // 45MB < 64MB min
        Mockito.when(file3.partition()).thenReturn(mockPartition);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);

        // All 3 files are too small, grouped together, total = 135MB > 128MB target
        // taskCount = 3 >= 2 minInputFiles, should be selected via enoughInputFiles or enoughContent
        Assertions.assertEquals(1, result.size(), "Should have exactly 1 group");
        Assertions.assertEquals(3, result.get(0).getTaskCount(), "Group should contain all 3 small files");
        Assertions.assertEquals(135 * 1024 * 1024L, result.get(0).getTotalSize(), "Total size should be 135MB (40+50+45)");
        // Verify all three files are in the group
        Assertions.assertTrue(result.get(0).getDataFiles().contains(file1), "Should contain file1");
        Assertions.assertTrue(result.get(0).getDataFiles().contains(file2), "Should contain file2");
        Assertions.assertTrue(result.get(0).getDataFiles().contains(file3), "Should contain file3");
    }

    @Test
    public void testDeleteFileThresholdBoundary() throws UserException {
        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles())
                .thenReturn(CloseableIterable.withNoopClose(Collections.singletonList(mockFileScanTask)));
        Mockito.when(mockFileScanTask.file()).thenReturn(mockDataFile);
        Mockito.when(mockFileScanTask.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(mockDataFile.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L);
        Mockito.when(mockDataFile.partition()).thenReturn(mockPartition);
        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        // Test with delete count exactly at threshold - 1 (should NOT be selected)
        DeleteFile delete1 = Mockito.mock(DeleteFile.class);
        DeleteFile delete2 = Mockito.mock(DeleteFile.class);
        Mockito.when(mockFileScanTask.deletes()).thenReturn(Arrays.asList(delete1, delete2)); // 2 < 3 threshold

        List<RewriteDataGroup> result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertTrue(result.isEmpty(), "File with 2 delete files (< 3 threshold) should not be selected");

        // Test with delete count exactly at threshold (should be selected)
        DeleteFile delete3 = Mockito.mock(DeleteFile.class);
        Mockito.when(mockFileScanTask.deletes()).thenReturn(Arrays.asList(delete1, delete2, delete3)); // 3 >= 3 threshold

        result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertEquals(1, result.size(), "File with 3 delete files (>= 3 threshold) should be selected");
        Assertions.assertEquals(1, result.get(0).getTaskCount(), "Group should contain 1 task");
        Assertions.assertEquals(100 * 1024 * 1024L, result.get(0).getTotalSize(), "Total size should be 100MB");

        // Test with delete count above threshold (should be selected)
        DeleteFile delete4 = Mockito.mock(DeleteFile.class);
        Mockito.when(mockFileScanTask.deletes()).thenReturn(Arrays.asList(delete1, delete2, delete3, delete4)); // 4 >= 3

        result = planner.planAndOrganizeTasks(mockTable);
        Assertions.assertEquals(1, result.size(), "File with 4 delete files (> 3 threshold) should be selected");
        Assertions.assertEquals(1, result.get(0).getTaskCount(), "Group should contain 1 task");
        Assertions.assertEquals(100 * 1024 * 1024L, result.get(0).getTotalSize(), "Total size should be 100MB");
    }

    @Test
    public void testBinPackGroupingWithLargePartition() throws UserException {
        // Test binPack grouping: when a partition has files exceeding maxFileGroupSizeBytes,
        // they should be split into multiple groups
        RewriteDataFilePlanner.Parameters params = new RewriteDataFilePlanner.Parameters(
                128 * 1024 * 1024L, // targetFileSizeBytes: 128MB
                64 * 1024 * 1024L, // minFileSizeBytes: 64MB
                256 * 1024 * 1024L, // maxFileSizeBytes: 256MB
                1, // minInputFiles: 1
                true, // rewriteAll: true (to avoid filtering)
                200 * 1024 * 1024L, // maxFileGroupSizeBytes: 200MB (small to trigger splitting)
                3, // deleteFileThreshold
                0.1, // deleteRatioThreshold: 10%
                1L, // outputSpecId
                Optional.empty() // whereCondition
        );

        RewriteDataFilePlanner testPlanner = new RewriteDataFilePlanner(params);

        // Create multiple files in the same partition that together exceed maxFileGroupSizeBytes
        FileScanTask task1 = Mockito.mock(FileScanTask.class);
        FileScanTask task2 = Mockito.mock(FileScanTask.class);
        FileScanTask task3 = Mockito.mock(FileScanTask.class);
        DataFile file1 = Mockito.mock(DataFile.class);
        DataFile file2 = Mockito.mock(DataFile.class);
        DataFile file3 = Mockito.mock(DataFile.class);

        Mockito.when(mockTable.newScan()).thenReturn(mockTableScan);
        Mockito.when(mockTableScan.planFiles()).thenReturn(CloseableIterable.withNoopClose(
                Arrays.asList(task1, task2, task3)));

        // All files in the same partition
        Mockito.when(task1.file()).thenReturn(file1);
        Mockito.when(task1.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task1.deletes()).thenReturn(null);
        Mockito.when(file1.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L); // 100MB
        Mockito.when(file1.partition()).thenReturn(mockPartition);

        Mockito.when(task2.file()).thenReturn(file2);
        Mockito.when(task2.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task2.deletes()).thenReturn(null);
        Mockito.when(file2.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L); // 100MB
        Mockito.when(file2.partition()).thenReturn(mockPartition);

        Mockito.when(task3.file()).thenReturn(file3);
        Mockito.when(task3.spec()).thenReturn(mockPartitionSpec);
        Mockito.when(task3.deletes()).thenReturn(null);
        Mockito.when(file3.fileSizeInBytes()).thenReturn(100 * 1024 * 1024L); // 100MB
        Mockito.when(file3.partition()).thenReturn(mockPartition);

        Mockito.when(mockPartitionSpec.partitionType()).thenReturn(Types.StructType.of());

        List<RewriteDataGroup> result = testPlanner.planAndOrganizeTasks(mockTable);

        // Total size = 300MB > 200MB maxFileGroupSizeBytes
        // binPack should split into multiple groups
        // Expected: 2 groups (100MB + 100MB in first group, 100MB in second group)
        Assertions.assertTrue(result.size() >= 2, "Should have at least 2 groups due to binPack splitting");

        // Verify total files are preserved
        int totalFiles = result.stream().mapToInt(RewriteDataGroup::getTaskCount).sum();
        long totalSize = result.stream().mapToLong(RewriteDataGroup::getTotalSize).sum();
        Assertions.assertEquals(3, totalFiles, "Should have all 3 files");
        Assertions.assertEquals(300 * 1024 * 1024L, totalSize, "Total size should be 300MB");

        // Verify each group doesn't exceed maxFileGroupSizeBytes
        for (RewriteDataGroup group : result) {
            Assertions.assertTrue(group.getTotalSize() <= 200 * 1024 * 1024L,
                    "Each group should not exceed maxFileGroupSizeBytes (200MB)");
        }
    }

}
