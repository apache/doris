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

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.paimon.PaimonFileExternalCatalog;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.qe.SessionVariable;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.source.DataSplit;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RunWith(MockitoJUnitRunner.class)
public class PaimonScanNodeTest {
    @Mock
    private SessionVariable sv;

    @Mock
    private PaimonFileExternalCatalog paimonFileExternalCatalog;

    @Test
    public void testSplitWeight() throws UserException {

        TupleDescriptor desc = new TupleDescriptor(new TupleId(3));
        PaimonScanNode paimonScanNode = new PaimonScanNode(new PlanNodeId(1), desc, false, sv);

        paimonScanNode.setSource(new PaimonSource());

        DataFileMeta dfm1 = DataFileMeta.forAppend("f1.parquet", 64 * 1024 * 1024, 1, SimpleStats.EMPTY_STATS, 1, 1, 1,
                Collections.emptyList(), null, null, null, null);
        BinaryRow binaryRow1 = BinaryRow.singleColumn(1);
        DataSplit ds1 = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow1)
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dfm1))
                .build();

        DataFileMeta dfm2 = DataFileMeta.forAppend("f2.parquet", 32 * 1024 * 1024, 2, SimpleStats.EMPTY_STATS, 1, 1, 1,
                Collections.emptyList(), null, null, null, null);
        BinaryRow binaryRow2 = BinaryRow.singleColumn(1);
        DataSplit ds2 = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow2)
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dfm2))
                .build();


        // Mock PaimonScanNode to return test data splits
        PaimonScanNode spyPaimonScanNode = Mockito.spy(paimonScanNode);
        Mockito.doReturn(new ArrayList<org.apache.paimon.table.source.Split>() {
            {
                add(ds1);
                add(ds2);
            }
        }).when(spyPaimonScanNode).getPaimonSplitFromAPI();

        // Note: The original PaimonSource is sufficient for this test
        // No need to mock catalog properties since doInitialize() is not called in this test
        // Mock SessionVariable behavior
        Mockito.when(sv.isForceJniScanner()).thenReturn(false);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");

        // native
        mockNativeReader(spyPaimonScanNode);
        List<org.apache.doris.spi.Split> s1 = spyPaimonScanNode.getSplits(1);
        PaimonSplit s11 = (PaimonSplit) s1.get(0);
        PaimonSplit s12 = (PaimonSplit) s1.get(1);
        Assert.assertEquals(2, s1.size());
        Assert.assertEquals(100, s11.getSplitWeight().getRawValue());
        Assert.assertNull(s11.getSplit());
        Assert.assertEquals(50, s12.getSplitWeight().getRawValue());
        Assert.assertNull(s12.getSplit());

        // jni
        mockJniReader(spyPaimonScanNode);
        List<org.apache.doris.spi.Split> s2 = spyPaimonScanNode.getSplits(1);
        PaimonSplit s21 = (PaimonSplit) s2.get(0);
        PaimonSplit s22 = (PaimonSplit) s2.get(1);
        Assert.assertEquals(2, s2.size());
        Assert.assertNotNull(s21.getSplit());
        Assert.assertNotNull(s22.getSplit());
        Assert.assertEquals(100, s21.getSplitWeight().getRawValue());
        Assert.assertEquals(50, s22.getSplitWeight().getRawValue());
    }

    @Test
    public void testValidateIncrementalReadParams() throws UserException {
        // Test valid parameter combinations

        // 1. Only startSnapshotId
        Map<String, String> params1 = new HashMap<>();
        params1.put("startSnapshotId", "5");
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "endSnapshotId is required when using snapshot-based incremental read",
                () -> PaimonScanNode.validateIncrementalReadParams(params1));

        // 2. Both startSnapshotId and endSnapshotId
        Map<String, String> params = new HashMap<>();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "5");
        Map<String, String> result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("1,5", result.get("incremental-between"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertEquals(3, result.size());

        // 3. startSnapshotId + endSnapshotId + incrementalBetweenScanMode
        params.clear();
        params.put("startSnapshotId", "2");
        params.put("endSnapshotId", "8");
        params.put("incrementalBetweenScanMode", "diff");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("2,8", result.get("incremental-between"));
        Assert.assertEquals("diff", result.get("incremental-between-scan-mode"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertEquals(4, result.size());

        // 4. Only startTimestamp
        params.clear();
        params.put("startTimestamp", "1000");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("1000," + Long.MAX_VALUE, result.get("incremental-between-timestamp"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertTrue(result.containsKey("scan.snapshot-id") && result.get("scan.snapshot-id") == null);
        Assert.assertEquals(3, result.size());

        // 5. Both startTimestamp and endTimestamp
        params.clear();
        params.put("startTimestamp", "1000");
        params.put("endTimestamp", "2000");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("1000,2000", result.get("incremental-between-timestamp"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertTrue(result.containsKey("scan.snapshot-id") && result.get("scan.snapshot-id") == null);
        Assert.assertEquals(3, result.size());

        // Test invalid parameter combinations

        // 6. Test mutual exclusivity - both snapshot and timestamp params
        params.clear();
        params.put("startSnapshotId", "1");
        params.put("startTimestamp", "1000");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for mutual exclusivity");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Cannot specify both snapshot-based parameters"));
        }

        // 7. Test snapshot params without required startSnapshotId
        params.clear();
        params.put("endSnapshotId", "5");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startSnapshotId is missing");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startSnapshotId is required"));
        }

        // 8. Test timestamp params without required startTimestamp
        params.clear();
        params.put("endTimestamp", "2000");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startTimestamp is missing");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startTimestamp is required"));
        }

        // 9. Test incrementalBetweenScanMode without endSnapshotId
        params.clear();
        params.put("startSnapshotId", "1");
        params.put("incrementalBetweenScanMode", "auto");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when incrementalBetweenScanMode appears without endSnapshotId");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("incrementalBetweenScanMode can only be specified when both"));
        }

        // 10. Test incrementalBetweenScanMode alone
        params.clear();
        params.put("incrementalBetweenScanMode", "auto");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when incrementalBetweenScanMode appears alone");
        } catch (UserException e) {
            Assert.assertTrue(
                    e.getMessage().contains("startSnapshotId is required when using snapshot-based incremental read"));
        }

        // 11. Test invalid snapshot ID values < 0)
        params.clear();
        params.put("startSnapshotId", "-1");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for startSnapshotId < 0");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startSnapshotId must be greater than or equal to 0"));
        }

        params.clear();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "-1");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for endSnapshotId < 0");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("endSnapshotId must be greater than or equal to 0"));
        }

        // 12. Test start > end for snapshot IDs
        params.clear();
        params.put("startSnapshotId", "6");
        params.put("endSnapshotId", "5");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startSnapshotId > endSnapshotId");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startSnapshotId must be less than or equal to endSnapshotId"));
        }

        // 12.1. Test startSnapshotId == endSnapshotId (should be allowed, consistent with Spark Paimon behavior)
        params.clear();
        params.put("startSnapshotId", "5");
        params.put("endSnapshotId", "5");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("5,5", result.get("incremental-between"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertEquals(3, result.size());

        // 13. Test invalid timestamp values (< 0)
        params.clear();
        params.put("startTimestamp", "-1");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for startTimestamp < 0");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startTimestamp must be greater than or equal to 0"));
        }

        params.clear();
        params.put("startTimestamp", "1000");
        params.put("endTimestamp", "0");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for endTimestamp ≤ 0");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("endTimestamp must be greater than 0"));
        }

        // 14. Test start ≥ end for timestamps
        params.clear();
        params.put("startTimestamp", "2000");
        params.put("endTimestamp", "2000");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startTimestamp = endTimestamp");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startTimestamp must be less than endTimestamp"));
        }

        params.clear();
        params.put("startTimestamp", "3000");
        params.put("endTimestamp", "2000");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startTimestamp > endTimestamp");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startTimestamp must be less than endTimestamp"));
        }

        // 15. Test invalid number format
        params.clear();
        params.put("startSnapshotId", "invalid");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for invalid number format");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid startSnapshotId format"));
        }

        params.clear();
        params.put("startTimestamp", "invalid");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for invalid timestamp format");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid startTimestamp format"));
        }

        // 16. Test invalid incrementalBetweenScanMode values
        params.clear();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "5");
        params.put("incrementalBetweenScanMode", "invalid");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for invalid scan mode");
        } catch (UserException e) {
            Assert.assertTrue(
                    e.getMessage().contains("incrementalBetweenScanMode must be one of: auto, diff, delta, changelog"));
        }

        // 17. Test valid incrementalBetweenScanMode values (case insensitive)
        String[] validModes = {"auto", "AUTO", "diff", "DIFF", "delta", "DELTA", "changelog", "CHANGELOG"};
        for (String mode : validModes) {
            params.clear();
            params.put("startSnapshotId", "1");
            params.put("endSnapshotId", "5");
            params.put("incrementalBetweenScanMode", mode);
            result = PaimonScanNode.validateIncrementalReadParams(params);
            Assert.assertEquals("1,5", result.get("incremental-between"));
            Assert.assertEquals(mode, result.get("incremental-between-scan-mode"));
            Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
            Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
            Assert.assertEquals(4, result.size());
        }

        // 18. Test no parameters at all
        params.clear();
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when no parameters provided");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("at least one valid parameter group must be specified"));
        }
    }

    @Test
    public void testMaxFileSplitsNum() throws UserException {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(3));
        PaimonScanNode paimonScanNode = new PaimonScanNode(new PlanNodeId(1), desc, false, sv);
        paimonScanNode.setSource(new PaimonSource());

        // Create large files that would generate many splits with small file_split_size
        // Total size: 1GB = 1024 * 1024 * 1024 bytes (smaller for faster test)
        long totalFileSize = 1024L * 1024 * 1024;
        long fileSize1 = 400L * 1024 * 1024; // 400MB
        long fileSize2 = 300L * 1024 * 1024; // 300MB
        long fileSize3 = 324L * 1024 * 1024; // 324MB

        DataFileMeta dfm1 = DataFileMeta.forAppend("f1.parquet", fileSize1, 1, SimpleStats.EMPTY_STATS, 1, 1, 1,
                Collections.emptyList(), null, null, null, null);
        DataFileMeta dfm2 = DataFileMeta.forAppend("f2.parquet", fileSize2, 2, SimpleStats.EMPTY_STATS, 1, 1, 1,
                Collections.emptyList(), null, null, null, null);
        DataFileMeta dfm3 = DataFileMeta.forAppend("f3.parquet", fileSize3, 3, SimpleStats.EMPTY_STATS, 1, 1, 1,
                Collections.emptyList(), null, null, null, null);

        BinaryRow binaryRow = BinaryRow.singleColumn(1);
        DataSplit ds1 = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow)
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dfm1))
                .build();

        DataSplit ds2 = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow)
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dfm2))
                .build();

        DataSplit ds3 = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow)
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dfm3))
                .build();

        PaimonScanNode spyPaimonScanNode = Mockito.spy(paimonScanNode);
        Mockito.doReturn(new ArrayList<org.apache.paimon.table.source.Split>() {
            {
                add(ds1);
                add(ds2);
                add(ds3);
            }
        }).when(spyPaimonScanNode).getPaimonSplitFromAPI();

        // Mock SessionVariable behavior
        Mockito.when(sv.isForceJniScanner()).thenReturn(false);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");
        long baseSplitSize = 10L * 1024 * 1024; // 10MB, small split size
        Mockito.when(sv.getFileSplitSize()).thenReturn(baseSplitSize);
        mockNativeReader(spyPaimonScanNode);

        // Test case 1: max_file_splits_num = 50 (should limit split count)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(50);
        List<org.apache.doris.spi.Split> splits1 = spyPaimonScanNode.getSplits(1);
        // With 1GB total and 10MB split size, would generate ~102 splits without limit
        // With limit of 50, should generate at most 50 splits (allow small tolerance due to split algorithm)
        Assert.assertTrue("Split count should be limited to around 50, actual: " + splits1.size(),
                splits1.size() <= 52); // Allow small tolerance for split algorithm boundary cases
        // Verify split size was adjusted: minSplitSize = ceil(1GB / 50) = ~20MB
        long minExpectedSplitSize1 = (totalFileSize + 50 - 1) / 50;
        for (org.apache.doris.spi.Split split : splits1) {
            PaimonSplit paimonSplit = (PaimonSplit) split;
            Assert.assertNotNull("Split should have target split size", paimonSplit.getTargetSplitSize());
            // Adjusted split size should be at least ceil(totalFileSize / maxFileSplitsNum)
            Assert.assertTrue("Split size should be adjusted to limit split count. Expected at least: "
                            + minExpectedSplitSize1 + ", got: " + paimonSplit.getTargetSplitSize(),
                    paimonSplit.getTargetSplitSize() >= minExpectedSplitSize1);
        }

        // Test case 2: max_file_splits_num = 20 (should further limit split count)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(20);
        List<org.apache.doris.spi.Split> splits2 = spyPaimonScanNode.getSplits(1);
        Assert.assertTrue("Split count should be limited to around 20, actual: " + splits2.size(),
                splits2.size() <= 22); // Allow small tolerance for split algorithm boundary cases
        // Adjusted split size should be at least ceil(1GB / 20) = ~50MB
        long minExpectedSplitSize2 = (totalFileSize + 20 - 1) / 20;
        for (org.apache.doris.spi.Split split : splits2) {
            PaimonSplit paimonSplit = (PaimonSplit) split;
            Assert.assertTrue("Split size should be adjusted to limit split count. Expected at least: "
                            + minExpectedSplitSize2 + ", got: " + paimonSplit.getTargetSplitSize(),
                    paimonSplit.getTargetSplitSize() >= minExpectedSplitSize2);
        }

        // Test case 3: max_file_splits_num = 0 (no limit)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(0);
        List<org.apache.doris.spi.Split> splits3 = spyPaimonScanNode.getSplits(1);
        // Without limit, should generate splits based on file_split_size (10MB)
        // Expected: approximately ceil(1GB / 10MB) = 102 splits
        long expectedSplitsWithoutLimit = (totalFileSize + baseSplitSize - 1) / baseSplitSize;
        Assert.assertTrue("Without limit, should generate more splits. Expected around: "
                        + expectedSplitsWithoutLimit + ", got: " + splits3.size(),
                splits3.size() >= expectedSplitsWithoutLimit - 5); // Allow some tolerance

        // Test case 4: max_file_splits_num = 200 (large limit, should not adjust)
        Mockito.when(sv.getMaxFileSplitsNum()).thenReturn(200);
        List<org.apache.doris.spi.Split> splits4 = spyPaimonScanNode.getSplits(1);
        // With large limit, should use original split size (10MB)
        // Should generate approximately the same number of splits as case 3
        Assert.assertTrue("With large limit, should generate similar number of splits. Expected around: "
                        + expectedSplitsWithoutLimit + ", got: " + splits4.size(),
                splits4.size() >= expectedSplitsWithoutLimit - 5);
    }

    private void mockJniReader(PaimonScanNode spyNode) {
        Mockito.doReturn(false).when(spyNode).supportNativeReader(ArgumentMatchers.any(Optional.class));
    }

    private void mockNativeReader(PaimonScanNode spyNode) {
        Mockito.doReturn(true).when(spyNode).supportNativeReader(ArgumentMatchers.any(Optional.class));
    }
}
