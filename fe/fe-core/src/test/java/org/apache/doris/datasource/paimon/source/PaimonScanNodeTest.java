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
import org.apache.doris.datasource.CatalogProperty;
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

        // Mock PaimonSource to return catalog
        PaimonSource mockPaimonSource = Mockito.mock(PaimonSource.class);
        Mockito.when(mockPaimonSource.getCatalog()).thenReturn(paimonFileExternalCatalog);
        spyPaimonScanNode.setSource(mockPaimonSource);

        // Mock ExternalCatalog properties
        CatalogProperty mockCatalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(paimonFileExternalCatalog.getCatalogProperty()).thenReturn(mockCatalogProperty);
        Mockito.when(mockCatalogProperty.getStoragePropertiesMap()).thenReturn(Collections.emptyMap());
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
        Map<String, String> params = new HashMap<>();
        params.put("startSnapshotId", "5");
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "endSnapshotId is required when using snapshot-based incremental read",
                () -> PaimonScanNode.validateIncrementalReadParams(params));

        // 2. Both startSnapshotId and endSnapshotId
        params.clear();
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

        // 11. Test invalid snapshot ID values (≤ 0)
        params.clear();
        params.put("startSnapshotId", "0");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for startSnapshotId ≤ 0");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startSnapshotId must be greater than 0"));
        }

        params.clear();
        params.put("startSnapshotId", "-1");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for negative startSnapshotId");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startSnapshotId must be greater than 0"));
        }

        params.clear();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "0");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception for endSnapshotId ≤ 0");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("endSnapshotId must be greater than 0"));
        }

        // 12. Test start ≥ end for snapshot IDs
        params.clear();
        params.put("startSnapshotId", "5");
        params.put("endSnapshotId", "5");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startSnapshotId = endSnapshotId");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startSnapshotId must be less than endSnapshotId"));
        }

        params.clear();
        params.put("startSnapshotId", "6");
        params.put("endSnapshotId", "5");
        try {
            PaimonScanNode.validateIncrementalReadParams(params);
            Assert.fail("Should throw exception when startSnapshotId > endSnapshotId");
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("startSnapshotId must be less than endSnapshotId"));
        }

        // 13. Test invalid timestamp values (≤ 0)
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

    private void mockJniReader(PaimonScanNode spyNode) {
        Mockito.doReturn(false).when(spyNode).supportNativeReader(ArgumentMatchers.any(Optional.class));
    }

    private void mockNativeReader(PaimonScanNode spyNode) {
        Mockito.doReturn(true).when(spyNode).supportNativeReader(ArgumentMatchers.any(Optional.class));
    }
}
