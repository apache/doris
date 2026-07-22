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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonFileExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonSysExternalTable;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.metastore.PaimonJdbcMetaStoreProperties;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPushAggOp;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

@RunWith(MockitoJUnitRunner.class)
public class PaimonScanNodeTest {
    @Mock
    private SessionVariable sv;

    @Mock
    private PaimonFileExternalCatalog paimonFileExternalCatalog;

    @Test
    public void testFilesCreationTimeLowerBoundBecomesPaimonScanOption() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(1), new TupleId(3), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Table table = Mockito.mock(Table.class);
        Table filteredTable = Mockito.mock(Table.class);
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(systemTable.getSysTableType()).thenReturn("files");
        Mockito.when(table.copy(ArgumentMatchers.anyMap())).thenReturn(filteredTable);
        node.setSource(source);
        node.addConjunct(new BinaryPredicate(BinaryPredicate.Operator.GE,
                new SlotRef(null, "creation_time"),
                new DateLiteral(2026, 7, 21, 1, 2, 3, 456000, Type.DATETIMEV2)));

        Table processedTable = (Table) invokePrivateMethod(node, "getProcessedTable");

        Assert.assertSame(filteredTable, processedTable);
        ArgumentCaptor<Map<String, String>> optionsCaptor = ArgumentCaptor.forClass(Map.class);
        Mockito.verify(table).copy(optionsCaptor.capture());
        Assert.assertEquals("1784595723456",
                optionsCaptor.getValue().get(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key()));
    }

    @Test
    public void testExtractFilesCreationTimeUsesStrongestSafeLowerBound() throws Exception {
        SlotRef creationTime = new SlotRef(null, "creation_time");
        DateLiteral older = new DateLiteral(2026, 7, 20, 1, 2, 3, 456000, Type.DATETIMEV2);
        DateLiteral newer = new DateLiteral(2026, 7, 21, 1, 2, 3, 456000, Type.DATETIMEV2);
        Expr lowerBound = new BinaryPredicate(BinaryPredicate.Operator.GE, creationTime, older);
        Expr strictReversedLowerBound = new BinaryPredicate(BinaryPredicate.Operator.LT, newer, creationTime);
        Expr upperBound = new BinaryPredicate(BinaryPredicate.Operator.LE, creationTime,
                new DateLiteral(2026, 7, 22, 1, 2, 3, 456000, Type.DATETIMEV2));

        OptionalLong result = PaimonScanNode.extractFileCreationTimeLowerBound(
                Arrays.asList(lowerBound, strictReversedLowerBound, upperBound));

        Assert.assertTrue(result.isPresent());
        Assert.assertEquals(1784595723457L, result.getAsLong());
    }

    @Test
    public void testExtractFilesCreationTimeDoesNotPushLowerBoundThroughOr() throws Exception {
        Expr creationTimeLowerBound = new BinaryPredicate(BinaryPredicate.Operator.GE,
                new SlotRef(null, "creation_time"),
                new DateLiteral(2026, 7, 21, 1, 2, 3, 456000, Type.DATETIMEV2));
        Expr otherColumn = new BinaryPredicate(BinaryPredicate.Operator.GE,
                new SlotRef(null, "record_count"), new org.apache.doris.analysis.IntLiteral(10));
        Expr disjunction = new CompoundPredicate(CompoundPredicate.Operator.OR,
                creationTimeLowerBound, otherColumn);

        Assert.assertFalse(PaimonScanNode.extractFileCreationTimeLowerBound(
                Collections.singletonList(disjunction)).isPresent());
    }

    @Test
    public void testCountColumnKeepsAllSplitsWhileCountStarUsesMergedRowCount() throws UserException {
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(1), new TupleId(3), sv));
        node.setSource(mockPaimonSourceWithPartitionKeys(Collections.emptyList()));
        List<org.apache.paimon.table.source.Split> dataSplits = Arrays.asList(
                mockCountDataSplit("f1.parquet", 4_000),
                mockCountDataSplit("f2.parquet", 5_000),
                mockCountDataSplit("f3.parquet", 6_000));
        Mockito.doReturn(dataSplits).when(node).getPaimonSplitFromAPI();
        Mockito.when(sv.isForceJniScanner()).thenReturn(true);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");
        Mockito.when(sv.getParallelExecInstanceNum(ArgumentMatchers.nullable(String.class))).thenReturn(1);

        // Before the fix, the raw COUNT opcode made this path keep only parallel representative
        // splits and attach the 15,000 metadata rows. BE rejects that shortcut for COUNT(col), so
        // it would scan only those representatives and silently miss the discarded DataSplits.
        node.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        node.setPushDownCountSlotIds(Collections.singletonList(new SlotId(7)));
        List<org.apache.doris.spi.Split> countColumnSplits = node.getSplits(1);
        Assert.assertEquals(3, countColumnSplits.size());
        for (org.apache.doris.spi.Split split : countColumnSplits) {
            Assert.assertFalse(((PaimonSplit) split).getRowCount().isPresent());
        }

        // COUNT(*) remains metadata-only. The 15,000 rows exceed the parallel threshold, so one
        // configured execution instance retains one representative split carrying the full sum.
        node.setPushDownCountSlotIds(Collections.emptyList());
        List<org.apache.doris.spi.Split> countStarSplits = node.getSplits(1);
        Assert.assertEquals(1, countStarSplits.size());
        Assert.assertEquals(Optional.of(15_000L), ((PaimonSplit) countStarSplits.get(0)).getRowCount());
    }

    @Test
    public void testSplitWeight() throws UserException {

        PaimonScanNode paimonScanNode = newTestNode(new PlanNodeId(1), new TupleId(3), sv);

        paimonScanNode.setSource(mockPaimonSourceWithPartitionKeys(Collections.emptyList()));

        DataFileMeta dfm1 = DataFileMeta.forAppend("f1.parquet", 64L * 1024 * 1024, 1L, SimpleStats.EMPTY_STATS,
                1L, 1L, 1L, Collections.<String>emptyList(), null, FileSource.APPEND,
                Collections.<String>emptyList(), null, null, Collections.<String>emptyList());
        BinaryRow binaryRow1 = BinaryRow.singleColumn(1);
        DataSplit ds1 = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow1)
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dfm1))
                .build();

        DataFileMeta dfm2 = DataFileMeta.forAppend("f2.parquet", 32L * 1024 * 1024, 2L, SimpleStats.EMPTY_STATS,
                1L, 1L, 1L, Collections.<String>emptyList(), null, FileSource.APPEND,
                Collections.<String>emptyList(), null, null, Collections.<String>emptyList());
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

        long maxInitialSplitSize = 32L * 1024L * 1024L;
        long maxSplitSize = 64L * 1024L * 1024L;
        // Ensure fileSplitter is initialized on the spy as doInitialize() is not called in this unit test
        FileSplitter fileSplitter = new FileSplitter(maxInitialSplitSize, maxSplitSize,
                0);
        try {
            java.lang.reflect.Field field = FileQueryScanNode.class.getDeclaredField("fileSplitter");
            field.setAccessible(true);
            field.set(spyPaimonScanNode, fileSplitter);

            java.lang.reflect.Field storagePropertiesField =
                    PaimonScanNode.class.getDeclaredField("storagePropertiesMap");
            storagePropertiesField.setAccessible(true);
            storagePropertiesField.set(spyPaimonScanNode, Collections.emptyMap());
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to inject test fields into PaimonScanNode", e);
        }

        // Note: The original PaimonSource is sufficient for this test
        // No need to mock catalog properties since doInitialize() is not called in this test
        // Mock SessionVariable behavior
        Mockito.when(sv.isForceJniScanner()).thenReturn(false);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");
        Mockito.when(sv.getMaxInitialSplitSize()).thenReturn(maxInitialSplitSize);
        Mockito.when(sv.getMaxSplitSize()).thenReturn(maxSplitSize);

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
    public void testPaimonDataSystemTableForceJniEvenWhenNativeSupported() throws UserException {
        PaimonScanNode paimonScanNode = newTestNode(new PlanNodeId(1), new TupleId(3), sv);
        PaimonScanNode spyPaimonScanNode = Mockito.spy(paimonScanNode);

        DataFileMeta dfm = DataFileMeta.forAppend("f1.parquet", 64L * 1024 * 1024, 1L, SimpleStats.EMPTY_STATS,
                1L, 1L, 1L, Collections.<String>emptyList(), null, FileSource.APPEND,
                Collections.<String>emptyList(), null, null, Collections.<String>emptyList());
        BinaryRow binaryRow = BinaryRow.singleColumn(1);
        DataSplit dataSplit = DataSplit.builder()
                .rawConvertible(true)
                .withPartition(binaryRow)
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dfm))
                .build();

        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable binlogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(binlogTable.getSysTableType()).thenReturn("binlog");
        Mockito.when(source.getExternalTable()).thenReturn(binlogTable);
        spyPaimonScanNode.setSource(source);

        Mockito.doReturn(Collections.singletonList(dataSplit)).when(spyPaimonScanNode).getPaimonSplitFromAPI();
        Assert.assertTrue(spyPaimonScanNode.supportNativeReader(dataSplit.convertToRawFiles()));

        long maxInitialSplitSize = 32L * 1024L * 1024L;
        long maxSplitSize = 64L * 1024L * 1024L;
        FileSplitter fileSplitter = new FileSplitter(maxInitialSplitSize, maxSplitSize, 0);
        try {
            java.lang.reflect.Field field = FileQueryScanNode.class.getDeclaredField("fileSplitter");
            field.setAccessible(true);
            field.set(spyPaimonScanNode, fileSplitter);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Failed to inject FileSplitter into PaimonScanNode test", e);
        }

        Mockito.when(sv.isForceJniScanner()).thenReturn(false);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");
        Mockito.when(sv.getMaxSplitSize()).thenReturn(maxSplitSize);

        Assert.assertTrue(spyPaimonScanNode.shouldForceJniForSystemTable());
        List<org.apache.doris.spi.Split> splits = spyPaimonScanNode.getSplits(1);
        Assert.assertEquals(1, splits.size());
        Assert.assertNotNull(((PaimonSplit) splits.get(0)).getSplit());

        PaimonSysExternalTable auditLogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(auditLogTable.getSysTableType()).thenReturn("audit_log");
        Mockito.when(source.getExternalTable()).thenReturn(auditLogTable);

        Assert.assertTrue(spyPaimonScanNode.shouldForceJniForSystemTable());
        List<org.apache.doris.spi.Split> auditLogSplits = spyPaimonScanNode.getSplits(1);
        Assert.assertEquals(1, auditLogSplits.size());
        Assert.assertNotNull(((PaimonSplit) auditLogSplits.get(0)).getSplit());
    }

    @Test
    public void testDetermineTargetFileSplitSizeHonorsMaxFileSplitNum() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);

        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getFileFormatFromTableProperties()).thenReturn("parquet");
        node.setSource(source);

        RawFile rawFile = Mockito.mock(RawFile.class);
        Mockito.when(rawFile.path()).thenReturn("file.parquet");
        Mockito.when(rawFile.fileSize()).thenReturn(10_000L * 1024L * 1024L);

        DataSplit dataSplit = Mockito.mock(DataSplit.class);
        Mockito.when(dataSplit.convertToRawFiles()).thenReturn(Optional.of(Collections.singletonList(rawFile)));

        Method method = PaimonScanNode.class.getDeclaredMethod("determineTargetFileSplitSize", List.class, boolean.class);
        method.setAccessible(true);
        long target = (long) method.invoke(node, Collections.singletonList(dataSplit), false);
        Assert.assertEquals(100L * 1024L * 1024L, target);
    }

    @Test
    public void testGetBackendPaimonOptionsForJdbcCatalog() throws Exception {
        String driverUrl = "file:///tmp/postgresql-42.5.0.jar";
        Map<String, String> props = new HashMap<>();
        props.put("type", "paimon");
        props.put("paimon.catalog.type", "jdbc");
        props.put("uri", "jdbc:postgresql://127.0.0.1:5442/postgres");
        props.put("warehouse", "s3://warehouse/path");
        props.put("paimon.jdbc.driver_url", driverUrl);
        props.put("paimon.jdbc.driver_class", "org.postgresql.Driver");
        PaimonJdbcMetaStoreProperties jdbcMetaStoreProperties =
                (PaimonJdbcMetaStoreProperties) MetastoreProperties.create(props);

        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalogProperty.getMetastoreProperties()).thenReturn(jdbcMetaStoreProperties);

        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);

        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getCatalog()).thenReturn(catalog);

        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        node.setSource(source);

        Map<String, String> backendOptions = node.getBackendPaimonOptions();
        Assert.assertEquals("org.postgresql.Driver", backendOptions.get("jdbc.driver_class"));
        Assert.assertEquals(driverUrl, backendOptions.get("jdbc.driver_url"));
        Assert.assertEquals(2, backendOptions.size());
    }

    @Test
    public void testGetBackendPaimonOptionsForJniIOManager() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.doris.enable_jni_io_manager", "true");
        props.put("paimon.doris.jni_io_manager.tmp_dir", "/tmp/doris-paimon");
        props.put("paimon.doris.jni_io_manager.impl_class", "org.example.CustomIOManager");

        CatalogProperty catalogProperty = Mockito.mock(CatalogProperty.class);
        Mockito.when(catalogProperty.getProperties()).thenReturn(props);
        Mockito.when(catalogProperty.getMetastoreProperties()).thenReturn(Mockito.mock(MetastoreProperties.class));

        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        Mockito.when(catalog.getCatalogProperty()).thenReturn(catalogProperty);

        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getCatalog()).thenReturn(catalog);

        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        node.setSource(source);

        Map<String, String> backendOptions = node.getBackendPaimonOptions();
        Assert.assertEquals("true", backendOptions.get("doris.enable_jni_io_manager"));
        Assert.assertEquals("/tmp/doris-paimon", backendOptions.get("doris.jni_io_manager.tmp_dir"));
        Assert.assertEquals("org.example.CustomIOManager",
                backendOptions.get("doris.jni_io_manager.impl_class"));
        Assert.assertEquals(3, backendOptions.size());
    }

    @Test
    public void testApplyBackendPaimonOptionsAtScanNodeLevel() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getTableLocation()).thenReturn("file:///warehouse");
        Table paimonTable = mockPaimonTableWithPartitionKeys(Collections.emptyList());
        Mockito.when(source.getPaimonTable()).thenReturn(paimonTable);
        node.setSource(source);

        Map<String, String> backendOptions = new HashMap<>();
        backendOptions.put("jdbc.driver_url", "file:///tmp/postgresql-42.5.0.jar");
        backendOptions.put("jdbc.driver_class", "org.postgresql.Driver");
        setField(FileQueryScanNode.class, node, "params", new TFileScanRangeParams());
        setField(PaimonScanNode.class, node, "backendPaimonOptions", backendOptions);
        setField(PaimonScanNode.class, node, "storagePropertiesMap", Collections.emptyMap());

        invokePrivateMethod(node, "setScanLevelPaimonOptions");

        Assert.assertEquals(backendOptions, node.getFileScanRangeParams().getPaimonOptions());

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                rangeDesc, new PaimonSplit(createDataSplit("scan_level.parquet")));
        Assert.assertFalse(rangeDesc.getTableFormatParams().getPaimonParams().isSetPaimonOptions());
    }

    @Test
    public void testGetPathPartitionKeysReturnsTablePartitionKeys() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table table = Mockito.mock(Table.class);
        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        Mockito.when(table.partitionKeys()).thenReturn(Arrays.asList("Dt", "Region"));
        Mockito.when(sysTable.isDataTable()).thenReturn(true);
        node.setSource(source);

        Assert.assertEquals(Arrays.asList("Dt", "Region"), node.getPathPartitionKeys());
    }

    @Test
    public void testGetPathPartitionKeysReturnsEmptyForMetadataSystemTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        Mockito.when(sysTable.isDataTable()).thenReturn(false);
        node.setSource(source);

        Assert.assertEquals(Collections.emptyList(), node.getPathPartitionKeys());
    }

    @Test
    public void testSetPaimonParamsUsesOrderedPartitionKeys() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table table = Mockito.mock(Table.class);
        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(source.getTableLocation()).thenReturn("file:///warehouse");
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        Mockito.when(sysTable.isDataTable()).thenReturn(true);
        Mockito.when(table.partitionKeys()).thenReturn(Arrays.asList("Pt", "Dt"));
        node.setSource(source);

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setColumnsFromPathKeys(Collections.singletonList("stale"));
        rangeDesc.setColumnsFromPath(Collections.singletonList("old"));
        rangeDesc.setColumnsFromPathIsNull(Collections.singletonList(false));
        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("Dt", "2025-01-01");
        partitionValues.put("Pt", "p1");
        PaimonSplit split = new PaimonSplit(createDataSplit("ordered.parquet"));
        split.setPaimonPartitionValues(partitionValues);

        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class}, rangeDesc, split);

        Assert.assertEquals(Arrays.asList("Pt", "Dt"), rangeDesc.getColumnsFromPathKeys());
        Assert.assertEquals(Arrays.asList("p1", "2025-01-01"), rangeDesc.getColumnsFromPath());
        Assert.assertEquals(Arrays.asList(false, false), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void testGetFieldIndexMatchesMixedCaseColumns() {
        List<String> fieldNames = Arrays.asList("data", "mIxEd_COL", "PART");

        Assert.assertEquals(1, PaimonScanNode.getFieldIndex(fieldNames, "mixed_col"));
        Assert.assertEquals(2, PaimonScanNode.getFieldIndex(fieldNames, "part"));
        Assert.assertEquals(-1, PaimonScanNode.getFieldIndex(fieldNames, "missing_col"));
    }

    private void mockJniReader(PaimonScanNode spyNode) {
        Mockito.doReturn(false).when(spyNode).supportNativeReader(ArgumentMatchers.any(Optional.class));
    }

    private void mockNativeReader(PaimonScanNode spyNode) {
        Mockito.doReturn(true).when(spyNode).supportNativeReader(ArgumentMatchers.any(Optional.class));
    }

    private PaimonScanNode newTestNode(PlanNodeId planNodeId, TupleId tupleId, SessionVariable sessionVariable) {
        TupleDescriptor desc = new TupleDescriptor(tupleId);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        Table paimonTable = mockPaimonTableWithPartitionKeys(Collections.emptyList());
        Mockito.when(externalTable.getPaimonTable(ArgumentMatchers.any())).thenReturn(paimonTable);
        desc.setTable(externalTable);
        return new PaimonScanNode(planNodeId, desc, false, sessionVariable, ScanContext.EMPTY);
    }

    private PaimonSource mockPaimonSourceWithPartitionKeys(List<String> partitionKeys) {
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table paimonTable = mockPaimonTableWithPartitionKeys(partitionKeys);
        Mockito.when(source.getPaimonTable()).thenReturn(paimonTable);
        return source;
    }

    private Table mockPaimonTableWithPartitionKeys(List<String> partitionKeys) {
        Table paimonTable = Mockito.mock(Table.class);
        Mockito.when(paimonTable.partitionKeys()).thenReturn(partitionKeys);
        return paimonTable;
    }

    private void setField(Class<?> clazz, Object target, String fieldName, Object value) throws Exception {
        java.lang.reflect.Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private Object invokePrivateMethod(Object target, String methodName, Class<?>[] parameterTypes, Object... args)
            throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, parameterTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }

    private Object invokePrivateMethod(Object target, String methodName) throws Exception {
        return invokePrivateMethod(target, methodName, new Class<?>[0]);
    }

    private DataSplit createDataSplit(String fileName) {
        DataFileMeta dataFileMeta = DataFileMeta.forAppend(fileName, 64L * 1024 * 1024, 1L, SimpleStats.EMPTY_STATS,
                1L, 1L, 1L, Collections.<String>emptyList(), null, FileSource.APPEND,
                Collections.<String>emptyList(), null, null, Collections.<String>emptyList());
        return DataSplit.builder()
                .rawConvertible(true)
                .withPartition(BinaryRow.singleColumn(1))
                .withBucket(1)
                .withBucketPath("file://b1")
                .withDataFiles(Collections.singletonList(dataFileMeta))
                .build();
    }

    private DataSplit mockCountDataSplit(String fileName, long rowCount) {
        DataFileMeta dataFileMeta = DataFileMeta.forAppend(fileName, 64L * 1024 * 1024, rowCount,
                SimpleStats.EMPTY_STATS, 1L, 1L, 1L, Collections.<String>emptyList(), null,
                FileSource.APPEND, Collections.<String>emptyList(), null, null,
                Collections.<String>emptyList());
        DataSplit dataSplit = Mockito.mock(DataSplit.class);
        Mockito.when(dataSplit.rowCount()).thenReturn(rowCount);
        Mockito.when(dataSplit.mergedRowCountAvailable()).thenReturn(true);
        Mockito.when(dataSplit.mergedRowCount()).thenReturn(rowCount);
        Mockito.when(dataSplit.partition()).thenReturn(BinaryRow.singleColumn(1));
        Mockito.when(dataSplit.dataFiles()).thenReturn(Collections.singletonList(dataFileMeta));
        Mockito.when(dataSplit.convertToRawFiles()).thenReturn(Optional.empty());
        Mockito.when(dataSplit.deletionFiles()).thenReturn(Optional.empty());
        return dataSplit;
    }
}
