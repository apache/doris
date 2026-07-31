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

import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonFileExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonScanParams;
import org.apache.doris.datasource.paimon.PaimonSysExternalTable;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.property.metastore.MetastoreProperties;
import org.apache.doris.datasource.property.metastore.PaimonJdbcMetaStoreProperties;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPaimonReaderType;
import org.apache.doris.thrift.TPushAggOp;

import com.google.common.collect.ImmutableMap;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.options.Options;
import org.apache.paimon.privilege.AllGrantedPrivilegeChecker;
import org.apache.paimon.privilege.PrivilegedFileStoreTable;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.utils.InstantiationUtil;
import org.apache.paimon.utils.SnapshotManager;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
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
    public void testCountColumnKeepsAllSplitsWhileCountStarUsesMergedRowCount() throws UserException {
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(1), new TupleId(3), sv));
        node.setSource(mockPaimonSourceWithPartitionKeys(Collections.<String>emptyList()));
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
    public void testIncrementalBinlogCountStarDoesNotUsePhysicalRowCount() throws UserException {
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(1), new TupleId(3), sv));
        PaimonSource source = mockPaimonSourceWithPartitionKeys(Collections.emptyList());
        PaimonSysExternalTable binlogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(binlogTable.getSysTableType()).thenReturn("binlog");
        Mockito.when(source.getExternalTable()).thenReturn(binlogTable);
        node.setSource(source);
        node.setScanParams(new TableScanParams(
                TableScanParams.INCREMENTAL_READ,
                ImmutableMap.of("startSnapshotId", "1", "endSnapshotId", "2"),
                Collections.emptyList()));
        Mockito.doReturn(Arrays.asList(
                mockCountDataSplit("before.parquet", 1),
                mockCountDataSplit("after.parquet", 1)))
                .when(node).getPaimonSplitFromAPI();
        Mockito.when(sv.isForceJniScanner()).thenReturn(false);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");

        node.setPushDownAggNoGrouping(TPushAggOp.COUNT);
        node.setPushDownCountSlotIds(Collections.emptyList());
        List<org.apache.doris.spi.Split> splits = node.getSplits(1);

        Assert.assertEquals(2, splits.size());
        for (org.apache.doris.spi.Split split : splits) {
            Assert.assertFalse(((PaimonSplit) split).getRowCount().isPresent());
        }
    }

    @Test
    public void testSplitWeight() throws UserException {

        TupleDescriptor desc = new TupleDescriptor(new TupleId(3));
        PaimonScanNode paimonScanNode = new PaimonScanNode(new PlanNodeId(1), desc, false, sv, ScanContext.EMPTY);

        PaimonSource source = Mockito.spy(new PaimonSource());
        Table paimonTable = Mockito.mock(Table.class);
        Mockito.doReturn(paimonTable).when(source).getPaimonTable();
        Mockito.when(paimonTable.partitionKeys()).thenReturn(Collections.emptyList());
        paimonScanNode.setSource(source);

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
        Assert.assertEquals(16, result.size());

        // 3. startSnapshotId + endSnapshotId + incrementalBetweenScanMode
        params.clear();
        params.put("startSnapshotId", "2");
        params.put("endSnapshotId", "8");
        params.put("incrementalBetweenScanMode", "diff");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("2,8", result.get("incremental-between"));
        Assert.assertEquals("diff", result.get("incremental-between-scan-mode"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertEquals(16, result.size());

        // 4. Only startTimestamp
        params.clear();
        params.put("startTimestamp", "1000");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("1000," + Long.MAX_VALUE, result.get("incremental-between-timestamp"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertTrue(result.containsKey("scan.snapshot-id") && result.get("scan.snapshot-id") == null);
        Assert.assertEquals(16, result.size());

        // 5. Both startTimestamp and endTimestamp
        params.clear();
        params.put("startTimestamp", "1000");
        params.put("endTimestamp", "2000");
        result = PaimonScanNode.validateIncrementalReadParams(params);
        Assert.assertEquals("1000,2000", result.get("incremental-between-timestamp"));
        Assert.assertTrue(result.containsKey("scan.mode") && result.get("scan.mode") == null);
        Assert.assertTrue(result.containsKey("scan.snapshot-id") && result.get("scan.snapshot-id") == null);
        Assert.assertEquals(16, result.size());

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
        Assert.assertEquals(16, result.size());

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
            Assert.assertEquals(16, result.size());
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
        TupleDescriptor desc = new TupleDescriptor(new TupleId(3));
        PaimonScanNode paimonScanNode = new PaimonScanNode(new PlanNodeId(1), desc, false, sv, ScanContext.EMPTY);
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

        Mockito.doReturn(Collections.singletonList(dataSplit)).when(spyPaimonScanNode).getPaimonSplitFromAPI();
        mockNativeReader(spyPaimonScanNode);

        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable binlogTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(binlogTable.getSysTableType()).thenReturn("binlog");
        Mockito.when(source.getExternalTable()).thenReturn(binlogTable);
        spyPaimonScanNode.setSource(source);

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

        PaimonSysExternalTable rowTrackingTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(rowTrackingTable.getSysTableType()).thenReturn("row_tracking");
        Mockito.when(source.getExternalTable()).thenReturn(rowTrackingTable);

        Assert.assertTrue(spyPaimonScanNode.shouldForceJniForSystemTable());
        List<org.apache.doris.spi.Split> rowTrackingSplits = spyPaimonScanNode.getSplits(1);
        Assert.assertEquals(1, rowTrackingSplits.size());
        Assert.assertNotNull(((PaimonSplit) rowTrackingSplits.get(0)).getSplit());
    }

    @Test
    public void testPaimonDataSystemTablesBypassCppReader() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        node.setSource(source);
        setField(PaimonScanNode.class, node, "storagePropertiesMap", Collections.emptyMap());

        for (String type : Arrays.asList("audit_log", "binlog", "row_tracking")) {
            Mockito.when(systemTable.getSysTableType()).thenReturn(type);
            TFileRangeDesc rangeDesc = new TFileRangeDesc();
            invokePrivateMethod(node, "setPaimonParams",
                    new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                    rangeDesc, new PaimonSplit(createDataSplit(type + ".parquet")));
            Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                    rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        }
    }

    @Test
    public void testSchemaSelectingOptionsBypassCppReader() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(table);
        Table baseTable = Mockito.mock(Table.class);
        Mockito.when(baseTable.partitionKeys()).thenReturn(Collections.emptyList());
        Mockito.when(source.getPaimonTable()).thenReturn(baseTable);
        node.setSource(source);
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.snapshot-id", "1"),
                Collections.emptyList()));
        setField(PaimonScanNode.class, node, "storagePropertiesMap", Collections.emptyMap());

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                rangeDesc, new PaimonSplit(createDataSplit("historical.parquet")));

        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
    }

    @Test
    public void testSystemTablePassesIncrementalOptionsToPaimonTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(systemTable.getSysTableType()).thenReturn("audit_log");
        Table baseTable = Mockito.mock(Table.class);
        Table copiedTable = Mockito.mock(Table.class);
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(baseTable);
        node.setSource(source);

        Map<String, String> params = new HashMap<>();
        params.put("startSnapshotId", "1");
        params.put("endSnapshotId", "2");
        node.setScanParams(new TableScanParams(
                TableScanParams.INCREMENTAL_READ, params, Collections.emptyList()));

        Map<String, String> expectedOptions = new HashMap<>();
        expectedOptions.put("scan.timestamp", null);
        expectedOptions.put("scan.timestamp-millis", null);
        expectedOptions.put("scan.watermark", null);
        expectedOptions.put("scan.file-creation-time-millis", null);
        expectedOptions.put("scan.creation-time-millis", null);
        expectedOptions.put("scan.snapshot-id", null);
        expectedOptions.put("scan.tag-name", null);
        expectedOptions.put("scan.version", null);
        expectedOptions.put("scan.bounded.watermark", null);
        expectedOptions.put("scan.mode", null);
        expectedOptions.put("log.scan", null);
        expectedOptions.put("log.scan.timestamp-millis", null);
        expectedOptions.put("incremental-between-timestamp", null);
        expectedOptions.put("incremental-between-scan-mode", null);
        expectedOptions.put("incremental-to-auto-tag", null);
        expectedOptions.put("incremental-between", "1,2");
        Mockito.when(baseTable.copy(expectedOptions)).thenReturn(copiedTable);

        try {
            Assert.assertSame(copiedTable, invokePrivateMethod(node, "getProcessedTable"));
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.fail("Paimon system table should accept incremental options, but got: "
                    + e.getTargetException().getMessage());
        }
        Mockito.verify(baseTable).copy(expectedOptions);
    }

    @Test
    public void testPinnedFileCreationScanPreservesBatchReaderFilters() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        FileStoreTable table = Mockito.mock(FileStoreTable.class);
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        SnapshotReader reader = Mockito.mock(SnapshotReader.class);
        SnapshotReader.Plan plan = Mockito.mock(SnapshotReader.Plan.class);
        CoreOptions coreOptions = Mockito.mock(CoreOptions.class);
        org.apache.paimon.options.Options configuration = new org.apache.paimon.options.Options();
        configuration.set(CoreOptions.BATCH_SCAN_MODE, CoreOptions.BatchScanMode.NONE);

        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class))).thenReturn(table);
        Mockito.when(snapshot.id()).thenReturn(23L);
        Mockito.when(table.latestSnapshot()).thenReturn(Optional.of(snapshot));
        Mockito.when(table.options()).thenReturn(ImmutableMap.of("scan.snapshot-id", "23"));
        Mockito.when(table.primaryKeys()).thenReturn(Collections.singletonList("id"));
        Mockito.when(table.coreOptions()).thenReturn(coreOptions);
        Mockito.when(coreOptions.batchScanSkipLevel0()).thenReturn(true);
        Mockito.when(coreOptions.toConfiguration()).thenReturn(configuration);
        Mockito.when(coreOptions.bucket()).thenReturn(BucketMode.POSTPONE_BUCKET);
        Mockito.when(table.newSnapshotReader()).thenReturn(reader);
        Mockito.when(reader.withMode(ScanMode.ALL)).thenReturn(reader);
        Mockito.when(reader.withSnapshot(23L)).thenReturn(reader);
        Mockito.when(reader.withManifestEntryFilter(ArgumentMatchers.any())).thenReturn(reader);
        Mockito.when(reader.withLevelFilter(ArgumentMatchers.any())).thenReturn(reader);
        Mockito.when(reader.enableValueFilter()).thenReturn(reader);
        Mockito.when(reader.onlyReadRealBuckets()).thenReturn(reader);
        Mockito.when(reader.read()).thenReturn(plan);
        Mockito.when(plan.splits()).thenReturn(Collections.emptyList());
        node.setSource(source);
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.file-creation-time-millis", "1234"),
                Collections.emptyList());
        scanParams.getOrResolveMapParams(options -> PaimonScanParams.resolveOptions(table, options));
        node.setScanParams(scanParams);

        Assert.assertTrue(node.getPaimonSplitFromAPI().isEmpty());

        Mockito.verify(reader).withLevelFilter(ArgumentMatchers.any());
        Mockito.verify(reader).enableValueFilter();
        Mockito.verify(reader).onlyReadRealBuckets();
    }

    @Test
    public void testSystemTableRejectsIncrementalReadWhenReaderIgnoresRange() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(systemTable.getSysTableType()).thenReturn("snapshots");
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(Mockito.mock(Table.class));
        node.setSource(source);
        node.setScanParams(new TableScanParams(
                TableScanParams.INCREMENTAL_READ,
                ImmutableMap.of("startSnapshotId", "1", "endSnapshotId", "2"),
                Collections.emptyList()));

        try {
            invokePrivateMethod(node, "getProcessedTable");
            Assert.fail("snapshots must reject an incremental range it does not consume");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getTargetException().getMessage()
                    .contains("does not support INCR"));
        }
    }

    @Test
    public void testSystemTablePassesDynamicOptionsToPaimonTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(systemTable.getSysTableType()).thenReturn("table_indexes");
        Table baseTable = Mockito.mock(Table.class);
        Table copiedTable = Mockito.mock(Table.class);
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(baseTable);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class)))
                .thenAnswer(invocation -> PaimonScanParams.applyOptions(
                        baseTable, invocation.<TableScanParams>getArgument(0).getMapParams()));
        node.setSource(source);

        Map<String, String> options = ImmutableMap.of(
                "scan.snapshot-id", "12345",
                "scan.mode", "from-snapshot");
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS, options, Collections.emptyList()));
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(copiedTable);

        try {
            Assert.assertSame(copiedTable, invokePrivateMethod(node, "getProcessedTable"));
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.fail("Paimon system table should accept dynamic options, but got: "
                    + e.getTargetException().getMessage());
        }
        Mockito.verify(baseTable).copy(ArgumentMatchers.argThat(applied ->
                "12345".equals(applied.get("scan.snapshot-id"))
                        && "from-snapshot".equals(applied.get("scan.mode"))
                        && applied.containsKey("scan.tag-name")
                        && applied.get("scan.tag-name") == null));
    }

    @Test
    public void testDataTableQueryOptionsOverrideDefaultsWithoutMutation() throws Exception {
        Map<String, String> defaultOptions = new HashMap<>();
        defaultOptions.put("scan.mode", "latest");
        TableSchema schema = new TableSchema(
                0,
                Collections.singletonList(new DataField(0, "id", new IntType())),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                defaultOptions,
                null);
        Table baseTable = new AppendOnlyFileStoreTable(
                Mockito.mock(FileIO.class),
                new Path("memory://paimon_dynamic_options"),
                schema,
                CatalogEnvironment.empty());

        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getExternalTable()).thenReturn(Mockito.mock(PaimonExternalTable.class));
        Mockito.when(source.getPaimonTable()).thenReturn(baseTable);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class)))
                .thenAnswer(invocation -> PaimonScanParams.applyOptions(
                        baseTable, invocation.<TableScanParams>getArgument(0).getMapParams()));
        node.setSource(source);

        Map<String, String> queryOptions = ImmutableMap.of(
                "scan.mode", "from-snapshot",
                "scan.snapshot-id", "2");
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS, queryOptions, Collections.emptyList()));

        Table processedTable = (Table) invokePrivateMethod(node, "getProcessedTable");
        Assert.assertEquals("from-snapshot", processedTable.options().get("scan.mode"));
        Assert.assertEquals("2", processedTable.options().get("scan.snapshot-id"));
        Assert.assertEquals("latest", baseTable.options().get("scan.mode"));
        Assert.assertFalse(baseTable.options().containsKey("scan.snapshot-id"));
    }

    @Test
    public void testDataTableOptionsUseRelationScopedCatalogHandle() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        Table statementSnapshotTable = Mockito.mock(Table.class);
        Table relationScopedTable = Mockito.mock(Table.class);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(statementSnapshotTable);
        node.setSource(source);

        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.snapshot-id", "1"),
                Collections.emptyList());
        node.setScanParams(scanParams);
        Mockito.when(source.getPaimonTable(scanParams)).thenReturn(relationScopedTable);

        Assert.assertSame(relationScopedTable, invokePrivateMethod(node, "getProcessedTable"));
        Mockito.verify(source).getPaimonTable(scanParams);
        Mockito.verify(statementSnapshotTable, Mockito.never()).copy(ArgumentMatchers.anyMap());
    }

    @Test
    public void testFileColumnPositionsUseProcessedHistoricalSchema() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.snapshot-id", "1"),
                Collections.emptyList()));
        Table historicalTable = Mockito.mock(Table.class);
        Mockito.when(historicalTable.rowType()).thenReturn(new org.apache.paimon.types.RowType(Arrays.asList(
                new DataField(0, "id", new IntType()),
                new DataField(1, "old_name", new org.apache.paimon.types.VarCharType()))));
        setField(PaimonScanNode.class, node, "processedTable", historicalTable);

        Assert.assertEquals(Arrays.asList("id", "old_name"), node.getFileColumnNames());
    }

    @Test
    public void testLatestScanUsesRefreshedDescriptorColumnPositions() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        Column latestColumn = Mockito.mock(Column.class);
        Mockito.when(latestColumn.getName()).thenReturn("renamed_name");
        Mockito.when(node.getTupleDesc().getTable().getFullSchema())
                .thenReturn(Collections.singletonList(latestColumn));

        Table staleTableHandle = Mockito.mock(Table.class);
        setField(PaimonScanNode.class, node, "processedTable", staleTableHandle);

        Assert.assertEquals(Collections.singletonList("renamed_name"), node.getFileColumnNames());
    }

    @Test
    public void testDataTableQueryOptionsReplaceInheritedSnapshotSelector() throws Exception {
        Map<String, String> defaultOptions = new HashMap<>();
        defaultOptions.put("scan.snapshot-id", "9");
        TableSchema schema = new TableSchema(
                0,
                Collections.singletonList(new DataField(0, "id", new IntType())),
                0,
                Collections.emptyList(),
                Collections.emptyList(),
                defaultOptions,
                null);
        Table pinnedLatestTable = new AppendOnlyFileStoreTable(
                Mockito.mock(FileIO.class),
                new Path("memory://paimon_dynamic_tag"),
                schema,
                CatalogEnvironment.empty());

        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getExternalTable()).thenReturn(Mockito.mock(PaimonExternalTable.class));
        Mockito.when(source.getPaimonTable()).thenReturn(pinnedLatestTable);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class)))
                .thenAnswer(invocation -> PaimonScanParams.applyOptions(
                        pinnedLatestTable, invocation.<TableScanParams>getArgument(0).getMapParams()));
        node.setSource(source);
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of("scan.tag-name", "tag1"),
                Collections.emptyList()));

        Table processedTable = (Table) invokePrivateMethod(node, "getProcessedTable");
        Assert.assertEquals("tag1", processedTable.options().get("scan.tag-name"));
        Assert.assertFalse(processedTable.options().containsKey("scan.snapshot-id"));
    }

    @Test
    public void testBackendSerializationUsesDynamicOptionsTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(systemTable.getSysTableType()).thenReturn("table_indexes");
        Table baseTable = Mockito.mock(Table.class);
        Table copiedTable = Mockito.mock(Table.class, Mockito.withSettings().serializable());
        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(baseTable);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class)))
                .thenAnswer(invocation -> PaimonScanParams.applyOptions(
                        baseTable, invocation.<TableScanParams>getArgument(0).getMapParams()));
        // The invocation happens on the deserialized mock copy, so Mockito cannot record it
        // against this test instance when checking strict stubbings.
        Mockito.lenient().when(copiedTable.name()).thenReturn("files-at-snapshot");
        node.setSource(source);

        Map<String, String> options = ImmutableMap.of("scan.snapshot-id", "1");
        node.setScanParams(new TableScanParams(
                TableScanParams.OPTIONS, options, Collections.emptyList()));
        Mockito.when(baseTable.copy(ArgumentMatchers.anyMap())).thenReturn(copiedTable);

        try {
            invokePrivateMethod(node, "serializeProcessedTable");
        } catch (NoSuchMethodException e) {
            Assert.fail("PaimonScanNode must serialize the processed table for backend JNI reads");
        }

        java.lang.reflect.Field field = PaimonScanNode.class.getDeclaredField("serializedTable");
        field.setAccessible(true);
        String encoded = (String) field.get(node);
        Table decoded = InstantiationUtil.deserializeObject(
                Base64.getUrlDecoder().decode(encoded), PaimonUtil.class.getClassLoader());
        Assert.assertEquals("files-at-snapshot", decoded.name());
    }

    @Test
    public void testSystemTableRejectsNonIncrementalScanParams() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getExternalTable()).thenReturn(Mockito.mock(PaimonSysExternalTable.class));
        Mockito.when(source.getPaimonTable()).thenReturn(Mockito.mock(Table.class));
        node.setSource(source);
        node.setScanParams(new TableScanParams(
                TableScanParams.BRANCH,
                Collections.singletonMap(TableScanParams.PARAMS_NAME, "branch1"),
                Collections.emptyList()));

        try {
            invokePrivateMethod(node, "getProcessedTable");
            Assert.fail("Paimon system table should reject non-incremental scan params");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getTargetException().getMessage()
                    .contains("only support INCR or OPTIONS scan params"));
        }
    }

    @Test
    public void testDetermineTargetFileSplitSizeHonorsMaxFileSplitNum() throws Exception {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                false, sv, ScanContext.EMPTY);

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

        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                false, sv, ScanContext.EMPTY);
        node.setSource(source);

        Map<String, String> backendOptions = node.getBackendPaimonOptions();
        Assert.assertEquals("org.postgresql.Driver", backendOptions.get("jdbc.driver_class"));
        Assert.assertEquals(driverUrl, backendOptions.get("jdbc.driver_url"));
        Assert.assertEquals(2, backendOptions.size());
    }

    @Test
    public void testGetBackendPaimonOptionsForJniIOManager() {
        Map<String, String> props = new HashMap<>();
        props.put("paimon.jni.enable_jni_io_manager", "true");
        props.put("paimon.jni.io_manager.tmp_dir", "/tmp/doris-paimon");
        props.put("paimon.jni.io_manager.impl_class", "org.example.CustomIOManager");

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
        Assert.assertEquals("true", backendOptions.get("jni.enable_jni_io_manager"));
        Assert.assertEquals("/tmp/doris-paimon", backendOptions.get("jni.io_manager.tmp_dir"));
        Assert.assertEquals("org.example.CustomIOManager",
                backendOptions.get("jni.io_manager.impl_class"));
        Assert.assertEquals(3, backendOptions.size());
    }

    @Test
    public void testApplyBackendPaimonOptionsAtScanNodeLevel() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
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
        PaimonSplit jniSplit = new PaimonSplit(createDataSplit("scan_level.parquet"));
        Assert.assertNotNull(jniSplit.getPartitionValues());
        Assert.assertTrue(jniSplit.getPartitionValues().isEmpty());
        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                rangeDesc, jniSplit);
        Assert.assertFalse(rangeDesc.getTableFormatParams().getPaimonParams().isSetPaimonOptions());
    }

    @Test
    public void testSetPartitionValuesBuildsAlignedMetadata() {
        PaimonScanNode node = new PaimonScanNode(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)),
                false, sv, ScanContext.EMPTY);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table paimonTable = Mockito.mock(Table.class);
        Mockito.when(source.getPaimonTable()).thenReturn(paimonTable);
        Mockito.when(paimonTable.partitionKeys()).thenReturn(Arrays.asList("region", "dt"));
        node.setSource(source);

        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("dt", null);
        partitionValues.put("region", "cn");
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        node.setPartitionValues(rangeDesc, partitionValues);

        Assert.assertEquals(Arrays.asList("region", "dt"), rangeDesc.getColumnsFromPathKeys());
        Assert.assertEquals(Arrays.asList("cn", ""), rangeDesc.getColumnsFromPath());
        Assert.assertEquals(Arrays.asList(false, true), rangeDesc.getColumnsFromPathIsNull());
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
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        Mockito.when(sysTable.isDataTable()).thenReturn(true);
        Mockito.when(table.partitionKeys()).thenReturn(Arrays.asList("Pt", "Dt"));
        node.setSource(source);

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        rangeDesc.setColumnsFromPathKeys(Collections.singletonList("stale"));
        rangeDesc.setColumnsFromPath(Collections.singletonList("old"));
        rangeDesc.setColumnsFromPathIsNull(Collections.singletonList(false));
        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("Dt", null);
        partitionValues.put("Pt", "p1");
        PaimonSplit split = new PaimonSplit(createDataSplit("ordered.parquet"));
        split.setPaimonPartitionValues(partitionValues);

        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class}, rangeDesc, split);

        Assert.assertEquals(Arrays.asList("Pt", "Dt"), rangeDesc.getColumnsFromPathKeys());
        Assert.assertEquals(Arrays.asList("p1", ""), rangeDesc.getColumnsFromPath());
        Assert.assertEquals(Arrays.asList(false, true), rangeDesc.getColumnsFromPathIsNull());
    }

    @Test
    public void testNativeSplitCarriesPartitionMetadataWithoutRuntimeFilterPruning() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonScanNode spyNode = Mockito.spy(node);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table table = Mockito.mock(Table.class);
        PaimonSysExternalTable externalTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(source.getPaimonTable()).thenReturn(table);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(table.partitionKeys()).thenReturn(Collections.singletonList("par"));
        Mockito.when(table.rowType()).thenReturn(DataTypes.ROW(
                DataTypes.FIELD(0, "par", DataTypes.INT())));
        Mockito.when(externalTable.isDataTable()).thenReturn(true);
        spyNode.setSource(source);

        Mockito.doReturn(Collections.singletonList(createDataSplit("partitioned.parquet")))
                .when(spyNode).getPaimonSplitFromAPI();
        mockNativeReader(spyNode);
        setField(FileQueryScanNode.class, spyNode, "fileSplitter",
                new FileSplitter(32L * 1024 * 1024, 64L * 1024 * 1024, 0));
        setField(PaimonScanNode.class, spyNode, "storagePropertiesMap", Collections.emptyMap());
        Mockito.when(sv.isForceJniScanner()).thenReturn(false);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");
        Mockito.when(sv.getMaxInitialSplitSize()).thenReturn(32L * 1024 * 1024);
        Mockito.when(sv.getMaxSplitSize()).thenReturn(64L * 1024 * 1024);
        Mockito.when(sv.getTimeZone()).thenReturn("UTC");

        List<org.apache.doris.spi.Split> splits = spyNode.getSplits(1);

        Assert.assertEquals(1, splits.size());
        PaimonSplit split = (PaimonSplit) splits.get(0);
        Assert.assertEquals(Collections.singletonMap("par", "1"),
                split.getPaimonPartitionValues());
        Assert.assertEquals(Collections.emptyList(), split.getPartitionValues());
    }

    @Test
    public void testSetPaimonParamsUsesJniForDataSplit() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table paimonTable = mockPaimonTableWithPartitionKeys(Collections.emptyList());
        Mockito.when(source.getPaimonTable()).thenReturn(paimonTable);
        node.setSource(source);

        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        invokePrivateMethod(node, "setPaimonParams",
                new Class<?>[] {TFileRangeDesc.class, PaimonSplit.class},
                rangeDesc, new PaimonSplit(createDataSplit("jni-only.parquet")));

        Assert.assertEquals(TPaimonReaderType.PAIMON_JNI,
                rangeDesc.getTableFormatParams().getPaimonParams().getReaderType());
        Assert.assertTrue(rangeDesc.getTableFormatParams().getPaimonParams().isSetPaimonSplit());
    }

    @Test
    public void testGetFieldIndexMatchesMixedCaseColumns() {
        List<String> fieldNames = Arrays.asList("data", "mIxEd_COL", "PART");

        Assert.assertEquals(1, PaimonScanNode.getFieldIndex(fieldNames, "mixed_col"));
        Assert.assertEquals(2, PaimonScanNode.getFieldIndex(fieldNames, "part"));
        Assert.assertEquals(-1, PaimonScanNode.getFieldIndex(fieldNames, "missing_col"));
    }

    @Test
    public void testSystemTableForBackendFollowsFeWrapperSchemaGeneration() throws Exception {
        // $audit_log / $binlog / $ro derive their row type from the base table schema. The FE plans
        // on the wrapper PaimonSysExternalTable holds; rebuilding the table serialized to the BE
        // over a *different* generation of that base table (the meta cache keeps one for up to 24h,
        // the catalog its own for 30 min) makes BE reject the query in
        // PaimonJniScanner#getProjected - "RequiredField c2 not found in schema" - or read a stale
        // type under the same name. Both wrappers must come from one generation.
        FileStoreTable planned = newPaimonTable(catalogEnvironment(false, Mockito.mock(Catalog.class)),
                new DataField(0, "c1", DataTypes.INT()), new DataField(1, "c2", DataTypes.INT()));
        FileStoreTable staleGeneration = newPaimonTable(catalogEnvironment(false, Mockito.mock(Catalog.class)),
                new DataField(0, "c1", DataTypes.INT()));
        Table feWrapper = SystemTableLoader.load("audit_log", planned);

        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(sysTable.getSysTableType()).thenReturn("audit_log");
        Mockito.when(sysTable.getSysBaseTable()).thenReturn(planned);
        PaimonExternalTable sourceTable = Mockito.mock(PaimonExternalTable.class);
        Mockito.lenient().when(sourceTable.getPaimonTable(Optional.empty())).thenReturn(staleGeneration);
        Mockito.lenient().when(sysTable.getSourceTable()).thenReturn(sourceTable);

        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getPaimonTable()).thenReturn(feWrapper);
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        PaimonScanNode node = newTestNode(new PlanNodeId(1), new TupleId(3), sv);
        node.setSource(source);

        Table forBackend = invokeGetPaimonTableForBackend(node);
        Assert.assertEquals(feWrapper.rowType(), forBackend.rowType());
        Assert.assertTrue(forBackend.rowType().getFieldNames().contains("c2"));
    }

    @Test
    public void testDropCatalogLoaderKeepsEverythingButTheLoader() throws Exception {
        FileStoreTable table = newPaimonTable(catalogEnvironment(false, Mockito.mock(Catalog.class)),
                new DataField(0, "c1", DataTypes.INT()));

        FileStoreTable catalogLess = invokeDropCatalogLoader(table);

        // The loader is the only thing the BE must not deserialize: it drags the whole Hive /
        // metastore stack onto the BE classpath.
        Assert.assertNull(catalogLess.catalogEnvironment().catalogLoader());
        Assert.assertEquals(table.rowType(), catalogLess.rowType());
        Assert.assertEquals(table.location(), catalogLess.location());
        Assert.assertEquals(table.schema().id(), catalogLess.schema().id());
    }

    @Test
    public void testPinsCatalogVisibleSnapshotForVersionManagedCatalog() throws Exception {
        // A version-managed (REST / DLF REST) catalog owns the committed snapshot pointer. Without
        // the catalog loader the BE resolves "latest" by listing the snapshot directory, and can
        // plan on a snapshot the catalog has not published yet (or one a rollback left behind)
        // while the FE planned on the previous one. Pin what the catalog sees.
        FileStoreTable versionManaged = Mockito.spy(newPaimonTable(
                catalogEnvironment(true, Mockito.mock(Catalog.class)),
                new DataField(0, "c1", DataTypes.INT())));
        SnapshotManager snapshotManager = Mockito.mock(SnapshotManager.class);
        Mockito.when(snapshotManager.latestSnapshotId()).thenReturn(42L);
        Mockito.doReturn(snapshotManager).when(versionManaged).snapshotManager();

        FileStoreTable pinned = invokePinCatalogSnapshot(invokeDropCatalogLoader(versionManaged), versionManaged);
        Assert.assertEquals("42", pinned.options().get("scan.snapshot-id"));

        // And the table actually handed to the BE must go through that pin: $files is the system
        // table that re-plans there.
        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(sysTable.getSysTableType()).thenReturn("files");
        Mockito.when(sysTable.getSysBaseTable()).thenReturn(versionManaged);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getPaimonTable()).thenReturn(SystemTableLoader.load("files", versionManaged));
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        PaimonScanNode node = newTestNode(new PlanNodeId(1), new TupleId(3), sv);
        node.setSource(source);
        invokeGetPaimonTableForBackend(node);
        Mockito.verify(snapshotManager, Mockito.atLeastOnce()).latestSnapshotId();

        // A catalog without version management keeps filesystem semantics, so there is nothing to
        // pin and the BE must stay on "latest".
        FileStoreTable plain = newPaimonTable(catalogEnvironment(false, Mockito.mock(Catalog.class)),
                new DataField(0, "c1", DataTypes.INT()));
        Assert.assertNull(invokePinCatalogSnapshot(invokeDropCatalogLoader(plain), plain)
                .options().get("scan.snapshot-id"));
    }

    @Test
    public void testDeferredScanIsAuthorizedBeforeDroppingTheCatalogLoader() throws Exception {
        // $files only plans partition-level splits on the FE and re-plans the base table on the BE,
        // where Catalog#authTableQuery is what enforces query.auth. A loader-less table turns that
        // check into a no-op, so a denied query would come back as a successful metadata read:
        // authorize here instead, while the loader is still around.
        Catalog catalog = Mockito.mock(Catalog.class);
        Mockito.when(catalog.authTableQuery(ArgumentMatchers.any(), ArgumentMatchers.any()))
                .thenThrow(new RuntimeException("no privilege for db.tbl"));
        Map<String, String> options = new HashMap<>();
        options.put("query-auth.enabled", "true");
        FileStoreTable authorized = newPaimonTable(catalogEnvironment(true, catalog), options,
                new DataField(0, "c1", DataTypes.INT()));

        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(sysTable.getSysTableType()).thenReturn("files");
        Mockito.when(sysTable.getSysBaseTable()).thenReturn(authorized);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getPaimonTable()).thenReturn(SystemTableLoader.load("files", authorized));
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        PaimonScanNode node = newTestNode(new PlanNodeId(1), new TupleId(3), sv);
        node.setSource(source);

        try {
            invokeGetPaimonTableForBackend(node);
            Assert.fail("denied query must not reach the BE as a catalog-less table");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getCause().getMessage().contains("no privilege for db.tbl"));
        }
    }

    @Test
    public void testDataSystemTablesKeepTheirOwnProjectedAuthorization() throws Exception {
        // $ro / $row_tracking / $audit_log / $binlog keep planning on the FE through
        // DataTableBatchScan, which already calls Catalog#authTableQuery with the slot projection
        // (AbstractDataTableScan#authQuery -> auth(readType.getFieldNames())). A second auth(null)
        // here means "every column", so a user allowed to read only c1 would be rejected for
        // "SELECT c1 FROM tbl$ro". Only $files, which loses its authorization by re-planning on the
        // BE, may transfer it.
        Catalog catalog = Mockito.mock(Catalog.class);
        Map<String, String> options = new HashMap<>();
        options.put("query-auth.enabled", "true");
        FileStoreTable dataTable = newPaimonTable(catalogEnvironment(true, catalog), options,
                new DataField(0, "c1", DataTypes.INT()));

        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(sysTable.getSysTableType()).thenReturn("ro");
        Mockito.when(sysTable.getSysBaseTable()).thenReturn(dataTable);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getPaimonTable()).thenReturn(SystemTableLoader.load("ro", dataTable));
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        PaimonScanNode node = newTestNode(new PlanNodeId(1), new TupleId(3), sv);
        node.setSource(source);

        invokeGetPaimonTableForBackend(node);

        Mockito.verify(catalog, Mockito.never())
                .authTableQuery(ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    @Test
    public void testPinIsKeptOffSystemTablesWhoseRowTypeFollowsTheBaseTable() throws Exception {
        // PaimonJniScanner#initTable calls table.copy(table.options()) unconditionally, and every
        // Paimon system-table wrapper delegates copy to FileStoreTable#copy, which time-travels the
        // schema to scan.snapshot-id. $audit_log / $ro / $binlog / $row_tracking derive their row
        // type from the base table, so pinning them would rewind the BE schema: c2, added after the
        // latest snapshot, is planned by the FE and then rejected by getProjected() with
        // "RequiredField c2 not found in schema". Only the fixed-row-type $files / $partitions,
        // which re-plan on the BE, get the pin.
        FileStoreTable versionManaged = Mockito.spy(newPaimonTable(
                catalogEnvironment(true, Mockito.mock(Catalog.class)),
                new DataField(0, "c1", DataTypes.INT()), new DataField(1, "c2", DataTypes.INT())));
        SnapshotManager snapshotManager = Mockito.mock(SnapshotManager.class);
        Mockito.lenient().when(snapshotManager.latestSnapshotId()).thenReturn(42L);
        Mockito.lenient().doReturn(snapshotManager).when(versionManaged).snapshotManager();

        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(sysTable.getSysTableType()).thenReturn("audit_log");
        Mockito.when(sysTable.getSysBaseTable()).thenReturn(versionManaged);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getPaimonTable()).thenReturn(SystemTableLoader.load("audit_log", versionManaged));
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        PaimonScanNode node = newTestNode(new PlanNodeId(1), new TupleId(3), sv);
        node.setSource(source);

        Table forBackend = invokeGetPaimonTableForBackend(node);

        Assert.assertNull(forBackend.options().get("scan.snapshot-id"));
        Assert.assertTrue(forBackend.rowType().getFieldNames().contains("c2"));
        Mockito.verify(snapshotManager, Mockito.never()).latestSnapshotId();
    }

    @Test
    public void testPinsCatalogVisibleSnapshotForMetadataTablesThatTravelOnBackend() throws Exception {
        // $manifests / $table_indexes / $statistics send a stateless marker split from the FE and
        // pick their snapshot inside the BE reader (ManifestsTable / TableIndexesTable ->
        // TimeTravelUtil#tryTravelOrLatest, StatisticTable -> AbstractFileStoreTable#statistics ->
        // the same helper). Without the catalog loader that resolves to whatever the snapshot
        // directory holds, so a version-managed catalog would expose an unpublished snapshot inside
        // the publication window, or a rollback-orphaned one. They honor scan.snapshot-id and their
        // row type is fixed, so pin them like $files / $partitions.
        for (String sysTableType : Arrays.asList("manifests", "table_indexes", "statistics")) {
            FileStoreTable versionManaged = Mockito.spy(newPaimonTable(
                    catalogEnvironment(true, Mockito.mock(Catalog.class)),
                    new DataField(0, "c1", DataTypes.INT())));
            SnapshotManager snapshotManager = Mockito.mock(SnapshotManager.class);
            Mockito.when(snapshotManager.latestSnapshotId()).thenReturn(42L);
            Mockito.doReturn(snapshotManager).when(versionManaged).snapshotManager();

            PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
            Mockito.when(sysTable.getSysTableType()).thenReturn(sysTableType);
            Mockito.when(sysTable.getSysBaseTable()).thenReturn(versionManaged);
            PaimonSource source = Mockito.mock(PaimonSource.class);
            Mockito.when(source.getPaimonTable())
                    .thenReturn(SystemTableLoader.load(sysTableType, versionManaged));
            Mockito.when(source.getExternalTable()).thenReturn(sysTable);
            PaimonScanNode node = newTestNode(new PlanNodeId(1), new TupleId(3), sv);
            node.setSource(source);

            invokeGetPaimonTableForBackend(node);

            Mockito.verify(snapshotManager, Mockito.atLeastOnce()).latestSnapshotId();
        }
    }

    @Test
    public void testRelationOptionsSurviveTheRebuiltSystemTable() throws Exception {
        // The FE applies @options to the wrapper the meta cache holds (PaimonSysExternalTable
        // #getSysPaimonTable(scanParams) -> PaimonScanParams#applyOptions), but the BE is handed a
        // wrapper rebuilt over a catalog-less base - a different object, on which that copy() never
        // ran. So the options have to be applied again on the way out, otherwise the reader that
        // materializes this table's splits would silently fall back to the unpinned latest state.
        FileStoreTable dataTable = newPaimonTable(catalogEnvironment(false, Mockito.mock(Catalog.class)),
                new DataField(0, "c1", DataTypes.INT()));

        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(sysTable.getSysTableType()).thenReturn("ro");
        Mockito.when(sysTable.getSysBaseTable()).thenReturn(dataTable);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table planned = SystemTableLoader.load("ro", dataTable);
        Mockito.when(source.getPaimonTable()).thenReturn(planned);
        Mockito.when(source.getPaimonTable(ArgumentMatchers.any(TableScanParams.class))).thenReturn(planned);
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        PaimonScanNode node = newTestNode(new PlanNodeId(1), new TupleId(3), sv);
        node.setSource(source);
        TableScanParams scanParams = new TableScanParams(
                TableScanParams.OPTIONS,
                ImmutableMap.of(CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), "4"),
                Collections.emptyList());
        // Binding already resolved these; this node only has to carry that decision to the BE.
        scanParams.getOrResolveMapParams(options -> options);
        node.setScanParams(scanParams);

        Table forBackend = invokeGetPaimonTableForBackend(node);

        // $ro delegates options() to the data table it wraps, so this reads the rebuilt base.
        Assert.assertEquals("4", forBackend.options().get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()));
    }

    @Test
    public void testFallbackBranchKeepsTheGenerationTheFeCaptured() throws Exception {
        // FallbackReadFileStoreTable#schema() only exposes its main branch, so rebuilding it through
        // FileStoreTableFactory#create re-reads the fallback branch from
        // SchemaManager(fallbackBranch).latest() instead of the object the FE planned with. After
        // external DDL publishes a new generation the BE would then get main M1 + fallback F2 and
        // die in FallbackReadFileStoreTable#validateSchema. Rebuild both branches from the captured
        // schemas instead.
        FileStoreTable[] captured = newFallbackBranchPairWithNewerGenerationOnDisk("doris_paimon_fallback_ut");

        FileStoreTable forBackend =
                invokeDropCatalogLoader(new FallbackReadFileStoreTable(captured[0], captured[1]));

        assertFallbackPairMatchesTheFeGeneration(forBackend, captured[0], captured[1]);
    }

    @Test
    public void testFallbackBranchSurvivesAPaimonTableDecorator() throws Exception {
        // With file based privileges enabled PrivilegedCatalog#getTable hands out
        // Privileged(FallbackRead(..)). A direct instanceof looks straight past that wrapper and
        // rebuilds an ordinary table from the delegated main branch, so the BE loses the
        // FallbackReadFileStoreTable.Read that dispatches a fallback split to the fallback branch -
        // while the FE keeps planning the wrapper and can still emit one. Peel decorators first.
        FileStoreTable[] captured = newFallbackBranchPairWithNewerGenerationOnDisk("doris_paimon_privileged_ut");
        FileStoreTable decorated = PrivilegedFileStoreTable.wrap(
                new FallbackReadFileStoreTable(captured[0], captured[1]),
                new AllGrantedPrivilegeChecker(), Identifier.create("db", "tbl"));

        FileStoreTable forBackend = invokeDropCatalogLoader(decorated);

        assertFallbackPairMatchesTheFeGeneration(forBackend, captured[0], captured[1]);
    }

    @Test
    public void testFallbackBranchIsAuthorizedBeforeDroppingTheCatalogLoader() throws Exception {
        // FallbackReadFileStoreTable#newScan builds a FallbackReadScan over both branches' own
        // scans, so each authorizes itself, and FileStoreTableFactory#create gives the fallback
        // branch a CatalogEnvironment of its own carrying a branch-qualified Identifier. The pair
        // delegates catalogEnvironment() to its main branch, so authorizing the pair checks main and
        // silently skips the fallback branch - and once the loaders are dropped that missing check
        // is a permanent allow, letting a user denied on the fallback branch read the fallback rows
        // of $files.
        Catalog catalog = Mockito.mock(Catalog.class);
        Identifier mainIdentifier = Identifier.create("db", "tbl");
        Identifier fallbackIdentifier = new Identifier("db", "tbl", "fb");
        Map<String, String> mainOptions = new HashMap<>();
        mainOptions.put("query-auth.enabled", "true");
        Map<String, String> fallbackOptions = new HashMap<>(mainOptions);
        fallbackOptions.put("branch", "fb");
        FileStoreTable main = newPaimonTable(
                new CatalogEnvironment(mainIdentifier, null, () -> catalog, null, null, true),
                mainOptions, new DataField(0, "c1", DataTypes.INT()));
        FileStoreTable fallback = newPaimonTable(
                new CatalogEnvironment(fallbackIdentifier, null, () -> catalog, null, null, true),
                fallbackOptions, new DataField(0, "c1", DataTypes.INT()));

        invokeAuthorizeDeferredScan(new FallbackReadFileStoreTable(main, fallback));

        Mockito.verify(catalog).authTableQuery(ArgumentMatchers.eq(mainIdentifier), ArgumentMatchers.isNull());
        Mockito.verify(catalog).authTableQuery(ArgumentMatchers.eq(fallbackIdentifier), ArgumentMatchers.isNull());
    }

    @Test
    public void testIncrementalRangeIsResolvedOnTheCatalogVisibleSnapshot() throws Exception {
        // Paimon selects the incremental scanner from incremental-between*, so pinCatalogSnapshot's
        // scan.snapshot-id cannot bound this scan - and isolateIncrementalRead clears it anyway. The
        // timestamp form would then resolve its endpoints inside the BE reader, through
        // SnapshotManager#earlierOrEqualTimeMills, whose search runs up to latestSnapshotId(): the
        // snapshot directory once the loader is gone, so a snapshot the catalog has not published
        // yet (or one a rollback left behind) would close the range. Resolve them here instead and
        // hand the BE the explicit id range IncrementalDeltaStartingScanner#betweenTimestamps would
        // have computed from the catalog's own view.
        FileStoreTable versionManaged = Mockito.spy(newPaimonTable(
                catalogEnvironment(true, Mockito.mock(Catalog.class)),
                new DataField(0, "c1", DataTypes.INT())));
        SnapshotManager snapshotManager = Mockito.mock(SnapshotManager.class);
        Mockito.doReturn(snapshotManager).when(versionManaged).snapshotManager();
        // Catalog snapshot 42, committed at t=5000; snapshot 43 may already sit on the filesystem.
        Snapshot catalogLatest = newSnapshot(42L, 5000L);
        Snapshot earliest = newSnapshot(10L, 1000L);
        Snapshot rangeStart = newSnapshot(20L, 2000L);
        Mockito.when(snapshotManager.latestSnapshot()).thenReturn(catalogLatest);
        Mockito.when(snapshotManager.earliestSnapshot()).thenReturn(earliest);
        Mockito.when(snapshotManager.earlierOrEqualTimeMills(2000L)).thenReturn(rangeStart);

        Map<String, String> unbounded = new HashMap<>();
        unbounded.put("incremental-between-timestamp", "2000," + Long.MAX_VALUE);
        Map<String, String> bound = invokeBindIncrementalRangeToCatalog(unbounded, versionManaged);

        Assert.assertEquals("20,42", bound.get("incremental-between"));
        // Cleared rather than dropped: Paimon's copy() removes a key only when it maps to null.
        Assert.assertTrue(bound.containsKey("incremental-between-timestamp"));
        Assert.assertNull(bound.get("incremental-between-timestamp"));

        // An end older than the catalog's snapshot already resolves to the same id on both sides,
        // because every snapshot the catalog has not published yet is younger than it.
        Map<String, String> past = new HashMap<>();
        past.put("incremental-between-timestamp", "2000,4000");
        Assert.assertSame(past, invokeBindIncrementalRangeToCatalog(past, versionManaged));

        // And a catalog that does not manage versions keeps filesystem semantics on both sides.
        FileStoreTable plain = newPaimonTable(catalogEnvironment(false, Mockito.mock(Catalog.class)),
                new DataField(0, "c1", DataTypes.INT()));
        Assert.assertSame(unbounded, invokeBindIncrementalRangeToCatalog(unbounded, plain));
    }

    @Test
    public void testIncrementalRangeIsNotBoundOnAFallbackBranchPair() throws Exception {
        // The two branches keep independent snapshot id sequences, and
        // FallbackReadFileStoreTable#rewriteFallbackOptions translates only scan.snapshot-id, so
        // incremental-between reaches the fallback branch verbatim. A main-branch id range bound
        // here would be validated against the fallback branch's own range in
        // IncrementalDeltaStartingScanner#betweenSnapshotIds and either fail out of range or select
        // unrelated commits. The timestamp form is branch-agnostic and each branch resolves it
        // against its own SnapshotManager, so it has to reach the BE untouched.
        Catalog catalog = Mockito.mock(Catalog.class);
        Map<String, String> fallbackOptions = new HashMap<>();
        fallbackOptions.put("branch", "fb");
        FileStoreTable main = Mockito.spy(newPaimonTable(catalogEnvironment(true, catalog),
                new DataField(0, "c1", DataTypes.INT())));
        FileStoreTable fallback = newPaimonTable(catalogEnvironment(true, catalog), fallbackOptions,
                new DataField(0, "c1", DataTypes.INT()));
        // Stubbed so that the main branch alone would have produced a range: without the guard this
        // test would see "20,42" written into incremental-between, not an unchanged map.
        SnapshotManager mainSnapshots = Mockito.mock(SnapshotManager.class);
        Mockito.doReturn(mainSnapshots).when(main).snapshotManager();
        Snapshot catalogLatest = newSnapshot(42L, 5000L);
        Snapshot earliest = newSnapshot(10L, 1000L);
        Snapshot rangeStart = newSnapshot(20L, 2000L);
        Mockito.lenient().when(mainSnapshots.latestSnapshot()).thenReturn(catalogLatest);
        Mockito.lenient().when(mainSnapshots.earliestSnapshot()).thenReturn(earliest);
        Mockito.lenient().when(mainSnapshots.earlierOrEqualTimeMills(2000L)).thenReturn(rangeStart);

        Map<String, String> range = new HashMap<>();
        range.put("incremental-between-timestamp", "2000," + Long.MAX_VALUE);
        FileStoreTable pair = new FallbackReadFileStoreTable(main, fallback);

        Assert.assertSame(range, invokeBindIncrementalRangeToCatalog(range, pair));
        // And through the decorator PrivilegedCatalog adds, since that is what the meta cache holds.
        Assert.assertSame(range, invokeBindIncrementalRangeToCatalog(range,
                PrivilegedFileStoreTable.wrap(pair, new AllGrantedPrivilegeChecker(),
                        Identifier.create("db", "tbl"))));

        // Not merely "the output equals the input": the main branch's snapshots must never be read,
        // because reading them is what produces an id range that cannot be carried to the fallback.
        Mockito.verify(mainSnapshots, Mockito.never())
                .earlierOrEqualTimeMills(ArgumentMatchers.anyLong());
    }

    @Test
    public void testIncrementalPartitionsScanBindsItsRangeBeforeTheBackend() throws Exception {
        // $partitions is the one system table that both re-plans on the BE
        // (PartitionsRead#createReader -> newScan().listPartitionEntries()) and accepts @incr, so it
        // is the one that has to reach the BE with an already-resolved range.
        FileStoreTable versionManaged = Mockito.spy(newPaimonTable(
                catalogEnvironment(true, Mockito.mock(Catalog.class)),
                new DataField(0, "c1", DataTypes.INT())));
        SnapshotManager snapshotManager = Mockito.mock(SnapshotManager.class);
        Mockito.doReturn(snapshotManager).when(versionManaged).snapshotManager();
        Snapshot catalogLatest = newSnapshot(42L, 5000L);
        Snapshot earliest = newSnapshot(10L, 1000L);
        Snapshot rangeStart = newSnapshot(20L, 2000L);
        Mockito.when(snapshotManager.latestSnapshotId()).thenReturn(42L);
        Mockito.when(snapshotManager.latestSnapshot()).thenReturn(catalogLatest);
        Mockito.when(snapshotManager.earliestSnapshot()).thenReturn(earliest);
        Mockito.when(snapshotManager.earlierOrEqualTimeMills(2000L)).thenReturn(rangeStart);

        PaimonSysExternalTable sysTable = Mockito.mock(PaimonSysExternalTable.class);
        Mockito.when(sysTable.getSysTableType()).thenReturn("partitions");
        Mockito.when(sysTable.getSysBaseTable()).thenReturn(versionManaged);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Mockito.when(source.getPaimonTable()).thenReturn(SystemTableLoader.load("partitions", versionManaged));
        Mockito.when(source.getExternalTable()).thenReturn(sysTable);
        PaimonScanNode node = newTestNode(new PlanNodeId(1), new TupleId(3), sv);
        node.setSource(source);
        node.setScanParams(new TableScanParams(TableScanParams.INCREMENTAL_READ,
                ImmutableMap.of("startTimestamp", "2000"), Collections.emptyList()));

        invokeGetPaimonTableForBackend(node);

        // The range was closed here, on the catalog's snapshot, instead of inside the BE reader.
        Mockito.verify(snapshotManager).earlierOrEqualTimeMills(2000L);
        Mockito.verify(snapshotManager, Mockito.atLeastOnce()).latestSnapshot();
    }

    private Snapshot newSnapshot(long id, long timeMillis) {
        Snapshot snapshot = Mockito.mock(Snapshot.class);
        Mockito.lenient().when(snapshot.id()).thenReturn(id);
        Mockito.lenient().when(snapshot.timeMillis()).thenReturn(timeMillis);
        return snapshot;
    }

    /**
     * The {@code {main, fallback}} pair the FE cache captured (M1 / F1), with a newer fallback
     * generation (F2, one column wider) already published on the filesystem - so a rebuild that
     * re-reads the branch instead of reusing the captured object is visible in the row type.
     */
    private FileStoreTable[] newFallbackBranchPairWithNewerGenerationOnDisk(String prefix) throws Exception {
        java.nio.file.Path tempDir = java.nio.file.Files.createTempDirectory(prefix);
        Path tablePath = new Path("file://" + tempDir.toString() + "/db.db/tbl");
        // F2: the generation external DDL has already published on the fallback branch.
        new SchemaManager(LocalFileIO.create(), tablePath, "fb").createTable(new Schema(
                Arrays.asList(new DataField(0, "c1", DataTypes.INT()), new DataField(1, "c2", DataTypes.INT())),
                Collections.emptyList(), Collections.emptyList(), Collections.emptyMap(), ""));

        // F1 / M1: what the FE cache captured and planned this query with.
        Map<String, String> mainOptions = new HashMap<>();
        mainOptions.put("scan.fallback-branch", "fb");
        Map<String, String> fallbackOptions = new HashMap<>();
        fallbackOptions.put("branch", "fb");
        return new FileStoreTable[] {newBranchTable(tablePath, mainOptions), newBranchTable(tablePath, fallbackOptions)};
    }

    private void assertFallbackPairMatchesTheFeGeneration(FileStoreTable forBackend, FileStoreTable main,
            FileStoreTable fallback) {
        Assert.assertTrue(forBackend instanceof FallbackReadFileStoreTable);
        FallbackReadFileStoreTable fallbackForBackend = (FallbackReadFileStoreTable) forBackend;
        // The fallback branch must stay on F1 - not the c2 generation sitting on the filesystem.
        Assert.assertEquals(fallback.rowType(), fallbackForBackend.fallback().rowType());
        Assert.assertEquals(main.rowType(), fallbackForBackend.wrapped().rowType());
        // And neither branch may carry the loader that drags the metastore stack onto the BE.
        Assert.assertNull(fallbackForBackend.wrapped().catalogEnvironment().catalogLoader());
        Assert.assertNull(fallbackForBackend.fallback().catalogEnvironment().catalogLoader());
    }

    private FileStoreTable newPaimonTable(CatalogEnvironment catalogEnvironment, DataField... fields) {
        return newPaimonTable(catalogEnvironment, new HashMap<>(), fields);
    }

    private FileStoreTable newPaimonTable(CatalogEnvironment catalogEnvironment, Map<String, String> options,
            DataField... fields) {
        List<DataField> fieldList = Arrays.asList(fields);
        TableSchema schema = new TableSchema(0L, fieldList, fieldList.size() - 1, Collections.emptyList(),
                Collections.emptyList(), options, "");
        return FileStoreTableFactory.create(LocalFileIO.create(),
                new Path("file:///tmp/doris_paimon_ut/db.db/tbl"), schema, catalogEnvironment);
    }

    /**
     * One branch of a {@code scan.fallback-branch} pair, built the way Paimon builds them: without
     * re-expanding the fallback branch, so the caller can wrap the two into a
     * {@link FallbackReadFileStoreTable} itself.
     */
    private FileStoreTable newBranchTable(Path tablePath, Map<String, String> options) {
        List<DataField> fieldList = Collections.singletonList(new DataField(0, "c1", DataTypes.INT()));
        TableSchema schema = new TableSchema(0L, fieldList, 0, Collections.emptyList(),
                Collections.emptyList(), options, "");
        return FileStoreTableFactory.createWithoutFallbackBranch(LocalFileIO.create(), tablePath, schema,
                new Options(), catalogEnvironment(false, Mockito.mock(Catalog.class)));
    }

    private CatalogEnvironment catalogEnvironment(boolean supportsVersionManagement, Catalog catalog) {
        return new CatalogEnvironment(Identifier.create("db", "tbl"), null, () -> catalog, null, null,
                supportsVersionManagement);
    }

    private Table invokeGetPaimonTableForBackend(PaimonScanNode node) throws Exception {
        Method method = PaimonScanNode.class.getDeclaredMethod("getPaimonTableForBackend");
        method.setAccessible(true);
        return (Table) method.invoke(node);
    }

    private FileStoreTable invokeDropCatalogLoader(FileStoreTable table) throws Exception {
        Method method = PaimonScanNode.class.getDeclaredMethod("dropCatalogLoader", FileStoreTable.class);
        method.setAccessible(true);
        return (FileStoreTable) method.invoke(null, table);
    }

    private void invokeAuthorizeDeferredScan(FileStoreTable table) throws Exception {
        Method method = PaimonScanNode.class.getDeclaredMethod("authorizeDeferredScan", FileStoreTable.class);
        method.setAccessible(true);
        method.invoke(null, table);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> invokeBindIncrementalRangeToCatalog(Map<String, String> incrementalParams,
            FileStoreTable dataTable) throws Exception {
        Method method = PaimonScanNode.class.getDeclaredMethod("bindIncrementalRangeToCatalog", Map.class,
                FileStoreTable.class);
        method.setAccessible(true);
        return (Map<String, String>) method.invoke(null, incrementalParams, dataTable);
    }

    private FileStoreTable invokePinCatalogSnapshot(FileStoreTable catalogLessTable, FileStoreTable dataTable)
            throws Exception {
        Method method = PaimonScanNode.class.getDeclaredMethod("pinCatalogSnapshot", FileStoreTable.class,
                FileStoreTable.class);
        method.setAccessible(true);
        return (FileStoreTable) method.invoke(null, catalogLessTable, dataTable);
    }

    private void mockJniReader(PaimonScanNode spyNode) {
        Mockito.doReturn(false).when(spyNode).supportNativeReader(ArgumentMatchers.any(Optional.class));
    }

    private PaimonScanNode newTestNode(PlanNodeId planNodeId, TupleId tupleId, SessionVariable sessionVariable) {
        TupleDescriptor desc = new TupleDescriptor(tupleId);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        Table paimonTable = mockPaimonTableWithPartitionKeys(Collections.emptyList());
        Mockito.when(externalTable.getPaimonTable(ArgumentMatchers.any(Optional.class))).thenReturn(paimonTable);
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

    private void mockNativeReader(PaimonScanNode spyNode) {
        Mockito.doReturn(true).when(spyNode).supportNativeReader(ArgumentMatchers.any(Optional.class));
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
