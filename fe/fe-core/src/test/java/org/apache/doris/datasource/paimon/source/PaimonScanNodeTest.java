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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.FileQueryScanNode;
import org.apache.doris.datasource.FileSplitter;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.datasource.paimon.PaimonFileExternalCatalog;
import org.apache.doris.datasource.paimon.PaimonMvccSnapshot;
import org.apache.doris.datasource.paimon.PaimonPartitionInfo;
import org.apache.doris.datasource.paimon.PaimonReaderOptions;
import org.apache.doris.datasource.paimon.PaimonScanParams;
import org.apache.doris.datasource.paimon.PaimonSnapshot;
import org.apache.doris.datasource.paimon.PaimonSnapshotCacheValue;
import org.apache.doris.datasource.paimon.PaimonSysExternalTable;
import org.apache.doris.datasource.paimon.PaimonUtil;
import org.apache.doris.datasource.paimon.PaimonUtils;
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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.CatalogEnvironment;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InstantiationUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
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
import java.util.OptionalLong;

@RunWith(MockitoJUnitRunner.class)
public class PaimonScanNodeTest {
    private boolean originalEnableVariantV2;

    @Mock
    private SessionVariable sv;

    @Mock
    private PaimonFileExternalCatalog paimonFileExternalCatalog;

    @Before
    public void saveVariantV2Config() {
        originalEnableVariantV2 = Config.enable_variant_v2;
    }

    @After
    public void restoreVariantV2Config() {
        Config.enable_variant_v2 = originalEnableVariantV2;
    }

    @Test
    public void testVariantProjectionRequiresVariantV2Recursively() throws UserException {
        List<Type> variantTypes = Arrays.asList(
                VariantType.COMPUTE_V2_INSTANCE,
                new ArrayType(VariantType.COMPUTE_V2_INSTANCE),
                new MapType(Type.STRING, VariantType.COMPUTE_V2_INSTANCE),
                new StructType(new StructField("payload", VariantType.COMPUTE_V2_INSTANCE)));

        for (Type variantType : variantTypes) {
            assertVariantProjectionRequiresVariantV2(variantType);
        }
    }

    private void assertVariantProjectionRequiresVariantV2(Type variantType) throws UserException {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        SlotDescriptor slot = new SlotDescriptor(new SlotId(0), desc);
        slot.setColumn(new Column("payload", variantType));
        desc.addSlot(slot);

        Config.enable_variant_v2 = false;
        ExceptionChecker.expectThrowsWithMsg(UserException.class,
                "Paimon VARIANT columns require FE config enable_variant_v2=true",
                () -> PaimonScanNode.checkVariantV2Enabled(desc));
        Config.enable_variant_v2 = true;
        PaimonScanNode.checkVariantV2Enabled(desc);
    }

    @Test
    public void testSerializedTableCacheKeyIsStablePerScanNode() {
        PaimonScanNode first = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonScanNode second = newTestNode(new PlanNodeId(1), new TupleId(1), sv);

        String firstKey = first.getSerializedTableCacheKey().orElse("");
        Assert.assertFalse(firstKey.isEmpty());
        Assert.assertEquals(firstKey, first.getSerializedTableCacheKey().orElse(""));
        Assert.assertNotEquals(firstKey, second.getSerializedTableCacheKey().orElse(""));
    }

    @Test
    public void testRegularScanDoesNotComputeMergedRowCount() throws UserException {
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(1), new TupleId(3), sv));
        node.setSource(mockPaimonSourceWithPartitionKeys(Collections.emptyList()));
        DataSplit dataSplit = Mockito.spy(createDataSplit("regular.parquet"));
        Mockito.doReturn(Collections.singletonList(dataSplit)).when(node).getPaimonSplitFromAPI();
        Mockito.when(sv.isForceJniScanner()).thenReturn(true);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");

        List<org.apache.doris.spi.Split> splits = node.getSplits(1);

        Assert.assertEquals(1, splits.size());
        Mockito.verify(dataSplit, Mockito.never()).mergedRowCount();
    }

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
    public void testNonCountScanDoesNotComputeMergedRowCount() throws UserException {
        PaimonScanNode node = Mockito.spy(newTestNode(new PlanNodeId(1), new TupleId(3), sv));
        node.setSource(mockPaimonSourceWithPartitionKeys(Collections.<String>emptyList()));
        DataSplit dataSplit = mockCountDataSplit("ordinary.parquet", 1_000);
        Mockito.clearInvocations(dataSplit);
        Mockito.doReturn(Collections.singletonList(dataSplit)).when(node).getPaimonSplitFromAPI();
        Mockito.when(sv.isForceJniScanner()).thenReturn(true);
        Mockito.when(sv.getIgnoreSplitType()).thenReturn("NONE");

        List<org.apache.doris.spi.Split> splits = node.getSplits(1);

        Assert.assertEquals(1, splits.size());
        Mockito.verify(dataSplit, Mockito.never()).mergedRowCount();
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
        Mockito.when(source.getPaimonTable((TableScanParams) null)).thenReturn(baseTable);
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
        Mockito.when(copiedTable.options()).thenReturn(Collections.emptyMap());

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
        // This reader-filter test deliberately has no bound MVCC snapshot; avoid asking the
        // otherwise unstubbed external-table mock to manufacture one from scan options.
        node.setRelationSnapshot(Optional.empty());

        Assert.assertTrue(node.getPaimonSplitFromAPI().isEmpty());

        Mockito.verify(reader).withLevelFilter(ArgumentMatchers.any());
        Mockito.verify(reader).enableValueFilter();
        Mockito.verify(reader).onlyReadRealBuckets();
    }

    @Test
    public void testBoundEmptySnapshotDoesNotReadLaterCommit() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        Table liveTable = Mockito.mock(Table.class);
        node.setSource(source);
        node.setRelationSnapshot(Optional.of(new PaimonMvccSnapshot(
                new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY,
                        new PaimonSnapshot(PaimonSnapshot.INVALID_SNAPSHOT_ID, 1L, liveTable)))));

        Assert.assertTrue(node.getPaimonSplitFromAPI().isEmpty());
        Mockito.verify(liveTable, Mockito.never()).newReadBuilder();
    }

    @Test
    public void testBoundEmptyDataSnapshotStillPlansMetadataSystemTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Table paimonTable = Mockito.mock(Table.class);
        ReadBuilder readBuilder = Mockito.mock(ReadBuilder.class);
        TableScan scan = Mockito.mock(TableScan.class);
        TableScan.Plan plan = Mockito.mock(TableScan.Plan.class);
        org.apache.paimon.table.source.Split schemaSplit =
                Mockito.mock(org.apache.paimon.table.source.Split.class);

        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(paimonTable);
        Mockito.when(systemTable.getSysTableType()).thenReturn("schemas");
        Mockito.when(source.getPaimonTable((TableScanParams) null)).thenReturn(paimonTable);
        Mockito.when(paimonTable.options()).thenReturn(Collections.emptyMap());
        Mockito.when(paimonTable.rowType()).thenReturn(RowType.of());
        Mockito.when(paimonTable.newReadBuilder()).thenReturn(readBuilder);
        Mockito.when(readBuilder.withFilter(ArgumentMatchers.anyList())).thenReturn(readBuilder);
        Mockito.when(readBuilder.withProjection(ArgumentMatchers.any(int[].class))).thenReturn(readBuilder);
        Mockito.when(readBuilder.newScan()).thenReturn(scan);
        Mockito.when(scan.plan()).thenReturn(plan);
        Mockito.when(plan.splits()).thenReturn(Collections.singletonList(schemaSplit));
        node.setSource(source);
        setField(PaimonScanNode.class, node, "predicates", Collections.emptyList());
        node.setRelationSnapshot(Optional.of(new PaimonMvccSnapshot(
                new PaimonSnapshotCacheValue(PaimonPartitionInfo.EMPTY,
                        new PaimonSnapshot(PaimonSnapshot.INVALID_SNAPSHOT_ID, 1L, paimonTable)))));

        Assert.assertEquals(Collections.singletonList(schemaSplit), node.getPaimonSplitFromAPI());
        Mockito.verify(paimonTable).newReadBuilder();
    }

    @Test
    public void testSystemWrapperIsNotRecappedAfterItsHiddenSource() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonSysExternalTable systemTable = Mockito.mock(PaimonSysExternalTable.class);
        Table rawSource = Mockito.mock(Table.class);
        Table safeWrapper = Mockito.mock(Table.class);

        Mockito.when(source.getExternalTable()).thenReturn(systemTable);
        Mockito.when(source.getPaimonTable()).thenReturn(rawSource);
        Mockito.when(systemTable.getSysTableType()).thenReturn("partitions");
        Mockito.when(source.getPaimonTable((TableScanParams) null)).thenReturn(safeWrapper);
        Mockito.when(safeWrapper.options()).thenReturn(Collections.emptyMap());
        node.setSource(source);

        // The system-table factory has already normalized each hidden fallback leaf. Copying the
        // outer wrapper with one cap would broadcast it and erase a smaller sibling preference.
        Assert.assertSame(safeWrapper, invokePrivateMethod(node, "getProcessedTable"));
        Mockito.verify(safeWrapper, Mockito.never()).copy(ArgumentMatchers.anyMap());
        Mockito.verify(source).validateEffectiveSystemDataTable(null);
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
        Mockito.when(copiedTable.options()).thenReturn(options);

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
    public void testRejectsUnsafePhysicalOptionsAtFinalPlanningBoundary() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        Table unsafePhysicalTable = Mockito.mock(Table.class);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(unsafePhysicalTable);
        Mockito.when(unsafePhysicalTable.options()).thenReturn(ImmutableMap.of("read.batch-size", "0"));
        node.setSource(source);

        try {
            invokePrivateMethod(node, "getProcessedTable");
            Assert.fail("The final planning boundary must reject an effective zero batch size");
        } catch (java.lang.reflect.InvocationTargetException e) {
            Assert.assertTrue(e.getTargetException().getMessage().contains("read.batch-size"));
        }
    }

    @Test
    public void testFinalPlanningBoundaryCapsAcceptedManifestParallelism() throws Exception {
        int localCapacity = Runtime.getRuntime().availableProcessors();
        org.junit.Assume.assumeTrue(localCapacity < PaimonReaderOptions.MAX_MANIFEST_PARALLELISM);
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        FileStoreTable rawTable = Mockito.mock(FileStoreTable.class);
        FileStoreTable safeTable = Mockito.mock(FileStoreTable.class);
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(rawTable);
        Mockito.when(rawTable.options()).thenReturn(ImmutableMap.of(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), String.valueOf(localCapacity + 1)));
        Mockito.when(rawTable.copyWithoutTimeTravel(ArgumentMatchers.anyMap())).thenReturn(safeTable);
        Mockito.when(safeTable.options()).thenReturn(ImmutableMap.of(
                CoreOptions.SCAN_MANIFEST_PARALLELISM.key(), String.valueOf(localCapacity)));
        node.setSource(source);

        Assert.assertSame(safeTable, invokePrivateMethod(node, "getProcessedTable"));
        Mockito.verify(rawTable).copyWithoutTimeTravel(ArgumentMatchers.argThat(options ->
                String.valueOf(localCapacity)
                        .equals(options.get(CoreOptions.SCAN_MANIFEST_PARALLELISM.key()))));
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
        Mockito.when(relationScopedTable.options()).thenReturn(Collections.emptyMap());

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
        PaimonExternalTable externalTable = (PaimonExternalTable) node.getTupleDesc().getTable();
        // File scan metadata is resolved against the relation snapshot, so mock the snapshot-aware lookup.
        Mockito.when(externalTable.getFullSchema(Mockito.<Optional<MvccSnapshot>>any()))
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
        Mockito.when(copiedTable.options()).thenReturn(options);

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
    public void testSetPaimonParamsUsesJniWhenCppOptionEnabled() throws Exception {
        // Keep this as real session state because the JNI-only path need not read the option;
        // strict mocks should not make the test depend on whether that implementation detail is consulted.
        SessionVariable cppEnabledSession = new SessionVariable();
        cppEnabledSession.setEnablePaimonCppReader(true);
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), cppEnabledSession);
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
    public void testHistorySchemaUsesRelationPaimonTable() throws Exception {
        PaimonScanNode node = newTestNode(new PlanNodeId(0), new TupleId(0), sv);
        PaimonSource source = Mockito.mock(PaimonSource.class);
        PaimonExternalTable externalTable = Mockito.mock(PaimonExternalTable.class);
        PaimonExternalCatalog catalog = Mockito.mock(PaimonExternalCatalog.class);
        DataTable branchTable = Mockito.mock(DataTable.class, Mockito.RETURNS_DEEP_STUBS);
        TableSchema branchSchema = Mockito.mock(TableSchema.class);
        Mockito.when(branchTable.schemaManager().schema(3L)).thenReturn(branchSchema);
        Mockito.when(branchSchema.id()).thenReturn(3L);
        Mockito.when(branchSchema.fields()).thenReturn(Collections.emptyList());
        Mockito.when(source.getExternalTable()).thenReturn(externalTable);
        Mockito.when(source.getPaimonTable()).thenReturn(branchTable);
        Mockito.when(source.getCatalog()).thenReturn(catalog);
        node.setSource(source);
        setField(FileQueryScanNode.class, node, "params", new TFileScanRangeParams());

        try (MockedStatic<PaimonUtils> paimonUtils = Mockito.mockStatic(PaimonUtils.class)) {
            invokePrivateMethod(node, "putHistorySchemaInfo", new Class<?>[] {Long.class}, 3L);
            paimonUtils.verify(
                    () -> PaimonUtils.getSchemaCacheValue(externalTable, 3L), Mockito.never());
        }

        Mockito.verify(branchTable.schemaManager()).schema(3L);
        Assert.assertEquals(3L, node.getFileScanRangeParams().getHistorySchemaInfo().get(0).getSchemaId());
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
        Mockito.when(dataSplit.mergedRowCount()).thenReturn(OptionalLong.of(rowCount));
        Mockito.when(dataSplit.partition()).thenReturn(BinaryRow.singleColumn(1));
        Mockito.when(dataSplit.dataFiles()).thenReturn(Collections.singletonList(dataFileMeta));
        Mockito.when(dataSplit.convertToRawFiles()).thenReturn(Optional.empty());
        Mockito.when(dataSplit.deletionFiles()).thenReturn(Optional.empty());
        return dataSplit;
    }
}
