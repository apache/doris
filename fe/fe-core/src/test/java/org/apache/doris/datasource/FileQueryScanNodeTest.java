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

package org.apache.doris.datasource;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.FunctionGenTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.property.fileformat.ParquetFileFormatProperties;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.tablefunction.ExternalFileTableValuedFunction;
import org.apache.doris.tablefunction.FileTableValuedFunction;
import org.apache.doris.tablefunction.JdbcQueryTableValueFunction;
import org.apache.doris.thrift.TColumnCategory;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TFileScanSlotInfo;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileQueryScanNodeTest {
    private static final long MB = 1024L * 1024L;
    private static final Method UPDATE_REQUIRED_SLOTS_METHOD;

    static {
        try {
            UPDATE_REQUIRED_SLOTS_METHOD = FileQueryScanNode.class.getDeclaredMethod("updateRequiredSlots");
            UPDATE_REQUIRED_SLOTS_METHOD.setAccessible(true);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private TableIf table;

    private static class TestFileQueryScanNode extends FileQueryScanNode {
        private TableIf targetTable;

        TestFileQueryScanNode(SessionVariable sv) {
            super(new PlanNodeId(0), new TupleDescriptor(new TupleId(0)), "test", ScanContext.EMPTY, false, sv);
        }

        TupleDescriptor getTupleDescriptor() {
            return desc;
        }

        void setTargetTable(TableIf targetTable) {
            this.targetTable = targetTable;
        }

        void initSchemaParamsForTest() throws UserException {
            initSchemaParams();
        }

        @Override
        protected TFileFormatType getFileFormatType() throws UserException {
            return TFileFormatType.FORMAT_ORC;
        }

        @Override
        protected List<String> getPathPartitionKeys() throws UserException {
            return Collections.emptyList();
        }

        @Override
        protected TableIf getTargetTable() throws UserException {
            return targetTable;
        }

        @Override
        protected Map<String, String> getLocationProperties() throws UserException {
            return Collections.emptyMap();
        }
    }

    @Before
    public void setUp() {
        table = Mockito.mock(TableIf.class);
        Mockito.when(table.getName()).thenReturn("test_table");
    }

    @Test
    public void testApplyMaxFileSplitNumLimitRaisesTargetSize() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 10_000L * MB);
        Assert.assertEquals(100 * MB, target);
    }

    @Test
    public void testApplyMaxFileSplitNumLimitKeepsTargetSizeWhenSmall() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(100);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 500L * MB);
        Assert.assertEquals(32 * MB, target);
    }

    @Test
    public void testApplyMaxFileSplitNumLimitDisabled() {
        SessionVariable sv = new SessionVariable();
        sv.setMaxFileSplitNum(0);
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        long target = node.applyMaxFileSplitNumLimit(32 * MB, 10_000L * MB);
        Assert.assertEquals(32 * MB, target);
    }

    @Test
    public void testHiveParquetTimezoneOverridesDifferentSessionTimezoneInScanParams() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("type", "hms");
        properties.put("hive.metastore.uris", "thrift://localhost:9083");
        properties.put(FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE, "8:00");
        HMSExternalCatalog catalog = new HMSExternalCatalog(1, "hms", null, properties, "");
        catalog.checkProperties();

        HMSExternalTable hmsTable = Mockito.mock(HMSExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(catalog).when(hmsTable).getCatalog();
        Mockito.doReturn(DLAType.HIVE).when(hmsTable).getDlaType();
        Mockito.doReturn(Collections.emptyList()).when(hmsTable).getBaseSchema(false);
        Mockito.doReturn(Collections.emptyList()).when(hmsTable).getFullSchema();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.setTimeZone("America/Los_Angeles");
        TestFileQueryScanNode node = new TestFileQueryScanNode(sessionVariable);
        node.setTargetTable(hmsTable);
        node.getTupleDescriptor().setTable(hmsTable);

        node.initSchemaParamsForTest();

        Assert.assertEquals("+08:00", node.getFileScanRangeParams().getHiveParquetTimeZone());
        Assert.assertNotEquals(sessionVariable.getTimeZone(),
                node.getFileScanRangeParams().getHiveParquetTimeZone());

        SessionVariable differentSessionVariable = new SessionVariable();
        differentSessionVariable.setTimeZone("Asia/Tokyo");
        TestFileQueryScanNode nodeWithDifferentSession = new TestFileQueryScanNode(differentSessionVariable);
        nodeWithDifferentSession.setTargetTable(hmsTable);
        nodeWithDifferentSession.getTupleDescriptor().setTable(hmsTable);
        nodeWithDifferentSession.initSchemaParamsForTest();

        Assert.assertEquals(node.getFileScanRangeParams().getHiveParquetTimeZone(),
                nodeWithDifferentSession.getFileScanRangeParams().getHiveParquetTimeZone());
    }

    @Test
    public void testHiveParquetTimezoneIsNotSetForHmsIcebergTable() throws Exception {
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(catalog.getHiveParquetTimeZone()).thenReturn("Asia/Shanghai");
        HMSExternalTable hmsTable = Mockito.mock(HMSExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(catalog).when(hmsTable).getCatalog();
        Mockito.doReturn(DLAType.ICEBERG).when(hmsTable).getDlaType();
        Mockito.doReturn(Collections.emptyList()).when(hmsTable).getBaseSchema(false);
        Mockito.doReturn(Collections.emptyList()).when(hmsTable).getFullSchema();

        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        node.setTargetTable(hmsTable);
        node.getTupleDescriptor().setTable(hmsTable);
        node.initSchemaParamsForTest();

        Assert.assertFalse(node.getFileScanRangeParams().isSetHiveParquetTimeZone());
    }

    @Test
    public void testHiveParquetTimezoneIsSetForHmsHudiTable() throws Exception {
        HMSExternalCatalog catalog = Mockito.mock(HMSExternalCatalog.class);
        Mockito.when(catalog.getHiveParquetTimeZone()).thenReturn("Asia/Shanghai");
        HMSExternalTable hmsTable = Mockito.mock(HMSExternalTable.class, Mockito.CALLS_REAL_METHODS);
        Mockito.doReturn(catalog).when(hmsTable).getCatalog();
        Mockito.doReturn(DLAType.HUDI).when(hmsTable).getDlaType();
        Mockito.doReturn(Collections.emptyList()).when(hmsTable).getBaseSchema(false);
        Mockito.doReturn(Collections.emptyList()).when(hmsTable).getFullSchema();

        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        node.setTargetTable(hmsTable);
        node.getTupleDescriptor().setTable(hmsTable);
        node.initSchemaParamsForTest();

        Assert.assertEquals("Asia/Shanghai", node.getFileScanRangeParams().getHiveParquetTimeZone());
    }

    @Test
    public void testHiveParquetTimezoneComesFromTableValuedFunction() throws Exception {
        ExternalFileTableValuedFunction tvf = Mockito.mock(ExternalFileTableValuedFunction.class);
        tvf.fileFormatProperties = new ParquetFileFormatProperties();
        Mockito.when(tvf.getHiveParquetTimeZone()).thenReturn("Asia/Shanghai");
        FunctionGenTable functionGenTable = Mockito.mock(FunctionGenTable.class);
        Mockito.when(functionGenTable.getTvf()).thenReturn(tvf);
        Mockito.when(functionGenTable.getBaseSchema(false)).thenReturn(Collections.emptyList());
        Mockito.when(functionGenTable.getFullSchema()).thenReturn(Collections.emptyList());

        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        node.setTargetTable(functionGenTable);
        node.getTupleDescriptor().setTable(functionGenTable);
        node.initSchemaParamsForTest();

        Assert.assertEquals("Asia/Shanghai", node.getFileScanRangeParams().getHiveParquetTimeZone());
    }

    @Test
    public void testHiveParquetTimezoneComesFromFileTableValuedFunctionDelegate() throws Exception {
        Map<String, String> properties = new HashMap<>();
        properties.put("uri", "s3://bucket/path/file.parquet");
        properties.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
        properties.put("s3.region", "us-east-1");
        properties.put("s3.access_key", "access-key");
        properties.put("s3.secret_key", "secret-key");
        properties.put(FileFormatConstants.PROP_FORMAT, FileFormatConstants.FORMAT_PARQUET);
        properties.put(FileFormatConstants.PROP_HIVE_PARQUET_TIME_ZONE, "Asia/Shanghai");

        boolean originalRunningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        try {
            FileTableValuedFunction tvf = new FileTableValuedFunction(properties);
            tvf.fileFormatProperties = new ParquetFileFormatProperties();
            FunctionGenTable functionGenTable = new FunctionGenTable(
                    -1, "file", TableIf.TableType.TABLE_VALUED_FUNCTION, Collections.emptyList(), tvf);

            TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
            node.setTargetTable(functionGenTable);
            node.getTupleDescriptor().setTable(functionGenTable);
            node.initSchemaParamsForTest();

            Assert.assertEquals("Asia/Shanghai", node.getFileScanRangeParams().getHiveParquetTimeZone());
        } finally {
            FeConstants.runningUnitTest = originalRunningUnitTest;
        }
    }

    @Test
    public void testHiveParquetTimezoneIgnoresJdbcQueryTableValuedFunction() throws Exception {
        JdbcQueryTableValueFunction tvf = Mockito.mock(
                JdbcQueryTableValueFunction.class, Mockito.CALLS_REAL_METHODS);
        FunctionGenTable functionGenTable = new FunctionGenTable(
                -1, "query", TableIf.TableType.TABLE_VALUED_FUNCTION, Collections.emptyList(), tvf);
        TestFileQueryScanNode node = new TestFileQueryScanNode(new SessionVariable());
        node.setTargetTable(functionGenTable);
        node.getTupleDescriptor().setTable(functionGenTable);

        node.initSchemaParamsForTest();

        Assert.assertFalse(node.getFileScanRangeParams().isSetHiveParquetTimeZone());
    }

    @Test
    public void testUpdateRequiredSlotsPreservesInlineDefaultValueExpr() throws Exception {
        SessionVariable sv = new SessionVariable();
        TestFileQueryScanNode node = new TestFileQueryScanNode(sv);
        node.setTargetTable(table);

        TupleDescriptor desc = node.getTupleDescriptor();
        desc.setTable(table);
        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), desc.getId());
        slot.setColumn(new Column("c1", Type.INT));
        desc.addSlot(slot);
        Mockito.when(table.getFullSchema()).thenReturn(Arrays.asList(slot.getColumn()));

        TExpr defaultExpr = new TExpr();
        defaultExpr.setNodes(Collections.emptyList());
        TFileScanSlotInfo slotInfo = new TFileScanSlotInfo();
        slotInfo.setSlotId(slot.getId().asInt());
        slotInfo.setCategory(TColumnCategory.REGULAR);
        slotInfo.setIsFileSlot(true);
        slotInfo.setDefaultValueExpr(defaultExpr);

        TFileScanRangeParams params = new TFileScanRangeParams();
        params.setRequiredSlots(Arrays.asList(slotInfo));
        node.params = params;

        UPDATE_REQUIRED_SLOTS_METHOD.invoke(node);

        TFileScanSlotInfo updatedSlotInfo = node.params.getRequiredSlots().get(0);
        Assert.assertSame(slotInfo, updatedSlotInfo);
        Assert.assertTrue(updatedSlotInfo.isSetDefaultValueExpr());
        Assert.assertSame(defaultExpr, updatedSlotInfo.getDefaultValueExpr());
    }
}
