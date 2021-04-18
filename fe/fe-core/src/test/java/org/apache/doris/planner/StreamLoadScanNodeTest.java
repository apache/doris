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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.load.Load;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TStreamLoadPutRequest;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

public class StreamLoadScanNodeTest {
    private static final Logger LOG = LogManager.getLogger(StreamLoadScanNodeTest.class);

    @Mocked
    Catalog catalog;

    @Injectable
    ConnectContext connectContext;

    @Injectable
    Database db;

    @Injectable
    OlapTable dstTable;

    @Mocked
    CastExpr castExpr;

    TStreamLoadPutRequest getBaseRequest() {
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setFileType(TFileType.FILE_STREAM);
        request.setFormatType(TFileFormatType.FORMAT_CSV_PLAIN);
        return request;
    }

    List<Column> getBaseSchema() {
        List<Column> columns = Lists.newArrayList();

        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        k1.setIsKey(true);
        k1.setIsAllowNull(false);
        columns.add(k1);

        Column k2 = new Column("k2", ScalarType.createVarchar(25));
        k2.setIsKey(true);
        k2.setIsAllowNull(true);
        columns.add(k2);

        Column v1 = new Column("v1", PrimitiveType.BIGINT);
        v1.setIsKey(false);
        v1.setIsAllowNull(true);
        v1.setAggregationType(AggregateType.SUM, false);

        columns.add(v1);

        Column v2 = new Column("v2", ScalarType.createVarchar(25));
        v2.setIsKey(false);
        v2.setAggregationType(AggregateType.REPLACE, false);
        v2.setIsAllowNull(false);
        columns.add(v2);

        return columns;
    }

    List<Column> getHllSchema() {
        List<Column> columns = Lists.newArrayList();

        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        k1.setIsKey(true);
        k1.setIsAllowNull(false);
        columns.add(k1);

        Column v1 = new Column("v1", PrimitiveType.HLL);
        v1.setIsKey(false);
        v1.setIsAllowNull(true);
        v1.setAggregationType(AggregateType.HLL_UNION, false);

        columns.add(v1);

        return columns;
    }

    List<Column> getSequenceColSchema() {
        List<Column> columns = Lists.newArrayList();

        Column k1 = new Column("k1", PrimitiveType.BIGINT);
        k1.setIsKey(true);
        k1.setIsAllowNull(false);
        columns.add(k1);

        Column k2 = new Column("k2", ScalarType.createVarchar(25));
        k2.setIsKey(true);
        k2.setIsAllowNull(true);
        columns.add(k2);

        // sequence column, it's hidden column
        Column sequenceCol = new Column(Column.SEQUENCE_COL, PrimitiveType.BIGINT);
        sequenceCol.setIsKey(false);
        sequenceCol.setAggregationType(AggregateType.REPLACE, false);
        sequenceCol.setIsAllowNull(false);
        sequenceCol.setIsVisible(false);
        columns.add(sequenceCol);

        // sequence column, it's visible column for user, it's equals to the hidden column
        Column visibleSequenceCol = new Column("visible_sequence_col", PrimitiveType.BIGINT);
        visibleSequenceCol.setIsKey(false);
        visibleSequenceCol.setAggregationType(AggregateType.REPLACE, false);
        visibleSequenceCol.setIsAllowNull(true);
        columns.add(visibleSequenceCol);

        Column v1 = new Column("v1", ScalarType.createVarchar(25));
        v1.setIsKey(false);
        v1.setAggregationType(AggregateType.REPLACE, false);
        v1.setIsAllowNull(false);
        columns.add(v1);

        return columns;
    }
    
    private StreamLoadScanNode getStreamLoadScanNode(TupleDescriptor dstDesc, TStreamLoadPutRequest request)
            throws UserException {
        StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
        StreamLoadScanNode scanNode = new StreamLoadScanNode(streamLoadTask.getId(), new PlanNodeId(1), dstDesc, dstTable, streamLoadTask);
        return scanNode;
    }

    @Test
    public void testNormal() throws UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        TStreamLoadPutRequest request = getBaseRequest();
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);
        new Expectations() {{
            dstTable.getBaseSchema(); result = columns;
            dstTable.getBaseSchema(anyBoolean); result = columns;
            dstTable.getFullSchema(); result = columns;
            dstTable.getColumn("k1"); result = columns.get(0);
            dstTable.getColumn("k2"); result = columns.get(1);
            dstTable.getColumn("v1"); result = columns.get(2);
            dstTable.getColumn("v2"); result = columns.get(3);
        }};
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);

        Assert.assertEquals(1, scanNode.getNumInstances());
        Assert.assertEquals(1, scanNode.getScanRangeLocations(0).size());
    }

    @Test(expected = AnalysisException.class)
    public void testLostV2() throws UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1, k2, v1");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test(expected = AnalysisException.class)
    public void testBadColumns() throws UserException, UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1 k2 v1");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test
    public void testColumnsNormal() throws UserException, UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        new Expectations() {
            {
                dstTable.getColumn("k1");
                result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();

                dstTable.getColumn("k2");
                result = columns.stream().filter(c -> c.getName().equals("k2")).findFirst().get();

                dstTable.getColumn("v1");
                result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();

                dstTable.getColumn("v2");
                result = columns.stream().filter(c -> c.getName().equals("v2")).findFirst().get();
            }
        };

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1,k2,v1, v2=k2");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);
        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test
    public void testHllColumnsNormal() throws UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getHllSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        new Expectations() {{
            catalog.getFunction((Function) any, (Function.CompareMode) any);
            result = new ScalarFunction(new FunctionName(FunctionSet.HLL_HASH), Lists.newArrayList(), Type.BIGINT, false);
        }};
        
        new Expectations() {
            {
                dstTable.getColumn("k1");
                result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();

                dstTable.getColumn("k2");
                result = null;

                dstTable.getColumn("v1");
                result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
            }
        };

        TStreamLoadPutRequest request = getBaseRequest();
        request.setFileType(TFileType.FILE_STREAM);

        request.setColumns("k1,k2, v1=" + FunctionSet.HLL_HASH + "(k2)");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test(expected = UserException.class)
    public void testHllColumnsNoHllHash() throws UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getHllSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        new Expectations() {
            {
                catalog.getFunction((Function) any, (Function.CompareMode) any);
                result = new ScalarFunction(new FunctionName("hll_hash1"), Lists.newArrayList(), Type.BIGINT, false);
                minTimes = 0;
            }
        };

        new Expectations() {
            {
                dstTable.getColumn("k1");
                result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("k2");
                result = null;
                minTimes = 0;

                dstTable.getColumn("v1");
                result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
                minTimes = 0;
            }
        };

        TStreamLoadPutRequest request = getBaseRequest();
        request.setFileType(TFileType.FILE_LOCAL);
        request.setColumns("k1,k2, v1=hll_hash1(k2)");
        StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test(expected = UserException.class)
    public void testHllColumnsFail() throws UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getHllSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        TStreamLoadPutRequest request = getBaseRequest();
        request.setFileType(TFileType.FILE_LOCAL);
        request.setColumns("k1,k2, v1=k2");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test(expected = UserException.class)
    public void testUnsupportedFType() throws UserException, UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        TStreamLoadPutRequest request = getBaseRequest();
        request.setFileType(TFileType.FILE_BROKER);
        request.setColumns("k1,k2,v1, v2=k2");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test(expected = UserException.class)
    public void testColumnsUnknownRef() throws UserException, UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1,k2,v1, v2=k3");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test
    public void testWhereNormal() throws UserException, UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        new Expectations() {
            {
                dstTable.getColumn("k1");
                result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();

                dstTable.getColumn("k2");
                result = columns.stream().filter(c -> c.getName().equals("k2")).findFirst().get();

                dstTable.getColumn("v1");
                result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();

                dstTable.getColumn("v2");
                result = columns.stream().filter(c -> c.getName().equals("v2")).findFirst().get();
            }
        };

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1,k2,v1, v2=k1");
        request.setWhere("k1 = 1");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test(expected = AnalysisException.class)
    public void testWhereBad() throws UserException, UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        new Expectations() {
            {
                dstTable.getColumn("k1");
                result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("k2");
                result = columns.stream().filter(c -> c.getName().equals("k2")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("v1");
                result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("v2");
                result = columns.stream().filter(c -> c.getName().equals("v2")).findFirst().get();
                minTimes = 0;
            }
        };

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1,k2,v1, v2=k2");
        request.setWhere("k1   1");
        StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
        StreamLoadScanNode scanNode = new StreamLoadScanNode(streamLoadTask.getId(), new PlanNodeId(1), dstDesc, dstTable,
                                                             streamLoadTask);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test(expected = UserException.class)
    public void testWhereUnknownRef() throws UserException, UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1,k2,v1, v2=k1");
        request.setWhere("k5 = 1");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test(expected = UserException.class)
    public void testWhereNotBool() throws UserException, UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getBaseSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1,k2,v1, v2=k1");
        request.setWhere("k1 + v2");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test
    public void testSequenceColumnWithSetColumns() throws UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getSequenceColSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            System.out.println(column);
            slot.setColumn(column);
            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        new Expectations() {
            {
                db.getTable(anyInt);
                result = dstTable;
                minTimes = 0;
                dstTable.hasSequenceCol();
                result = true;
            }
        };

        new Expectations() {
            {
                dstTable.getColumn("k1");
                result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("k2");
                result = columns.stream().filter(c -> c.getName().equals("k2")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn(Column.SEQUENCE_COL);
                result = columns.stream().filter(c -> c.getName().equals(Column.SEQUENCE_COL)).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("visible_sequence_col");
                result = columns.stream().filter(c -> c.getName().equals("visible_sequence_col")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("v1");
                result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
                minTimes = 0;
                // there is no "source_sequence" column in the Table
                dstTable.getColumn("source_sequence");
                result = null;
                minTimes = 0;
            }
        };

        TStreamLoadPutRequest request = getBaseRequest();
        request.setColumns("k1,k2,source_sequence,v1");
        request.setFileType(TFileType.FILE_STREAM);
        request.setSequenceCol("source_sequence");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }

    @Test
    public void testSequenceColumnWithoutSetColumns() throws UserException {
        Analyzer analyzer = new Analyzer(catalog, connectContext);
        DescriptorTable descTbl = analyzer.getDescTbl();

        List<Column> columns = getSequenceColSchema();
        TupleDescriptor dstDesc = descTbl.createTupleDescriptor("DstTableDesc");
        for (Column column : columns) {
            SlotDescriptor slot = descTbl.addSlotDescriptor(dstDesc);
            slot.setColumn(column);

            slot.setIsMaterialized(true);
            if (column.isAllowNull()) {
                slot.setIsNullable(true);
            } else {
                slot.setIsNullable(false);
            }
        }

        new Expectations() {
            {
                db.getTable(anyInt);
                result = dstTable;
                minTimes = 0;
                dstTable.hasSequenceCol();
                result = true;
            }
        };

        new Expectations() {
            {
                dstTable.getBaseSchema(anyBoolean); result = columns;
                dstTable.getFullSchema(); result = columns;

                dstTable.getColumn("k1");
                result = columns.stream().filter(c -> c.getName().equals("k1")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("k2");
                result = columns.stream().filter(c -> c.getName().equals("k2")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn(Column.SEQUENCE_COL);
                result = columns.stream().filter(c -> c.getName().equals(Column.SEQUENCE_COL)).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("visible_sequence_col");
                result = columns.stream().filter(c -> c.getName().equals("visible_sequence_col")).findFirst().get();
                minTimes = 0;

                dstTable.getColumn("v1");
                result = columns.stream().filter(c -> c.getName().equals("v1")).findFirst().get();
                minTimes = 0;

                dstTable.hasSequenceCol();
                result = true;
                minTimes = 0;
            }
        };

        TStreamLoadPutRequest request = getBaseRequest();
        request.setFileType(TFileType.FILE_STREAM);
        request.setSequenceCol("visible_sequence_col");
        StreamLoadScanNode scanNode = getStreamLoadScanNode(dstDesc, request);

        scanNode.init(analyzer);
        scanNode.finalize(analyzer);
        scanNode.getNodeExplainString("", TExplainLevel.NORMAL);
        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
    }
}
