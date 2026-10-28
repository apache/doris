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

package org.apache.doris.datasource.es.source;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.EsResource;
import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.es.QueryBuilders;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TEsScanRange;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TScanRangeLocations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

class EsScanNodeFileScannerV2Test {
    @AfterEach
    void tearDown() {
        ConnectContext.remove();
    }

    @Test
    void enabledScannerUsesFileScanPlan() throws Exception {
        TestScanNode node = createNode(true);
        TPlanNode plan = node.serialize();
        Assertions.assertEquals(TPlanNodeType.FILE_SCAN_NODE, plan.getNodeType());
        Assertions.assertEquals(7, plan.getFileScanNode().getTupleId());
        Assertions.assertFalse(plan.isSetEsScanNode());
    }

    @Test
    void disabledScannerKeepsLegacyPlanAndRanges() throws Exception {
        TestScanNode node = createNode(false);
        Assertions.assertEquals(TPlanNodeType.ES_HTTP_SCAN_NODE, node.serialize().getNodeType());
        Assertions.assertTrue(node.getScanRangeLocations(0).get(0).getScanRange().isSetEsScanRange());
    }

    @Test
    void enabledScannerConvertsShardToFileRange() throws Exception {
        TestScanNode node = createNode(true);
        TScanRange range = node.getScanRangeLocations(0).get(0).getScanRange();
        Assertions.assertTrue(range.isSetExtScanRange());
        Assertions.assertFalse(range.isSetEsScanRange());
        Assertions.assertEquals(7, range.getExtScanRange().getFileScanRange().getParams().getDestTupleId());
        Assertions.assertEquals(-1, range.getExtScanRange().getFileScanRange().getParams().getSrcTupleId());
        Assertions.assertEquals("es", range.getExtScanRange().getFileScanRange()
                .getRanges().get(0).getTableFormatParams().getTableFormatType());
        TFileScanRange fileRange = range.getExtScanRange().getFileScanRange();
        Assertions.assertEquals(1, fileRange.getParams().getNumOfColumnsFromFile());
        Assertions.assertEquals(4, fileRange.getParams().getRequiredSlots().get(0).getSlotId());
        Assertions.assertTrue(fileRange.getParams().getRequiredSlots().get(0).isIsFileSlot());
        Assertions.assertEquals("{\"match_all\":{}}", fileRange.getParams().getEsProperties().get("query_dsl"));
        Assertions.assertEquals("es_index", fileRange.getRanges().get(0)
                .getTableFormatParams().getEsParams().get("index"));
        Assertions.assertEquals("3", fileRange.getRanges().get(0)
                .getTableFormatParams().getEsParams().get("shard_id"));
        Assertions.assertEquals("127.0.0.1:9200", fileRange.getRanges().get(0)
                .getTableFormatParams().getEsParams().get("es_hosts"));
        Assertions.assertEquals(1, node.getScanRangeLocations(0).get(0)
                .getScanRange().getExtScanRange().getFileScanRange().getRangesSize());
    }

    @Test
    void scanProtocolSurvivesPlanningThreadContext() throws Exception {
        TestScanNode node = createNode(true);
        ConnectContext.remove();
        Assertions.assertEquals(TPlanNodeType.FILE_SCAN_NODE, node.serialize().getNodeType());
        Assertions.assertTrue(node.getScanRangeLocations(0).get(0).getScanRange().isSetExtScanRange());
    }

    @Test
    void compoundResidualsRemainOnScanNode() throws Exception {
        for (CompoundPredicate.Operator operator : Arrays.asList(
                CompoundPredicate.Operator.OR, CompoundPredicate.Operator.NOT)) {
            TestScanNode node = createNode(false);
            Expr equality = new BinaryPredicate(BinaryPredicate.Operator.EQ,
                    new SlotRef(null, "id"), new IntLiteral(1));
            Expr local = new FunctionCallExpr("unsupported_predicate",
                    new ArrayList<>(Collections.singletonList(new SlotRef(null, "id"))));
            Expr partial = new CompoundPredicate(CompoundPredicate.Operator.AND, equality, local);
            Expr compound = new CompoundPredicate(operator, partial,
                    operator == CompoundPredicate.Operator.NOT ? null : equality);
            node.getConjuncts().add(compound);
            Method build = EsScanNode.class.getDeclaredMethod("buildQuery");
            build.setAccessible(true);
            build.invoke(node);
            Assertions.assertEquals(Collections.singletonList(compound), node.getConjuncts());
            Assertions.assertEquals("{\"match_all\":{}}",
                    node.serialize().getEsScanNode().getProperties().get(EsResource.QUERY_DSL));
        }
    }

    @Test
    void docvalueEligibilityHonorsFieldLimitAndSourceOnlyColumns() throws Exception {
        Assertions.assertEquals("1", projectionParams(Arrays.asList("a"), 1, true)
                .getEsProperties().get(EsResource.DOC_VALUES_MODE));
        Assertions.assertEquals("0", projectionParams(Arrays.asList("a", "b"), 1, true)
                .getEsProperties().get(EsResource.DOC_VALUES_MODE));
        Assertions.assertEquals("1", projectionParams(Arrays.asList("a", "b"), 2, true)
                .getEsProperties().get(EsResource.DOC_VALUES_MODE));
        Assertions.assertEquals("0", projectionParams(Arrays.asList("a", "_id"), 2, true)
                .getEsProperties().get(EsResource.DOC_VALUES_MODE));
        Assertions.assertFalse(projectionParams(Arrays.asList("a"), 1, false).isSetEsDocvalueContext());
    }

    @Test
    void keywordAliasesSurviveFileScanThriftPopulation() throws Exception {
        org.apache.doris.thrift.TFileScanRangeParams params = projectionParams(Arrays.asList("a"), 2, true);
        Assertions.assertEquals("a.keyword", params.getEsDocvalueContext().get("a"));
        Assertions.assertEquals(ImmutableMap.of("a", "a.keyword"), params.getEsFieldsContext());
        Assertions.assertEquals("{\"match_all\":{}}", params.getEsProperties().get(EsResource.QUERY_DSL));
    }

    private org.apache.doris.thrift.TFileScanRangeParams projectionParams(
            List<String> names, int maxFields, boolean docvalues) throws Exception {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().enableFileScannerV2 = true;
        context.setThreadLocalInfo();
        List<Column> columns = new ArrayList<>();
        for (String name : names) {
            columns.add(new Column(name, Type.STRING));
        }
        EsTable table = Mockito.spy(new EsTable(1, "es_table", columns, ImmutableMap.of(
                EsResource.HOSTS, "127.0.0.1:9200", EsResource.INDEX, "es_index",
                EsResource.DOC_VALUE_SCAN, Boolean.toString(docvalues),
                EsResource.MAX_DOCVALUE_FIELDS, Integer.toString(maxFields), EsResource.KEYWORD_SNIFF, "true"), null));
        Mockito.doReturn(ImmutableMap.of("a", "a.keyword", "b", "b")).when(table).docValueContext();
        Mockito.doReturn(ImmutableMap.of("a", "a.keyword")).when(table).fieldsContext();
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(7));
        tuple.setTable(table);
        for (int i = 0; i < columns.size(); i++) {
            SlotDescriptor slot = new SlotDescriptor(new SlotId(i), tuple);
            slot.setColumn(columns.get(i));
            tuple.addSlot(slot);
        }
        TestScanNode node = new TestScanNode(tuple);
        Field builder = EsScanNode.class.getDeclaredField("queryBuilder");
        builder.setAccessible(true);
        builder.set(node, QueryBuilders.matchAllQuery());
        TEsScanRange range = new TEsScanRange().setIndex("es_index").setShardId(0)
                .setEsHosts(ImmutableList.of(new TNetworkAddress("127.0.0.1", 9200)));
        node.installRanges(ImmutableList.of(new TScanRangeLocations().setScanRange(
                new TScanRange().setEsScanRange(range))));
        return node.getScanRangeLocations(0).get(0).getScanRange().getExtScanRange().getFileScanRange().getParams();
    }

    private TestScanNode createNode(boolean enabled) throws Exception {
        ConnectContext context = new ConnectContext();
        context.getSessionVariable().enableFileScannerV2 = enabled;
        context.setThreadLocalInfo();
        Column column = new Column("id", Type.INT);
        EsTable table = Mockito.spy(new EsTable(1, "es_table", ImmutableList.of(column), ImmutableMap.of(
                EsResource.HOSTS, "127.0.0.1:9200", EsResource.INDEX, "es_index",
                EsResource.DOC_VALUE_SCAN, "false", EsResource.KEYWORD_SNIFF, "false"), null));
        Mockito.doReturn(ImmutableMap.of("id", "integer")).when(table).getColumn2typeMap();
        TupleDescriptor tuple = new TupleDescriptor(new TupleId(7));
        tuple.setTable(table);
        SlotDescriptor slot = new SlotDescriptor(new SlotId(4), tuple);
        slot.setColumn(column);
        tuple.addSlot(slot);
        TestScanNode node = new TestScanNode(tuple);
        Field builder = EsScanNode.class.getDeclaredField("queryBuilder");
        builder.setAccessible(true);
        builder.set(node, QueryBuilders.matchAllQuery());
        TEsScanRange esRange = new TEsScanRange();
        esRange.setIndex("es_index");
        esRange.setShardId(3);
        esRange.setEsHosts(ImmutableList.of(new TNetworkAddress("127.0.0.1", 9200)));
        TScanRangeLocations locations = new TScanRangeLocations();
        locations.setScanRange(new TScanRange().setEsScanRange(esRange));
        node.installRanges(ImmutableList.of(locations));
        return node;
    }

    private static class TestScanNode extends EsScanNode {
        TestScanNode(TupleDescriptor tuple) {
            super(new PlanNodeId(1), tuple, false, ScanContext.EMPTY);
        }

        TPlanNode serialize() {
            TPlanNode plan = new TPlanNode();
            toThrift(plan);
            return plan;
        }

        void installRanges(List<TScanRangeLocations> ranges) {
            scanRangeLocations = ranges;
        }
    }
}
