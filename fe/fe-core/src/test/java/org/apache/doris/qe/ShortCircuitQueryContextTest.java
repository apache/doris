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

package org.apache.doris.qe;

import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.Queriable;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Placeholder;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.Planner;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TStorageType;

import org.apache.thrift.TDeserializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

public class ShortCircuitQueryContextTest {
    private OlapTable table(String name, int schemaVersion) {
        OlapTable table = Mockito.spy(new OlapTable());
        Mockito.doReturn(name).when(table).getName();
        Mockito.doReturn(schemaVersion).when(table).getBaseSchemaVersion();
        return table;
    }

    private OlapTable pointQueryTable(List<Column> keyColumns) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getName()).thenReturn("tbl");
        Mockito.when(table.getBaseSchemaKeyColumns()).thenReturn(keyColumns);
        Mockito.when(table.getBaseSchemaVersion()).thenReturn(1);
        return table;
    }

    private ConnectContext connectContext(long fileCacheQueryLimitBytes) {
        ConnectContext ctx = new ConnectContext();
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.fileCacheQueryLimitBytes = fileCacheQueryLimitBytes;
        ctx.setSessionVariable(sessionVariable);
        return ctx;
    }

    @Test
    public void testReusableRequiresSameFileCacheQueryLimitBytes() {
        ShortCircuitQueryContext context =
                new ShortCircuitQueryContext(table("tbl", 10), "tbl", 10, -1);

        Assertions.assertTrue(context.isReusable(connectContext(-1)));
        Assertions.assertFalse(context.isReusable(connectContext(0)));
    }

    @Test
    public void testReusableStillChecksTableMetadata() {
        ShortCircuitQueryContext context =
                new ShortCircuitQueryContext(table("tbl", 11), "tbl", 10, 0);

        Assertions.assertFalse(context.isReusable(connectContext(0)));
    }

    @Test
    public void testReusableRequiresSamePartitionTopologyVersion() {
        long baseIndexId = 2L;
        Column key = new Column("k", PrimitiveType.INT);
        key.setIsKey(true);
        List<Column> baseSchema = Collections.singletonList(key);
        OlapTable table = new OlapTable(1L, "tbl", baseSchema, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo(1));
        table.setIndexMeta(baseIndexId, "tbl", baseSchema, 10, 0, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS);
        table.setBaseIndexId(baseIndexId);
        ShortCircuitQueryContext context = new ShortCircuitQueryContext(table, "tbl", 10, -1);

        Assertions.assertTrue(context.isReusable(connectContext(-1)));
        table.addPartition(new Partition(3L, "p1",
                new MaterializedIndex(baseIndexId, MaterializedIndex.IndexState.NORMAL),
                new RandomDistributionInfo(1)));
        Assertions.assertFalse(context.isReusable(connectContext(-1)));
    }

    @Test
    public void testSerializedQueryOptionsKeepBitmapOpCountVersion() throws Exception {
        TQueryOptions queryOptions = new SessionVariable().toThrift();
        Planner planner = Mockito.mock(Planner.class);
        Mockito.when(planner.getQueryOptions()).thenReturn(queryOptions);
        DescriptorTable descriptorTable = new DescriptorTable();
        descriptorTable.createTupleDescriptor();
        Mockito.when(planner.getDescTable()).thenReturn(descriptorTable);

        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        OlapTable table = table("tbl", 10);
        Mockito.when(scanNode.getPointQueryProjectList()).thenReturn(Collections.emptyList());
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getTableNameInPlan()).thenReturn("tbl");
        Mockito.when(planner.getScanNodes()).thenReturn(Collections.singletonList(scanNode));

        ShortCircuitQueryContext context =
                new ShortCircuitQueryContext(planner, Mockito.mock(Queriable.class));
        TQueryOptions serializedQueryOptions = new TQueryOptions();
        new TDeserializer().deserialize(serializedQueryOptions, context.serializedQueryOptions.toByteArray());

        Assertions.assertTrue(serializedQueryOptions.isSetNewVersionBitmapOpCount());
        Assertions.assertTrue(serializedQueryOptions.isNewVersionBitmapOpCount());
    }

    @Test
    public void testPreparedKeyTemplateKeepsFixedConstraintsAcrossExecutions() {
        Column parameterKey = new Column("parameter_key", PrimitiveType.INT);
        parameterKey.setIsKey(true);
        Column policyKey = new Column("policy_key", PrimitiveType.INT);
        policyKey.setIsKey(true);
        List<Column> schema = List.of(parameterKey, policyKey);
        OlapTable table = pointQueryTable(schema);
        SlotReference parameterSlot = SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), table, parameterKey, Collections.emptyList());
        SlotReference policySlot = SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), table, policyKey, Collections.emptyList());

        PlaceholderId placeholderId = new PlaceholderId(0);
        StatementContext templateContext = new StatementContext();
        templateContext.setPlaceholders(Collections.singletonList(new Placeholder(placeholderId)));
        templateContext.getIdToComparisonSlot().put(placeholderId, parameterSlot);
        // This models a restrictive policy on the same key as the placeholder, plus a
        // policy-fixed column in a composite key.
        templateContext.addPointQueryFixedKeyConstraint(parameterSlot, new IntegerLiteral(1));
        templateContext.addPointQueryFixedKeyConstraint(policySlot, new IntegerLiteral(9));

        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getTableNameInPlan()).thenReturn("tbl");
        ShortCircuitQueryContext cached = new ShortCircuitQueryContext(scanNode, templateContext);

        StatementContext first = execution(placeholderId, new IntegerLiteral(1));
        ShortCircuitQueryContext.PointQueryExecutionContext firstExecution =
                cached.createPointQueryExecutionContext(first);
        Assertions.assertEquals(ShortCircuitQueryContext.PointQueryExecutionContext.Decision.LOOKUP,
                firstExecution.getDecision());
        Assertions.assertEquals("1", firstExecution.getKeyValues().get("parameter_key").getStringValue());
        Assertions.assertEquals("9", firstExecution.getKeyValues().get("policy_key").getStringValue());

        StatementContext second = execution(placeholderId, new IntegerLiteral(2));
        Assertions.assertEquals(ShortCircuitQueryContext.PointQueryExecutionContext.Decision.EMPTY,
                cached.createPointQueryExecutionContext(second).getDecision());

        // Reusing the same prepared handle with 1 -> 2 -> 1 must not contaminate the template.
        Assertions.assertEquals(ShortCircuitQueryContext.PointQueryExecutionContext.Decision.LOOKUP,
                cached.createPointQueryExecutionContext(first).getDecision());

        StatementContext nullValue = execution(placeholderId, new NullLiteral());
        Assertions.assertEquals(ShortCircuitQueryContext.PointQueryExecutionContext.Decision.EMPTY,
                cached.createPointQueryExecutionContext(nullValue).getDecision());
        Mockito.verify(scanNode, Mockito.never()).getConjuncts();
    }

    @Test
    public void testFixedOnlyKeyTemplate() {
        Column key = new Column("k", PrimitiveType.INT);
        key.setIsKey(true);
        OlapTable table = pointQueryTable(Collections.singletonList(key));
        SlotReference slot = SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), table, key, Collections.emptyList());
        StatementContext templateContext = new StatementContext();
        templateContext.addPointQueryFixedKeyConstraint(slot, new IntegerLiteral(7));
        ShortCircuitQueryContext cached = new ShortCircuitQueryContext(scanNode(table), templateContext);

        ShortCircuitQueryContext.PointQueryExecutionContext execution =
                cached.createPointQueryExecutionContext(new StatementContext());
        Assertions.assertEquals(ShortCircuitQueryContext.PointQueryExecutionContext.Decision.LOOKUP,
                execution.getDecision());
        Assertions.assertEquals("7", execution.getKeyValues().get("k").getStringValue());
    }

    @Test
    public void testPlaceholderOnlyKeyTemplate() {
        Column key = new Column("k", PrimitiveType.INT);
        key.setIsKey(true);
        OlapTable table = pointQueryTable(Collections.singletonList(key));
        SlotReference slot = SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), table, key, Collections.emptyList());
        PlaceholderId placeholderId = new PlaceholderId(0);
        StatementContext templateContext = new StatementContext();
        templateContext.setPlaceholders(Collections.singletonList(new Placeholder(placeholderId)));
        templateContext.getIdToComparisonSlot().put(placeholderId, slot);
        ShortCircuitQueryContext cached = new ShortCircuitQueryContext(scanNode(table), templateContext);

        ShortCircuitQueryContext.PointQueryExecutionContext execution =
                cached.createPointQueryExecutionContext(execution(placeholderId, new IntegerLiteral(8)));
        Assertions.assertEquals(ShortCircuitQueryContext.PointQueryExecutionContext.Decision.LOOKUP,
                execution.getDecision());
        Assertions.assertEquals("8", execution.getKeyValues().get("k").getStringValue());
    }

    @Test
    public void testInexactPhysicalKeyFallsBack() {
        Column key = new Column("k", PrimitiveType.INT);
        key.setIsKey(true);
        OlapTable table = pointQueryTable(Collections.singletonList(key));
        SlotReference slot = SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), table, key, Collections.emptyList());
        PlaceholderId placeholderId = new PlaceholderId(0);
        StatementContext templateContext = new StatementContext();
        templateContext.setPlaceholders(Collections.singletonList(new Placeholder(placeholderId)));
        templateContext.getIdToComparisonSlot().put(placeholderId, slot);
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getTableNameInPlan()).thenReturn("tbl");
        ShortCircuitQueryContext cached = new ShortCircuitQueryContext(scanNode, templateContext);

        StatementContext execution = execution(placeholderId, new DecimalLiteral(new BigDecimal("1.2")));
        Assertions.assertEquals(ShortCircuitQueryContext.PointQueryExecutionContext.Decision.FALLBACK,
                cached.createPointQueryExecutionContext(execution).getDecision());
    }

    @Test
    public void testNonSlotFixedConstraintFallsBack() {
        Column key = new Column("k", PrimitiveType.INT);
        key.setIsKey(true);
        OlapTable table = pointQueryTable(Collections.singletonList(key));
        SlotReference slot = SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), table, key, Collections.emptyList());
        PlaceholderId placeholderId = new PlaceholderId(0);
        StatementContext templateContext = new StatementContext();
        templateContext.setPlaceholders(Collections.singletonList(new Placeholder(placeholderId)));
        templateContext.getIdToComparisonSlot().put(placeholderId, slot);
        // ExpressionAnalyzer uses this marker for a fixed predicate such as
        // CAST(k AS CHAR(1)) = '1', whose cast cannot identify an exact physical key.
        templateContext.markPointQueryFixedKeyConstraintsIncomplete();
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getTableNameInPlan()).thenReturn("tbl");
        ShortCircuitQueryContext cached = new ShortCircuitQueryContext(scanNode, templateContext);

        Assertions.assertEquals(ShortCircuitQueryContext.PointQueryExecutionContext.Decision.FALLBACK,
                cached.createPointQueryExecutionContext(
                        execution(placeholderId, new IntegerLiteral(1))).getDecision());
    }

    @Test
    public void testFixedNonKeyConstraintFallsBack() {
        Column key = new Column("k", PrimitiveType.INT);
        key.setIsKey(true);
        Column value = new Column("v", PrimitiveType.INT);
        OlapTable table = pointQueryTable(Collections.singletonList(key));
        SlotReference keySlot = SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), table, key, Collections.emptyList());
        SlotReference valueSlot = SlotReference.fromColumn(
                StatementScopeIdGenerator.newExprId(), table, value, Collections.emptyList());
        PlaceholderId placeholderId = new PlaceholderId(0);
        StatementContext templateContext = new StatementContext();
        templateContext.setPlaceholders(Collections.singletonList(new Placeholder(placeholderId)));
        templateContext.getIdToComparisonSlot().put(placeholderId, keySlot);
        templateContext.addPointQueryFixedKeyConstraint(valueSlot, new IntegerLiteral(1));
        ShortCircuitQueryContext cached = new ShortCircuitQueryContext(scanNode(table), templateContext);

        Assertions.assertEquals(ShortCircuitQueryContext.PointQueryExecutionContext.Decision.FALLBACK,
                cached.createPointQueryExecutionContext(
                        execution(placeholderId, new IntegerLiteral(1))).getDecision());
    }

    private OlapScanNode scanNode(OlapTable table) {
        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getTableNameInPlan()).thenReturn("tbl");
        return scanNode;
    }

    private StatementContext execution(PlaceholderId placeholderId,
            org.apache.doris.nereids.trees.expressions.Expression value) {
        StatementContext context = new StatementContext();
        context.getIdToPlaceholderRealExpr().put(placeholderId, value);
        return context;
    }
}
