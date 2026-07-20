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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.planner.PredicateLmStage1Selector.SelectionReason;
import org.apache.doris.planner.PredicateLmStage1Selector.SelectionResult;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.ColumnStatisticBuilder;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class PredicateLmStage1SelectorTest {
    private final Column cheapSelectiveColumn = new Column("tenant_id", PrimitiveType.INT);
    private final Column lateStringColumn = new Column("event_type", PrimitiveType.VARCHAR);
    private final Column anotherLateColumn = new Column("payload", PrimitiveType.VARCHAR);

    @Test
    public void testSelectCheapSelectiveStage1Column() {
        OlapScanNode scanNode = scanNode(20_000_000_000L,
                eq(cheapSelectiveColumn),
                eq(lateStringColumn),
                eq(anotherLateColumn));
        SessionVariable sessionVariable = sessionVariable();
        Map<String, ColumnStatistic> stats = ImmutableMap.of(
                "tenant_id", stats(20_000_000_000L, 1000, 4),
                "event_type", stats(20_000_000_000L, 10, 32),
                "payload", stats(20_000_000_000L, 5, 128));

        SelectionResult result = select(scanNode, sessionVariable, stats);

        Assertions.assertFalse(result.isEmpty());
        Assertions.assertEquals(SelectionReason.SELECTED, result.getReason());
        Assertions.assertEquals(Lists.newArrayList(0), result.getStage1ColumnIds());
        Assertions.assertEquals(Lists.newArrayList("tenant_id"), result.getStage1ColumnNames());
        Assertions.assertTrue(result.getEstimatedSavedBytes() >= result.getMinEstimatedSavedBytes());
    }

    @Test
    public void testRejectWhenDisabled() {
        OlapScanNode scanNode = scanNode(20_000_000_000L,
                eq(cheapSelectiveColumn),
                eq(lateStringColumn));
        SessionVariable sessionVariable = sessionVariable();
        sessionVariable.enableMultiStagePredicateLm = false;

        SelectionResult result = select(scanNode, sessionVariable, defaultStats());

        Assertions.assertTrue(result.isEmpty());
        Assertions.assertEquals(SelectionReason.DISABLED, result.getReason());
    }

    @Test
    public void testRejectTooFewConjunctsBeforeStatsLookup() {
        OlapScanNode scanNode = scanNode(20_000_000_000L, eq(cheapSelectiveColumn));

        SelectionResult result = new PredicateLmStage1Selector(scanNode, sessionVariable(), columnName -> {
            throw new AssertionError("Statistics should not be loaded for too few conjuncts");
        }).select();

        Assertions.assertTrue(result.isEmpty());
        Assertions.assertEquals(SelectionReason.TOO_FEW_CONJUNCTS, result.getReason());
    }

    @Test
    public void testRejectTooFewCandidatesBeforeStatsLookup() {
        OlapScanNode scanNode = scanNode(20_000_000_000L,
                eq(cheapSelectiveColumn),
                new BinaryPredicate(BinaryPredicate.Operator.GT, slot(cheapSelectiveColumn), new IntLiteral(10)));

        SelectionResult result = new PredicateLmStage1Selector(scanNode, sessionVariable(), columnName -> {
            throw new AssertionError("Statistics should not be loaded for too few candidates");
        }).select();

        Assertions.assertTrue(result.isEmpty());
        Assertions.assertEquals(SelectionReason.TOO_FEW_CANDIDATES, result.getReason());
    }

    @Test
    public void testRejectHighSelectivityEvenWhenSavedBytesIsLarge() {
        OlapScanNode scanNode = scanNode(20_000_000_000L,
                eq(cheapSelectiveColumn),
                gt(lateStringColumn),
                gt(anotherLateColumn));
        SessionVariable sessionVariable = sessionVariable();
        Map<String, ColumnStatistic> stats = ImmutableMap.of(
                "tenant_id", stats(20_000_000_000L, 5, 4),
                "event_type", stats(20_000_000_000L, 10, 128),
                "payload", stats(20_000_000_000L, 10, 128));

        SelectionResult result = select(scanNode, sessionVariable, stats);

        Assertions.assertTrue(result.isEmpty());
        Assertions.assertEquals(SelectionReason.HIGH_SELECTIVITY, result.getReason());
        Assertions.assertTrue(result.getEstimatedSavedBytes() >= result.getMinEstimatedSavedBytes());
    }

    @Test
    public void testRejectLowSavedBytesByStrongGate() {
        OlapScanNode scanNode = scanNode(1_000_000L,
                eq(cheapSelectiveColumn),
                eq(lateStringColumn));
        SessionVariable sessionVariable = sessionVariable();
        sessionVariable.predicateLmMinScanRows = 0;
        Map<String, ColumnStatistic> stats = ImmutableMap.of(
                "tenant_id", stats(1_000_000L, 1000, 4),
                "event_type", stats(1_000_000L, 10, 16));

        SelectionResult result = select(scanNode, sessionVariable, stats);

        Assertions.assertTrue(result.isEmpty());
        Assertions.assertEquals(SelectionReason.LOW_SAVED_BYTES, result.getReason());
        Assertions.assertEquals(1L << 30, result.getMinEstimatedSavedBytes(), 0.1);
    }

    @Test
    public void testMinSavedBytesUsesLargerMinScanRowsGate() {
        OlapScanNode scanNode = scanNode(100_000_000_000L,
                eq(cheapSelectiveColumn),
                eq(lateStringColumn));
        SessionVariable sessionVariable = sessionVariable();
        sessionVariable.predicateLmMinScanRows = 100_000_000_000L;
        Map<String, ColumnStatistic> stats = ImmutableMap.of(
                "tenant_id", stats(100_000_000_000L, 1000, 4),
                "event_type", stats(100_000_000_000L, 10, 128));

        SelectionResult result = select(scanNode, sessionVariable, stats);

        Assertions.assertTrue(result.isEmpty());
        Assertions.assertEquals(SelectionReason.LOW_SAVED_BYTES, result.getReason());
        Assertions.assertEquals(12_800_000_000_000.0, result.getMinEstimatedSavedBytes(), 0.1);
    }

    private SelectionResult select(OlapScanNode scanNode, SessionVariable sessionVariable,
            Map<String, ColumnStatistic> stats) {
        return new PredicateLmStage1Selector(scanNode, sessionVariable,
                columnName -> stats.getOrDefault(columnName, ColumnStatistic.UNKNOWN)).select();
    }

    private SessionVariable sessionVariable() {
        SessionVariable sessionVariable = new SessionVariable();
        sessionVariable.enableMultiStagePredicateLm = true;
        sessionVariable.predicateLmStage1SurvivalRatioThreshold = 0.1;
        sessionVariable.predicateLmMinScanRows = 65536;
        return sessionVariable;
    }

    private Map<String, ColumnStatistic> defaultStats() {
        return ImmutableMap.of(
                "tenant_id", stats(20_000_000_000L, 1000, 4),
                "event_type", stats(20_000_000_000L, 10, 32));
    }

    private ColumnStatistic stats(double count, double ndv, double avgSizeByte) {
        return new ColumnStatisticBuilder(count)
                .setNdv(ndv)
                .setAvgSizeByte(avgSizeByte)
                .build();
    }

    private OlapScanNode scanNode(long cardinality, Expr... conjuncts) {
        OlapTable table = Mockito.mock(OlapTable.class);
        List<Column> schema = Lists.newArrayList(cheapSelectiveColumn, lateStringColumn, anotherLateColumn);
        Mockito.when(table.getBaseSchema()).thenReturn(schema);
        Mockito.when(table.getBaseIndexId()).thenReturn(1L);

        OlapScanNode scanNode = Mockito.mock(OlapScanNode.class);
        Mockito.when(scanNode.getOlapTable()).thenReturn(table);
        Mockito.when(scanNode.getSelectedIndexId()).thenReturn(-1L);
        Mockito.when(scanNode.getConjuncts()).thenReturn(Lists.newArrayList(conjuncts));
        Mockito.when(scanNode.getCardinality()).thenReturn(cardinality);
        return scanNode;
    }

    private Expr eq(Column column) {
        return new BinaryPredicate(BinaryPredicate.Operator.EQ, slot(column), new IntLiteral(1));
    }

    private Expr gt(Column column) {
        return new BinaryPredicate(BinaryPredicate.Operator.GT, slot(column), new IntLiteral(1));
    }

    private SlotRef slot(Column column) {
        SlotDescriptor descriptor = new SlotDescriptor(new SlotId(0), new TupleId(0));
        descriptor.setColumn(column);
        descriptor.setLabel(column.getName());
        SlotRef slotRef = new SlotRef(descriptor);
        slotRef.setCol(column.getName());
        return slotRef;
    }
}
