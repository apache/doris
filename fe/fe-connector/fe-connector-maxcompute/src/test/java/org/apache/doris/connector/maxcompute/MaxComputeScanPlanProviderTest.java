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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import com.aliyun.odps.PartitionSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Guards {@link MaxComputeScanPlanProvider}'s pure helpers (the connector module has no
 * fe-core / Mockito, so these are exercised directly with no network or live ODPS).
 *
 * <p>Two concerns:</p>
 * <ul>
 *   <li>{@code toPartitionSpecs} — FIX-PRUNE-PUSHDOWN (DG-1): the bridge that turns the engine's
 *       pruned partition names into ODPS {@link PartitionSpec}s fed to the read session.</li>
 *   <li>{@code isLimitOptEnabled} / {@code shouldUseLimitOptimization} /
 *       {@code checkOnlyPartitionEquality} — FIX-LIMIT-SPLIT-DEFAULT (P3-9 / NG-5): the restored
 *       default-OFF three-gate for the LIMIT-split optimization, mirroring legacy
 *       {@code MaxComputeScanNode}'s {@code enableMcLimitSplitOptimization &&
 *       onlyPartitionEqualityPredicate && hasLimit()}. <b>Why this matters:</b> the optimization
 *       collapses the scan into a single row-offset split, so it must fire ONLY when the user
 *       opted in AND every row in the (pruned) partitions qualifies (no filter, or pure
 *       partition-column equality) — otherwise it would silently change query planning and, on a
 *       residual row-level filter, under-read.</li>
 * </ul>
 */
public class MaxComputeScanPlanProviderTest {

    // Literal var-name key — intentionally NOT the prod constant, so a prod-side typo in
    // MaxComputeScanPlanProvider.ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION (or drift from
    // SessionVariable.ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION) is caught here.
    private static final String VAR_KEY = "enable_mc_limit_split_optimization";

    private static final Set<String> PART_COLS = new HashSet<>(Arrays.asList("pt", "region"));

    private static ConnectorColumnRef col(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("INT"));
    }

    private static ConnectorComparison eq(ConnectorExpression left, ConnectorExpression right) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ, left, right);
    }

    // ---- toPartitionSpecs (FIX-PRUNE-PUSHDOWN) ----

    @Test
    public void testNullInputMeansScanAll() {
        Assertions.assertTrue(MaxComputeScanPlanProvider.toPartitionSpecs(null).isEmpty());
    }

    @Test
    public void testEmptyInputMeansScanAll() {
        Assertions.assertTrue(
                MaxComputeScanPlanProvider.toPartitionSpecs(Collections.emptyList()).isEmpty());
    }

    @Test
    public void testConvertsPartitionNamesToSpecs() {
        List<PartitionSpec> specs = MaxComputeScanPlanProvider.toPartitionSpecs(
                Arrays.asList("pt=1", "pt=2,region=cn"));

        Assertions.assertEquals(2, specs.size());

        PartitionSpec single = specs.get(0);
        Assertions.assertEquals(Collections.singleton("pt"), single.keys());
        Assertions.assertEquals("1", single.get("pt"));

        PartitionSpec multi = specs.get(1);
        Assertions.assertEquals("2", multi.get("pt"));
        Assertions.assertEquals("cn", multi.get("region"));
    }

    // ---- isLimitOptEnabled — gate (1): session var, default OFF ----

    @Test
    public void testLimitOptDisabledWhenVarAbsent() {
        // No SET → var not in the session-property map → default OFF (legacy default).
        Assertions.assertFalse(MaxComputeScanPlanProvider.isLimitOptEnabled(new HashMap<>()));
    }

    @Test
    public void testLimitOptEnabledWhenVarTrue() {
        Map<String, String> props = new HashMap<>();
        props.put(VAR_KEY, "true");
        Assertions.assertTrue(MaxComputeScanPlanProvider.isLimitOptEnabled(props));
    }

    @Test
    public void testLimitOptDisabledWhenVarFalse() {
        Map<String, String> props = new HashMap<>();
        props.put(VAR_KEY, "false");
        Assertions.assertFalse(MaxComputeScanPlanProvider.isLimitOptEnabled(props));
    }

    // ---- shouldUseLimitOptimization — gate composition ----

    @Test
    public void testGateClosedWhenVarDisabled() {
        // Gate (1) off: even with a LIMIT and no filter, the opt stays off.
        Assertions.assertFalse(MaxComputeScanPlanProvider.shouldUseLimitOptimization(
                false, 10, Optional.empty(), PART_COLS));
    }

    @Test
    public void testGateClosedWhenNoLimit() {
        // Gate (3) off: enabled var but limit <= 0.
        Assertions.assertFalse(MaxComputeScanPlanProvider.shouldUseLimitOptimization(
                true, 0, Optional.empty(), PART_COLS));
    }

    @Test
    public void testGateOpenWhenEnabledLimitAndNoFilter() {
        // Enabled + LIMIT + no predicate → every row qualifies → eligible.
        Assertions.assertTrue(MaxComputeScanPlanProvider.shouldUseLimitOptimization(
                true, 10, Optional.empty(), PART_COLS));
    }

    @Test
    public void testGateOpenWhenEnabledLimitAndPartitionEquality() {
        ConnectorExpression filter = eq(col("pt"), ConnectorLiteral.ofInt(1));
        Assertions.assertTrue(MaxComputeScanPlanProvider.shouldUseLimitOptimization(
                true, 10, Optional.of(filter), PART_COLS));
    }

    @Test
    public void testGateClosedWhenEnabledLimitButNonPartitionFilter() {
        ConnectorExpression filter = eq(col("data_col"), ConnectorLiteral.ofInt(5));
        Assertions.assertFalse(MaxComputeScanPlanProvider.shouldUseLimitOptimization(
                true, 10, Optional.of(filter), PART_COLS));
    }

    // ---- checkOnlyPartitionEquality — gate (2): predicate shapes ----

    @Test
    public void testSinglePartitionEqualityEligible() {
        ConnectorExpression filter = eq(col("pt"), ConnectorLiteral.ofInt(1));
        Assertions.assertTrue(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testPartitionInListEligible() {
        ConnectorExpression filter = new ConnectorIn(col("region"),
                Arrays.asList(ConnectorLiteral.ofString("cn"), ConnectorLiteral.ofString("us")),
                false);
        Assertions.assertTrue(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testAndOfPartitionEqualitiesEligible() {
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                eq(col("pt"), ConnectorLiteral.ofInt(1)),
                eq(col("region"), ConnectorLiteral.ofString("cn"))));
        Assertions.assertTrue(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testAndWithNonPartitionConjunctIneligible() {
        // One conjunct on a data column → the whole AND is ineligible (legacy parity).
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                eq(col("pt"), ConnectorLiteral.ofInt(1)),
                eq(col("data_col"), ConnectorLiteral.ofInt(5))));
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testDataColumnEqualityIneligible() {
        ConnectorExpression filter = eq(col("data_col"), ConnectorLiteral.ofInt(5));
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testNonEqOperatorOnPartitionIneligible() {
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.GT, col("pt"), ConnectorLiteral.ofInt(1));
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testNotInOnPartitionIneligible() {
        ConnectorExpression filter = new ConnectorIn(col("pt"),
                Arrays.asList(ConnectorLiteral.ofInt(1), ConnectorLiteral.ofInt(2)),
                true);
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testInWithNonLiteralElementIneligible() {
        ConnectorExpression filter = new ConnectorIn(col("pt"),
                Arrays.asList(ConnectorLiteral.ofInt(1), col("region")),
                false);
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testLiteralOnLeftIneligible() {
        // Mirror legacy: only `col = literal`, not `literal = col`.
        ConnectorExpression filter = eq(ConnectorLiteral.ofInt(1), col("pt"));
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testPartitionColumnEqualsPartitionColumnIneligible() {
        // `pt = region`: left is a valid partition col-ref (reaches the RHS check), but the RHS
        // is a column-ref, not a literal → ineligible. Guards the right-side literal check
        // (legacy MaxComputeScanNode:346 requires child(1) instanceof LiteralExpr).
        ConnectorExpression filter = eq(col("pt"), col("region"));
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testInValueDataColumnIneligible() {
        // `data_col IN ('a','b')`: the IN value column is NOT a partition column → ineligible.
        // Guards the IN-value partition-column check (legacy MaxComputeScanNode:358-364 requires
        // child(0) be a partition-column SlotRef). Without this guard a residual data-column IN
        // filter would wrongly enable the single-split row-offset path and silently under-read.
        ConnectorExpression filter = new ConnectorIn(col("data_col"),
                Arrays.asList(ConnectorLiteral.ofString("a"), ConnectorLiteral.ofString("b")),
                false);
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testEqForNullOnPartitionIneligible() {
        // `pt <=> 1` (EQ_FOR_NULL): only plain EQ is eligible (legacy requires Operator.EQ).
        ConnectorExpression filter = new ConnectorComparison(
                ConnectorComparison.Operator.EQ_FOR_NULL, col("pt"), ConnectorLiteral.ofInt(1));
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testBothLiteralsComparisonIneligible() {
        // `1 = 2`: left is not a column-ref → ineligible.
        ConnectorExpression filter = eq(ConnectorLiteral.ofInt(1), ConnectorLiteral.ofInt(2));
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testAndContainingNonLeafConjunctIneligible() {
        // `pt=1 AND (pt=1 OR region='cn')`: the OR conjunct is neither a comparison nor an IN →
        // isPartitionEqualityLeaf rejects it → the whole AND is ineligible.
        ConnectorExpression or = new ConnectorOr(Arrays.asList(
                eq(col("pt"), ConnectorLiteral.ofInt(1)),
                eq(col("region"), ConnectorLiteral.ofString("cn"))));
        ConnectorExpression filter = new ConnectorAnd(Arrays.asList(
                eq(col("pt"), ConnectorLiteral.ofInt(1)), or));
        Assertions.assertFalse(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    @Test
    public void testEmptyInListMatchesLegacyEligible() {
        // `pt IN ()` on a partition column → eligible (the all-literal loop is vacuously true).
        // Mirrors legacy MaxComputeScanNode:365 (its literal loop is also vacuous on an empty
        // list). Unreachable in practice — Nereids folds an empty IN to FALSE before pushdown —
        // and the converted filterPredicate is still applied to the read session as a backstop.
        // Pinned to document the deliberate legacy-parity choice.
        ConnectorExpression filter = new ConnectorIn(col("pt"),
                Collections.emptyList(), false);
        Assertions.assertTrue(
                MaxComputeScanPlanProvider.checkOnlyPartitionEquality(filter, PART_COLS));
    }

    // ---- reject reading ODPS external tables / logical views ----
    // Migrated from MaxComputeScanNode.getSplits / MaxComputeScanNodeTest (PR apache/doris#64119).
    // planScan now gates via MaxComputeTableHandle.checkOperationSupported("Reading") before any
    // split generation; the ODPS Storage API cannot scan external tables or logical views. The guard
    // is exercised directly here (the connector test module has no Mockito to fake an ODPS Table).

    @Test
    public void testReadRejectsOdpsExternalTable() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> MaxComputeTableHandle.checkOperationSupported(
                        true, false, "Reading", "default", "mc_external_table"));
        Assertions.assertTrue(ex.getMessage().contains(
                "Reading MaxCompute external table or logical view is not supported: "
                        + "default.mc_external_table"),
                "got: " + ex.getMessage());
    }

    @Test
    public void testReadRejectsOdpsLogicalView() {
        DorisConnectorException ex = Assertions.assertThrows(DorisConnectorException.class,
                () -> MaxComputeTableHandle.checkOperationSupported(
                        false, true, "Reading", "default", "mc_logical_view"));
        Assertions.assertTrue(ex.getMessage().contains(
                "Reading MaxCompute external table or logical view is not supported: "
                        + "default.mc_logical_view"),
                "got: " + ex.getMessage());
    }

    @Test
    public void testReadAllowsManagedTable() {
        // a normal (non-external, non-view) table must not be rejected (guards against over-rejection)
        Assertions.assertDoesNotThrow(() -> MaxComputeTableHandle.checkOperationSupported(
                false, false, "Reading", "default", "mc_managed_table"));
    }
}
