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

package org.apache.doris.datasource.connector.converter;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorOr;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.GreaterThanEqual;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.types.StringType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Unit tests for the reverse {@link ConnectorExpressionToNereidsConverter}: it turns a connector-neutral
 * residual predicate (e.g. hudi's incremental {@code _hoodie_commit_time > c1 AND <= c2}) back into a bound
 * Nereids {@link Expression}, binding column refs to scan-output slots by name.
 *
 * <p>The converter is TOTAL + FAIL-LOUD (the inverse of the forward converter's safe drop-on-unknown): dropping
 * a conjunct from a scan residual predicate would UNDER-filter and leak rows, so anything outside the supported
 * grammar throws rather than returning null. These tests pin both the happy conversion and every fail-loud arm.</p>
 */
public class ConnectorExpressionToNereidsConverterTest {

    private static final ConnectorType STRING = ConnectorType.of("STRING");

    private final SlotReference commitTime = new SlotReference("_hoodie_commit_time", StringType.INSTANCE);

    private Map<String, SlotReference> slots() {
        Map<String, SlotReference> m = new HashMap<>();
        m.put("_hoodie_commit_time", commitTime);
        return m;
    }

    private static ConnectorComparison cmp(ConnectorComparison.Operator op, String bound) {
        return new ConnectorComparison(op,
                new ConnectorColumnRef("_hoodie_commit_time", STRING), ConnectorLiteral.ofString(bound));
    }

    @Test
    public void convertsComparisonBindingColumnRefToTheScanSlot() {
        Expression result = ConnectorExpressionToNereidsConverter.convert(
                cmp(ConnectorComparison.Operator.GT, "c1"), slots());
        Assertions.assertTrue(result instanceof GreaterThan, "GT must map to a Nereids GreaterThan");
        Assertions.assertSame(commitTime, result.child(0),
                "the column ref must bind to the SAME scan-output slot instance (a fresh slot would be unbound)");
        Assertions.assertEquals(new StringLiteral("c1"), result.child(1),
                "the bound must be a STRING literal so the compare is lexicographic over instants");
    }

    @Test
    public void mapsEachOrderEqualityOperatorToItsNereidsNode() {
        Assertions.assertTrue(convert(ConnectorComparison.Operator.EQ) instanceof EqualTo);
        Assertions.assertTrue(convert(ConnectorComparison.Operator.LT) instanceof LessThan);
        Assertions.assertTrue(convert(ConnectorComparison.Operator.LE) instanceof LessThanEqual);
        Assertions.assertTrue(convert(ConnectorComparison.Operator.GT) instanceof GreaterThan);
        Assertions.assertTrue(convert(ConnectorComparison.Operator.GE) instanceof GreaterThanEqual);
    }

    private Expression convert(ConnectorComparison.Operator op) {
        return ConnectorExpressionToNereidsConverter.convert(cmp(op, "c1"), slots());
    }

    @Test
    public void convertsConnectorAndToNereidsAnd() {
        ConnectorExpression window = new ConnectorAnd(Arrays.asList(
                cmp(ConnectorComparison.Operator.GT, "c1"), cmp(ConnectorComparison.Operator.LE, "c2")));
        Expression result = ConnectorExpressionToNereidsConverter.convert(window, slots());
        Assertions.assertTrue(result instanceof And, "a ConnectorAnd must map to a Nereids And");
        Assertions.assertEquals(2, result.children().size(), "the And must keep both conjuncts");
        Assertions.assertTrue(result.child(0) instanceof GreaterThan);
        Assertions.assertTrue(result.child(1) instanceof LessThanEqual);
    }

    @Test
    public void singleConjunctConnectorAndIsUnwrapped() {
        // Nereids And(List) rejects < 2 children, so a 1-element ConnectorAnd must return the bare conjunct.
        ConnectorExpression single = new ConnectorAnd(
                Collections.singletonList(cmp(ConnectorComparison.Operator.GT, "c1")));
        Expression result = ConnectorExpressionToNereidsConverter.convert(single, slots());
        Assertions.assertTrue(result instanceof GreaterThan, "a single-conjunct AND must unwrap, not build And(1)");
    }

    @Test
    public void failsLoudWhenColumnNotInScanOutput() {
        // Dropping a residual predicate whose column is absent would UNDER-filter (leak out-of-window rows) — the
        // inverse of the forward converter's safe drop. So an unresolvable column MUST throw, not no-op.
        Assertions.assertThrows(AnalysisException.class, () -> ConnectorExpressionToNereidsConverter.convert(
                cmp(ConnectorComparison.Operator.GT, "c1"), Collections.emptyMap()));
    }

    @Test
    public void failsLoudOnUnsupportedNode() {
        ConnectorExpression or = new ConnectorOr(Arrays.asList(
                cmp(ConnectorComparison.Operator.GT, "c1"), cmp(ConnectorComparison.Operator.LE, "c2")));
        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorExpressionToNereidsConverter.convert(or, slots()));
    }

    @Test
    public void failsLoudOnNonStringLiteral() {
        // A numeric literal must NOT be silently coerced to a string (would change lexicographic compare).
        ConnectorExpression gt = new ConnectorComparison(ConnectorComparison.Operator.GT,
                new ConnectorColumnRef("_hoodie_commit_time", STRING), ConnectorLiteral.ofInt(42));
        Assertions.assertThrows(AnalysisException.class,
                () -> ConnectorExpressionToNereidsConverter.convert(gt, slots()));
    }

    @Test
    public void failsLoudOnUnsupportedComparisonOperator() {
        Assertions.assertThrows(AnalysisException.class, () -> ConnectorExpressionToNereidsConverter.convert(
                cmp(ConnectorComparison.Operator.NE, "c1"), slots()));
    }
}
