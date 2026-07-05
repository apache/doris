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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorOr;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.And;
import org.apache.doris.nereids.trees.expressions.Between;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Unit tests for {@link UnboundExpressionToConnectorPredicateConverter} (WS-REWRITE R7).
 *
 * <p><b>WHY this matters:</b> the {@code ALTER TABLE EXECUTE rewrite_data_files(...) WHERE <cond>} predicate
 * arrives UNBOUND ({@link UnboundSlot}, never analysed), so the bound-slot
 * {@code NereidsToConnectorExpressionConverter} would silently drop every leaf and rewrite the whole table.
 * This converter resolves each column by name against the target table and is strictly FAIL-LOUD — it throws
 * rather than emit a partial/empty predicate that would widen the rewrite scope. These tests pin that the
 * common WHERE shapes lower to a non-null neutral predicate (the regression guard) and that every
 * unrepresentable shape throws (the "never widen" guarantee).</p>
 */
public class UnboundExpressionToConnectorPredicateConverterTest {

    private static ExternalTable table() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        Mockito.when(table.getColumn("a")).thenReturn(new Column("a", ScalarType.INT));
        Mockito.when(table.getColumn("b")).thenReturn(new Column("b", ScalarType.INT));
        Mockito.when(table.getColumn("s")).thenReturn(new Column("s", ScalarType.createVarcharType(20)));
        // "c" is intentionally not stubbed -> getColumn("c") returns null (unknown column).
        return table;
    }

    private static UnboundSlot col(String name) {
        return new UnboundSlot(name);
    }

    // ---- common WHERE shapes lower to a non-null neutral predicate (the F2 regression guard) ----

    @Test
    public void unboundComparisonLowersToConnectorComparison() throws Exception {
        // The crux: an UnboundSlot comparison must NOT silently drop (that would scope the rewrite to the whole
        // table). It must produce a real ConnectorComparison with the column resolved by name + its real type.
        ConnectorPredicate predicate = UnboundExpressionToConnectorPredicateConverter.convert(
                new GreaterThan(col("a"), new IntegerLiteral(5)), table());
        ConnectorExpression expr = predicate.getExpression();
        Assertions.assertInstanceOf(ConnectorComparison.class, expr);
        ConnectorComparison cmp = (ConnectorComparison) expr;
        Assertions.assertEquals(ConnectorComparison.Operator.GT, cmp.getOperator());
        Assertions.assertInstanceOf(ConnectorColumnRef.class, cmp.getLeft());
        Assertions.assertEquals("a", ((ConnectorColumnRef) cmp.getLeft()).getColumnName());
        Assertions.assertInstanceOf(ConnectorLiteral.class, cmp.getRight());
        Assertions.assertEquals(5L, ((ConnectorLiteral) cmp.getRight()).getValue());
        // The column-ref type is the table column's real type (resolved from the schema), not a placeholder —
        // it equals what the analyzed-plan-side converter would produce for the same Doris type.
        Assertions.assertEquals(ExprToConnectorExpressionConverter.typeToConnectorType(ScalarType.INT),
                ((ConnectorColumnRef) cmp.getLeft()).getType());
    }

    @Test
    public void andLowersEveryConjunct() throws Exception {
        ConnectorExpression expr = UnboundExpressionToConnectorPredicateConverter.convert(
                new And(new EqualTo(col("a"), new IntegerLiteral(1)),
                        new EqualTo(col("b"), new IntegerLiteral(2))), table()).getExpression();
        Assertions.assertInstanceOf(ConnectorAnd.class, expr);
        Assertions.assertEquals(2, ((ConnectorAnd) expr).getConjuncts().size());
    }

    @Test
    public void inLowersToConnectorIn() throws Exception {
        ConnectorExpression expr = UnboundExpressionToConnectorPredicateConverter.convert(new InPredicate(
                col("a"), ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(2))), table()).getExpression();
        Assertions.assertInstanceOf(ConnectorIn.class, expr);
        Assertions.assertEquals("a", ((ConnectorColumnRef) ((ConnectorIn) expr).getValue()).getColumnName());
    }

    @Test
    public void isNullLowersToConnectorIsNull() throws Exception {
        ConnectorExpression expr = UnboundExpressionToConnectorPredicateConverter.convert(
                new IsNull(col("a")), table()).getExpression();
        Assertions.assertInstanceOf(ConnectorIsNull.class, expr);
    }

    @Test
    public void betweenLowersToConnectorBetween() throws Exception {
        ConnectorExpression expr = UnboundExpressionToConnectorPredicateConverter.convert(new Between(
                col("a"), new IntegerLiteral(1), new IntegerLiteral(9)), table()).getExpression();
        Assertions.assertInstanceOf(ConnectorBetween.class, expr);
    }

    @Test
    public void crossColumnOrIsRepresentedByFeCore() throws Exception {
        // The two-layer split: fe-core REPRESENTS a cross-column OR (it does not know iceberg's pushdown limits
        // — iron law). The connector's RewriteDataFilePlanner is the layer that rejects an un-pushable conjunct.
        // So fe-core must NOT throw here (only the connector does), or the layers would double-reject differently.
        ConnectorExpression expr = UnboundExpressionToConnectorPredicateConverter.convert(
                new Or(new EqualTo(col("a"), new IntegerLiteral(1)),
                        new EqualTo(col("b"), new IntegerLiteral(2))), table()).getExpression();
        Assertions.assertInstanceOf(ConnectorOr.class, expr);
        Assertions.assertEquals(2, ((ConnectorOr) expr).getDisjuncts().size());
    }

    // ---- fail-loud: an unrepresentable WHERE throws rather than widening the rewrite scope ----

    @Test
    public void unsupportedNodeThrows() {
        // A column-to-column comparison cannot be represented (no literal). Dropping it would leave an empty
        // predicate -> whole-table rewrite, so it must throw (legacy live-rewrite parity).
        Assertions.assertThrows(AnalysisException.class, () ->
                UnboundExpressionToConnectorPredicateConverter.convert(new EqualTo(col("a"), col("b")), table()));
    }

    @Test
    public void partialAndThrowsAllOrNothing() {
        // One good conjunct (a=1) and one unrepresentable (col-col). The converter must NOT keep just the good
        // one (that would widen the rewrite past the user's WHERE); it must fail the whole predicate.
        Assertions.assertThrows(AnalysisException.class, () ->
                UnboundExpressionToConnectorPredicateConverter.convert(
                        new And(new EqualTo(col("a"), new IntegerLiteral(1)),
                                new EqualTo(col("a"), col("b"))), table()));
    }

    @Test
    public void unknownColumnThrows() {
        AnalysisException e = Assertions.assertThrows(AnalysisException.class, () ->
                UnboundExpressionToConnectorPredicateConverter.convert(
                        new EqualTo(col("c"), new IntegerLiteral(1)), table()));
        Assertions.assertTrue(e.getMessage().contains("Column not found"),
                "an unknown column must fail loud with a clear message, not silently drop");
    }

    @Test
    public void multiPartColumnThrows() {
        // `t.a` (a qualified reference) is not a bare column name; legacy IcebergNereidsUtils.extractColumnName
        // rejected multi-part too. A silent drop here would again widen the rewrite.
        Assertions.assertThrows(AnalysisException.class, () ->
                UnboundExpressionToConnectorPredicateConverter.convert(
                        new EqualTo(new UnboundSlot("t", "a"), new IntegerLiteral(1)), table()));
    }
}
