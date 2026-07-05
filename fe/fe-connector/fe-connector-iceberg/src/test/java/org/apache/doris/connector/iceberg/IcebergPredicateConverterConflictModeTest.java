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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.pushdown.ConnectorBetween;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorIsNull;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorNot;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;

/**
 * Conflict-mode (O5-2 write-time conflict detection) tests for {@link IcebergPredicateConverter}, the
 * connector consume-side of P6.3-T07b. The 3-arg {@code conflictMode=true} constructor selects a port of
 * legacy {@code IcebergConflictDetectionFilterUtils.convertPredicateToIcebergExpression}: a strictly
 * different matrix from scan pushdown. Assertions encode the BE-irrelevant but iceberg-wire-relevant contract
 * (which forms push, which drop, and to what shape), so a behavior drift turns the test red.
 *
 * <p>The last two tests pin that the {@code conflictMode=false} (default, 2-arg) path is unchanged — the flag
 * is the only thing that flips IS NULL / BETWEEN from "dropped" (scan) to "pushed" (conflict).</p>
 */
public class IcebergPredicateConverterConflictModeTest {

    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "c_int", Types.IntegerType.get()),
            Types.NestedField.required(2, "c_long", Types.LongType.get()),
            Types.NestedField.required(3, "c_str", Types.StringType.get()),
            Types.NestedField.optional(4, "c_list", Types.ListType.ofOptional(5, Types.IntegerType.get())),
            Types.NestedField.optional(6, "c_uuid", Types.UUIDType.get()));

    private static IcebergPredicateConverter conflict() {
        return new IcebergPredicateConverter(SCHEMA, ZoneOffset.UTC, true);
    }

    private static IcebergPredicateConverter scan() {
        return new IcebergPredicateConverter(SCHEMA, ZoneOffset.UTC);
    }

    private static ConnectorColumnRef col(String name) {
        return new ConnectorColumnRef(name, ConnectorType.of("UNKNOWN"));
    }

    private static ConnectorLiteral intLit(long v) {
        return new ConnectorLiteral(ConnectorType.of("INT"), v);
    }

    private static ConnectorComparison cmp(ConnectorComparison.Operator op, String c, ConnectorLiteral lit) {
        return new ConnectorComparison(op, col(c), lit);
    }

    private static String pushed(IcebergPredicateConverter conv, ConnectorExpression expr) {
        List<Expression> out = conv.convert(expr);
        Assertions.assertEquals(1, out.size(), "expected exactly one pushed conflict expression");
        return out.get(0).toString();
    }

    private static void dropped(IcebergPredicateConverter conv, ConnectorExpression expr) {
        Assertions.assertTrue(conv.convert(expr).isEmpty(), "expected the predicate to be dropped");
    }

    // ---- comparisons ----

    @Test
    public void comparisonOperatorsPushed() {
        Assertions.assertEquals(Expressions.equal("c_int", 1).toString(),
                pushed(conflict(), cmp(ConnectorComparison.Operator.EQ, "c_int", intLit(1))));
        Assertions.assertEquals(Expressions.greaterThan("c_int", 1).toString(),
                pushed(conflict(), cmp(ConnectorComparison.Operator.GT, "c_int", intLit(1))));
        Assertions.assertEquals(Expressions.greaterThanOrEqual("c_int", 1).toString(),
                pushed(conflict(), cmp(ConnectorComparison.Operator.GE, "c_int", intLit(1))));
        Assertions.assertEquals(Expressions.lessThan("c_int", 1).toString(),
                pushed(conflict(), cmp(ConnectorComparison.Operator.LT, "c_int", intLit(1))));
        Assertions.assertEquals(Expressions.lessThanOrEqual("c_int", 1).toString(),
                pushed(conflict(), cmp(ConnectorComparison.Operator.LE, "c_int", intLit(1))));
    }

    @Test
    public void notEqualAndEqForNullOperatorsDropped() {
        // legacy conflict matrix has no NE and no NullSafeEqual case
        dropped(conflict(), cmp(ConnectorComparison.Operator.NE, "c_int", intLit(1)));
        dropped(conflict(), cmp(ConnectorComparison.Operator.EQ_FOR_NULL, "c_int", intLit(1)));
    }

    @Test
    public void comparisonAgainstNullLiteralBecomesIsNull() {
        // legacy convertNereidsBinaryPredicate: any of EQ/GT/GE/LT/LE against NULL -> IS NULL
        ConnectorLiteral nullLit = ConnectorLiteral.ofNull(ConnectorType.of("INT"));
        Assertions.assertEquals(Expressions.isNull("c_int").toString(),
                pushed(conflict(), cmp(ConnectorComparison.Operator.EQ, "c_int", nullLit)));
        Assertions.assertEquals(Expressions.isNull("c_int").toString(),
                pushed(conflict(), cmp(ConnectorComparison.Operator.GT, "c_int", nullLit)));
    }

    // ---- IS NULL / NOT(IS NULL) ----

    @Test
    public void isNullPushed() {
        Assertions.assertEquals(Expressions.isNull("c_int").toString(),
                pushed(conflict(), new ConnectorIsNull(col("c_int"), false)));
    }

    @Test
    public void notIsNullPushed() {
        Assertions.assertEquals(Expressions.not(Expressions.isNull("c_int")).toString(),
                pushed(conflict(), new ConnectorNot(new ConnectorIsNull(col("c_int"), false))));
    }

    @Test
    public void notOfComparisonDropped() {
        // legacy restricts NOT to NOT(IS NULL); NOT(comparison) is dropped
        dropped(conflict(), new ConnectorNot(cmp(ConnectorComparison.Operator.EQ, "c_int", intLit(1))));
    }

    // ---- BETWEEN ----

    @Test
    public void betweenDecomposesToGreaterEqualAndLessEqual() {
        Expression expected = Expressions.and(
                Expressions.greaterThanOrEqual("c_int", 1), Expressions.lessThanOrEqual("c_int", 9));
        Assertions.assertEquals(expected.toString(),
                pushed(conflict(), new ConnectorBetween(col("c_int"), intLit(1), intLit(9))));
    }

    // ---- OR same-column guard ----

    @Test
    public void sameColumnOrPushed() {
        Expression expected = Expressions.or(Expressions.equal("c_int", 1), Expressions.equal("c_int", 2));
        Assertions.assertEquals(expected.toString(), pushed(conflict(), new ConnectorOr(Arrays.asList(
                cmp(ConnectorComparison.Operator.EQ, "c_int", intLit(1)),
                cmp(ConnectorComparison.Operator.EQ, "c_int", intLit(2))))));
    }

    @Test
    public void crossColumnOrDropped() {
        // dropping an OR arm would narrow the conflict filter -> missed conflict; legacy drops cross-column OR
        dropped(conflict(), new ConnectorOr(Arrays.asList(
                cmp(ConnectorComparison.Operator.EQ, "c_int", intLit(1)),
                cmp(ConnectorComparison.Operator.EQ, "c_long", intLit(2)))));
    }

    // ---- IN ----

    @Test
    public void inPushed() {
        Expression expected = Expressions.in("c_int", Arrays.asList(1, 2));
        Assertions.assertEquals(expected.toString(), pushed(conflict(),
                new ConnectorIn(col("c_int"), Arrays.asList(intLit(1), intLit(2)), false)));
    }

    @Test
    public void inWithNullElementAddsIsNull() {
        Expression expected = Expressions.or(
                Expressions.isNull("c_int"), Expressions.in("c_int", Arrays.asList(1)));
        Assertions.assertEquals(expected.toString(), pushed(conflict(), new ConnectorIn(col("c_int"),
                Arrays.asList(intLit(1), ConnectorLiteral.ofNull(ConnectorType.of("INT"))), false)));
    }

    @Test
    public void notInDropped() {
        dropped(conflict(), new ConnectorIn(col("c_int"), Arrays.asList(intLit(1)), true));
    }

    // ---- structural / UUID / metadata / unknown guards ----

    @Test
    public void structuralColumnIsNullDropped() {
        dropped(conflict(), new ConnectorIsNull(col("c_list"), false));
    }

    @Test
    public void uuidNonNullComparisonDroppedButNullBecomesIsNull() {
        dropped(conflict(), cmp(ConnectorComparison.Operator.EQ, "c_uuid",
                new ConnectorLiteral(ConnectorType.of("STRING"), "00000000-0000-0000-0000-000000000001")));
        Assertions.assertEquals(Expressions.isNull("c_uuid").toString(), pushed(conflict(),
                cmp(ConnectorComparison.Operator.EQ, "c_uuid", ConnectorLiteral.ofNull(ConnectorType.of("STRING")))));
    }

    @Test
    public void bareBooleanLiteralDropped() {
        dropped(conflict(), new ConnectorLiteral(ConnectorType.of("BOOLEAN"), true));
    }

    @Test
    public void metadataAndUnknownColumnsDropped() {
        dropped(conflict(), new ConnectorIsNull(col("_row_id"), false));
        dropped(conflict(), cmp(ConnectorComparison.Operator.EQ, "nope", intLit(1)));
    }

    // ---- regression: the default (scan) mode is unchanged; the flag is what flips behavior ----

    @Test
    public void scanModeStillDropsIsNullAndBetween() {
        dropped(scan(), new ConnectorIsNull(col("c_int"), false));
        dropped(scan(), new ConnectorBetween(col("c_int"), intLit(1), intLit(9)));
    }

    @Test
    public void scanModeEqualityStillPushed() {
        Assertions.assertEquals(Expressions.equal("c_int", 1).toString(),
                pushed(scan(), cmp(ConnectorComparison.Operator.EQ, "c_int", intLit(1))));
    }
}
