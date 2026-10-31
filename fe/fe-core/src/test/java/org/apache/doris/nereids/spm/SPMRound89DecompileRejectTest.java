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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.spm.builder.SPMPlan2SQLBuilder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

/**
 * Round-89 review fix: the frozen SQL must never turn an IN / EXISTS / scalar SUBQUERY into a
 * user-visible function call.
 *
 * <p>{@code SPMExprSqlBuilder#visitSubqueryExpr} has no SQL rendering for a subquery - it emits the
 * structural digest markers {@code INSUBQUERY(compare)[...]}, {@code EXISTS[...]},
 * {@code SCALARSUBQUERY[...]}. Stored as frozen SQL that text is an ordinary, unreserved function
 * call: a user alias UDF of the same name with compatible arguments made capture-time validation
 * succeed, the baseline was stored with {@code planFrozen = true}, and the replay evaluated that
 * UDF instead of the original subquery. The decompiler now refuses any residual subquery, and the
 * marker names are reserved.
 */
public class SPMRound89DecompileRejectTest {

    /** A plan whose expression still carries a subquery is never frozen as text. */
    @Test
    public void testResidualSubqueryIsNotFreezable() {
        SubqueryExpr subquery = Mockito.mock(SubqueryExpr.class);
        Mockito.doReturn(List.of()).when(subquery).children();
        LogicalPlan plan = planOf(List.of(subquery));

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> SPMPlan2SQLBuilder.rejectSubqueryExpressions(plan),
                "a residual subquery has no SQL rendering: it must be refused");
    }

    /** A NESTED subquery (inside another expression) is refused as well. */
    @Test
    public void testNestedSubqueryIsNotFreezable() {
        SubqueryExpr subquery = Mockito.mock(SubqueryExpr.class);
        Mockito.doReturn(List.of()).when(subquery).children();
        // an outer expression that carries the subquery as one of its children
        Expression outer = Mockito.mock(Expression.class);
        Mockito.doReturn(List.of(subquery)).when(outer).children();
        LogicalPlan plan = planOf(List.of(outer));

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> SPMPlan2SQLBuilder.rejectSubqueryExpressions(plan));
    }

    /** An ordinary expression plan passes the check (no false refusal). */
    @Test
    public void testPlainExpressionIsFreezable() {
        LogicalPlan plan = planOf(List.of(new UnboundFunction("abs",
                List.of(new IntegerLiteral(1)))));
        Assertions.assertDoesNotThrow(() -> SPMPlan2SQLBuilder.rejectSubqueryExpressions(plan));
    }

    /**
     * The pseudo names are RESERVED: a plan that calls a USER function of one of them keeps the
     * baseline on the parameterized-plan-tree path instead of producing text that means two
     * different things.
     */
    @Test
    public void testUserFunctionUnderAnInternalNameIsNotFreezable() {
        LogicalPlan plan = planOf(List.of(new UnboundFunction("insubquery",
                List.of(new IntegerLiteral(1)))));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> SPMPlan2SQLBuilder.rejectReservedInternalFunctionNames(plan),
                "an internal pseudo name is reserved for SPM");
    }

    /** The reservation is by NAME, so a QUALIFIED call of one of them is refused too. */
    @Test
    public void testQualifiedInternalNameIsNotFreezable() {
        LogicalPlan plan = planOf(List.of(new UnboundFunction("db1", "scalarSubquery",
                List.of(new IntegerLiteral(1)))));
        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> SPMPlan2SQLBuilder.rejectReservedInternalFunctionNames(plan));
    }

    /** The SPM placeholders themselves are not user functions and pass the reservation check. */
    @Test
    public void testPlaceholderCallsPassTheReservationCheck() {
        LogicalPlan plan = planOf(List.of(new UnboundFunction("abs",
                List.of(new IntegerLiteral(1)))));
        Assertions.assertDoesNotThrow(
                () -> SPMPlan2SQLBuilder.rejectReservedInternalFunctionNames(plan));
    }

    private static LogicalPlan planOf(List<Expression> expressions) {
        LogicalPlan plan = Mockito.mock(LogicalPlan.class);
        Mockito.doReturn(expressions).when(plan).getExpressions();
        Mockito.doReturn(List.of()).when(plan).children();
        return plan;
    }
}
