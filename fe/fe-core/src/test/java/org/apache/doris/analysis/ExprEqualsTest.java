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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.Type;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

// Covers equals()/hashCode() for non-literal Expr subclasses that override
// equals. Expr.equals already checks getClass() and recursive children
// equality; subclasses additionally compare their distinguishing fields
// (operator, isNot* flag, name, etc.). These tests pin both pieces.
//
// Skipped:
// - BetweenPredicate / SearchPredicate: no usable public constructor for unit
//   tests (BetweenPredicate is rewritten away pre-execution, SearchPredicate
//   needs a parsed QsPlan).
// - ColumnRefExpr / EncryptKeyRef / LambdaFunctionExpr / TimestampArithmeticExpr:
//   no equals override (inherit Expr.equals as-is, already exercised via the
//   subclass tests below through children comparison).
public class ExprEqualsTest {

    private static IntLiteral intLit(long v) {
        return new IntLiteral(v);
    }

    @Nested
    class ArithmeticExprEquals {
        private ArithmeticExpr make(ArithmeticExpr.Operator op, long l, long r) {
            return new ArithmeticExpr(op, intLit(l), intLit(r),
                    Type.BIGINT, NullableMode.DEPEND_ON_ARGUMENT);
        }

        @Test
        void sameOpAndChildren() {
            Assertions.assertEquals(
                    make(ArithmeticExpr.Operator.ADD, 1, 2),
                    make(ArithmeticExpr.Operator.ADD, 1, 2));
        }

        @Test
        void differentOp() {
            Assertions.assertNotEquals(
                    make(ArithmeticExpr.Operator.ADD, 1, 2),
                    make(ArithmeticExpr.Operator.SUBTRACT, 1, 2));
        }

        @Test
        void differentChildren() {
            Assertions.assertNotEquals(
                    make(ArithmeticExpr.Operator.ADD, 1, 2),
                    make(ArithmeticExpr.Operator.ADD, 1, 3));
        }

        @Test
        void hashCodeMatchesEquality() {
            Assertions.assertEquals(
                    make(ArithmeticExpr.Operator.ADD, 1, 2).hashCode(),
                    make(ArithmeticExpr.Operator.ADD, 1, 2).hashCode());
        }
    }

    @Nested
    class BinaryPredicateEquals {
        @Test
        void sameOpAndChildren() {
            Assertions.assertEquals(
                    new BinaryPredicate(BinaryPredicate.Operator.EQ, intLit(1), intLit(2)),
                    new BinaryPredicate(BinaryPredicate.Operator.EQ, intLit(1), intLit(2)));
        }

        @Test
        void differentOp() {
            Assertions.assertNotEquals(
                    new BinaryPredicate(BinaryPredicate.Operator.EQ, intLit(1), intLit(2)),
                    new BinaryPredicate(BinaryPredicate.Operator.NE, intLit(1), intLit(2)));
        }

        @Test
        void differentChildren() {
            Assertions.assertNotEquals(
                    new BinaryPredicate(BinaryPredicate.Operator.EQ, intLit(1), intLit(2)),
                    new BinaryPredicate(BinaryPredicate.Operator.EQ, intLit(1), intLit(3)));
        }
    }

    @Nested
    class CastExprEquals {
        @Test
        void sameTargetTypeAndChild() {
            Assertions.assertEquals(
                    new CastExpr(Type.BIGINT, intLit(1), (Void) null),
                    new CastExpr(Type.BIGINT, intLit(1), (Void) null));
        }

        @Test
        void differentChild() {
            Assertions.assertNotEquals(
                    new CastExpr(Type.BIGINT, intLit(1), (Void) null),
                    new CastExpr(Type.BIGINT, intLit(2), (Void) null));
        }
    }

    @Nested
    class CompoundPredicateEquals {
        private CompoundPredicate make(CompoundPredicate.Operator op) {
            BinaryPredicate left = new BinaryPredicate(
                    BinaryPredicate.Operator.EQ, intLit(1), intLit(1));
            BinaryPredicate right = new BinaryPredicate(
                    BinaryPredicate.Operator.EQ, intLit(2), intLit(2));
            return new CompoundPredicate(op, left, right);
        }

        @Test
        void sameOpAndChildren() {
            Assertions.assertEquals(make(CompoundPredicate.Operator.AND),
                    make(CompoundPredicate.Operator.AND));
        }

        @Test
        void differentOp() {
            Assertions.assertNotEquals(make(CompoundPredicate.Operator.AND),
                    make(CompoundPredicate.Operator.OR));
        }
    }

    @Nested
    class FunctionCallExprEquals {
        @Test
        void sameNameAndArgs() {
            Assertions.assertEquals(
                    new FunctionCallExpr("abs", ImmutableList.of(intLit(1))),
                    new FunctionCallExpr("abs", ImmutableList.of(intLit(1))));
        }

        @Test
        void differentFunctionName() {
            Assertions.assertNotEquals(
                    new FunctionCallExpr("abs", ImmutableList.of(intLit(1))),
                    new FunctionCallExpr("ceil", ImmutableList.of(intLit(1))));
        }

        @Test
        void differentArgs() {
            Assertions.assertNotEquals(
                    new FunctionCallExpr("abs", ImmutableList.of(intLit(1))),
                    new FunctionCallExpr("abs", ImmutableList.of(intLit(2))));
        }
    }

    @Nested
    class InformationFunctionEquals {
        @Test
        void sameFuncType() {
            Assertions.assertEquals(new InformationFunction("CURRENT_USER"),
                    new InformationFunction("CURRENT_USER"));
        }

        @Test
        void differentFuncType() {
            Assertions.assertNotEquals(new InformationFunction("CURRENT_USER"),
                    new InformationFunction("DATABASE"));
        }
    }

    @Nested
    class InPredicateEquals {
        @Test
        void sameContent() {
            Assertions.assertEquals(
                    new InPredicate(intLit(0),
                            ImmutableList.of(intLit(1), intLit(2)), false),
                    new InPredicate(intLit(0),
                            ImmutableList.of(intLit(1), intLit(2)), false));
        }

        @Test
        void differentIsNotIn() {
            Assertions.assertNotEquals(
                    new InPredicate(intLit(0),
                            ImmutableList.of(intLit(1), intLit(2)), false),
                    new InPredicate(intLit(0),
                            ImmutableList.of(intLit(1), intLit(2)), true));
        }

        @Test
        void differentList() {
            Assertions.assertNotEquals(
                    new InPredicate(intLit(0),
                            ImmutableList.of(intLit(1), intLit(2)), false),
                    new InPredicate(intLit(0),
                            ImmutableList.of(intLit(1), intLit(3)), false));
        }
    }

    @Nested
    class IsNullPredicateEquals {
        @Test
        void sameContent() {
            Assertions.assertEquals(
                    new IsNullPredicate(intLit(1), false),
                    new IsNullPredicate(intLit(1), false));
        }

        @Test
        void differentIsNotNull() {
            Assertions.assertNotEquals(
                    new IsNullPredicate(intLit(1), false),
                    new IsNullPredicate(intLit(1), true));
        }
    }

    @Nested
    class LikePredicateEquals {
        @Test
        void sameOpAndChildren() {
            Assertions.assertEquals(
                    new LikePredicate(LikePredicate.Operator.LIKE,
                            new StringLiteral("abc"), new StringLiteral("a%")),
                    new LikePredicate(LikePredicate.Operator.LIKE,
                            new StringLiteral("abc"), new StringLiteral("a%")));
        }

        @Test
        void differentOp() {
            Assertions.assertNotEquals(
                    new LikePredicate(LikePredicate.Operator.LIKE,
                            new StringLiteral("abc"), new StringLiteral("a%")),
                    new LikePredicate(LikePredicate.Operator.REGEXP,
                            new StringLiteral("abc"), new StringLiteral("a%")));
        }

        @Test
        void differentPattern() {
            Assertions.assertNotEquals(
                    new LikePredicate(LikePredicate.Operator.LIKE,
                            new StringLiteral("abc"), new StringLiteral("a%")),
                    new LikePredicate(LikePredicate.Operator.LIKE,
                            new StringLiteral("abc"), new StringLiteral("b%")));
        }
    }

    @Nested
    class MatchPredicateEquals {
        private MatchPredicate make(MatchPredicate.Operator op) {
            return new MatchPredicate(op,
                    new StringLiteral("col"), new StringLiteral("term"),
                    Type.BOOLEAN, NullableMode.ALWAYS_NULLABLE, null, true);
        }

        @Test
        void sameOp() {
            Assertions.assertEquals(make(MatchPredicate.Operator.MATCH_ANY),
                    make(MatchPredicate.Operator.MATCH_ANY));
        }

        @Test
        void differentOp() {
            Assertions.assertNotEquals(make(MatchPredicate.Operator.MATCH_ANY),
                    make(MatchPredicate.Operator.MATCH_ALL));
        }
    }

    @Nested
    class SlotRefEquals {
        @Test
        void sameTypeAndNullableAndNoDesc() {
            // Without a SlotDescriptor both refs hit the notCheckDescIdEquals
            // path which compares table name + column name (both null here).
            Assertions.assertEquals(
                    new SlotRef(Type.INT, true),
                    new SlotRef(Type.INT, true));
        }

        @Test
        void notEqualToDifferentExprClass() {
            Assertions.assertNotEquals(new SlotRef(Type.INT, true), intLit(1));
        }
    }

    @Nested
    class VariableExprEquals {
        @Test
        void sameNameAndScope() {
            Assertions.assertEquals(new VariableExpr("autocommit"),
                    new VariableExpr("autocommit"));
        }

        @Test
        void differentName() {
            Assertions.assertNotEquals(new VariableExpr("autocommit"),
                    new VariableExpr("time_zone"));
        }

        @Test
        void differentScope() {
            Assertions.assertNotEquals(
                    new VariableExpr("autocommit", SetType.SESSION),
                    new VariableExpr("autocommit", SetType.GLOBAL));
        }
    }

    @Nested
    class CaseExprEquals {
        private CaseExpr make(boolean withCaseExpr, boolean withElse) {
            CaseWhenClause clause = new CaseWhenClause(intLit(1), intLit(10));
            // CaseExpr's first child is the optional case-expr (nullable),
            // followed by each when/then pair, optionally followed by an else.
            if (withCaseExpr) {
                CaseExpr ce = new CaseExpr(ImmutableList.of(clause),
                        withElse ? intLit(99) : null);
                ce.getChildren().add(0, intLit(0));
                return ce;
            }
            return new CaseExpr(ImmutableList.of(clause),
                    withElse ? intLit(99) : null);
        }

        @Test
        void sameShape() {
            Assertions.assertEquals(make(false, true), make(false, true));
        }

        @Test
        void differentHasElse() {
            Assertions.assertNotEquals(make(false, true), make(false, false));
        }
    }
}
