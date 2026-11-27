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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.scalar.RegexpPosition;
import org.apache.doris.nereids.trees.expressions.literal.CharLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class RegexpPositionTest implements MemoPatternMatchSupported {
    private final ConnectContext connectContext = MemoTestUtils.createConnectContext();

    @Test
    public void testArity() {
        List<String> sqlArguments = ImmutableList.of("'abc'", "'a'", "1", "1", "0", "''", "0", "0");
        List<Expression> expressions = ImmutableList.of(new StringLiteral("abc"), new StringLiteral("a"),
                new IntegerLiteral(1), new IntegerLiteral(1), new IntegerLiteral(0),
                new StringLiteral(""), new IntegerLiteral(0), new IntegerLiteral(0));
        RegexpPosition function = new RegexpPosition(expressions.get(0), expressions.get(1));

        for (int arity = 2; arity <= 7; arity++) {
            String sql = "select regexp_position(" + String.join(", ", sqlArguments.subList(0, arity)) + ")";
            PlanChecker.from(connectContext).analyze(sql)
                    .matches(logicalOneRowRelation().when(relation -> {
                        Expression result = relation.getProjects().get(0).child(0);
                        Assertions.assertInstanceOf(RegexpPosition.class, result);
                        Assertions.assertEquals(7, ((RegexpPosition) result).getArguments().size());
                        return true;
                    }));
            Assertions.assertEquals(7, function.withChildren(expressions.subList(0, arity)).getArguments().size());
        }

        for (int arity : new int[] {1, 8}) {
            String sql = "select regexp_position(" + String.join(", ", sqlArguments.subList(0, arity)) + ")";
            Assertions.assertThrows(AnalysisException.class, () -> PlanChecker.from(connectContext).analyze(sql));
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> function.withChildren(expressions.subList(0, arity)));
        }
    }

    @Test
    public void testDefaults() {
        List<Expression> supplied = ImmutableList.of(new StringLiteral("source"), new StringLiteral("pattern"),
                new IntegerLiteral(2), new IntegerLiteral(3), new IntegerLiteral(1),
                new StringLiteral("i"), new IntegerLiteral(4));
        List<Expression> defaults = ImmutableList.of(new IntegerLiteral(1), new IntegerLiteral(1),
                new IntegerLiteral(0), new StringLiteral(""), new IntegerLiteral(0));
        FunctionRegistry registry = new FunctionRegistry();
        RegexpPosition function = new RegexpPosition(supplied.get(0), supplied.get(1));

        for (int arity = 2; arity <= 7; arity++) {
            List<Expression> arguments = supplied.subList(0, arity);
            List<Expression> expected = ImmutableList.<Expression>builder()
                    .addAll(arguments)
                    .addAll(defaults.subList(arity - 2, defaults.size()))
                    .build();
            FunctionBuilder builder = registry.findFunctionBuilder("regexp_position", arguments);
            Expression result = builder.build("regexp_position", arguments).first;
            Assertions.assertInstanceOf(RegexpPosition.class, result);
            Assertions.assertEquals(expected, ((RegexpPosition) result).getArguments());
            Assertions.assertEquals(expected, function.withChildren(arguments).getArguments());
        }
    }

    @Test
    public void testAliases() {
        List<String> names = ImmutableList.of("regexp_position", "regexp_instr", "regexpPosition", "regexpInstr");
        for (String name : names) {
            for (String spelling : ImmutableList.of(name, name.toUpperCase(Locale.ROOT))) {
                PlanChecker.from(connectContext).analyze("select " + spelling + "('abc', 'a')")
                        .matches(logicalOneRowRelation().when(relation -> {
                            Expression result = relation.getProjects().get(0).child(0);
                            Assertions.assertInstanceOf(RegexpPosition.class, result);
                            Assertions.assertEquals("regexp_position", ((RegexpPosition) result).getName());
                            return true;
                        }));
            }
        }
    }

    @Test
    public void testAcceptedRawTypes() {
        for (int index : new int[] {0, 1, 5}) {
            assertAcceptedRawType(index, "cast('a' as string)");
            assertAcceptedRawType(index, "cast('a' as varchar(8))");
        }

        for (int index : new int[] {2, 3, 4, 6}) {
            for (String type : ImmutableList.of("tinyint", "smallint", "int", "bigint")) {
                assertAcceptedRawType(index, "cast(1 as " + type + ")");
            }
        }

        for (int index = 0; index < 7; index++) {
            assertAcceptedRawType(index, "NULL");
        }

        assertAcceptedRawType(0, "cast(cast('abc' as char(3)) as string)");
        assertAcceptedRawType(1, "cast(cast('a' as char(1)) as varchar(8))");
        assertAcceptedRawType(2, "cast(cast(1 as largeint) as bigint)");
        assertAcceptedRawType(3, "cast('1' as int)");
        assertAcceptedRawType(5, "cast(cast('i' as char(1)) as string)");
    }

    @Test
    public void testRejectedRawTypes() {
        for (int index : new int[] {0, 1, 5}) {
            List<Expression> arguments = Arrays.asList(new StringLiteral("abc"), new StringLiteral("a"),
                    new IntegerLiteral(1), new IntegerLiteral(1), new IntegerLiteral(0),
                    new StringLiteral(""), new IntegerLiteral(0));
            arguments.set(index, new CharLiteral("a", 3));
            RegexpPosition function = new RegexpPosition(new StringLiteral("abc"), new StringLiteral("a"))
                    .withChildren(arguments);
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    function::checkLegalityBeforeTypeCoercion);
            Assertions.assertTrue(exception.getMessage().contains("argument of regexp_position must be"));
            assertRejectedRawType(index, "1");
        }

        for (int index : new int[] {2, 3, 4, 6}) {
            for (String expression : ImmutableList.of("cast(1 as largeint)", "cast(1.5 as float)",
                    "cast(1.5 as double)", "cast(1.5 as decimal(9, 2))", "true", "'1'")) {
                assertRejectedRawType(index, expression);
            }
        }
    }

    @Test
    public void testPatternConstant() {
        String sql = "select regexp_position('abc', concat('a', 'b'))";
        PlanChecker.from(connectContext).analyze(sql)
                .matches(logicalOneRowRelation().when(relation -> {
                    Expression result = relation.getProjects().get(0).child(0);
                    Assertions.assertInstanceOf(RegexpPosition.class, result);
                    Assertions.assertTrue(((RegexpPosition) result).getArgument(1).isConstant());
                    return true;
                }));

        for (String query : ImmutableList.of(
                "select regexp_position('abc', s) from (select 'a' s) t",
                "select regexp_position('abc', uuid())")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> PlanChecker.from(connectContext).analyze(query), query);
            Assertions.assertTrue(exception.getMessage().contains(
                    "The pattern argument of regexp_position must be constant"),
                    query + ": " + exception.getMessage());
        }
    }

    @Test
    public void testFlagsConstant() {
        String sql = "select regexp_position('abc', 'a', 1, 1, 0, concat('i', 'm'))";
        PlanChecker.from(connectContext).analyze(sql)
                .matches(logicalOneRowRelation().when(relation -> {
                    Expression result = relation.getProjects().get(0).child(0);
                    Assertions.assertInstanceOf(RegexpPosition.class, result);
                    Assertions.assertTrue(((RegexpPosition) result).getArgument(5).isConstant());
                    return true;
                }));

        for (String query : ImmutableList.of(
                "select regexp_position('abc', 'a', 1, 1, 0, f) from (select 'i' f) t",
                "select regexp_position('abc', 'a', 1, 1, 0, uuid())")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> PlanChecker.from(connectContext).analyze(query), query);
            Assertions.assertTrue(exception.getMessage().contains(
                    "The flags argument of regexp_position must be constant"),
                    query + ": " + exception.getMessage());
        }
    }

    @Test
    public void testReturnType() {
        for (String sql : ImmutableList.of(
                "select regexp_position('abc', 'a')",
                "select regexp_position('abc', 'a', 1, 1, 0, '', 0)")) {
            PlanChecker.from(connectContext).analyze(sql)
                    .matches(logicalOneRowRelation().when(relation -> {
                        Expression result = relation.getProjects().get(0).child(0);
                        Assertions.assertInstanceOf(RegexpPosition.class, result);
                        Assertions.assertEquals(BigIntType.INSTANCE, result.getDataType());
                        Assertions.assertFalse(result.nullable());
                        return true;
                    }));
        }
    }

    @Test
    public void testNullable() {
        String[] nonNullArguments = {"'abc'", "'a'", "1", "1", "0", "''", "0"};
        for (int index = 0; index < nonNullArguments.length; index++) {
            int nullableIndex = index;
            for (String nullableArgument : ImmutableList.of(
                    "NULL", "if(true, " + nonNullArguments[index] + ", NULL)")) {
                String[] arguments = nonNullArguments.clone();
                arguments[index] = nullableArgument;
                String sql = "select regexp_position(" + String.join(", ", Arrays.asList(arguments)) + ")";
                PlanChecker.from(connectContext).analyze(sql)
                        .matches(logicalOneRowRelation().when(relation -> {
                            Expression result = relation.getProjects().get(0).child(0);
                            Assertions.assertInstanceOf(RegexpPosition.class, result, sql);
                            Assertions.assertTrue(((RegexpPosition) result).getArgument(nullableIndex).nullable(), sql);
                            Assertions.assertEquals(BigIntType.INSTANCE, result.getDataType(), sql);
                            Assertions.assertTrue(result.nullable(), sql);
                            return true;
                        }));
            }
        }
    }

    @Test
    public void testConstantFoldBoundary() {
        for (String sql : ImmutableList.of(
                "select regexp_position(NULL, 1)",
                "select regexp_position(NULL, 'a', '1')",
                "select regexp_position(NULL, p) from (select 'a' p) t",
                "select regexp_position(NULL, 'a', 1, 1, 0, f) from (select 'i' f) t")) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> PlanChecker.from(connectContext).analyze(sql), sql);
        }

        for (int nullIndex = 0; nullIndex < 7; nullIndex++) {
            String[] arguments = {"'abc'", "'['", "0", "1", "0", "'z'", "0"};
            arguments[nullIndex] = "NULL";
            String sql = "select regexp_position(" + String.join(", ", Arrays.asList(arguments)) + ")";
            Expression function = analyzedFunction(sql);
            Assertions.assertEquals(new NullLiteral(BigIntType.INSTANCE),
                    FoldConstantRuleOnFE.evaluateWithoutContext(function), sql);
        }

        for (String sql : ImmutableList.of(
                "select regexp_position('abc', '[', 0, 1, 0, 'z')",
                "select regexp_position('abc', 'a', 0)")) {
            Expression folded = FoldConstantRuleOnFE.evaluateWithoutContext(analyzedFunction(sql));
            Assertions.assertInstanceOf(RegexpPosition.class, folded, sql);
        }
    }

    private Expression analyzedFunction(String sql) {
        LogicalResultSink<?> sink = (LogicalResultSink<?>) PlanChecker.from(connectContext).analyze(sql).getPlan();
        LogicalOneRowRelation relation = (LogicalOneRowRelation) sink.child();
        Expression function = relation.getProjects().get(0).child(0);
        Assertions.assertInstanceOf(RegexpPosition.class, function, sql);
        return function;
    }

    private void assertAcceptedRawType(int index, String expression) {
        String[] arguments = {"'abc'", "'a'", "1", "1", "0", "''", "0"};
        arguments[index] = expression;
        String sql = "select regexp_position(" + String.join(", ", Arrays.asList(arguments)) + ")";
        PlanChecker.from(connectContext).analyze(sql)
                .matches(logicalOneRowRelation().when(relation -> {
                    Expression result = relation.getProjects().get(0).child(0);
                    Assertions.assertInstanceOf(RegexpPosition.class, result, sql);
                    return true;
                }));
    }

    private void assertRejectedRawType(int index, String expression) {
        String[] arguments = {"'abc'", "'a'", "1", "1", "0", "''", "0"};
        arguments[index] = expression;
        String sql = "select regexp_position(" + String.join(", ", Arrays.asList(arguments)) + ")";
        String[] argumentNames = {"haystack", "pattern", "position", "occurrence", "return_option", "flags",
                "subexpression"};
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql), sql);
        Assertions.assertTrue(exception.getMessage().contains("The " + argumentNames[index]
                + " argument of regexp_position must be"), sql + ": " + exception.getMessage());
    }
}
