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

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateFormat;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Floor;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Hour;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DoubleType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;

public class AliasFunctionRewriteTest extends TestWithFeService implements MemoPatternMatchSupported {
    @Override
    public void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("default_cluster:test");

        createFunction("CREATE ALIAS FUNCTION f1(DATETIMEV2(3), INT)\n"
                + " with PARAMETER (datetime1, int1) as date_trunc(days_sub(datetime1, int1), 'day')");
        createFunction("CREATE ALIAS FUNCTION f2(DATETIMEV2(3), INT)\n"
                + " with PARAMETER (datetime1, int1) as DATE_FORMAT(HOURS_ADD(date_trunc(datetime1, 'day'),\n"
                + " add(multiply(floor(divide(HOUR(datetime1), divide(24, int1))), 1), 1)), '%Y%m%d:%H')");
        createFunction("CREATE ALIAS FUNCTION f3(INT)"
                + " with PARAMETER (int1) as f2(f1(now(3), 2), int1)");
    }

    @Test
    public void testSimpleAliasFunction() {
        String sql = "select f1('2023-06-01', 3)";
        Expression expected = new DateTrunc(
                new DaysSub(
                        new Cast(new VarcharLiteral("2023-06-01"), DateTimeType.INSTANCE),
                        new IntegerLiteral(3)
                ),
                new VarcharLiteral("day")
        );


        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().size() == 1)
                                .when(relation -> relation.getProjects().get(0).child(0).equals(expected))
                );
    }

    @Test
    public void testNestedWithBuiltinFunction() {
        String sql = "select f1(now(3), 3);";
        Expression expected = new DateTrunc(
                new DaysSub(
                        new Now(
                                new IntegerLiteral(3)
                        ),
                        new IntegerLiteral(3)
                ),
                new VarcharLiteral("day")
        );
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().size() == 1)
                                .when(relation -> relation.getProjects().get(0).child(0).equals(expected))
                );
    }

    @Test
    public void testNestedWithAliasFunction() {
        String sql = "select f2(f1(now(3), 2), 3);";
        Expression f1 = new DateTrunc(
                new DaysSub(
                        new Now(new IntegerLiteral(3)),
                        new IntegerLiteral(2)
                ),
                new VarcharLiteral("day")
        );
        Expression expected = new DateFormat(
                new HoursAdd(
                        new DateTrunc(f1, new VarcharLiteral("day")),
                        new Cast(new Add(
                                new Multiply(
                                        new Floor(new Divide(
                                                new Cast(new Hour(f1), DoubleType.INSTANCE),
                                                new Divide(
                                                        new Cast(new TinyIntLiteral((byte) 24), DoubleType.INSTANCE),
                                                        new Cast(new TinyIntLiteral((byte) 3), DoubleType.INSTANCE)
                                                )
                                        )),
                                        new Cast(new TinyIntLiteral((byte) 1), DoubleType.INSTANCE)
                                ),
                                new Cast(new TinyIntLiteral((byte) 1), DoubleType.INSTANCE)
                        ), IntegerType.INSTANCE)
                ),
                new VarcharLiteral("%Y%m%d:%H")
        );
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().size() == 1)
                                .when(relation -> relation.getProjects().get(0).child(0).equals(expected))
                );
    }

    @Test
    public void testNestedAliasFunctionWithNestedFunction() {
        String sql = "select f3(4);";
        Expression f1 = new DateTrunc(
                new DaysSub(
                        new Now(new IntegerLiteral(3)),
                        new IntegerLiteral(2)
                ),
                new VarcharLiteral("day")
        );
        Expression expected = new DateFormat(
                new HoursAdd(
                        new DateTrunc(f1, new VarcharLiteral("day")),
                        new Cast(new Add(
                                new Multiply(
                                        new Floor(new Divide(
                                                new Cast(new Hour(f1), DoubleType.INSTANCE),
                                                new Divide(
                                                        new Cast(new TinyIntLiteral((byte) 24), DoubleType.INSTANCE),
                                                        new Cast(new TinyIntLiteral((byte) 4), DoubleType.INSTANCE)
                                                )
                                        )),
                                        new Cast(new TinyIntLiteral((byte) 1), DoubleType.INSTANCE)
                                ),
                                new Cast(new TinyIntLiteral((byte) 1), DoubleType.INSTANCE)
                        ), IntegerType.INSTANCE)
                ),
                new VarcharLiteral("%Y%m%d:%H")
        );
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().size() == 1)
                                .when(relation -> relation.getProjects().get(0).child(0).equals(expected))
                );
    }
}