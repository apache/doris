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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.analysis.DropFunctionStmt;
import org.apache.doris.analysis.FunctionArgsDef;
import org.apache.doris.analysis.FunctionName;
import org.apache.doris.analysis.SetType;
import org.apache.doris.analysis.TypeDef;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayOfMonth;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DayOfWeek;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DaysSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursAdd;
import org.apache.doris.nereids.trees.expressions.functions.scalar.HoursSub;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Now;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.DateV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanPatternMatchSupported;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class UdfTest extends TestWithFeService implements PlanPatternMatchSupported {
    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        createDatabase("test_1");
    }

    @Override
    protected void runBeforeEach() throws Exception {
        connectContext.setDatabase("default_cluster:test");
    }

    @Test
    public void testSimpleAliasFunction() throws Exception {
        createFunction("create global alias function f(int) with parameter(n) as hours_add(now(3), n)");
        createFunction("create alias function f(int) with parameter(n) as hours_sub(now(3), n)");

        String sql = "select f(3)";
        Expression expected = new HoursSub(new Now(new IntegerLiteral(3)), new IntegerLiteral(3));
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().get(0).child(0).equals(expected))
                );

        connectContext.setDatabase("default_cluster:test_1");
        Expression expected1 = new HoursAdd(new Now(new IntegerLiteral(3)), new IntegerLiteral(3));
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().get(0).child(0).equals(expected1))
                );

        sql = "select test.f(3)";
        Expression expected2 = new HoursSub(new Now(new IntegerLiteral(3)), new IntegerLiteral(3));
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().get(0).child(0).equals(expected2))
                );
    }

    @Test
    public void testNestedAliasFunction() throws Exception {
        createFunction("create global alias function f1(int) with parameter(n) as hours_add(now(3), n)");
        createFunction("create global alias function f2(int) with parameter(n) as dayofweek(days_add(f1(3), n))");
        createFunction("create global alias function f3(date) with parameter(dt) as hours_sub(days_sub(dt, f2(3)), dayofmonth(f1(f2(4))))");

        Assertions.assertEquals(1, Env.getCurrentEnv().getFunctionRegistry()
                .findUdfBuilder(connectContext.getDatabase(), "f3").size());

        String sql = "select f3(now(3))";
        Expression expected = new HoursSub(
                new DaysSub(
                        new Cast(new Now(new IntegerLiteral(3)), DateV2Type.INSTANCE),
                        new Cast(new DayOfWeek(new DaysAdd(
                                new HoursAdd(
                                        new Now(new IntegerLiteral(3)),
                                        new IntegerLiteral(3)
                                ),
                                new IntegerLiteral(3))
                        ), IntegerType.INSTANCE)),
                new Cast(new DayOfMonth(new HoursAdd(
                        new Now(new IntegerLiteral(3)),
                        new Cast(new DayOfWeek(new DaysAdd(
                                new HoursAdd(
                                        new Now(new IntegerLiteral(3)),
                                        new IntegerLiteral(3)
                                ),
                                new IntegerLiteral(4))
                        ), IntegerType.INSTANCE)
                )), IntegerType.INSTANCE)
        );

        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().size() == 1
                                        && relation.getProjects().get(0).child(0).equals(expected))
                );
    }
}
