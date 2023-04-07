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

import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
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
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().size() == 1)
                                .when(relation -> relation.getProjects().get(0)
                                        .anyMatch(expr -> expr instanceof BoundFunction
                                                && "date_trunc".equals(((BoundFunction) expr).getName())))
                );
    }

    @Test
    public void testNestedWithBuiltinFunction() {
        String sql = "select f1(now(3), 3);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().size() == 1)
                                .when(relation -> relation.getProjects().get(0)
                                        .anyMatch(expr -> expr instanceof BoundFunction
                                                && "date_trunc".equals(((BoundFunction) expr).getName())))
                );
    }

    @Test
    public void testNestedWithAliasFunction() {
        String sql = "select f2(f1(now(3), 2), 3);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().size() == 1)
                                .when(relation -> relation.getProjects().get(0)
                                        .anyMatch(expr -> expr instanceof BoundFunction
                                                && "date_trunc".equals(((BoundFunction) expr).getName())))
                );
    }

    @Test
    public void testNestedAliasFunctionWithNestedFunction() {
        String sql = "select f3(3);";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .matches(
                        logicalOneRowRelation()
                                .when(relation -> relation.getProjects().size() == 1)
                                .when(relation -> relation.getProjects().get(0)
                                        .anyMatch(expr -> expr instanceof BoundFunction
                                                && "date_trunc".equals(((BoundFunction) expr).getName())))
                );
    }
}
