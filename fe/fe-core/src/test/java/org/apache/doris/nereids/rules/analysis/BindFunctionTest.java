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

import org.apache.doris.common.Config;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference.ArrayItemSlot;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class BindFunctionTest extends TestWithFeService implements MemoPatternMatchSupported {

    private final NereidsParser parser = new NereidsParser();

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        createTables(
                "CREATE TABLE t1 (col1 date, col2 int) DISTRIBUTED BY HASH(col2)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n" + ");",
                "CREATE TABLE t2 (col1 date, col2 int) DISTRIBUTED BY HASH(col2)\n" + "BUCKETS 1\n" + "PROPERTIES(\n"
                        + "    \"replication_num\"=\"1\"\n" + ");",
                "CREATE TABLE t_arr (id int, flag boolean, arr1 array<varchar(10)>, arr2 array<int>)\n"
                        + "DISTRIBUTED BY HASH(id) BUCKETS 1\n"
                        + "PROPERTIES(\n    \"replication_num\"=\"1\"\n);"
        );
    }

    @Test
    public void testTimeArithmExpr() {
        // TODO: need to fix the UT for datev2
        if (!Config.enable_date_conversion) {
            String sql = "SELECT * FROM t1 WHERE col1 < date '1994-01-01' + interval '1' year";

            PlanChecker.from(connectContext)
                    .analyze(sql)
                    .rewrite()
                    .matches(
                            logicalFilter(logicalOlapScan())
                                    .when(f -> ((LessThan) f.getPredicate()).right() instanceof DateLiteral)
                    );
        }
    }

    @Test
    void testJoinBindFunction() {
        String sql = "SELECT * FROM t1 LEFT JOIN t2 ON abs(t1.col2) = t2.col2 where t1.col2 > 10";

        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        leftOuterLogicalJoin(
                                logicalProject(logicalFilter()),
                                logicalProject(logicalOlapScan())
                        ).when(join -> join.getHashJoinConjuncts().size() == 1)
                );
    }

    @Test
    void testNestedLambdaCapturesOuterColumn() {
        List<String> sqls = ImmutableList.of(
                "SELECT array_map(a -> array_sum(array_map(b -> if(flag, b, 0), arr2)), arr1) FROM t_arr",
                "SELECT array_map(a -> array_sum(array_sortby(b -> if(flag, b, 0), arr2)), arr1) FROM t_arr",
                "SELECT array_map(a -> array_map(b -> array_map(c -> if(flag, b + c, id), arr2), arr2), arr1)"
                        + " FROM t_arr"
        );
        for (String sql : sqls) {
            Lambda innermostLambda = innermostLambda(PlanChecker.from(connectContext).analyze(sql).getPlan());
            // getInputSlots() never contains the lambda argument slots
            Set<String> capturedColumns = innermostLambda.getLambdaFunction().getInputSlots().stream()
                    .map(Slot::getName)
                    .collect(Collectors.toSet());
            Assertions.assertTrue(capturedColumns.contains("flag"), sql);
        }
    }

    @Test
    void testNestedLambdaSeesAllEnclosingLambdaArguments() {
        String sql = "SELECT array_map(a -> array_map(b -> array_map(c -> concat(a, b, c), arr1), arr1), arr1)"
                + " FROM t_arr";
        Lambda innermostLambda = innermostLambda(PlanChecker.from(connectContext).analyze(sql).getPlan());
        Set<ArrayItemSlot> lambdaArguments = innermostLambda.getLambdaFunction()
                .collect(ArrayItemSlot.class::isInstance);
        Assertions.assertEquals(ImmutableList.of("a", "b", "c"),
                lambdaArguments.stream().map(Slot::getName).sorted().collect(Collectors.toList()));
    }

    @Test
    void testNestedLambdaArgumentShadowsEnclosingArgument() {
        String sql = "SELECT array_map(x -> array_map(x -> x + 1, arr2), arr1) FROM t_arr";
        Lambda innermostLambda = innermostLambda(PlanChecker.from(connectContext).analyze(sql).getPlan());
        Set<ArrayItemSlot> bodySlots = innermostLambda.getLambdaFunction().collect(ArrayItemSlot.class::isInstance);
        Assertions.assertEquals(1, bodySlots.size());
        Assertions.assertEquals(innermostLambda.getLambdaArgument(0).getExprId(),
                bodySlots.iterator().next().getExprId());
        Assertions.assertTrue(innermostLambda.getLambdaFunction().getInputSlots().isEmpty());
    }

    @Test
    void testNestedLambdaUnknownSlot() {
        String sql = "SELECT array_map(a -> array_map(b -> unknown_col + b, arr2), arr1) FROM t_arr";
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext).analyze(sql));
        Assertions.assertTrue(exception.getMessage().contains("Unknown lambda slot 'unknown_col"),
                exception.getMessage());
    }

    private static Lambda innermostLambda(Plan plan) {
        List<Lambda> innermostLambdas = plan.<Plan>collect(node -> true).stream()
                .flatMap(node -> node.getExpressions().stream())
                .flatMap(expression -> expression.<Lambda>collect(Lambda.class::isInstance).stream())
                .filter(lambda -> lambda.getLambdaFunction().<Expression>collect(Lambda.class::isInstance).isEmpty())
                .collect(Collectors.toList());
        Assertions.assertEquals(1, innermostLambdas.size());
        return innermostLambdas.get(0);
    }
}
