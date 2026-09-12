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
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLikeLiteral;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class BindFunctionTest extends TestWithFeService implements MemoPatternMatchSupported {

    private static final String AI_RESOURCE_NAME = "bind_function_ai_resource";
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
                        + "    \"replication_num\"=\"1\"\n" + ");"
        );
        LogicalPlan createResource = parser.parseSingle("CREATE RESOURCE \"" + AI_RESOURCE_NAME + "\"\n"
                + "PROPERTIES (\n"
                + "  \"type\" = \"ai\",\n"
                + "  \"ai.provider_type\" = \"deepseek\",\n"
                + "  \"ai.endpoint\" = \"https://api.deepseek.com/chat/completions\",\n"
                + "  \"ai.model_name\" = \"deepseek-chat\",\n"
                + "  \"ai.api_key\" = \"sk-xxx\",\n"
                + "  \"ai.validity_check\" = \"false\"\n"
                + ");");
        ((CreateResourceCommand) createResource).run(connectContext, null);
        connectContext.getSessionVariable().defaultAIResource = AI_RESOURCE_NAME;
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
    void testAIAggStateDefaultResourceTypeCoercion() {
        PlanChecker.from(connectContext)
                .analyze("SELECT ai_agg_state(col2, 'task') FROM t1")
                .matches(logicalResultSink(logicalProject().when(project -> {
                    List<StateCombinator> stateExpressions = project.getProjects().stream()
                            .flatMap(expression -> expression.collectToList(
                                    StateCombinator.class::isInstance).stream())
                            .map(StateCombinator.class::cast)
                            .toList();
                    Assertions.assertEquals(1, stateExpressions.size());

                    StateCombinator state = stateExpressions.get(0);
                    Assertions.assertEquals(3, state.arity());
                    Assertions.assertEquals(3, state.getNestedFunction().arity());
                    Assertions.assertEquals(state.children(), state.getNestedFunction().children());
                    Assertions.assertEquals(AI_RESOURCE_NAME,
                            ((StringLikeLiteral) state.child(0)).getStringValue());
                    Assertions.assertInstanceOf(Cast.class, state.child(1));
                    Assertions.assertEquals(StringType.INSTANCE, state.child(1).getDataType());
                    Assertions.assertEquals("task", ((StringLikeLiteral) state.child(2)).getStringValue());
                    return true;
                })));
    }
}
