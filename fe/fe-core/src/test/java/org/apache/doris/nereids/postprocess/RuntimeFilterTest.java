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

package org.apache.doris.nereids.postprocess;

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.datasets.ssb.SSBUtils;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TQueryOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RuntimeFilterTest extends SSBTestBase implements PatternMatchSupported {

    @Override
    public void runBeforeEach() throws Exception {
        NamedExpressionUtil.clear();
    }

    @Test
    public void testGenerateRuntimeFilter() throws AnalysisException {
        String sql = "SELECT * FROM lineorder JOIN customer on c_custkey = lo_custkey";
        PlanChecker.from(connectContext)
                .implement(new NereidsParser().parseSingle(sql))
                .matchesPhysicalPlan(
                        physicalProject(
                                physicalHashJoin(
                                        physicalOlapScan(),
                                        physicalOlapScan()
                                ).when(join -> {
                                    Expression expr = join.getHashJoinConjuncts().get(0);
                                    Assertions.assertTrue(expr instanceof EqualTo);
                                    List<RuntimeFilter> filters = join.getRuntimeFilters().getFiltersByExprId()
                                            .get(((SlotReference) expr.child(1)).getExprId());
                                    return filters.size() == 1;
                                })
                        )
                );
    }

    @Test
    public void testGenerateRuntimeFilterByIllegalSrcExpr() throws AnalysisException {
        String sql = "SELECT * FROM lineorder JOIN customer on c_custkey = c_custkey";
        PlanChecker.from(connectContext)
                .implement(new NereidsParser().parseSingle(sql))
                .matchesPhysicalPlan(
                        physicalProject(
                                physicalNestedLoopJoin(
                                        physicalOlapScan(),
                                        physicalOlapScan()
                                )
                        )
                );
    }

    @Test
    public void testComplexExpressionToRuntimeFilter() throws AnalysisException {
        String sql
                = "SELECT * FROM supplier JOIN customer on c_name = s_name and s_city = c_city and s_nation = c_nation";
        PlanChecker.from(connectContext)
                .implement(new NereidsParser().parseSingle(sql))
                .matchesPhysicalPlan(
                        physicalProject(
                                physicalHashJoin(
                                        physicalOlapScan(),
                                        physicalOlapScan()
                                ).when(join -> join.getRuntimeFilters().getFiltersByExprId().keySet().size() == 3)
                        )
                );
    }

    @Test
    public void testAddRuntimeFilterToHashJoinNode() throws AnalysisException {
        String sql = "SELECT * FROM lineorder JOIN customer on c_custkey = lo_custkey";
        PhysicalPlan plan = new NereidsPlanner(createStatementCtx(sql)).plan(
                new NereidsParser().parseSingle(sql),
                PhysicalProperties.ANY
        );
        PlanFragment fragment = new PhysicalPlanTranslator().translatePlan(plan, new PlanTranslatorContext());
        Assertions.assertTrue((fragment.getChild(0).getExplainString(TExplainLevel.NORMAL).contains("runtime filter")),
                "No runtime filter on HashJoinNode");
    }

    @Test
    public void testTranslateSSB() throws UserException {
        String[] sqls = {SSBUtils.Q1_1, SSBUtils.Q1_2, SSBUtils.Q1_3,
                SSBUtils.Q2_1, SSBUtils.Q2_2, SSBUtils.Q2_3,
                SSBUtils.Q3_1, SSBUtils.Q3_2, SSBUtils.Q3_3, SSBUtils.Q3_4,
                SSBUtils.Q4_1, SSBUtils.Q4_2, SSBUtils.Q4_3};
        for (String sql : sqls) {
            System.out.println("sql: " + sql);
            NereidsPlanner planner = new NereidsPlanner(createStatementCtx(sql));
            planner.plan(
                    new LogicalPlanAdapter(new NereidsParser().parseSingle(sql)),
                    new TQueryOptions()
            );
            System.out.println(planner.getExplainString(new ExplainOptions(false, false)));
        }
    }
}
