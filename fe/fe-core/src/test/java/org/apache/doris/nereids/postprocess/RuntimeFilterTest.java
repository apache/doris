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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.datasets.ssb.SSBTestBase;
import org.apache.doris.nereids.glue.translator.PhysicalPlanTranslator;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.NamedExpressionUtil;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.RuntimeFilter;
import org.apache.doris.nereids.util.FieldChecker;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.planner.PlanFragment;
import org.apache.doris.thrift.TExplainLevel;

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
                                List<RuntimeFilter> filters = join.getRuntimeFilter();
                                if (filters.size() != 1) {
                                    return false;
                                }
                                RuntimeFilter filter = filters.get(0);
                                return FieldChecker.check("builderPlan", join).test(filter);
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
                                physicalHashJoin(
                                        physicalOlapScan(),
                                        physicalOlapScan()
                                ).when(join -> join.getRuntimeFilter().size() == 0)
                        )
                );
    }

    @Test
    public void testComplexExpressionToRuntimeFilter() throws AnalysisException {
        String sql = "SELECT * FROM supplier JOIN customer on c_name = s_name and s_city = c_city and s_nation = c_nation";
        PlanChecker.from(connectContext)
                .implement(new NereidsParser().parseSingle(sql))
                .matchesPhysicalPlan(
                        physicalProject(
                                physicalHashJoin(
                                        physicalOlapScan(),
                                        physicalOlapScan()
                                ).when(join -> {
                                    List<RuntimeFilter> filters = join.getRuntimeFilter();
                                    return filters.size() == 3;
                                })
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
}
