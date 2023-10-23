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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

/**
 * test ELIMINATE_UNNECESSARY_PROJECT rule.
 */
class EliminateUnnecessaryProjectTest extends TestWithFeService implements MemoPatternMatchSupported {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");

        connectContext.setDatabase("default_cluster:test");

        createTable("CREATE TABLE t1 (col1 int not null, col2 int not null, col3 int not null)\n"
                + "DISTRIBUTED BY HASH(col3)\n"
                + "BUCKETS 1\n"
                + "PROPERTIES(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ");");
    }

    @Test
    void testEliminateNonTopUnnecessaryProject() {
        LogicalPlan unnecessaryProject = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .project(ImmutableList.of(1, 0))
                .filter(BooleanLiteral.FALSE)
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), unnecessaryProject)
                .customRewrite(new EliminateUnnecessaryProject())
                .matchesFromRoot(logicalFilter(logicalOlapScan()));
    }

    @Test
    void testEliminateTopUnnecessaryProject() {
        LogicalPlan unnecessaryProject = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .project(ImmutableList.of(0, 1))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), unnecessaryProject)
                .customRewrite(new EliminateUnnecessaryProject())
                .matchesFromRoot(logicalOlapScan());
    }

    @Test
    void testEliminateTopProjectWhenOutputNotEquals() {
        LogicalPlan necessaryProject = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .project(ImmutableList.of(1, 0))
                .build();

        PlanChecker.from(MemoTestUtils.createConnectContext(), necessaryProject)
                .customRewrite(new EliminateUnnecessaryProject())
                .notMatchesFromRoot(logicalProject());
    }

    @Test
    void testEliminateProjectWhenEmptyRelationChild() {
        LogicalPlan unnecessaryProject = new LogicalPlanBuilder(new LogicalEmptyRelation(new RelationId(1),
                ImmutableList.of(
                        new SlotReference("k1", IntegerType.INSTANCE),
                        new SlotReference("k2", IntegerType.INSTANCE))))
                .project(ImmutableList.of(1, 0))
                .build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), unnecessaryProject)
                .customRewrite(new EliminateUnnecessaryProject())
                .matchesFromRoot(logicalEmptyRelation());
    }

    // TODO: uncomment this after the Elimination project rule is correctly implemented
    // @Test
    // public void testEliminationForThoseNeitherDoPruneNorDoExprCalc() {
    //     PlanChecker.from(connectContext).checkPlannerResult("SELECT col1 FROM t1",
    //             p -> {
    //                 List<PlanFragment> fragments = p.getFragments();
    //                 Assertions.assertTrue(fragments.stream()
    //                         .flatMap(fragment -> {
    //                             Set<OlapScanNode> scans = Sets.newHashSet();
    //                             fragment.getPlanRoot().collect(OlapScanNode.class, scans);
    //                             return scans.stream();
    //                         })
    //                         .noneMatch(s -> s.getProjectList() != null));
    //             });
    // }
}
