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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class EliminateFilterTest {
    @Test
    public void testEliminateFilterFalse() {
        LogicalPlan filterFalse = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .filter(BooleanLiteral.FALSE)
                .build();

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(filterFalse);
        List<Rule> rules = Lists.newArrayList(new EliminateFilter().build());
        cascadesContext.topDownRewrite(rules);

        Plan actual = cascadesContext.getMemo().copyOut();
        Assertions.assertTrue(actual instanceof LogicalEmptyRelation);
    }

    @Test
    public void testEliminateFilterTrue() {
        LogicalPlan filterFalse = new LogicalPlanBuilder(PlanConstructor.newLogicalOlapScan(0, "t1", 0))
                .filter(BooleanLiteral.TRUE)
                .build();

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(filterFalse);
        List<Rule> rules = Lists.newArrayList(new EliminateFilter().build());
        cascadesContext.topDownRewrite(rules);

        Plan actual = cascadesContext.getMemo().copyOut();
        Assertions.assertTrue(actual instanceof LogicalOlapScan);
    }
}
