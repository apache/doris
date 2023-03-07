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
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * MergeConsecutiveFilter ut
 */
public class EliminateLimitTest {
    @Test
    public void testEliminateLimit() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalLimit<LogicalOlapScan> limit = new LogicalLimit<>(0, 0, LimitPhase.ORIGIN, scan);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(limit);
        List<Rule> rules = Lists.newArrayList(new EliminateLimit().build());
        cascadesContext.topDownRewrite(rules);

        Plan actual = cascadesContext.getMemo().copyOut();
        Assertions.assertTrue(actual instanceof LogicalEmptyRelation);
    }

    @Test
    public void testLimitSort() {
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalLimit limit = new LogicalLimit<>(1, 1, LimitPhase.ORIGIN,
                new LogicalSort<>(scan.getOutput().stream().map(c -> new OrderKey(c, true, true)).collect(Collectors.toList()),
                        scan));

        Plan actual = PlanChecker.from(MemoTestUtils.createConnectContext(), limit)
                .rewrite()
                .getPlan();
        Assertions.assertTrue(actual instanceof LogicalTopN);
    }
}
