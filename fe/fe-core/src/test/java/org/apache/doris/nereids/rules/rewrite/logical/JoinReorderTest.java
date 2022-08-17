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

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.expressions.BooleanLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class JoinReorderTest implements PatternMatchSupported {

    /**
     * To test do not throw unexpected exception when join type is not inner or cross.
     */
    @Test
    public void testWithOuterJoin() {
        UnboundRelation relation1 = new UnboundRelation(Lists.newArrayList("db", "table1"));
        UnboundRelation relation2 = new UnboundRelation(Lists.newArrayList("db", "table2"));
        LogicalJoin outerJoin = new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN, relation1, relation2);
        LogicalFilter logicalFilter = new LogicalFilter<>(new BooleanLiteral(false), outerJoin);

        PlanChecker.from(MemoTestUtils.createConnectContext(), logicalFilter)
                .applyBottomUp(new ReorderJoin())
                .matches(logicalFilter(leftOuterLogicalJoin(unboundRelation(), unboundRelation())));
    }
}
