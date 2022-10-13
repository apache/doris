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
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MemoTestUtils;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

/**
 * MergeConsecutiveFilter ut
 */
public class MergeFiltersTest {
    @Test
    public void testMergeConsecutiveFilters() {
        UnboundRelation relation = new UnboundRelation(Lists.newArrayList("db", "table"));
        Expression expression1 = new IntegerLiteral(1);
        LogicalFilter filter1 = new LogicalFilter<>(expression1, relation);
        Expression expression2 = new IntegerLiteral(2);
        LogicalFilter filter2 = new LogicalFilter<>(expression2, filter1);
        Expression expression3 = new IntegerLiteral(3);
        LogicalFilter filter3 = new LogicalFilter<>(expression3, filter2);

        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(filter3);
        List<Rule> rules = Lists.newArrayList(new MergeFilters().build());
        cascadesContext.bottomUpRewrite(rules);
        //check transformed plan
        Plan resultPlan = cascadesContext.getMemo().copyOut();
        System.out.println(resultPlan.treeString());
        Assertions.assertTrue(resultPlan instanceof LogicalFilter);
        Expression allPredicates = ExpressionUtils.and(expression3,
                ExpressionUtils.and(expression2, expression1));
        Assertions.assertEquals(((LogicalFilter<?>) resultPlan).getPredicates(), allPredicates);
        Assertions.assertTrue(resultPlan.child(0) instanceof UnboundRelation);
    }
}
