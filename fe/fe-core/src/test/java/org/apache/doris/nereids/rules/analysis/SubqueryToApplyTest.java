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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class SubqueryToApplyTest {

    @Test
    void testAddNvlAggregate() {
        SlotReference slotReference = new SlotReference("col1", IntegerType.INSTANCE);
        NamedExpression aggregateFunction = new Alias(new ExprId(12345), new Count(slotReference), "count");
        LogicalOlapScan logicalOlapScan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
        LogicalAggregate<Plan> logicalAggregate = new LogicalAggregate<>(
                ImmutableList.of(), ImmutableList.of(aggregateFunction), logicalOlapScan);
        LogicalPlanBuilder planBuilder = new LogicalPlanBuilder(logicalAggregate);
        LogicalPlan plan = planBuilder.projectAll().build();
        Optional<Expression> correlatedOuterExpr = Optional
                .of(new EqualTo(plan.getOutput().get(0), new BigIntLiteral(1)));
        ScalarSubquery subquery = new ScalarSubquery(plan);
        SubqueryToApply subqueryToApply = new SubqueryToApply();
        SubqueryToApply.SubQueryRewriteResult rewriteResult = subqueryToApply.addNvlForScalarSubqueryOutput(
                ImmutableList.of(aggregateFunction), plan.getOutput().get(0), subquery, correlatedOuterExpr);
        Assertions.assertInstanceOf(EqualTo.class, rewriteResult.correlatedOuterExpr.get());
        Assertions.assertInstanceOf(Alias.class, rewriteResult.subqueryOutput);
        Assertions.assertInstanceOf(Nvl.class, rewriteResult.subqueryOutput.child(0));
        Assertions.assertEquals(rewriteResult.subqueryOutput.toSlot(),
                rewriteResult.correlatedOuterExpr.get().child(0));
    }
}
