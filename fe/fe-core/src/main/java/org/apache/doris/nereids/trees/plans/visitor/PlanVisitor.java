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

package org.apache.doris.nereids.trees.plans.visitor;

import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.CreatePolicyCommand;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalTVFRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLocalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRepeat;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;

/**
 * Base class for the processing of logical and physical plan.
 *
 * @param <R> Return type of each visit method.
 * @param <C> Context type.
 */
public abstract class PlanVisitor<R, C> {

    public abstract R visit(Plan plan, C context);

    // *******************************
    // commands
    // *******************************

    public R visitCommand(Command command, C context) {
        return visit(command, context);
    }

    public R visitExplainCommand(ExplainCommand explain, C context) {
        return visitCommand(explain, context);
    }

    public R visitCreatePolicyCommand(CreatePolicyCommand explain, C context) {
        return visitCommand(explain, context);
    }

    // *******************************
    // Logical plans
    // *******************************

    public R visitLogicalCTE(LogicalCTE<? extends Plan> cte, C context) {
        return visit(cte, context);
    }

    public R visitSubQueryAlias(LogicalSubQueryAlias<? extends Plan> alias, C context) {
        return visit(alias, context);
    }

    public R visitUnboundOneRowRelation(UnboundOneRowRelation oneRowRelation, C context) {
        return visit(oneRowRelation, context);
    }

    public R visitLogicalEmptyRelation(LogicalEmptyRelation emptyRelation, C context) {
        return visit(emptyRelation, context);
    }

    public R visitLogicalOneRowRelation(LogicalOneRowRelation oneRowRelation, C context) {
        return visit(oneRowRelation, context);
    }

    public R visitUnboundRelation(UnboundRelation relation, C context) {
        return visit(relation, context);
    }

    public R visitUnboundTVFRelation(UnboundTVFRelation unboundTVFRelation, C context) {
        return visit(unboundTVFRelation, context);
    }

    public R visitLogicalRelation(LogicalRelation relation, C context) {
        return visit(relation, context);
    }

    public R visitLogicalSelectHint(LogicalSelectHint<? extends Plan> hint, C context) {
        return visit(hint, context);
    }

    public R visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, C context) {
        return visit(aggregate, context);
    }

    public R visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, C context) {
        return visit(repeat, context);
    }

    public R visitLogicalFilter(LogicalFilter<? extends Plan> filter, C context) {
        return visit(filter, context);
    }

    public R visitLogicalCheckPolicy(LogicalCheckPolicy<? extends Plan> checkPolicy, C context) {
        return visit(checkPolicy, context);
    }

    public R visitLogicalOlapScan(LogicalOlapScan olapScan, C context) {
        return visitLogicalRelation(olapScan, context);
    }

    public R visitLogicalTVFRelation(LogicalTVFRelation tvfRelation, C context) {
        return visitLogicalRelation(tvfRelation, context);
    }

    public R visitLogicalProject(LogicalProject<? extends Plan> project, C context) {
        return visit(project, context);
    }

    public R visitLogicalSort(LogicalSort<? extends Plan> sort, C context) {
        return visit(sort, context);
    }

    public R visitLogicalTopN(LogicalTopN<? extends Plan> topN, C context) {
        return visit(topN, context);
    }

    public R visitLogicalLimit(LogicalLimit<? extends Plan> limit, C context) {
        return visit(limit, context);
    }

    public R visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, C context) {
        return visit(join, context);
    }

    public R visitGroupPlan(GroupPlan groupPlan, C context) {
        return visit(groupPlan, context);
    }

    public R visitLogicalApply(LogicalApply<? extends Plan, ? extends Plan> apply, C context) {
        return visit(apply, context);
    }

    public R visitLogicalAssertNumRows(LogicalAssertNumRows<? extends Plan> assertNumRows, C context) {
        return visit(assertNumRows, context);
    }

    public R visitLogicalHaving(LogicalHaving<Plan> having, C context) {
        return visit(having, context);
    }

    // *******************************
    // Physical plans
    // *******************************

    public R visitPhysicalAggregate(PhysicalAggregate<? extends Plan> agg, C context) {
        return visit(agg, context);
    }

    public R visitPhysicalRepeat(PhysicalRepeat<? extends Plan> repeat, C context) {
        return visit(repeat, context);
    }

    public R visitPhysicalScan(PhysicalRelation scan, C context) {
        return visit(scan, context);
    }

    public R visitPhysicalEmptyRelation(PhysicalEmptyRelation emptyRelation, C context) {
        return visit(emptyRelation, context);
    }

    public R visitPhysicalOneRowRelation(PhysicalOneRowRelation oneRowRelation, C context) {
        return visit(oneRowRelation, context);
    }

    public R visitPhysicalOlapScan(PhysicalOlapScan olapScan, C context) {
        return visitPhysicalScan(olapScan, context);
    }

    public R visitPhysicalTVFRelation(PhysicalTVFRelation tvfRelation, C context) {
        return visitPhysicalScan(tvfRelation, context);
    }

    public R visitAbstractPhysicalSort(AbstractPhysicalSort<? extends Plan> sort, C context) {
        return visit(sort, context);
    }

    public R visitPhysicalQuickSort(PhysicalQuickSort<? extends Plan> sort, C context) {
        return visitAbstractPhysicalSort(sort, context);
    }

    public R visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, C context) {
        return visit(topN, context);
    }

    public R visitPhysicalLimit(PhysicalLimit<? extends Plan> limit, C context) {
        return visit(limit, context);
    }

    public R visitAbstractPhysicalJoin(AbstractPhysicalJoin<? extends Plan, ? extends Plan> join, C context) {
        return visit(join, context);
    }

    public R visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin, C context) {
        return visitAbstractPhysicalJoin(hashJoin, context);
    }

    public R visitPhysicalNestedLoopJoin(
            PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin, C context) {
        return visitAbstractPhysicalJoin(nestedLoopJoin, context);
    }

    public R visitPhysicalProject(PhysicalProject<? extends Plan> project, C context) {
        return visit(project, context);
    }

    public R visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, C context) {
        return visit(filter, context);
    }

    // *******************************
    // Physical enforcer
    // *******************************

    public R visitPhysicalDistribute(PhysicalDistribute<? extends Plan> distribute, C context) {
        return visit(distribute, context);
    }

    public R visitPhysicalLocalQuickSort(PhysicalLocalQuickSort<? extends Plan> sort, C context) {
        return visitAbstractPhysicalSort(sort, context);
    }

    public R visitPhysicalAssertNumRows(PhysicalAssertNumRows<? extends Plan> assertNumRows, C context) {
        return visit(assertNumRows, context);
    }
}
