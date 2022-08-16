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

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAssertNumRows;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribution;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalQuickSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
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

    // *******************************
    // Logical plans
    // *******************************

    public R visitSubQueryAlias(LogicalSubQueryAlias<Plan> alias, C context) {
        return visit(alias, context);
    }

    public R visitUnboundRelation(UnboundRelation relation, C context) {
        return visit(relation, context);
    }

    public R visitLogicalRelation(LogicalRelation relation, C context) {
        return visit(relation, context);
    }

    public R visitLogicalSelectHint(LogicalSelectHint<Plan> hint, C context) {
        return visit(hint, context);
    }


    public R visitLogicalAggregate(LogicalAggregate<Plan> aggregate, C context) {
        return visit(aggregate, context);
    }

    public R visitLogicalFilter(LogicalFilter<Plan> filter, C context) {
        return visit(filter, context);
    }

    public R visitLogicalOlapScan(LogicalOlapScan olapScan, C context) {
        return visitLogicalRelation(olapScan, context);
    }

    public R visitLogicalProject(LogicalProject<Plan> project, C context) {
        return visit(project, context);
    }

    public R visitLogicalSort(LogicalSort<Plan> sort, C context) {
        return visit(sort, context);
    }

    public R visitLogicalTopN(LogicalTopN<Plan> topN, C context) {
        return visit(topN, context);
    }

    public R visitLogicalLimit(LogicalLimit<Plan> limit, C context) {
        return visit(limit, context);
    }

    public R visitLogicalJoin(LogicalJoin<Plan, Plan> join, C context) {
        return visit(join, context);
    }

    public R visitGroupPlan(GroupPlan groupPlan, C context) {
        return visit(groupPlan, context);
    }

    public R visitLogicalApply(LogicalApply<Plan, Plan> apply, C context) {
        return visit(apply, context);
    }

    public R visitLogicalAssertNumRows(LogicalAssertNumRows<Plan> assertNumRows, C context) {
        return visit(assertNumRows, context);
    }

    public R visitLogicalHaving(LogicalHaving<Plan> having, C context) {
        return visit(having, context);
    }

    // *******************************
    // Physical plans
    // *******************************

    public R visitPhysicalAggregate(PhysicalAggregate<Plan> agg, C context) {
        return visit(agg, context);
    }

    public R visitPhysicalScan(PhysicalRelation scan, C context) {
        return visit(scan, context);
    }

    public R visitPhysicalOlapScan(PhysicalOlapScan olapScan, C context) {
        return visitPhysicalScan(olapScan, context);
    }

    public R visitAbstractPhysicalSort(AbstractPhysicalSort<Plan> sort, C context) {
        return visit(sort, context);
    }

    public R visitPhysicalQuickSort(PhysicalQuickSort<Plan> sort, C context) {
        return visitAbstractPhysicalSort(sort, context);
    }

    public R visitPhysicalTopN(PhysicalTopN<Plan> topN, C context) {
        return visit(topN, context);
    }

    public R visitPhysicalLimit(PhysicalLimit<Plan> limit, C context) {
        return visit(limit, context);
    }

    public R visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> hashJoin, C context) {
        return visit(hashJoin, context);
    }

    public R visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<Plan, Plan> nestedLoopJoin, C context) {
        return visit(nestedLoopJoin, context);
    }

    public R visitPhysicalProject(PhysicalProject<? extends Plan> project, C context) {
        return visit(project, context);
    }

    public R visitPhysicalFilter(PhysicalFilter<Plan> filter, C context) {
        return visit(filter, context);
    }

    public R visitPhysicalDistribution(PhysicalDistribution<Plan> distribution, C context) {
        return visit(distribution, context);
    }

    public R visitPhysicalAssertNumRows(PhysicalAssertNumRows<Plan> assertNumRows, C context) {
        return visit(assertNumRows, context);
    }
}
