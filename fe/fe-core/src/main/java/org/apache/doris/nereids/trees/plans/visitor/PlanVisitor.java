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
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalCorrelatedJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalEnforceSingleRow;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribution;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHeapSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLimit;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;

/**
 * Base class for the processing of logical and physical plan.
 *
 * @param <R> Return type of each visit method.
 * @param <C> Context type.
 */
public abstract class PlanVisitor<R, C> {

    public abstract R visit(Plan plan, C context);

    // *******************************
    // Logical plans
    // *******************************

    public R visitUnboundRelation(UnboundRelation relation, C context) {
        return visit(relation, context);
    }

    public R visitLogicalRelation(LogicalRelation relation, C context) {
        return visit(relation, context);
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

    public R visitLogicalCorrelated(LogicalCorrelatedJoin<Plan, Plan> correlatedJoin, C context) {
        return visit(correlatedJoin, context);
    }

    public R visitLogicalEnforceSingleRow(LogicalEnforceSingleRow<Plan> enforceSingleRow, C context) {
        return visit(enforceSingleRow, context);
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

    public R visitPhysicalHeapSort(PhysicalHeapSort<Plan> sort, C context) {
        return visit(sort, context);
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

    public R visitPhysicalProject(PhysicalProject<Plan> project, C context) {
        return visit(project, context);
    }

    public R visitPhysicalFilter(PhysicalFilter<Plan> filter, C context) {
        return visit(filter, context);
    }

    public R visitPhysicalDistribution(PhysicalDistribution<Plan> distribution, C context) {
        return visit(distribution, context);
    }
}
