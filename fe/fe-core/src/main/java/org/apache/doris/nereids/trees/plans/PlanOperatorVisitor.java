// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.nereids.operators.plans.logical.LogicalFilter;
import org.apache.doris.nereids.operators.plans.logical.LogicalJoin;
import org.apache.doris.nereids.operators.plans.logical.LogicalRelation;
import org.apache.doris.nereids.operators.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.operators.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.operators.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.operators.plans.physical.PhysicalHeapSort;
import org.apache.doris.nereids.operators.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.operators.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalLeafPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnaryPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLeafPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnaryPlan;

/**
 * Base class for the processing of logical and physical plan.
 *
 * @param <R> Return type of each visit method.
 * @param <C> Context type.
 */
public abstract class PlanOperatorVisitor<R, C> {

    public abstract R visit(Plan plan, C context);

    // *******************************
    // Logical plans
    // *******************************

    public R visitLogicalRelation(LogicalLeafPlan<LogicalRelation> relation, C context) {
        return visit(relation, context);
    }

    public R visitLogicalFilter(LogicalUnaryPlan<LogicalFilter, Plan> filter, C context) {
        return visit(filter, context);
    }

    public R visitLogicalJoin(LogicalBinaryPlan<LogicalJoin, Plan, Plan> join, C context) {
        return visit(join, context);
    }

    public R visitGroupPlan(GroupPlan groupPlan, C context) {
        return visit(groupPlan, context);
    }

    // *******************************
    // Physical plans
    // *******************************

    public R visitPhysicalAggregate(PhysicalUnaryPlan<PhysicalAggregate, Plan> agg, C context) {
        return visit(agg, context);
    }

    public R visitPhysicalOlapScan(PhysicalLeafPlan<PhysicalOlapScan> olapScan, C context) {
        return visit(olapScan, context);
    }

    public R visitPhysicalHeapSort(PhysicalUnaryPlan<PhysicalHeapSort, Plan> sort, C context) {
        return visit(sort, context);
    }

    public R visitPhysicalHashJoin(PhysicalBinaryPlan<PhysicalHashJoin, Plan, Plan> hashJoin, C context) {
        return visit(hashJoin, context);
    }

    public R visitPhysicalProject(PhysicalUnaryPlan<PhysicalProject, Plan> project, C context) {
        return visit(project, context);
    }

    public R visitPhysicalFilter(PhysicalUnaryPlan<PhysicalFilter, Plan> filter, C context) {
        return visit(filter, context);
    }
}
