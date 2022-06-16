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

package org.apache.doris.nereids;

import org.apache.doris.nereids.operators.Operator;
import org.apache.doris.nereids.operators.plans.physical.PhysicalAggregation;
import org.apache.doris.nereids.operators.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.operators.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.operators.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.operators.plans.physical.PhysicalProject;
import org.apache.doris.nereids.operators.plans.physical.PhysicalSort;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalBinaryPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLeafPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnaryPlan;

/**
 * Base class for the processing of logical and physical plan.
 *
 * @param <R> Return type of each visit method.
 * @param <C> Context type.
 */
@SuppressWarnings("rawtypes")
public abstract class PlanOperatorVisitor<R, C> {

    public abstract R visitPlan(Plan plan, C context);

    public R visitPhysicalAggregationPlan(PhysicalUnaryPlan<PhysicalAggregation, Plan> aggPlan, C context) {
        return null;
    }

    public R visitPhysicalOlapScanPlan(PhysicalLeafPlan<PhysicalOlapScan> olapScanPlan, C context) {
        return null;
    }

    public R visitPhysicalSortPlan(PhysicalUnaryPlan<PhysicalSort, Plan> sortPlan, C context) {
        return null;
    }

    public R visitPhysicalHashJoinPlan(PhysicalBinaryPlan<PhysicalHashJoin, Plan, Plan> hashJoinPlan, C context) {
        return null;
    }

    public R visitPhysicalProject(PhysicalUnaryPlan<PhysicalProject, Plan> projectPlan, C context) {
        return null;
    }

    public R visitPhysicalFilter(PhysicalUnaryPlan<PhysicalFilter, Plan> filterPlan, C context) {
        return null;
    }

    /* Operator */

    public abstract R visitOperator(Operator operator, C context);

    public R visitPhysicalAggregation(PhysicalAggregation physicalAggregation, C context) {
        return null;
    }

    public R visitPhysicalOlapScan(PhysicalOlapScan physicalOlapScan, C context) {
        return null;
    }

    public R visitPhysicalSort(PhysicalSort physicalSort, C context) {
        return null;
    }

    public R visitPhysicalHashJoin(PhysicalHashJoin physicalHashJoin, C context) {
        return null;
    }

    public R visitPhysicalProject(PhysicalProject physicalProject,  C context) {
        return null;
    }

    public R visitPhysicalFilter(PhysicalFilter physicalFilter, C context) {
        return null;
    }
}
