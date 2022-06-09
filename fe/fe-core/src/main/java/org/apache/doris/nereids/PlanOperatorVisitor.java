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
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

/**
 * Base class for the processing of logical and physical plan
 * @param <R> Return type of each visit method
 * @param <C> Context type
 */
@SuppressWarnings("rawtypes")
public abstract class PlanOperatorVisitor<R, C> {

    public abstract R visit(Plan<? extends Plan, ? extends Operator> plan, C context);

    public R visitPhysicalAggregationPlan(PhysicalPlan<? extends PhysicalPlan, PhysicalAggregation> aggPlan,
            C context) {
        return null;
    }

    public R visitPhysicalOlapScanPlan(PhysicalPlan<? extends PhysicalPlan, PhysicalOlapScan> olapScanPlan,
            C context) {
        return null;
    }

    public R visitPhysicalSortPlan(PhysicalPlan<? extends PhysicalPlan, PhysicalSort> sortPlan,
            C context) {
        return null;
    }

    public R visitPhysicalHashJoinPlan(PhysicalPlan<? extends PhysicalPlan, PhysicalHashJoin> hashJoinPlan,
            C context) {
        return null;
    }

    public R visitPhysicalProject(PhysicalPlan<? extends PhysicalPlan, PhysicalProject> projectPlan,
            C context) {
        return null;
    }

    public R visitPhysicalFilter(PhysicalPlan<? extends PhysicalPlan, PhysicalFilter> filterPlan,
            C context) {
        return null;
    }

}
