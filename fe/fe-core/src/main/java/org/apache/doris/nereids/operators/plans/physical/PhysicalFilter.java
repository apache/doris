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

package org.apache.doris.nereids.operators.plans.physical;

import org.apache.doris.nereids.PlanOperatorVisitor;
import org.apache.doris.nereids.operators.PlanType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnaryPlan;

import java.util.Objects;

/**
 * Physical filter plan operator.
 */
public class PhysicalFilter extends PhysicalUnaryOperator {

    private final Expression predicates;

    public PhysicalFilter(Expression predicates) {
        super(PlanType.PHYSICAL_FILTER);
        this.predicates = Objects.requireNonNull(predicates, "predicates can not be null");
    }

    public Expression getPredicates() {
        return predicates;
    }

    @Override
    public String toString() {
        String cond;
        if (predicates == null) {
            cond = "<null>";
        } else {
            cond = predicates.toString();
        }
        return "Filter (" + cond + ")";
    }

    @Override
    public <R, C> R accept(PlanOperatorVisitor<R, C> visitor, Plan plan, C context) {
        return visitor.visitPhysicalFilter((PhysicalUnaryPlan<PhysicalFilter, Plan>) plan, context);
    }
}
