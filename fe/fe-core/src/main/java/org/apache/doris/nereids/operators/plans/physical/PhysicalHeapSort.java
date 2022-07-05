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

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanOperatorVisitor;
import org.apache.doris.nereids.trees.plans.physical.PhysicalUnaryPlan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Physical sort plan operator.
 */
public class PhysicalHeapSort extends PhysicalUnaryOperator {
    // Default offset is 0.
    private final int offset;

    private final List<OrderKey> orderKeys;

    /**
     * Constructor of PhysicalHashJoinNode.
     */
    public PhysicalHeapSort(List<OrderKey> orderKeys, long limit, int offset) {
        super(OperatorType.PHYSICAL_SORT, limit);
        this.offset = offset;
        this.orderKeys = orderKeys;
    }

    public int getOffset() {
        return offset;
    }

    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }

    public boolean hasLimit() {
        return limit > -1;
    }

    @Override
    public <R, C> R accept(PlanOperatorVisitor<R, C> visitor, Plan plan, C context) {
        return visitor.visitPhysicalHeapSort((PhysicalUnaryPlan<PhysicalHeapSort, Plan>) plan, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return ImmutableList.<Expression>builder()
                .addAll(orderKeys.stream().map(o -> o.getExpr()).collect(Collectors.toList())).build();
    }
}
