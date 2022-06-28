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

package org.apache.doris.nereids.operators.plans.logical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Logical Sort plan operator.
 * <p>
 * eg: select * from table order by a, b desc;
 * orderKeys: list of column information after order by. eg:[a, asc],[b, desc].
 * OrderKey: Contains order expression information and sorting method. Default is ascending.
 */
public class LogicalSort extends LogicalUnaryOperator {

    // Default offset is 0.
    private int offset = 0;

    private final List<OrderKey> orderKeys;

    /**
     * Constructor for LogicalSort.
     */
    public LogicalSort(List<OrderKey> orderKeys) {
        super(OperatorType.LOGICAL_SORT);
        this.orderKeys = Objects.requireNonNull(orderKeys, "orderKeys can not be null");
    }

    @Override
    public List<Slot> computeOutput(Plan input) {
        return input.getOutput();
    }

    public List<OrderKey> getOrderKeys() {
        return orderKeys;
    }

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "Sort (" + StringUtils.join(orderKeys, ", ") + ")";
    }

    @Override
    public List<Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>().addAll(
                orderKeys.stream().map(OrderKey::getExpr).collect(Collectors.toList())).build();
    }
}
