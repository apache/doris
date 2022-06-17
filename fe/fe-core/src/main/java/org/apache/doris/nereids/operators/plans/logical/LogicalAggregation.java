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

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 * Logical Aggregation plan operator.
 */
public class LogicalAggregation extends LogicalUnaryOperator {

    private final List<? extends NamedExpression> aggregation;

    /**
     * Desc: Constructor for LogicalAggregation.
     *
     * @param aggregation agg expression.
     */
    public LogicalAggregation(List<? extends NamedExpression> aggregation) {
        super(OperatorType.LOGICAL_AGGREGATION);
        this.aggregation = Objects.requireNonNull(aggregation, "aggregation can not be null");
    }

    /**
     * Get Aggregation list.
     *
     * @return all aggregation of this node.
     */
    public List<? extends NamedExpression> getAggregation() {
        return aggregation;
    }

    @Override
    public String toString() {
        return "Aggregation (" + StringUtils.join(aggregation, ", ") + ")";
    }

    @Override
    public List<Slot> computeOutput(Plan input) {
        return aggregation.stream()
                .map(namedExpr -> {
                    try {
                        return namedExpr.toSlot();
                    } catch (UnboundException e) {
                        throw new IllegalStateException(e);
                    }
                })
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>().addAll(aggregation).build();
    }
}
