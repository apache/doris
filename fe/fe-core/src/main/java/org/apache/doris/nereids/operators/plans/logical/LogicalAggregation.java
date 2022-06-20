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

/**
 * Logical Aggregation plan operator.
 *
 *eg:select a, sum(b), c from table group by a, c;
 * groupByExpressions: Column field after group by. eg: a, c;
 * outputExpressions: Column field after select. eg: a, sum(b), c;
 *
 * Each agg node only contains the select statement field of the same layer,
 * and other agg nodes in the subquery contain.
 */
public class LogicalAggregation extends LogicalUnaryOperator {

    private final List<Expression> groupByExpressions;
    private final List<? extends NamedExpression> outputExpressions;

    /**
     * Desc: Constructor for LogicalAggregation.
     */
    public LogicalAggregation(List<Expression> groupByExpressions,
            List<? extends NamedExpression> outputExpressions) {
        super(OperatorType.LOGICAL_AGGREGATION);
        this.groupByExpressions = groupByExpressions;
        this.outputExpressions = outputExpressions;
    }

    /**
     * Get GroupByAggregation list.
     *
     * @return all group by of this node.
     */
    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    /**
     * Get outputExpressions list.
     *
     * @return all agg expressions.
     */
    public List<? extends NamedExpression> getoutputExpressions() {
        return outputExpressions;
    }

    @Override
    public String toString() {
        return "Aggregation (" + "outputExpressions: " + StringUtils.join(outputExpressions, ", ")
                + ", groupByExpressions: " + StringUtils.join(groupByExpressions, ", ") + ")";
    }

    @Override
    public List<Slot> computeOutput(Plan input) {
        return outputExpressions.stream()
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
        return new ImmutableList.Builder<Expression>().addAll(groupByExpressions).addAll(outputExpressions).build();
    }
}
