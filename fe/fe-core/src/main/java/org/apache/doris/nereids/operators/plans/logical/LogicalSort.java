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
 *
 * eg: select * from table order by a, b desc;
 * sortItems: list of column information after order by. eg:[a, asc],[b, desc].
 * SortItems: Contains order expression information and sorting method. Default is ascending.
 */
public class LogicalSort extends LogicalUnaryOperator {

    private List<SortItems> sortItems;

    /**
     * Constructor for SortItems.
     */
    public LogicalSort(List<SortItems> sortItems) {
        super(OperatorType.LOGICAL_SORT);
        this.sortItems = Objects.requireNonNull(sortItems, "sorItems can not be null");
    }

    @Override
    public String toString() {
        return "Sort (" + StringUtils.join(sortItems, ", ") + ")";
    }

    @Override
    public List<Slot> computeOutput(Plan input) {
        return input.getOutput();
    }

    /**
     * Get SortItems.
     *
     * @return List of SortItems.
     */
    public List<SortItems> getSortItems() {
        return sortItems;
    }

    @Override
    public List<Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>().addAll(
                sortItems.stream().map(expr -> expr.getSort()).collect(Collectors.toList()))
                .build();
    }

    /**
     * SortItem. Show sort expressions and their order types.
     */
    public static class SortItems {
        /**
         * enum of OrderDirection.
         */
        public enum OrderDirection {
            ASC,
            DESC
        }

        private final Expression sort;
        private final OrderDirection orderDirection;

        public SortItems(Expression sort, OrderDirection orderDirection) {
            this.sort = sort;
            this.orderDirection = orderDirection;
        }

        public Expression getSort() {
            return sort;
        }

        /**
         * Get OrderDirection.
         *
         * @return boolean.
         */
        public OrderDirection getOrderDirection() {
            return orderDirection;
        }

        public String toString() {
            return "Expression: " + sort.sql() + " OrderDirection: " + orderDirection.toString();
        }
    }
}
