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

import java.util.List;
import java.util.Objects;

/**
 * Logical filter plan operator.
 */
public class LogicalFilter extends LogicalUnaryOperator {

    private final Expression predicates;

    public LogicalFilter(Expression predicates) {
        super(OperatorType.LOGICAL_FILTER);
        this.predicates = Objects.requireNonNull(predicates, "predicates can not be null");
    }

    public Expression getPredicates() {
        return predicates;
    }


    @Override
    public List<Slot> computeOutput(Plan input) {
        return input.getOutput();
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
}
