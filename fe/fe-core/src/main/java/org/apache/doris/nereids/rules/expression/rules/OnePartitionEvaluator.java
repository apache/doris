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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;

import java.util.List;
import java.util.Map;

/** the evaluator of the partition which represent one partition */
public interface OnePartitionEvaluator {
    long getPartitionId();

    /**
     * return a slot to expression mapping to replace the input.
     * for example, list partition [('1', 'a'), ('10', 'd')) with 2 column part_col1 and part_col2
     * will return a map: [{part_col1: '1', part_col2: 'a'}, {part_col1: '10', part_col2: 'd'}],
     * if any mapping replace slot and evaluate in the PartitionPredicateEvaluator return an
     * expression which not equals to BooleanLiteral.FALSE, we will scan the partition and skip
     * subsequent mapping to evaluate.
     */
    List<Map<Slot, PartitionSlotInput>> getOnePartitionInputs();

    /**
     * process children context and return current expression's context.
     * for example, range partition [('1', 'a'), ('10', 'd')) with 2 column part_col1 and part_col2,
     * if the child context contains `part_col1 = '1'`, then we will return a context which record
     * the constraint: `part_col2 >= 'a'`, further more, if both exist `part_col2 < 'a'`,
     * we will return a context which result expression is BooleanLiteral.FALSE
     */
    Expression evaluate(Expression expression, Map<Slot, PartitionSlotInput> currentInputs);

    default Expression evaluateWithDefaultPartition(Expression expression, Map<Slot, PartitionSlotInput> inputs) {
        if (isDefaultPartition()) {
            return BooleanLiteral.TRUE;
        }
        return evaluate(expression, inputs);
    }

    boolean isDefaultPartition();
}
