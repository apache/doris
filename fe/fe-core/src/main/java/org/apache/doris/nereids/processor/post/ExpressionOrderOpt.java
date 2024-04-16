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

package org.apache.doris.nereids.processor.post;

import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.qe.ConnectContext;

/**
 * expression conjuncts order opt
 */
public class ExpressionOrderOpt extends PlanPostProcessor {

    // @Override
    // public PhysicalFilter visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, CascadesContext ctx) {
    //     List<Expression> orderedConjuncts = filter.getConjuncts().stream()
    //             .sorted((conj1, conj2) ->
    //                     Integer.compare(estimateCostByDataType(conj1), estimateCostByDataType(conj2)))
    //             .collect(ImmutableList.toImmutableList());
    //     return filter;
    //
    // }

    /**
     * est by data type
     */
    public static int estimateCostByDataType(Expression conj) {
        int sum = 0;
        int defaultCost = ConnectContext.get().getSessionVariable().exprDefaultCost;
        for (Slot slot : conj.getInputSlots()) {
            if (slot.getDataType().isDecimalLikeType()) {
                sum += 10;
            } else {
                sum += defaultCost;
            }
        }
        return sum;
    }
}
