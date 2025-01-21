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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.FuncDeps;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.UnaryPlan;
import org.apache.doris.nereids.trees.plans.algebra.Sort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**EliminateOrderByKey*/
@DependsRules({NormalizeSort.class})
public class EliminateOrderByKey implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalSort(any()).then(EliminateOrderByKey::eliminateSort).toRule(RuleType.ELIMINATE_ORDER_BY_KEY),
                logicalTopN(any()).then(EliminateOrderByKey::eliminateTopN).toRule(RuleType.ELIMINATE_ORDER_BY_KEY));
    }

    private static Plan eliminateSort(LogicalSort<Plan> sort) {
        Optional<List<OrderKey>> retainExpression = eliminate(sort);
        return retainExpression.map(sort::withOrderKeys).orElse(null);
    }

    private static Plan eliminateTopN(LogicalTopN<Plan> topN) {
        Optional<List<OrderKey>> retainExpression = eliminate(topN);
        return retainExpression.map(topN::withOrderKeys).orElse(null);
    }

    private static <T extends UnaryPlan<Plan> & Sort> Optional<List<OrderKey>> eliminate(T sort) {
        List<OrderKey> originOrderKeys = sort.getOrderKeys();
        List<OrderKey> retainExpression = eliminateByFd(sort);
        retainExpression = eliminateDuplicate(retainExpression);
        retainExpression = eliminateByUniform(sort, retainExpression);
        if (retainExpression.equals(originOrderKeys)) {
            return Optional.empty();
        }
        return Optional.of(retainExpression);
    }

    private static <T extends UnaryPlan<Plan> & Sort> List<OrderKey> eliminateByUniform(T sort,
            List<OrderKey> orderKeys) {
        List<OrderKey> retainExpression = new ArrayList<>();
        DataTrait dataTrait = sort.child().getLogicalProperties().getTrait();
        for (OrderKey orderKey : orderKeys) {
            if (orderKey.getExpr() instanceof Slot && dataTrait.isUniformAndNotNull((Slot) orderKey.getExpr())) {
                continue;
            }
            retainExpression.add(orderKey);
        }
        return retainExpression;
    }

    private static <T extends UnaryPlan<Plan> & Sort> List<OrderKey> eliminateByFd(T sort) {
        // eliminate order by key by fd. e.g. order by a,abs(a) -> order by a
        Map<OrderKey, Set<Slot>> orderBySlotMap = new HashMap<>();
        Set<Slot> validSlots = new HashSet<>();
        for (OrderKey orderKey : sort.getOrderKeys()) {
            Expression expr = orderKey.getExpr();
            orderBySlotMap.put(orderKey, expr.getInputSlots());
            validSlots.addAll(expr.getInputSlots());
        }

        FuncDeps funcDeps = sort.child().getLogicalProperties().getTrait().getAllValidFuncDeps(validSlots);
        if (funcDeps.isEmpty()) {
            return sort.getOrderKeys();
        }

        Set<Set<Slot>> minOrderBySlots = funcDeps.eliminateDeps(new HashSet<>(orderBySlotMap.values()),
                new HashSet<>());
        List<OrderKey> retainExpression = new ArrayList<>();
        for (Map.Entry<OrderKey, Set<Slot>> entry : orderBySlotMap.entrySet()) {
            if (minOrderBySlots.contains(entry.getValue())) {
                retainExpression.add(entry.getKey());
            }
        }
        return retainExpression;
    }

    private static <T extends UnaryPlan<Plan> & Sort> List<OrderKey> eliminateDuplicate(List<OrderKey> orderKeys) {
        // eliminate same order by expr. e.g. order by a,a -> order by a
        List<OrderKey> uniqueOrderKeys = new ArrayList<>();
        Set<Expression> set = new HashSet<>();
        for (OrderKey orderKey : orderKeys) {
            if (set.contains(orderKey.getExpr())) {
                continue;
            }
            uniqueOrderKeys.add(orderKey);
            set.add(orderKey.getExpr());
        }
        return uniqueOrderKeys;
    }
}
