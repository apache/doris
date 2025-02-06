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

import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.FuncDeps;
import org.apache.doris.nereids.properties.FuncDeps.FuncDepsItem;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 1.eliminate by duplicate
 * select a from t1 order by a, a;
 * ->
 * select a from t1 order by a;
 * 2.eliminate by function dependency
 * select a from t1 order by a, a+1;
 * select a from t1 order by a, abs(a) ;
 * select a from t1 where a=c order by a,c
 * ->
 * select a from t1 order by a;
 * 3.eliminate by uniform
 * select a,b,c from test where a=1 order by a;
 * ->
 * select a,b,c from test where a=1;
 * */
@DependsRules({
        NormalizeSort.class,
        ExtractAndNormalizeWindowExpression.class,
        CheckAndStandardizeWindowFunctionAndFrame.class})
public class EliminateOrderByKey implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalSort(any()).then(EliminateOrderByKey::eliminateSort).toRule(RuleType.ELIMINATE_ORDER_BY_KEY),
                logicalWindow(any()).then(EliminateOrderByKey::eliminateWindow)
                        .toRule(RuleType.ELIMINATE_ORDER_BY_KEY));
    }

    private static Plan eliminateWindow(LogicalWindow<Plan> window) {
        DataTrait dataTrait = window.child().getLogicalProperties().getTrait();
        List<NamedExpression> newNamedExpressions = new ArrayList<>();
        boolean changed = false;
        for (NamedExpression expr : window.getWindowExpressions()) {
            Alias alias = (Alias) expr;
            WindowExpression windowExpression = (WindowExpression) alias.child();
            List<OrderExpression> orderExpressions = windowExpression.getOrderKeys();
            if (orderExpressions.stream().anyMatch((
                    orderKey -> orderKey.getDataType().isOnlyMetricType()))) {
                throw new AnalysisException(Type.OnlyMetricTypeErrorMsg);
            }
            List<OrderKey> orderKeys = new ArrayList<>();
            for (OrderExpression orderExpression : orderExpressions) {
                orderKeys.add(orderExpression.getOrderKey());
            }
            List<OrderKey> retainExpression = eliminate(dataTrait, orderKeys);
            if (retainExpression.size() == orderKeys.size()) {
                newNamedExpressions.add(expr);
                continue;
            }
            changed = true;
            List<OrderExpression> newOrderExpressions = new ArrayList<>();
            for (OrderKey orderKey : retainExpression) {
                newOrderExpressions.add(new OrderExpression(orderKey));
            }
            WindowExpression newWindowExpression = windowExpression.withOrderKeys(newOrderExpressions);
            newNamedExpressions.add(alias.withChildren(ImmutableList.of(newWindowExpression)));
        }
        return changed ? window.withExpressionsAndChild(newNamedExpressions, window.child()) : window;
    }

    private static Plan eliminateSort(LogicalSort<Plan> sort) {
        DataTrait dataTrait = sort.child().getLogicalProperties().getTrait();
        List<OrderKey> retainExpression = eliminate(dataTrait, sort.getOrderKeys());
        if (retainExpression.isEmpty()) {
            return sort.child();
        } else if (retainExpression.size() == sort.getOrderKeys().size()) {
            return sort;
        }
        return sort.withOrderKeys(retainExpression);
    }

    private static List<OrderKey> eliminate(DataTrait dataTrait, List<OrderKey> orderKeys) {
        List<OrderKey> retainExpression = eliminateDuplicate(orderKeys);
        retainExpression = eliminateByFd(dataTrait, retainExpression);
        retainExpression = eliminateByUniform(dataTrait, retainExpression);
        return retainExpression;
    }

    private static List<OrderKey> eliminateByUniform(DataTrait dataTrait, List<OrderKey> orderKeys) {
        List<OrderKey> retainExpression = new ArrayList<>();
        for (OrderKey orderKey : orderKeys) {
            if (orderKey.getExpr() instanceof Slot && dataTrait.isUniformAndNotNull((Slot) orderKey.getExpr())) {
                continue;
            }
            retainExpression.add(orderKey);
        }
        return retainExpression;
    }

    private static List<OrderKey> eliminateByFd(DataTrait dataTrait, List<OrderKey> inputOrderKeys) {
        Map<Expression, Integer> slotToIndex = new HashMap<>();
        Set<Slot> validSlots = new HashSet<>();
        List<Boolean> retainOrderKey = new ArrayList<>(inputOrderKeys.size());
        for (int i = 0; i < inputOrderKeys.size(); ++i) {
            Expression expr = inputOrderKeys.get(i).getExpr();
            validSlots.addAll(ImmutableList.of((Slot) expr));
            slotToIndex.put(expr, i);
            retainOrderKey.add(true);
        }
        FuncDeps funcDeps = dataTrait.getAllValidFuncDeps(validSlots);
        if (funcDeps.isEmpty()) {
            return inputOrderKeys;
        }

        for (FuncDepsItem funcDep : funcDeps.getItems()) {
            Set<Slot> determinants = funcDep.determinants;
            Set<Slot> dependencies = funcDep.dependencies;
            if (!canEliminateSortKey(determinants, dependencies, slotToIndex)) {
                continue;
            }
            for (Slot slot : dependencies) {
                retainOrderKey.set(slotToIndex.get(slot), false);
            }
        }
        List<OrderKey> retainExpression = new ArrayList<>();
        for (int i = 0; i < retainOrderKey.size(); ++i) {
            if (retainOrderKey.get(i)) {
                retainExpression.add(inputOrderKeys.get(i));
            }
        }
        return retainExpression;
    }

    private static boolean canEliminateSortKey(Set<Slot> determinants, Set<Slot> dependencies,
            Map<Expression, Integer> slotToIndex) {
        int max = -1;
        for (Slot slot : determinants) {
            max = slotToIndex.get(slot) > max ? slotToIndex.get(slot) : max;
        }
        int min = Integer.MAX_VALUE;
        for (Slot slot : dependencies) {
            min = slotToIndex.get(slot) < min ? slotToIndex.get(slot) : min;
        }
        return max < min;
    }

    private static List<OrderKey> eliminateDuplicate(List<OrderKey> orderKeys) {
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
