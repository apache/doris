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

package org.apache.doris.nereids.rules.exploration.mv.mapping;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Expression mapping, maybe one expression map to multi expression
 */
public class ExpressionMapping extends Mapping {
    private final Multimap<Expression, Expression> expressionMapping;

    public ExpressionMapping(Multimap<Expression, Expression> expressionMapping) {
        this.expressionMapping = expressionMapping;
    }

    public Multimap<Expression, Expression> getExpressionMapping() {
        return expressionMapping;
    }

    /**
     * ExpressionMapping flatten
     */
    public List<Map<Expression, Expression>> flattenMap() {
        List<List<Pair<Expression, Expression>>> tmpExpressionPairs = new ArrayList<>(this.expressionMapping.size());
        Map<? extends Expression, ? extends Collection<? extends Expression>> expressionMappingMap =
                expressionMapping.asMap();
        for (Map.Entry<? extends Expression, ? extends Collection<? extends Expression>> entry
                : expressionMappingMap.entrySet()) {
            List<Pair<Expression, Expression>> targetExpressionList = new ArrayList<>(entry.getValue().size());
            for (Expression valueExpression : entry.getValue()) {
                targetExpressionList.add(Pair.of(entry.getKey(), valueExpression));
            }
            tmpExpressionPairs.add(targetExpressionList);
        }
        List<List<Pair<Expression, Expression>>> cartesianExpressionMap = Lists.cartesianProduct(tmpExpressionPairs);

        final List<Map<Expression, Expression>> flattenedMap = new ArrayList<>();
        for (List<Pair<Expression, Expression>> listPair : cartesianExpressionMap) {
            final Map<Expression, Expression> expressionMap = new HashMap<>();
            listPair.forEach(pair -> expressionMap.put(pair.key(), pair.value()));
            flattenedMap.add(expressionMap);
        }
        return flattenedMap;
    }

    /**
     * Permute the key of expression mapping. this is useful for expression rewrite, if permute key to query based
     * then when expression rewrite success, we can get the mv scan expression directly.
     */
    public ExpressionMapping keyPermute(SlotMapping slotMapping) {
        Multimap<Expression, Expression> permutedExpressionMapping = ArrayListMultimap.create();
        Map<? extends Expression, ? extends Collection<? extends Expression>> expressionMap =
                this.getExpressionMapping().asMap();
        for (Map.Entry<? extends Expression, ? extends Collection<? extends Expression>> entry :
                expressionMap.entrySet()) {
            Expression replacedExpr = ExpressionUtils.replace(entry.getKey(), slotMapping.toSlotReferenceMap());
            permutedExpressionMapping.putAll(replacedExpr, entry.getValue());
        }
        return new ExpressionMapping(permutedExpressionMapping);
    }

    /**
     * ExpressionMapping generate
     */
    public static ExpressionMapping generate(
            List<? extends Expression> sourceExpressions,
            List<? extends Expression> targetExpressions) {
        final Multimap<Expression, Expression> expressionMultiMap =
                ArrayListMultimap.create();
        for (int i = 0; i < sourceExpressions.size(); i++) {
            expressionMultiMap.put(sourceExpressions.get(i), targetExpressions.get(i));
        }
        return new ExpressionMapping(expressionMultiMap);
    }

    @Override
    public Mapping chainedFold(Mapping target) {

        ImmutableMultimap.Builder<Expression, Expression> foldedMappingBuilder =
                ImmutableMultimap.builder();

        Multimap<Expression, Expression> targetMapping
                = ((ExpressionMapping) target).getExpressionMapping();
        for (Entry<Expression, ? extends Collection<Expression>> exprMapping :
                this.getExpressionMapping().asMap().entrySet()) {
            Collection<? extends Expression> valueExpressions = exprMapping.getValue();
            valueExpressions.forEach(valueExpr -> {
                if (targetMapping.containsKey(valueExpr)) {
                    targetMapping.get(valueExpr).forEach(
                            targetValue -> foldedMappingBuilder.put(exprMapping.getKey(), targetValue));
                }
            });
        }
        return new ExpressionMapping(foldedMappingBuilder.build());
    }
}
