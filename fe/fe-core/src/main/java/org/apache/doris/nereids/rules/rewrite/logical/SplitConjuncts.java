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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.CacheContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCache;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Split Conjuncts for partition cache
 */
public class SplitConjuncts implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.PARTITION_CACHE_REWRITE.build(
                logicalFilter().thenApply(ctx -> {
                    CacheContext cacheContext = ctx.statementContext.getCacheContext();
                    final LogicalFilter<GroupPlan> filter = ctx.root;
                    String columnName = cacheContext.getPartColumn().getName();

                    Map<Boolean, List<Expression>> splitConjunts = filter.getConjuncts().stream().collect(
                            Collectors.partitioningBy(expr -> {
                                if (!(expr instanceof ComparisonPredicate)) {
                                    return false;
                                }
                                ComparisonPredicate predicate = (ComparisonPredicate) expr;
                                if (!(predicate.children().get(0) instanceof SlotReference)) {
                                    return false;
                                }
                                return ((SlotReference) predicate.children().get(0)).getName().equals(columnName);
                            })
                    );
                    Set<Expression> cachedConjuncts = Sets.newHashSet(splitConjunts.get(true));
                    Set<Expression> remainConjuncts = Sets.newHashSet(splitConjunts.get(false));
                    if (cachedConjuncts.size() == 2
                            && cachedConjuncts.stream().anyMatch(ComparisonPredicate.class::isInstance)) {
                        Plan child = remainConjuncts.isEmpty() ? filter.child() :
                                new LogicalFilter(remainConjuncts, filter.child());
                        return new LogicalCache(cachedConjuncts, columnName, child);
                    }

                    return filter;
                })
            )
        );
    }
}
