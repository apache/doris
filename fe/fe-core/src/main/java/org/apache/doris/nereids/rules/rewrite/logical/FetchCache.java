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

import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.nereids.CacheContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalCache;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.qe.cache.PartitionCache;
import org.apache.doris.qe.cache.PartitionRange;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * fetch cache and rewrite conjuncts
 */
public class FetchCache implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.PARTITION_CACHE_FETCH.build(
                logicalCache().thenApply(ctx -> {
                    CacheContext cacheContext = ctx.statementContext.getCacheContext();
                    LogicalCache<GroupPlan> logicalCache = ctx.root;
                    PartitionRange range = new PartitionRange(
                            logicalCache.getConjuncts(),
                            cacheContext.getLastOlapTable(),
                            (RangePartitionInfo) cacheContext.getLastOlapTable().getPartitionInfo());
                    cacheContext.setRange(range);
                    cacheContext.setCacheKey(ctx.cascadesContext.getMemo().copyOut(false).treeString());

                    if (PartitionCache.getCacheDataForNereids(cacheContext, range)) {
                        List<PartitionRange.PartitionSingle> rangeList = new ArrayList<>();
                        cacheContext.setHitRange(range.buildDiskPartitionRange(rangeList));
                        Set<Expression> newConjuncts = range.rewriteConjuncts(logicalCache.getConjuncts(), rangeList);
                        return new LogicalFilter(newConjuncts, logicalCache.child());
                    } else {
                        return new LogicalFilter(logicalCache.getConjuncts(), logicalCache.child());
                    }
                })
            )
        );
    }
}
