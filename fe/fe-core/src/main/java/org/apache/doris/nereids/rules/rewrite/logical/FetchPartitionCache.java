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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalCache;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.qe.cache.PartitionCache;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.stream.Collectors;

/**
 * fetch partition cache and rewrite conjuncts
 *
 *               （fetch）      Union
 *                 -->        /      \
 *     Filter             Filter    Cache
 *        |                 |          |
 *     OlapScan          OlapScan    Filter
 *                                     |
 *                                   OlapScan
 *                        (miss)      (hit)
 */
public class FetchPartitionCache implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.FETCH_PARTITION_CACHE.build(
                logicalFilter(logicalOlapScan().whenNot(LogicalOlapScan::isPartitionCached)).thenApply(ctx -> {
                    CacheContext cacheContext = ctx.statementContext.getCacheContext();
                    final LogicalFilter<LogicalOlapScan> filter = ctx.root;
                    final LogicalOlapScan olapScan = filter.child();
                    List<Long> partitionIds = ImmutableList.of(cacheContext.getLastPartition().getId());

                    if (PartitionCache.getCacheDataForNereids(cacheContext)) {
                        // miss
                        LogicalOlapScan missScan = olapScan.withCachePartition(ImmutableList.of());
                        LogicalFilter missFilter = new LogicalFilter(cacheContext.getMissConjuncts(), missScan);

                        // hit
                        LogicalOlapScan hitScan = olapScan.withCachePartition(partitionIds);
                        LogicalFilter hitFilter = new LogicalFilter(cacheContext.getHitConjuncts(), hitScan);
                        LogicalCache hitCache = new LogicalCache(cacheContext.getCacheKey(), hitFilter);

                        List<NamedExpression> slots = filter.getOutput().stream()
                                .map(m -> (NamedExpression) m).collect(Collectors.toList());
                        return new LogicalUnion(SetOperation.Qualifier.ALL, slots,
                                false, false, ImmutableList.of(missFilter, hitCache)
                        );
                    } else {
                        LogicalOlapScan checkedScan = olapScan.withCachePartition(partitionIds);
                        return filter.withChildren(checkedScan);
                    }
                })
            )
        );
    }
}
