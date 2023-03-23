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
import org.apache.doris.nereids.trees.plans.logical.LogicalCache;
import org.apache.doris.qe.cache.SqlCache;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *   Fetch Sql Cache
 *
 *               （fetch）      Cache
 *      RootPlan   -->           |
 *                            RootPlan
 */
public class FetchSqlCache implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.FETCH_SQL_CACHE.build(
                any().whenNot(LogicalCache.class::isInstance).thenApply(ctx -> {
                    if (!ctx.isRewriteRoot()) {
                        return ctx.root;
                    }
                    CacheContext cacheContext = ctx.statementContext.getCacheContext();
                    String cacheKey = ctx.statementContext.getOriginStatement().originStmt.toLowerCase();
                    if (ctx.statementContext.getParsedStatement().isExplain()) {
                        // todo
                        cacheKey = cacheKey.replaceFirst(
                                "^explain((\\s+(parsed)?(analyzed)?(rewritten)?(optimized)?)?\\s+plan)?\\s+", "");
                    }
                    cacheContext.setCacheKey(cacheKey);
                    if (SqlCache.getCacheDataForNereids(cacheContext)) {
                        return new LogicalCache(cacheKey, ctx.root);
                    } else {
                        return ctx.root;
                    }
                })
            )
        );
    }
}
