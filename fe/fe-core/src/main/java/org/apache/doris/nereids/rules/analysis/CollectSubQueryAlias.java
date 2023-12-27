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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * Eliminate the logical sub query and alias node after analyze and before rewrite
 * If we match the alias node and return its child node, in the execute() of the job
 * <p>
 * TODO: refactor group merge strategy to support the feature above
 */
public class CollectSubQueryAlias implements RewriteRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalSubQueryAlias().thenApply(ctx -> {
                    if (ctx.cascadesContext.isLeadingJoin()) {
                        String aliasName = ctx.root.getAlias();
                        LeadingHint leading = (LeadingHint) ctx.cascadesContext.getHintMap().get("Leading");
                        RelationId newId = ctx.statementContext.getNextRelationId();
                        leading.putRelationIdAndTableName(Pair.of(newId, aliasName));
                        leading.getRelationIdToScanMap().put(newId, ctx.root);
                        ctx.root.setRelationId(newId);
                    }
                    return ctx.root;
                }).toRule(RuleType.COLLECT_JOIN_CONSTRAINT),
                logicalRelation().thenApply(ctx -> {
                    if (ctx.cascadesContext.isLeadingJoin()) {
                        LeadingHint leading = (LeadingHint) ctx.cascadesContext.getHintMap().get("Leading");
                        LogicalRelation relation = (LogicalRelation) ctx.root;
                        RelationId relationId = relation.getRelationId();
                        if (ctx.root instanceof LogicalCatalogRelation) {
                            String relationName = ((LogicalCatalogRelation) ctx.root).getTable().getName();
                            leading.putRelationIdAndTableName(Pair.of(relationId, relationName));
                        }
                        leading.getRelationIdToScanMap().put(relationId, ctx.root);
                    }
                    return ctx.root;
                }).toRule(RuleType.COLLECT_JOIN_CONSTRAINT)
        );
    }
}
