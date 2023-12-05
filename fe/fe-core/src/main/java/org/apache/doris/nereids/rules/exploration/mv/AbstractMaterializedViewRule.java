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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionIndexMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The abstract class for all materialized view rules
 */
public abstract class AbstractMaterializedViewRule {

    /**
     * The abstract template method for query rewrite, it contains the main logic and different query
     * pattern should override the sub logic.
     */
    protected List<Plan> rewrite(Plan queryPlan, CascadesContext cascadesContext) {
        List<MaterializationContext> materializationContexts = cascadesContext.getMaterializationContexts();
        List<Plan> rewriteResults = new ArrayList<>();
        if (materializationContexts.isEmpty()) {
            return rewriteResults;
        }
        StructInfo queryStructInfo = extractStructInfo(queryPlan, cascadesContext);
        // Check query queryPlan
        if (!checkPattern(queryStructInfo)) {
            return rewriteResults;
        }

        for (MaterializationContext materializationContext : materializationContexts) {
            Plan mvPlan = materializationContext.getMvPlan();
            StructInfo viewStructInfo = extractStructInfo(mvPlan, cascadesContext);
            if (!checkPattern(viewStructInfo)) {
                continue;
            }
            if (!StructInfo.isGraphLogicalEquals(queryStructInfo.getHyperGraph(), viewStructInfo.getHyperGraph())) {
                continue;
            }
            MatchMode matchMode = decideMatchMode(queryStructInfo.getRelations(), viewStructInfo.getRelations());
            if (MatchMode.NOT_MATCH == matchMode) {
                continue;
            }
            List<RelationMapping> queryToViewTableMappings =
                    RelationMapping.generate(queryStructInfo.getRelations(), viewStructInfo.getRelations());
            for (RelationMapping queryToViewTableMapping : queryToViewTableMappings) {
                SplitPredicate compensatePredicates = predicatesCompensate(queryStructInfo, viewStructInfo,
                        queryToViewTableMapping);
                // Can not compensate, bail out
                if (compensatePredicates == null || compensatePredicates.isEmpty()) {
                    continue;
                }
                Plan rewritedPlan;
                Plan mvScan = materializationContext.getScanPlan();
                if (compensatePredicates.isAlwaysTrue()) {
                    rewritedPlan = mvScan;
                } else {
                    // Try to rewrite compensate predicates by using mv scan
                    List<NamedExpression> rewriteCompensatePredicates = rewriteExpression(
                            compensatePredicates.toList(),
                            queryStructInfo,
                            viewStructInfo,
                            queryToViewTableMapping,
                            mvScan);
                    if (rewriteCompensatePredicates.isEmpty()) {
                        continue;
                    }
                    rewritedPlan = new LogicalFilter<>(Sets.newHashSet(rewriteCompensatePredicates), mvScan);
                }
                // Rewrite query by view
                rewritedPlan = rewriteQueryByView(matchMode, queryStructInfo, viewStructInfo,
                        queryToViewTableMapping, rewritedPlan);
                if (rewritedPlan == null) {
                    continue;
                }
                rewriteResults.add(rewritedPlan);
            }
        }
        return rewriteResults;
    }

    /**Rewrite query by view, for aggregate or join rewriting should be different inherit class implementation*/
    protected Plan rewriteQueryByView(MatchMode matchMode,
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            RelationMapping queryToViewTableMappings,
            Plan tempRewritedPlan) {
        return tempRewritedPlan;
    }

    /**Use target output expression to represent the source expression*/
    protected List<NamedExpression> rewriteExpression(List<? extends Expression> sourceExpressions,
            StructInfo sourceStructInfo,
            StructInfo targetStructInfo,
            RelationMapping sourceToTargetMapping,
            Plan targetScanNode) {
        // TODO represent the sourceExpressions by using target scan node
        // Firstly, rewrite the target plan output expression using query with inverse mapping
        // then try to use the mv expression to represent the query. if any of source expressions
        // can not be represented by mv, return null
        //
        // example as following:
        //     source                           target
        //        project(slot 1, 2)              project(slot 3, 2, 1)
        //          scan(table)                        scan(table)
        //
        //     transform source to:
        //        project(slot 2, 1)
        //            target
        List<? extends Expression> targetTopExpressions = targetStructInfo.getExpressions();
        List<? extends Expression> shuttledTargetExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                targetTopExpressions, targetStructInfo.getOriginalPlan(), Sets.newHashSet(), Sets.newHashSet());
        SlotMapping sourceToTargetSlotMapping = SlotMapping.generate(sourceToTargetMapping);
        // mv sql plan expressions transform to query based
        List<? extends Expression> queryBasedExpressions = ExpressionUtils.replace(
                shuttledTargetExpressions.stream().map(Expression.class::cast).collect(Collectors.toList()),
                sourceToTargetSlotMapping.inverse().getSlotMap());
        // mv sql query based expression and index mapping
        ExpressionIndexMapping.generate(queryBasedExpressions);
        // TODO visit source expression and replace the expression with expressionIndexMapping
        return ImmutableList.of();
    }

    /**
     * Compensate mv predicates by query predicates, compensate predicate result is query based.
     * Such as a > 5 in mv, and a > 10 in query, the compensatory predicate is a > 10.
     * For another example as following:
     * predicate a = b in mv, and a = b and c = d in query, the compensatory predicate is c = d
     */
    protected SplitPredicate predicatesCompensate(
            StructInfo queryStructInfo,
            StructInfo viewStructInfo,
            RelationMapping queryToViewTableMapping
    ) {
        // TODO Equal predicate compensate
        EquivalenceClass queryEquivalenceClass = queryStructInfo.getEquivalenceClass();
        EquivalenceClass viewEquivalenceClass = viewStructInfo.getEquivalenceClass();
        if (queryEquivalenceClass.isEmpty()
                && !viewEquivalenceClass.isEmpty()) {
            return null;
        }
        // TODO range predicates and residual predicates compensate
        return SplitPredicate.empty();
    }

    /**
     * Decide the match mode
     * @see MatchMode
     */
    private MatchMode decideMatchMode(List<CatalogRelation> queryRelations, List<CatalogRelation> viewRelations) {
        List<TableIf> queryTableRefs = queryRelations
                .stream()
                .map(CatalogRelation::getTable)
                .collect(Collectors.toList());
        List<TableIf> viewTableRefs = viewRelations
                .stream()
                .map(CatalogRelation::getTable)
                .collect(Collectors.toList());
        boolean sizeSame = viewTableRefs.size() == queryTableRefs.size();
        boolean queryPartial = viewTableRefs.containsAll(queryTableRefs);
        if (!sizeSame && queryPartial) {
            return MatchMode.QUERY_PARTIAL;
        }
        boolean viewPartial = queryTableRefs.containsAll(viewTableRefs);
        if (!sizeSame && viewPartial) {
            return MatchMode.VIEW_PARTIAL;
        }
        if (sizeSame && queryPartial && viewPartial) {
            return MatchMode.COMPLETE;
        }
        return MatchMode.NOT_MATCH;
    }

    /**
     * Extract struct info from plan, support to get struct info from logical plan or plan in group.
     */
    protected StructInfo extractStructInfo(Plan plan, CascadesContext cascadesContext) {

        if (plan.getGroupExpression().isPresent()
                && plan.getGroupExpression().get().getOwnerGroup().getStructInfo().isPresent()) {
            Group belongGroup = plan.getGroupExpression().get().getOwnerGroup();
            return belongGroup.getStructInfo().get();
        } else {
            // TODO build graph from plan and extract struct from graph and set to group if exist
            // Should get structInfo from hyper graph and add into current group
            StructInfo structInfo = StructInfo.of(plan);
            if (plan.getGroupExpression().isPresent()) {
                plan.getGroupExpression().get().getOwnerGroup().setStructInfo(structInfo);
            }
            return structInfo;
        }
    }

    /**
     * Check the pattern of query or materializedView is supported or not.
     */
    protected boolean checkPattern(StructInfo structInfo) {
        if (structInfo.getRelations().isEmpty()) {
            return false;
        }
        return false;
    }

    /**
     * Query and mv match node
     */
    protected enum MatchMode {
        /**
         * The tables in query are same to the tables in view
         */
        COMPLETE,
        /**
         * The tables in query contains all the tables in view
         */
        VIEW_PARTIAL,
        /**
         * The tables in view contains all the tables in query
         */
        QUERY_PARTIAL,
        /**
         * Except for COMPLETE and VIEW_PARTIAL and QUERY_PARTIAL
         */
        NOT_MATCH
    }
}
