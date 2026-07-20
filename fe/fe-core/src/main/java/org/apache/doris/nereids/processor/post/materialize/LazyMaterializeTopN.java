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

package org.apache.doris.nereids.processor.post.materialize;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.processor.post.PlanPostProcessor;
import org.apache.doris.nereids.processor.post.Validator;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalLazyMaterialize;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTVFRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTopN;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Post rule to insert MaterializeNode for TopN lazy materialization.
 * Expression pull-up is handled by PullUpProjectExprUnderTopN in the logical phase. That stage may only move
 * expressions accepted by TopNLazyExpressionEligibility and must preserve their input slots below TopN. This
 * physical stage never moves expressions; it resolves source lineage, chooses a fetch schedule, and rewrites slots.
 *
 * <p>Separate branches receive independent specs. For nested TopNs, the outermost eligible TopN wins so that
 * materialization remains above the complete local/merge TopN chain. Inner TopNs are considered only when the
 * outer candidate is not applicable.
 */
public class LazyMaterializeTopN extends PlanPostProcessor {
    private static final Logger LOG = LogManager.getLogger(LazyMaterializeTopN.class);

    @Override
    public Plan visitPhysicalTopN(PhysicalTopN<? extends Plan> topN, CascadesContext ctx) {
        Plan result = computeTopN(topN);
        if (result != topN) {
            if (SessionVariable.isFeDebug()) {
                new Validator().processRoot(result, ctx);
            }
            return result;
        }
        return DefaultPlanRewriter.visitChildren(this, topN, ctx);
    }

    private Plan computeTopN(PhysicalTopN<? extends Plan> topN) {
        if (SessionVariable.getTopNLazyMaterializationThreshold() < topN.getLimit()) {
            return topN;
        }
        List<Slot> userVisibleOutput = ImmutableList.copyOf(topN.getOutput());
        List<Slot> effectiveOutput = distinctOutput(topN.getOutput());
        Plan result = doComputeTopN(topN, effectiveOutput);
        if (result == topN) {
            return topN;
        }
        return new PhysicalProject(ImmutableList.copyOf(userVisibleOutput), null, result);
    }

    static List<Slot> distinctOutput(List<Slot> output) {
        return ImmutableList.copyOf(new LinkedHashSet<>(output));
    }

    private Plan doComputeTopN(PhysicalTopN<? extends Plan> topN, List<Slot> effectiveOutput) {
        MaterializationAnalysisResult<TopNLazyAnalysis> analysisResult =
                new TopNLazyMaterializationAnalyzer().analyze(topN, effectiveOutput);
        if (!analysisResult.isApplicable()) {
            LOG.debug("Skip TopN lazy materialization: {} ({})",
                    analysisResult.getReason().orElse(null), analysisResult.getDetail());
            return topN;
        }
        TopNLazyAnalysis analysis = analysisResult.getValue();
        Map<Slot, MaterializeSource> materializeMap = analysis.getLazyOutputs();
        List<Slot> materializedSlots = new ArrayList<>(analysis.getMaterializedSlots());
        Map<RelationId, Relation> relationsById = new TreeMap<>();
        for (MaterializeSource source : materializeMap.values()) {
            relationsById.putIfAbsent(source.getRelation().getRelationId(), source.getRelation());
        }

        Map<RelationId, SlotReference> rowIdsByRelationId = new TreeMap<>();
        StatementContext threadStatementContext = StatementScopeIdGenerator.getStatementContext();
        Set<RelationId> nullableRelationIds = collectNullableRelationIds(topN);
        for (Relation relation : relationsById.values()) {
            SlotReference rowIdSlot = createRowIdSlot(relation, threadStatementContext);
            rowIdsByRelationId.put(relation.getRelationId(), nullableRelationIds.contains(relation.getRelationId())
                    ? (SlotReference) rowIdSlot.withNullable(true) : rowIdSlot);
        }

        TopNLazyMaterializationSpec spec = TopNLazyMaterializationSpec.create(
                effectiveOutput, materializedSlots, rowIdsByRelationId, materializeMap);
        Plan result = new TopNLazyMaterializationRewriter(spec).rewrite(topN);
        if (!result.getOutput().equals(spec.getMaterializeInput())) {
            List<NamedExpression> materializeInput = new ArrayList<>();
            for (Slot slot : spec.getMaterializeInput()) {
                materializeInput.add((SlotReference) slot);
            }
            result = new PhysicalProject(materializeInput, null, result);
        }
        result = new PhysicalLazyMaterialize(result, spec, null, ((AbstractPlan) result).getStats());
        return result;
    }

    private SlotReference createRowIdSlot(Relation relation, StatementContext statementContext) {
        TableIf table;
        String sourceName;
        List<String> qualifier;
        if (relation instanceof CatalogRelation) {
            CatalogRelation catalogRelation = (CatalogRelation) relation;
            table = catalogRelation.getTable();
            sourceName = table.getName();
            qualifier = catalogRelation.getQualifier();
        } else if (relation instanceof PhysicalTVFRelation) {
            PhysicalTVFRelation tvfRelation = (PhysicalTVFRelation) relation;
            table = tvfRelation.getFunction().getTable();
            sourceName = tvfRelation.getFunction().getName();
            qualifier = ImmutableList.of();
        } else {
            throw new IllegalStateException("Unsupported lazy source relation: " + relation);
        }
        Column rowIdColumn = new Column(Column.GLOBAL_ROWID_COL + sourceName,
                Type.STRING, false, AggregateType.REPLACE, false,
                sourceName + ".global_row_id", false, Integer.MAX_VALUE);
        return SlotReference.fromColumn(statementContext.getNextExprId(), table, rowIdColumn, qualifier);
    }

    private Set<RelationId> collectNullableRelationIds(Plan plan) {
        Set<RelationId> nullableRelationIds = new LinkedHashSet<>();
        collectNullableRelationIds(plan, false, nullableRelationIds);
        return nullableRelationIds;
    }

    private void collectNullableRelationIds(Plan plan, boolean nullableFromParent,
            Set<RelationId> nullableRelationIds) {
        if (plan instanceof Relation) {
            if (nullableFromParent) {
                nullableRelationIds.add(((Relation) plan).getRelationId());
            }
            return;
        }
        if (plan instanceof AbstractPhysicalJoin) {
            AbstractPhysicalJoin<?, ?> join = (AbstractPhysicalJoin<?, ?>) plan;
            // A row-id follows the same nullability as its relation's join output. Propagate the
            // null-supplying side through the whole subtree because it may contain more than one relation.
            boolean nullableLeft = nullableFromParent || join.getJoinType().isFullOuterJoin()
                    || join.getJoinType().isRightOuterJoin() || join.getJoinType().isAsofRightOuterJoin();
            boolean nullableRight = nullableFromParent || join.getJoinType().isFullOuterJoin()
                    || join.getJoinType().isLeftOuterJoin() || join.getJoinType().isAsofLeftOuterJoin();
            collectNullableRelationIds(join.left(), nullableLeft, nullableRelationIds);
            collectNullableRelationIds(join.right(), nullableRight, nullableRelationIds);
            return;
        }
        for (Plan child : plan.children()) {
            collectNullableRelationIds(child, nullableFromParent, nullableRelationIds);
        }
    }
}
