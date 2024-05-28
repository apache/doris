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

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalRelation;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Abstract context for query rewrite by materialized view
 */
public abstract class MaterializationContext {
    private static final Logger LOG = LogManager.getLogger(MaterializationContext.class);
    public final Map<RelationMapping, SlotMapping> queryToMaterializationSlotMappingCache = new HashMap<>();
    protected List<Table> baseTables;
    protected List<Table> baseViews;
    // The plan of materialization def sql
    protected Plan plan;
    // The original plan of materialization sql
    protected Plan originalPlan;
    // Should regenerate when materialization is already rewritten successfully because one query may hit repeatedly
    // make sure output is different in multi using
    protected Plan scanPlan;
    // The materialization plan output shuttled expression, this is used by generate field
    // exprToScanExprMapping
    protected List<? extends Expression> planOutputShuttledExpressions;
    // Generated mapping from materialization plan out shuttled expr to materialization scan plan out slot mapping,
    // this is used for later used
    protected ExpressionMapping exprToScanExprMapping;
    // This mark the materialization context is available or not,
    // will not be used in query transparent rewritten if false
    protected boolean available = true;
    // Mark the materialization plan in the context is already rewritten successfully or not
    protected boolean success = false;
    // Mark enable record failure detail info or not, because record failure detail info is performance-depleting
    protected final boolean enableRecordFailureDetail;
    // The materialization plan struct info
    protected final StructInfo structInfo;
    // Group id set that are rewritten unsuccessfully by this materialization for reducing rewrite times
    protected final Set<GroupId> matchedFailGroups = new HashSet<>();
    // Group id set that are rewritten successfully by this materialization for reducing rewrite times
    protected final Set<GroupId> matchedSuccessGroups = new HashSet<>();
    // Record the reason, if rewrite by materialization fail. The failReason should be empty if success.
    // The key is the query belonged group expression objectId, the value is the fail reasons because
    // for one materialization query may be multi when nested materialized view.
    protected final Multimap<ObjectId, Pair<String, String>> failReason = HashMultimap.create();

    /**
     * MaterializationContext, this contains necessary info for query rewriting by materialization
     */
    public MaterializationContext(Plan plan, Plan originalPlan,
            Plan scanPlan, CascadesContext cascadesContext) {
        this.plan = plan;
        this.originalPlan = originalPlan;
        this.scanPlan = scanPlan;

        StatementBase parsedStatement = cascadesContext.getStatementContext().getParsedStatement();
        this.enableRecordFailureDetail = parsedStatement != null && parsedStatement.isExplain()
                && ExplainLevel.MEMO_PLAN == parsedStatement.getExplainOptions().getExplainLevel();

        this.planOutputShuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                originalPlan.getOutput(),
                originalPlan,
                new BitSet());
        // materialization output expression shuttle, this will be used to expression rewrite
        this.exprToScanExprMapping = ExpressionMapping.generate(
                this.planOutputShuttledExpressions,
                this.scanPlan.getOutput());
        // Construct materialization struct info, catch exception which may cause planner roll back
        List<StructInfo> viewStructInfos;
        try {
            viewStructInfos = MaterializedViewUtils.extractStructInfo(plan, cascadesContext, new BitSet());
            if (viewStructInfos.size() > 1) {
                // view struct info should only have one, log error and use the first struct info
                LOG.warn(String.format("view strut info is more than one, materialization scan plan is %s, "
                                + "materialization plan is %s",
                        scanPlan.treeString(), plan.treeString()));
            }
        } catch (Exception exception) {
            LOG.warn(String.format("construct materialization struct info fail, materialization scan plan is %s, "
                            + "materialization plan is %s",
                    scanPlan.treeString(), plan.treeString()), exception);
            this.available = false;
            this.structInfo = null;
            return;
        }
        this.structInfo = viewStructInfos.get(0);
    }

    public boolean alreadyRewrite(GroupId groupId) {
        return this.matchedFailGroups.contains(groupId) || this.matchedSuccessGroups.contains(groupId);
    }

    public void addMatchedGroup(GroupId groupId, boolean rewriteSuccess) {
        if (rewriteSuccess) {
            this.matchedSuccessGroups.add(groupId);
        } else {
            this.matchedFailGroups.add(groupId);
        }
    }

    /**
     * Try to generate scan plan for materialization
     * if MaterializationContext is already rewritten successfully, then should generate new scan plan in later
     * query rewrite, because one plan may hit the materialized view repeatedly and the materialization scan output
     * should be different.
     * This method should be called when query rewrite successfully
     */
    public void tryReGenerateScanPlan(CascadesContext cascadesContext) {
        this.scanPlan = doGenerateScanPlan(cascadesContext);
        // materialization output expression shuttle, this will be used to expression rewrite
        this.exprToScanExprMapping = ExpressionMapping.generate(
                this.planOutputShuttledExpressions,
                this.scanPlan.getOutput());
    }

    public void addSlotMappingToCache(RelationMapping relationMapping, SlotMapping slotMapping) {
        queryToMaterializationSlotMappingCache.put(relationMapping, slotMapping);
    }

    public SlotMapping getSlotMappingFromCache(RelationMapping relationMapping) {
        return queryToMaterializationSlotMappingCache.get(relationMapping);
    }

    /**
     * Try to generate scan plan for materialization
     * if MaterializationContext is already rewritten successfully, then should generate new scan plan in later
     * query rewrite, because one plan may hit the materialized view repeatedly and the materialization scan output
     * should be different
     */
    abstract Plan doGenerateScanPlan(CascadesContext cascadesContext);

    /**
     * Get materialization unique qualifier which identify it
     */
    abstract List<String> getMaterializationQualifier();

    /**
     * Get String info which is used for to string
     */
    abstract String getStringInfo();

    /**
     * Get materialization plan statistics, the key is the identifier of statistics
     * the value is Statistics.
     * the statistics is used by cost estimation when the materialization is used
     */
    abstract Optional<Pair<Id, Statistics>> getPlanStatistics(CascadesContext cascadesContext);

    /**
     * Calc the relation is chosen finally or not
     */
    abstract boolean isFinalChosen(Relation relation);

    public Plan getPlan() {
        return plan;
    }

    public Plan getOriginalPlan() {
        return originalPlan;
    }

    public Plan getScanPlan() {
        return scanPlan;
    }

    public List<Table> getBaseTables() {
        return baseTables;
    }

    public List<Table> getBaseViews() {
        return baseViews;
    }

    public ExpressionMapping getExprToScanExprMapping() {
        return exprToScanExprMapping;
    }

    public boolean isAvailable() {
        return available;
    }

    public Multimap<ObjectId, Pair<String, String>> getFailReason() {
        return failReason;
    }

    public boolean isEnableRecordFailureDetail() {
        return enableRecordFailureDetail;
    }

    public void setSuccess(boolean success) {
        this.success = success;
        // TODO clear the fail message by according planId ?
        this.failReason.clear();
    }

    public StructInfo getStructInfo() {
        return structInfo;
    }

    public boolean isSuccess() {
        return success;
    }

    /**
     * Record fail reason when in rewriting
     */
    public void recordFailReason(StructInfo structInfo, String summary, Supplier<String> failureReasonSupplier) {
        // record it's rewritten
        if (structInfo.getTopPlan().getGroupExpression().isPresent()) {
            this.addMatchedGroup(structInfo.getTopPlan().getGroupExpression().get().getOwnerGroup().getGroupId(),
                    false);
        }
        // once success, do not record the fail reason
        if (this.success) {
            return;
        }
        this.failReason.put(structInfo.getOriginalPlanId(),
                Pair.of(summary, this.isEnableRecordFailureDetail() ? failureReasonSupplier.get() : ""));
    }

    @Override
    public String toString() {
        return getStringInfo();
    }

    /**
     * ToSummaryString, this contains only summary info.
     */
    public static String toSummaryString(List<MaterializationContext> materializationContexts,
            PhysicalPlan physicalPlan) {
        if (materializationContexts.isEmpty()) {
            return "";
        }
        Set<MaterializationContext> rewrittenSuccessMaterializationSet = materializationContexts.stream()
                .filter(MaterializationContext::isSuccess)
                .collect(Collectors.toSet());
        Set<List<String>> chosenMaterializationQualifiers = new HashSet<>();
        physicalPlan.accept(new DefaultPlanVisitor<Void, Void>() {
            @Override
            public Void visitPhysicalRelation(PhysicalRelation physicalRelation, Void context) {
                for (MaterializationContext rewrittenContext : rewrittenSuccessMaterializationSet) {
                    if (rewrittenContext.isFinalChosen(physicalRelation)) {
                        chosenMaterializationQualifiers.add(rewrittenContext.getMaterializationQualifier());
                    }
                }
                return null;
            }
        }, null);

        StringBuilder builder = new StringBuilder();
        builder.append("\nMaterializedView");
        // rewrite success and chosen
        builder.append("\nMaterializedViewRewriteSuccessAndChose:\n");
        if (!chosenMaterializationQualifiers.isEmpty()) {
            chosenMaterializationQualifiers.forEach(materializationQualifier ->
                    builder.append(generateQualifierName(materializationQualifier)).append(", \n"));
        }
        // rewrite success but not chosen
        builder.append("\nMaterializedViewRewriteSuccessButNotChose:\n");
        Set<List<String>> rewriteSuccessButNotChoseQualifiers = rewrittenSuccessMaterializationSet.stream()
                .map(MaterializationContext::getMaterializationQualifier)
                .filter(materializationQualifier -> !chosenMaterializationQualifiers.contains(materializationQualifier))
                .collect(Collectors.toSet());
        if (!rewriteSuccessButNotChoseQualifiers.isEmpty()) {
            builder.append("  Names: ");
            rewriteSuccessButNotChoseQualifiers.forEach(materializationQualifier ->
                    builder.append(generateQualifierName(materializationQualifier)).append(", "));
        }
        // rewrite fail
        builder.append("\nMaterializedViewRewriteFail:");
        for (MaterializationContext ctx : materializationContexts) {
            if (!ctx.isSuccess()) {
                Set<String> failReasonSet =
                        ctx.getFailReason().values().stream().map(Pair::key).collect(ImmutableSet.toImmutableSet());
                builder.append("\n")
                        .append("  Name: ").append(generateQualifierName(ctx.getMaterializationQualifier()))
                        .append("\n")
                        .append("  FailSummary: ").append(String.join(", ", failReasonSet));
            }
        }
        return builder.toString();
    }

    private static String generateQualifierName(List<String> qualifiers) {
        return String.join("#", qualifiers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MaterializationContext context = (MaterializationContext) o;
        return getMaterializationQualifier().equals(context.getMaterializationQualifier());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMaterializationQualifier());
    }
}
