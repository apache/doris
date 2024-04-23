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
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Abstract context for query rewrite by materialized view
 */
public abstract class MaterializationContext {
    private static final Logger LOG = LogManager.getLogger(MaterializationContext.class);
    protected List<Table> baseTables;
    protected List<Table> baseViews;
    // The plan of mv def sql
    protected Plan mvPlan;
    // The original plan of mv def sql
    protected Plan originalMvPlan;
    // Should regenerate when materialization is already rewritten successfully because one query may hit repeatedly
    // make sure output is different in multi using
    protected Plan mvScanPlan;
    // The mvPlan output shuttled expression, this is used by generate field mvExprToMvScanExprMapping
    protected List<? extends Expression> mvPlanOutputShuttledExpressions;
    // Generated mapping from mv plan out shuttled expr to mv scan plan out slot mapping, this is used for later used
    protected ExpressionMapping mvExprToMvScanExprMapping;
    // This mark the materialization context is available or not,
    // will not be used in query transparent rewritten if false
    protected boolean available = true;
    // Mark the mv plan in the context is already rewritten successfully or not
    protected boolean success = false;
    // Mark enable record failure detail info or not, because record failure detail info is performance-depleting
    protected final boolean enableRecordFailureDetail;
    // The mv plan struct info
    protected final StructInfo structInfo;
    // Group id set that are rewritten unsuccessfully by this mv for reducing rewrite times
    protected final Set<GroupId> matchedFailGroups = new HashSet<>();
    // Group id set that are rewritten successfully by this mv for reducing rewrite times
    protected final Set<GroupId> matchedSuccessGroups = new HashSet<>();
    // Record the reason, if rewrite by mv fail. The failReason should be empty if success.
    // The key is the query belonged group expression objectId, the value is the fail reasons because
    // for one materialization query may be multi when nested materialized view.
    protected final Multimap<ObjectId, Pair<String, String>> failReason = HashMultimap.create();

    /**
     * MaterializationContext, this contains necessary info for query rewriting by mv
     */
    public MaterializationContext(Plan mvPlan, Plan originalMvPlan, Plan mvScanPlan, CascadesContext cascadesContext) {
        this.mvPlan = mvPlan;
        this.originalMvPlan = originalMvPlan;
        this.mvScanPlan = mvScanPlan;

        StatementBase parsedStatement = cascadesContext.getStatementContext().getParsedStatement();
        this.enableRecordFailureDetail = parsedStatement != null && parsedStatement.isExplain()
                && ExplainLevel.MEMO_PLAN == parsedStatement.getExplainOptions().getExplainLevel();

        this.mvPlanOutputShuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                originalMvPlan.getOutput(),
                originalMvPlan,
                new BitSet());
        // mv output expression shuttle, this will be used to expression rewrite
        this.mvExprToMvScanExprMapping = ExpressionMapping.generate(this.mvPlanOutputShuttledExpressions,
                this.mvScanPlan.getExpressions());
        // copy the plan from cache, which the plan in cache may change
        List<StructInfo> viewStructInfos = MaterializedViewUtils.extractStructInfo(
                mvPlan, cascadesContext, new BitSet());
        if (viewStructInfos.size() > 1) {
            // view struct info should only have one, log error and use the first struct info
            LOG.warn(String.format("view strut info is more than one, materialization name is %s, mv plan is %s",
                    getMaterializationQualifier(), getMvPlan().treeString()));
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
     * Try to generate scan plan for materialized view
     * if MaterializationContext is already rewritten by materialized view, then should generate in real time
     * when query rewrite, because one plan may hit the materialized view repeatedly and the mv scan output
     * should be different
     */
    public void tryReGenerateMvScanPlan(CascadesContext cascadesContext) {
        if (!this.matchedSuccessGroups.isEmpty()) {
            this.mvScanPlan = doGenerateMvPlan(cascadesContext);
            // mv output expression shuttle, this will be used to expression rewrite
            this.mvExprToMvScanExprMapping = ExpressionMapping.generate(this.mvPlanOutputShuttledExpressions,
                    this.mvScanPlan.getExpressions());
        }
    }

    abstract Plan doGenerateMvPlan(CascadesContext cascadesContext);

    abstract List<String> getMaterializationQualifier();

    public Plan getMvPlan() {
        return mvPlan;
    }

    public Plan getOriginalMvPlan() {
        return originalMvPlan;
    }

    public Plan getMvScanPlan() {
        return mvScanPlan;
    }

    public List<Table> getBaseTables() {
        return baseTables;
    }

    public List<Table> getBaseViews() {
        return baseViews;
    }

    public ExpressionMapping getMvExprToMvScanExprMapping() {
        return mvExprToMvScanExprMapping;
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
        StringBuilder failReasonBuilder = new StringBuilder("[").append("\n");
        for (Map.Entry<ObjectId, Collection<Pair<String, String>>> reasonEntry : this.failReason.asMap().entrySet()) {
            failReasonBuilder
                    .append("\n")
                    .append("ObjectId : ").append(reasonEntry.getKey()).append(".\n");
            for (Pair<String, String> reason : reasonEntry.getValue()) {
                failReasonBuilder.append("Summary : ").append(reason.key()).append(".\n")
                        .append("Reason : ").append(reason.value()).append(".\n");
            }
        }
        failReasonBuilder.append("\n").append("]");
        return Utils.toSqlString("MaterializationContext[" + getMaterializationQualifier() + "]",
                "rewriteSuccess", this.success,
                "failReason", failReasonBuilder.toString());
    }

    /**
     * toString, this contains summary and detail info.
     */
    public static String toDetailString(List<MaterializationContext> materializationContexts) {
        StringBuilder builder = new StringBuilder();
        builder.append("materializationContexts:").append("\n");
        for (MaterializationContext ctx : materializationContexts) {
            builder.append("\n").append(ctx).append("\n");
        }
        return builder.toString();
    }

    /**
     * toSummaryString, this contains only summary info.
     */
    public static String toSummaryString(List<MaterializationContext> materializationContexts,
            List<MTMV> chosenMaterializationNames) {
        if (materializationContexts.isEmpty()) {
            return "";
        }
        Set<String> materializationChosenNameSet = chosenMaterializationNames.stream()
                .map(MTMV::getName)
                .collect(Collectors.toSet());
        StringBuilder builder = new StringBuilder();
        builder.append("\nMaterializedView");
        // rewrite success and chosen
        builder.append("\nMaterializedViewRewriteSuccessAndChose:\n");
        if (!materializationChosenNameSet.isEmpty()) {
            builder.append("  Names: ").append(String.join(", ", materializationChosenNameSet));
        }
        // rewrite success but not chosen
        builder.append("\nMaterializedViewRewriteSuccessButNotChose:\n");
        Set<String> rewriteSuccessButNotChoseNameSet = materializationContexts.stream()
                .filter(materializationContext -> materializationContext.isSuccess()
                        && !materializationChosenNameSet.contains(materializationContext.getMaterializationQualifier()))
                .map(context -> context.getMaterializationQualifier().toString())
                .collect(Collectors.toSet());
        if (!rewriteSuccessButNotChoseNameSet.isEmpty()) {
            builder.append("  Names: ").append(String.join(", ", rewriteSuccessButNotChoseNameSet));
        }
        // rewrite fail
        builder.append("\nMaterializedViewRewriteFail:");
        for (MaterializationContext ctx : materializationContexts) {
            if (!ctx.isSuccess()) {
                Set<String> failReasonSet =
                        ctx.getFailReason().values().stream().map(Pair::key).collect(ImmutableSet.toImmutableSet());
                builder.append("\n")
                        .append("  Name: ").append(ctx.getMaterializationQualifier())
                        .append("\n")
                        .append("  FailSummary: ").append(String.join(", ", failReasonSet));
            }
        }
        return builder.toString();
    }
}
