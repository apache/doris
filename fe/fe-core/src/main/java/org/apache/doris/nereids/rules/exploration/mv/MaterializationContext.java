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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Maintain the context for query rewrite by materialized view
 */
public class MaterializationContext {

    private static final Logger LOG = LogManager.getLogger(MaterializationContext.class);

    private final MTMV mtmv;
    private final List<Table> baseTables;
    private final List<Table> baseViews;
    // Group ids that are rewritten by this mv to reduce rewrite times
    private final Set<GroupId> matchedFailGroups = new HashSet<>();
    private final Set<GroupId> matchedSuccessGroups = new HashSet<>();
    // if rewrite by mv fail, record the reason, if success the failReason should be empty.
    // The key is the query belonged group expression objectId, the value is the fail reason
    private final Map<ObjectId, Pair<String, String>> failReason = new LinkedHashMap<>();
    // Should regenerate when materialization is already rewritten successfully because one query may hit repeatedly
    // make sure output is different in multi using
    private Plan mvScanPlan;
    // generated expressions form mv scan plan
    private ExpressionMapping mvExprToMvScanExprMapping;
    private List<? extends Expression> mvPlanOutputShuttledExpressions;
    private boolean available = true;
    // the mv plan from cache at present, record it to make sure query rewrite by mv is right when cache change.
    private Plan mvPlan;
    // mark rewrite success or not
    private boolean success = false;
    private boolean enableRecordFailureDetail = false;
    private StructInfo structInfo;

    /**
     * MaterializationContext, this contains necessary info for query rewriting by mv
     */
    public MaterializationContext(MTMV mtmv, Plan mvScanPlan, List<Table> baseTables, List<Table> baseViews,
            CascadesContext cascadesContext) {
        this.mtmv = mtmv;
        this.mvScanPlan = mvScanPlan;
        this.baseTables = baseTables;
        this.baseViews = baseViews;
        StatementBase parsedStatement = cascadesContext.getStatementContext().getParsedStatement();
        this.enableRecordFailureDetail = parsedStatement != null && parsedStatement.isExplain()
                && ExplainLevel.MEMO_PLAN == parsedStatement.getExplainOptions().getExplainLevel();
        MTMVCache mtmvCache = null;
        try {
            mtmvCache = mtmv.getOrGenerateCache();
        } catch (AnalysisException e) {
            LOG.warn("MaterializationContext init mv cache generate fail", e);
        }
        if (mtmvCache == null) {
            this.available = false;
            return;
        }
        this.mvPlanOutputShuttledExpressions = ExpressionUtils.shuttleExpressionWithLineage(
                mtmvCache.getOriginalPlan().getOutput(),
                mtmvCache.getOriginalPlan(),
                new BitSet());
        // mv output expression shuttle, this will be used to expression rewrite
        this.mvExprToMvScanExprMapping = ExpressionMapping.generate(this.mvPlanOutputShuttledExpressions,
                this.mvScanPlan.getExpressions());
        // copy the plan from cache, which the plan in cache may change
        this.mvPlan = mtmvCache.getLogicalPlan();
        List<StructInfo> viewStructInfos = MaterializedViewUtils.extractStructInfo(
                mtmvCache.getLogicalPlan(), cascadesContext, new BitSet());
        if (viewStructInfos.size() > 1) {
            // view struct info should only have one, log error and use the first struct info
            LOG.warn(String.format("view strut info is more than one, mv name is %s, mv plan is %s",
                    mtmv.getName(), mvPlan.treeString()));
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
            this.mvScanPlan = MaterializedViewUtils.generateMvScanPlan(this.mtmv, cascadesContext);
            // mv output expression shuttle, this will be used to expression rewrite
            this.mvExprToMvScanExprMapping = ExpressionMapping.generate(this.mvPlanOutputShuttledExpressions,
                    this.mvScanPlan.getExpressions());
        }
    }

    public MTMV getMTMV() {
        return mtmv;
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

    public Plan getMvPlan() {
        return mvPlan;
    }

    public Map<ObjectId, Pair<String, String>> getFailReason() {
        return failReason;
    }

    public boolean isEnableRecordFailureDetail() {
        return enableRecordFailureDetail;
    }

    public void setSuccess(boolean success) {
        this.success = success;
        this.failReason.clear();
    }

    public StructInfo getStructInfo() {
        return structInfo;
    }

    /**
     * recordFailReason
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

    public boolean isSuccess() {
        return success;
    }

    @Override
    public String toString() {
        StringBuilder failReasonBuilder = new StringBuilder("[").append("\n");
        for (Map.Entry<ObjectId, Pair<String, String>> reason : this.failReason.entrySet()) {
            failReasonBuilder
                    .append("\n")
                    .append("ObjectId : ").append(reason.getKey()).append(".\n")
                    .append("Summary : ").append(reason.getValue().key()).append(".\n")
                    .append("Reason : ").append(reason.getValue().value()).append(".\n");
        }
        failReasonBuilder.append("\n").append("]");
        return Utils.toSqlString("MaterializationContext[" + mtmv.getName() + "]",
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
                        && !materializationChosenNameSet.contains(materializationContext.getMTMV().getName()))
                .map(materializationContext -> materializationContext.getMTMV().getName())
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
                        .append("  Name: ").append(ctx.getMTMV().getName())
                        .append("\n")
                        .append("  FailSummary: ").append(String.join(", ", failReasonSet));
            }
        }
        return builder.toString();
    }

    /**
     * MaterializationContext fromMaterializedView
     */
    public static MaterializationContext fromMaterializedView(MTMV materializedView, Plan mvScanPlan,
            CascadesContext cascadesContext) {
        return new MaterializationContext(materializedView, mvScanPlan, ImmutableList.of(), ImmutableList.of(),
                cascadesContext);
    }
}
