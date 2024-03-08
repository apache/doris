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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Maintain the context for query rewrite by materialized view
 */
public class MaterializationContext {

    private static final Logger LOG = LogManager.getLogger(MaterializationContext.class);

    private final MTMV mtmv;
    // Should use stmt id generator in query context
    private final Plan mvScanPlan;
    private final List<Table> baseTables;
    private final List<Table> baseViews;
    // Group ids that are rewritten by this mv to reduce rewrite times
    private final Set<GroupId> matchedGroups = new HashSet<>();
    // generate form mv scan plan
    private ExpressionMapping mvExprToMvScanExprMapping;
    private boolean available = true;
    // the mv plan from cache at present, record it to make sure query rewrite by mv is right when cache change.
    private Plan mvPlan;
    // mark rewrite success or not
    private boolean success = false;
    // if rewrite by mv fail, record the reason, if success the failReason should be empty.
    // The key is the query belonged group expression objectId, the value is the fail reason
    private final Map<ObjectId, Pair<String, String>> failReason = new HashMap<>();

    /**
     * MaterializationContext, this contains necessary info for query rewriting by mv
     */
    public MaterializationContext(MTMV mtmv, Plan mvScanPlan, List<Table> baseTables, List<Table> baseViews) {
        this.mtmv = mtmv;
        this.mvScanPlan = mvScanPlan;
        this.baseTables = baseTables;
        this.baseViews = baseViews;

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
        // mv output expression shuttle, this will be used to expression rewrite
        this.mvExprToMvScanExprMapping = ExpressionMapping.generate(
                ExpressionUtils.shuttleExpressionWithLineage(
                        mtmvCache.getOriginalPlan().getOutput(),
                        mtmvCache.getOriginalPlan()),
                mvScanPlan.getExpressions());
        // copy the plan from cache, which the plan in cache may change
        this.mvPlan = mtmvCache.getLogicalPlan();
    }

    public Set<GroupId> getMatchedGroups() {
        return matchedGroups;
    }

    public boolean alreadyRewrite(GroupId groupId) {
        return this.matchedGroups.contains(groupId);
    }

    public void addMatchedGroup(GroupId groupId) {
        matchedGroups.add(groupId);
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

    public void setSuccess(boolean success) {
        this.success = success;
        this.failReason.clear();
    }

    /**
     * recordFailReason
     */
    public void recordFailReason(ObjectId objectId, Pair<String, String> summaryAndReason) {
        // once success, do not record the fail reason
        if (this.success) {
            return;
        }
        this.success = false;
        this.failReason.put(objectId, summaryAndReason);
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
    public static String toString(List<MaterializationContext> materializationContexts) {
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
        builder.append("\nMaterializedViewRewriteFail:");
        for (MaterializationContext ctx : materializationContexts) {
            if (!ctx.isSuccess()) {
                Set<String> failReasonSet =
                        ctx.getFailReason().values().stream().map(Pair::key).collect(Collectors.toSet());
                builder.append("\n")
                        .append("  Name: ").append(ctx.getMTMV().getName())
                        .append("\n")
                        .append("  FailSummary: ").append(String.join(", ", failReasonSet));
            }
        }
        builder.append("\nMaterializedViewRewriteSuccessButNotChose:\n");
        builder.append("  Names: ").append(materializationContexts.stream()
                .filter(materializationContext -> materializationContext.isSuccess()
                        && !materializationChosenNameSet.contains(materializationContext.getMTMV().getName()))
                .map(materializationContext -> materializationContext.getMTMV().getName())
                .collect(Collectors.joining(", ")));
        builder.append("\nMaterializedViewRewriteSuccessAndChose:\n");
        builder.append("  Names: ").append(String.join(", ", materializationChosenNameSet));
        return builder.toString();
    }

    /**
     * MaterializationContext fromMaterializedView
     */
    public static MaterializationContext fromMaterializedView(MTMV materializedView, Plan mvScanPlan) {
        return new MaterializationContext(
                materializedView,
                mvScanPlan,
                ImmutableList.of(),
                ImmutableList.of());
    }
}
