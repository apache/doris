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
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Maintain the context for query rewrite by materialized view
 */
public class MaterializationContext {

    private static final Logger LOG = LogManager.getLogger(MaterializationContext.class);
    private MTMV mtmv;
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

    /**
     * MaterializationContext, this contains necessary info for query rewriting by mv
     */
    public MaterializationContext(MTMV mtmv,
            Plan mvScanPlan,
            CascadesContext cascadesContext,
            List<Table> baseTables,
            List<Table> baseViews) {
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
                        mtmvCache.getMvOutputExpressions(),
                        mtmvCache.getLogicalPlan()),
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

    /**
     * MaterializationContext fromMaterializedView
     */
    public static MaterializationContext fromMaterializedView(MTMV materializedView,
            Plan mvScanPlan,
            CascadesContext cascadesContext) {
        return new MaterializationContext(
                materializedView,
                mvScanPlan,
                cascadesContext,
                ImmutableList.of(),
                ImmutableList.of());
    }
}
