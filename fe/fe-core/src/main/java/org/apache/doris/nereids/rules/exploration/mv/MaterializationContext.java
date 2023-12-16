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
import org.apache.doris.mtmv.MVCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.memo.GroupId;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Maintain the context for query rewrite by materialized view
 */
public class MaterializationContext {

    private MTMV mtmv;
    // Should use stmt id generator in query context
    private final Plan mvScanPlan;
    private final List<Table> baseTables;
    private final List<Table> baseViews;
    // Group ids that are rewritten by this mv to reduce rewrite times
    private final Set<GroupId> matchedGroups = new HashSet<>();
    // generate form mv scan plan
    private ExpressionMapping mvExprToMvScanExprMapping;

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
        MVCache mvCache = mtmv.getMvCache();
        // TODO This logic should move to materialized view cache manager
        if (mvCache == null) {
            mvCache = MVCache.from(mtmv, cascadesContext.getConnectContext());
            mtmv.setMvCache(mvCache);
        }
        // mv output expression shuttle, this will be used to expression rewrite
        this.mvExprToMvScanExprMapping = ExpressionMapping.generate(
                ExpressionUtils.shuttleExpressionWithLineage(
                        mvCache.getMvOutputExpressions(),
                        mvCache.getLogicalPlan()),
                mvScanPlan.getExpressions());
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

    public MTMV getMtmv() {
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
