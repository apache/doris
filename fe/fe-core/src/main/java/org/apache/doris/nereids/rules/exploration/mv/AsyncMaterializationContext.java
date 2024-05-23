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
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Async context for query rewrite by materialized view
 */
public class AsyncMaterializationContext extends MaterializationContext {

    private static final Logger LOG = LogManager.getLogger(AsyncMaterializationContext.class);
    private final MTMV mtmv;

    /**
     * MaterializationContext, this contains necessary info for query rewriting by mv
     */
    public AsyncMaterializationContext(MTMV mtmv, Plan mvPlan, Plan mvOriginalPlan, List<Table> baseTables,
            List<Table> baseViews, CascadesContext cascadesContext) {
        super(mvPlan, mvOriginalPlan, MaterializedViewUtils.generateMvScanPlan(mtmv, cascadesContext), cascadesContext);
        this.mtmv = mtmv;
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    @Override
    Plan doGenerateMvPlan(CascadesContext cascadesContext) {
        return MaterializedViewUtils.generateMvScanPlan(this.mtmv, cascadesContext);
    }

    @Override
    List<String> getMaterializationQualifier() {
        return this.mtmv.getFullQualifiers();
    }

    @Override
    String getStringInfo() {
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

    @Override
    boolean isFinalChosen(Relation relation) {
        if (!(relation instanceof PhysicalCatalogRelation)) {
            return false;
        }
        return ((PhysicalCatalogRelation) relation).getTable() instanceof MTMV;
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

    public Multimap<ObjectId, Pair<String, String>> getFailReason() {
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

    public boolean isSuccess() {
        return success;
    }
}
