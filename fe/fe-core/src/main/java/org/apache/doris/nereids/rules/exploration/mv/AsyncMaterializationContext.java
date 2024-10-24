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
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.exploration.mv.mapping.ExpressionMapping;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
            List<Table> baseViews, CascadesContext cascadesContext, StructInfo structInfo) {
        super(mvPlan, mvOriginalPlan, MaterializedViewUtils.generateMvScanPlan(mtmv, mtmv.getBaseIndexId(),
                        mtmv.getPartitionIds(), PreAggStatus.on(), cascadesContext),
                cascadesContext, structInfo);
        this.mtmv = mtmv;
    }

    public MTMV getMtmv() {
        return mtmv;
    }

    @Override
    Plan doGenerateScanPlan(CascadesContext cascadesContext) {
        return MaterializedViewUtils.generateMvScanPlan(this.mtmv, this.mtmv.getBaseIndexId(),
                this.mtmv.getPartitionIds(), PreAggStatus.on(), cascadesContext);
    }

    @Override
    List<String> generateMaterializationIdentifier() {
        if (super.identifier == null) {
            super.identifier = MaterializationContext.generateMaterializationIdentifier(mtmv, null);
        }
        return super.identifier;
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
        return Utils.toSqlString("MaterializationContext[" + generateMaterializationIdentifier() + "]",
                "rewriteSuccess", this.success,
                "failReason", failReasonBuilder.toString());
    }

    @Override
    public Optional<Pair<Id, Statistics>> getPlanStatistics(CascadesContext cascadesContext) {
        MTMVCache mtmvCache;
        try {
            mtmvCache = mtmv.getOrGenerateCache(cascadesContext.getConnectContext());
        } catch (AnalysisException e) {
            LOG.warn(String.format("get mv plan statistics fail, materialization qualifier is %s",
                    generateMaterializationIdentifier()), e);
            return Optional.empty();
        }
        RelationId relationId = null;
        Optional<LogicalOlapScan> logicalOlapScan = this.getScanPlan(null)
                .collectFirst(LogicalOlapScan.class::isInstance);
        if (logicalOlapScan.isPresent()) {
            relationId = logicalOlapScan.get().getRelationId();
        }
        return Optional.of(Pair.of(relationId, normalizeStatisticsColumnExpression(mtmvCache.getStatistics())));
    }

    @Override
    boolean isFinalChosen(Relation relation) {
        if (!(relation instanceof PhysicalCatalogRelation)) {
            return false;
        }
        if (!(((PhysicalCatalogRelation) relation).getTable() instanceof MTMV)) {
            return false;
        }
        return ((PhysicalCatalogRelation) relation).getTable().getFullQualifiers().equals(
                this.generateMaterializationIdentifier()
        );
    }

    @Override
    public Plan getScanPlan(StructInfo queryInfo) {
        return scanPlan;
    }

    public List<Table> getBaseTables() {
        return baseTables;
    }

    public List<Table> getBaseViews() {
        return baseViews;
    }

    public ExpressionMapping getShuttledExprToScanExprMapping() {
        return shuttledExprToScanExprMapping;
    }

    public boolean isAvailable() {
        return available;
    }

    public Plan getPlan() {
        return plan;
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
