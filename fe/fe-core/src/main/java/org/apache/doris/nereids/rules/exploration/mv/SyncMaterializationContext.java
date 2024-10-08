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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.plans.ObjectId;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PreAggStatus;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Relation;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapScan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Async context for query rewrite by materialized view
 */
public class SyncMaterializationContext extends MaterializationContext {

    private final OlapTable olapTable;

    private final long indexId;

    private final String indexName;

    private final Statistics statistics;

    /**
     * MaterializationContext, this contains necessary info for query rewriting by mv
     */
    public SyncMaterializationContext(Plan mvPlan, Plan mvOriginalPlan, OlapTable olapTable,
            long indexId, String indexName, CascadesContext cascadesContext, Statistics statistics) {
        super(mvPlan, mvOriginalPlan,
                MaterializedViewUtils.generateMvScanPlan(olapTable, indexId, olapTable.getPartitionIds(),
                        PreAggStatus.unset(), cascadesContext), cascadesContext, null);
        this.olapTable = olapTable;
        this.indexId = indexId;
        this.indexName = indexName;
        this.statistics = statistics;
    }

    @Override
    Plan doGenerateScanPlan(CascadesContext cascadesContext) {
        return MaterializedViewUtils.generateMvScanPlan(olapTable, indexId, olapTable.getPartitionIds(),
                PreAggStatus.unset(), cascadesContext);
    }

    @Override
    List<String> generateMaterializationIdentifier() {
        if (super.identifier == null) {
            // for performance
            super.identifier = MaterializationContext.generateMaterializationIdentifier(olapTable, indexName);
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
    Optional<Pair<Id, Statistics>> getPlanStatistics(CascadesContext cascadesContext) {
        RelationId relationId = null;
        Optional<LogicalOlapScan> scanObj = this.getScanPlan(null)
                .collectFirst(LogicalOlapScan.class::isInstance);
        if (scanObj.isPresent()) {
            relationId = scanObj.get().getRelationId();
        }
        return Optional.of(Pair.of(relationId, normalizeStatisticsColumnExpression(statistics)));
    }

    @Override
    public Plan getScanPlan(StructInfo queryStructInfo) {
        if (queryStructInfo == null) {
            return scanPlan;
        }
        if (queryStructInfo.getRelations().size() == 1
                && queryStructInfo.getRelations().get(0) instanceof LogicalOlapScan
                && !((LogicalOlapScan) queryStructInfo.getRelations().get(0)).getSelectedPartitionIds().isEmpty()) {
            // Partition prune if sync materialized view
            return scanPlan.accept(new DefaultPlanRewriter<Void>() {
                @Override
                public Plan visitLogicalOlapScan(LogicalOlapScan olapScan, Void context) {
                    return olapScan.withSelectedPartitionIds(
                            ((LogicalOlapScan) queryStructInfo.getRelations().get(0)).getSelectedPartitionIds());
                }
            }, null);
        }
        return scanPlan;
    }

    /**
     * Calc the relation is chosen finally or not
     */
    @Override
    boolean isFinalChosen(Relation relation) {
        if (!(relation instanceof PhysicalOlapScan)) {
            return false;
        }
        return ((PhysicalOlapScan) relation).getSelectedIndexId() == indexId;
    }

}
