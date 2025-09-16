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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.exploration.mv.PartitionCompensator;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import com.google.common.collect.Multimap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Used to collect query partitions, only collect once
 * */
public class QueryPartitionCollector extends DefaultPlanVisitor<Void, CascadesContext> {

    public static final Logger LOG = LogManager.getLogger(QueryPartitionCollector.class);

    @Override
    public Void visitLogicalCatalogRelation(LogicalCatalogRelation catalogRelation, CascadesContext context) {
        TableIf table = catalogRelation.getTable();
        if (table.getDatabase() == null) {
            LOG.error("QueryPartitionCollector visitLogicalCatalogRelation database is null, table is "
                    + table.getName());
            return null;
        }
        StatementContext statementContext = context.getStatementContext();
        Map<Integer, Integer> relationIdToTableId = statementContext.getRelationIdToCommonTableIdMap();
        relationIdToTableId.put(catalogRelation.getRelationId().asInt(),
                statementContext.getTableId(catalogRelation.getTable()).asInt());
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap = statementContext
                .getTableUsedPartitionNameMap();
        Set<String> tablePartitions = new HashSet<>();
        if (catalogRelation instanceof LogicalOlapScan) {
            // Handle olap table
            LogicalOlapScan logicalOlapScan = (LogicalOlapScan) catalogRelation;
            for (Long partitionId : logicalOlapScan.getSelectedPartitionIds()) {
                tablePartitions.add(logicalOlapScan.getTable().getPartition(partitionId).getName());
            }
            tableUsedPartitionNameMap.put(table.getFullQualifiers(),
                    Pair.of(catalogRelation.getRelationId(), tablePartitions));
        } else if (catalogRelation instanceof LogicalFileScan
                && catalogRelation.getTable() != null
                && ((ExternalTable) catalogRelation.getTable()).supportInternalPartitionPruned()) {
            LogicalFileScan logicalFileScan = (LogicalFileScan) catalogRelation;
            SelectedPartitions selectedPartitions = logicalFileScan.getSelectedPartitions();
            tablePartitions.addAll(selectedPartitions.selectedPartitions.keySet());
            tableUsedPartitionNameMap.put(table.getFullQualifiers(),
                    Pair.of(catalogRelation.getRelationId(), tablePartitions));
        } else {
            // not support get partition scene, we consider query all partitions from table
            tableUsedPartitionNameMap.put(table.getFullQualifiers(), PartitionCompensator.ALL_PARTITIONS);
        }
        return null;
    }
}
