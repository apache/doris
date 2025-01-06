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
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.exploration.mv.PartitionCompensator;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Multimap;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Used to collect query partitions, only collect once
 * */
public class QueryPartitionCollector extends DefaultPlanRewriter<ConnectContext> implements CustomRewriter {

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {

        ConnectContext connectContext = ConnectContext.get();
        if (connectContext != null && connectContext.getSessionVariable().internalSession) {
            return plan;
        }
        plan.accept(this, connectContext);
        return plan;
    }

    @Override
    public Plan visitLogicalCatalogRelation(LogicalCatalogRelation catalogRelation, ConnectContext context) {

        TableIf table = catalogRelation.getTable();
        if (table.getDatabase() == null) {
            // logic for test
            return catalogRelation;
        }
        Multimap<List<String>, Pair<RelationId, Set<String>>> tableUsedPartitionNameMap = context.getStatementContext()
                .getTableUsedPartitionNameMap();
        Set<String> tablePartitions = new HashSet<>();
        if (catalogRelation instanceof LogicalOlapScan) {
            // Handle olap table
            LogicalOlapScan logicalOlapScan = (LogicalOlapScan) catalogRelation;
            for (Long partitionId : logicalOlapScan.getSelectedPartitionIds()) {
                tablePartitions.add(logicalOlapScan.getTable().getPartition(partitionId).getName());
            }
        } else if (catalogRelation instanceof LogicalFileScan
                && catalogRelation.getTable() != null
                && ((ExternalTable) catalogRelation.getTable()).supportInternalPartitionPruned()) {
            LogicalFileScan logicalFileScan = (LogicalFileScan) catalogRelation;
            SelectedPartitions selectedPartitions = logicalFileScan.getSelectedPartitions();
            tablePartitions.addAll(selectedPartitions.selectedPartitions.keySet());
        } else {
            // todo when supported get query used partitions, should put actual used partitions but not ALL_PARTITIONS
            tableUsedPartitionNameMap.put(table.getFullQualifiers(), PartitionCompensator.ALL_PARTITIONS);
        }
        // only collect once and maybe query more than once, we collect all of them to make sure query data is
        // correct
        tableUsedPartitionNameMap.put(table.getFullQualifiers(),
                Pair.of(catalogRelation.getRelationId(), tablePartitions));
        return catalogRelation;
    }
}
