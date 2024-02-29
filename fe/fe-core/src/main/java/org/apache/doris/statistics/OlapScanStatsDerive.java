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

package org.apache.doris.statistics;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Id;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.OlapScanNode;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Derive OlapScanNode Statistics.
 */
public class OlapScanStatsDerive extends BaseStatsDerive {

    private OlapScanNode scanNode;

    private Map<Id, String> slotIdToTableIdAndColumnName;

    @Override
    public void init(PlanStats node) throws UserException {
        Preconditions.checkState(node instanceof OlapScanNode);
        super.init(node);
        scanNode = (OlapScanNode) node;
        buildStructure(scanNode);
    }

    @Override
    public StatsDeriveResult deriveStats() {
        /*
         * Compute InAccurate cardinality before mv selector and tablet pruning.
         * - Accurate statistical information relies on the selector of materialized views and bucket reduction.
         * - However, Those both processes occur after the reorder algorithm is completed.
         * - When Join reorder is turned on, the cardinality must be calculated before the reorder algorithm.
         * - So only an inaccurate cardinality can be calculated here.
         */

        Map<Id, ColumnStatistic> columnStatisticMap = new HashMap<>();
        Table table = scanNode.getOlapTable();
        double rowCount = table.getRowCountForNereids();
        for (Map.Entry<Id, String> entry : slotIdToTableIdAndColumnName.entrySet()) {
            String colName = entry.getValue();
            // TODO. Get index id for materialized view.
            ColumnStatistic statistic =
                    Env.getCurrentEnv().getStatisticsCache().getColumnStatistics(
                        table.getDatabase().getCatalog().getId(),
                        table.getDatabase().getId(), table.getId(), -1, colName);
            if (!statistic.isUnKnown) {
                rowCount = statistic.count;
            }
            columnStatisticMap.put(entry.getKey(), statistic);
        }
        return new StatsDeriveResult(rowCount, columnStatisticMap);
    }

    /**
     * Desc: Build OlapScaNode infrastructure.
     *
     * @param: node
     * @return: void
     */
    public void buildStructure(OlapScanNode node) throws AnalysisException {
        slotIdToTableIdAndColumnName = new HashMap<>();
        for (SlotDescriptor slot : node.getTupleDesc().getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }
            String columnName = slot.getColumn().getName();
            slotIdToTableIdAndColumnName.put(slot.getId(), columnName);
        }
    }
}
