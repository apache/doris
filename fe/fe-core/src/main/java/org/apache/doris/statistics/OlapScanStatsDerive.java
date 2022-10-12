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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Id;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.OlapScanNode;

import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Derive OlapScanNode Statistics.
 */
public class OlapScanStatsDerive extends BaseStatsDerive {
    // Currently, due to the structure of doris,
    // the selected materialized view is not determined when calculating the statistical information of scan,
    // so baseIndex is used for calculation when generating Planner.

    // The rowCount here is the number of rows.
    private long inputRowCount = -1;
    private Map<Id, Float> slotIdToDataSize;
    private Map<Id, Long> slotIdToNdv;
    private Map<Id, Pair<Long, String>> slotIdToTableIdAndColumnName;

    @Override
    public void init(PlanStats node) throws UserException {
        Preconditions.checkState(node instanceof OlapScanNode);
        super.init(node);
        buildStructure((OlapScanNode) node);
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
        rowCount = inputRowCount;
        for (Map.Entry<Id, Pair<Long, String>> pairEntry : slotIdToTableIdAndColumnName.entrySet()) {
            Pair<Long, Float> ndvAndDataSize = getNdvAndDataSizeFromStatistics(pairEntry.getValue());
            long ndv = ndvAndDataSize.first;
            float dataSize = ndvAndDataSize.second;
            slotIdToNdv.put(pairEntry.getKey(), ndv);
            slotIdToDataSize.put(pairEntry.getKey(), dataSize);
        }
        return new StatsDeriveResult(deriveRowCount(), slotIdToDataSize, slotIdToNdv);
    }

    /**
     * Desc: Build OlapScaNode infrastructure.
     *
     * @param: node
     * @return: void
     */
    public void buildStructure(OlapScanNode node) throws AnalysisException {
        slotIdToDataSize = new HashMap<>();
        slotIdToNdv = new HashMap<>();
        slotIdToTableIdAndColumnName = new HashMap<>();
        if (node.getTupleDesc() != null
                && node.getTupleDesc().getTable() != null) {
            long tableId = node.getTupleDesc().getTable().getId();
            inputRowCount = (long) Env.getCurrentEnv().getStatisticsManager().getStatistics().getTableStats(tableId)
                    .getRowCount();
        }
        for (SlotDescriptor slot : node.getTupleDesc().getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }

            long tableId = slot.getParent().getTable().getId();
            String columnName = slot.getColumn().getName();
            slotIdToTableIdAndColumnName.put(slot.getId(), Pair.of(tableId, columnName));
        }
    }

    //TODO:Implement the getStatistics interface
    //now there is nothing in statistics, need to wait for collection finished
    /**
     * Desc: Get ndv and dataSize from statistics.
     *
     * @param pair TableId and ColumnName
     * @return {@link Pair}
     */
    public Pair<Long, Float> getNdvAndDataSizeFromStatistics(Pair<Long, String> pair) {
        long ndv = -1;
        float dataSize = -1;
        /*
        if (Catalog.getCurrentEnv()
                    .getStatisticsManager()
                    .getStatistics()
                    .getColumnStats(pair.first) != null) {
                ndv = Catalog.getCurrentEnv()
                        .getStatisticsManager()
                        .getStatistics()
                        .getColumnStats(pair.first).get(pair.second).getNdv();
                dataSize = Catalog.getCurrentEnv()
                        .getStatisticsManager()
                        .getStatistics()
                        .getColumnStats(pair.first).get(pair.second).getDataSize();
         }
         */
        return Pair.of(ndv, dataSize);
    }
}
