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

import com.google.common.base.Preconditions;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotId;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNode;

import java.util.HashMap;
import java.util.Map;

public class OlapScanStatsDerive extends BaseStatsDerive {
    // Currently, due to the structure of doris,
    // the selected materialized view is not determined when calculating the statistical information of scan,
    // so baseIndex is used for calculation when generating Planner.

    // The rowCount here is the number of rows.
    private long inputRowCount = -1;
    private Map<SlotId, Float> slotIdToDataSize;
    private Map<SlotId, Long> slotIdToNdv;
    private Map<SlotId, Pair<Long, String>> slotIdToTableIdAndColumnName;

    @Override
    public void init(PlanNode node) throws UserException {
        Preconditions.checkState(node instanceof OlapScanNode);
        super.init(node);
        buildStructure((OlapScanNode)node);
    }

    @Override
    public StatsDeriveResult deriveStats() {
        /**
         * Compute InAccurate cardinality before mv selector and tablet pruning.
         * - Accurate statistical information relies on the selector of materialized views and bucket reduction.
         * - However, Those both processes occur after the reorder algorithm is completed.
         * - When Join reorder is turned on, the cardinality must be calculated before the reorder algorithm.
         * - So only an inaccurate cardinality can be calculated here.
         */
        rowCount = inputRowCount;
        for (Map.Entry<SlotId, Pair<Long, String>> pairEntry : slotIdToTableIdAndColumnName.entrySet()) {
            Pair<Long, Float> ndvAndDataSize = getNdvAndDataSizeFromStatistics(pairEntry.getValue());
            long ndv = ndvAndDataSize.first;
            float dataSize = ndvAndDataSize.second;
            slotIdToNdv.put(pairEntry.getKey(), ndv);
            slotIdToDataSize.put(pairEntry.getKey(), dataSize);
        }
        return new StatsDeriveResult(deriveRowCount(), slotIdToDataSize, slotIdToNdv);
    }

    public void buildStructure(OlapScanNode node) {
        slotIdToDataSize = new HashMap<>();
        slotIdToNdv = new HashMap<>();
        if (node.getTupleDesc() != null
            && node.getTupleDesc().getTable() != null) {
            long tableId = node.getTupleDesc().getTable().getId();
            inputRowCount = Catalog.getCurrentCatalog().getStatisticsManager()
                    .getStatistics().getTableStats(tableId).getRowCount();
        }
        for (SlotDescriptor slot : node.getTupleDesc().getSlots()) {
            if (!slot.isMaterialized()) {
                continue;
            }

            long tableId = slot.getParent().getTable().getId();
            String columnName = slot.getColumn().getName();
            slotIdToTableIdAndColumnName.put(slot.getId(), new Pair<>(tableId, columnName));
        }
    }

    //TODO:Implement the getStatistics interface
    //now there is nothing in statistics, need to wait for collection finished
    public Pair<Long, Float> getNdvAndDataSizeFromStatistics(Pair<Long, String> pair) {
        long ndv = -1;
        float dataSize = -1;
        /*
        if (Catalog.getCurrentCatalog()
                    .getStatisticsManager()
                    .getStatistics()
                    .getColumnStats(pair.first) != null) {
                ndv = Catalog.getCurrentCatalog()
                        .getStatisticsManager()
                        .getStatistics()
                        .getColumnStats(pair.first).get(pair.second).getNdv();
                dataSize = Catalog.getCurrentCatalog()
                        .getStatisticsManager()
                        .getStatistics()
                        .getColumnStats(pair.first).get(pair.second).getDataSize();
         }
         */
        return new Pair<>(ndv, dataSize);
    }
}
