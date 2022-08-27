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

import org.apache.doris.common.Id;
import org.apache.doris.nereids.trees.expressions.Slot;

import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * This structure is maintained in each operator to store the statistical information results obtained by the operator.
 */
public class StatsDeriveResult {
    private long rowCount = -1;
    // The data size of the corresponding column in the operator
    // The actual key is slotId
    private final Map<Id, Float> columnIdToDataSize = Maps.newHashMap();
    // The ndv of the corresponding column in the operator
    // The actual key is slotId
    private final Map<Id, Long> columnIdToNdv = Maps.newHashMap();

    private Map<Slot, ColumnStats> slotToColumnStats;

    public StatsDeriveResult(long rowCount, Map<Slot, ColumnStats> slotToColumnStats) {
        this.rowCount = rowCount;
        this.slotToColumnStats = slotToColumnStats;
    }

    public StatsDeriveResult(long rowCount, Map<Id, Float> columnIdToDataSize, Map<Id, Long> columnIdToNdv) {
        this.rowCount = rowCount;
        this.columnIdToDataSize.putAll(columnIdToDataSize);
        this.columnIdToNdv.putAll(columnIdToNdv);
    }

    public StatsDeriveResult(StatsDeriveResult another) {
        this.rowCount = another.rowCount;
        this.columnIdToDataSize.putAll(another.columnIdToDataSize);
        this.columnIdToNdv.putAll(another.columnIdToNdv);
        slotToColumnStats = new HashMap<>();
        for (Entry<Slot, ColumnStats> entry : another.slotToColumnStats.entrySet()) {
            slotToColumnStats.put(entry.getKey(), entry.getValue().copy());
        }
    }

    public float computeSize() {
        return Math.max(1, columnIdToDataSize.values().stream().reduce((float) 0, Float::sum)) * rowCount;
    }

    /**
     * Compute the data size of all input columns.
     *
     * @param slotIds all input columns.
     * @return sum data size.
     */
    public float computeColumnSize(List<Id> slotIds) {
        float count = 0;
        boolean exist = false;

        for (Entry<Id, Float> entry : columnIdToDataSize.entrySet()) {
            if (slotIds.contains(entry.getKey())) {
                count += entry.getValue();
                exist = true;
            }
        }
        if (!exist) {
            count = (float) 1.0;
        }
        return count * rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getRowCount() {
        return rowCount;
    }

    public Map<Id, Long> getColumnIdToNdv() {
        return columnIdToNdv;
    }

    public Map<Id, Float> getColumnIdToDataSize() {
        return columnIdToDataSize;
    }

    public Map<Slot, ColumnStats> getSlotToColumnStats() {
        return slotToColumnStats;
    }

    public void setSlotToColumnStats(Map<Slot, ColumnStats> slotToColumnStats) {
        this.slotToColumnStats = slotToColumnStats;
    }

    public StatsDeriveResult updateRowCountBySelectivity(double selectivity) {
        rowCount *= selectivity;
        for (Entry<Slot, ColumnStats> entry : slotToColumnStats.entrySet()) {
            entry.getValue().updateBySelectivity(selectivity);
        }
        return this;
    }

    public StatsDeriveResult updateRowCountByLimit(long limit) {
        if (limit > 0 && rowCount > 0 && rowCount > limit) {
            double selectivity = ((double) limit) / rowCount;
            rowCount = limit;
            for (Entry<Slot, ColumnStats> entry : slotToColumnStats.entrySet()) {
                entry.getValue().updateBySelectivity(selectivity);
            }
        }
        return this;
    }

    public StatsDeriveResult merge(StatsDeriveResult other) {
        for (Entry<Slot, ColumnStats> entry : other.getSlotToColumnStats().entrySet()) {
            this.slotToColumnStats.put(entry.getKey(), entry.getValue().copy());
        }
        return this;
    }

    public StatsDeriveResult copy() {
        return new StatsDeriveResult(this);
    }
}
