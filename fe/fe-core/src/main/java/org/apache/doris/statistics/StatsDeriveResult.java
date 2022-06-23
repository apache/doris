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

import com.google.common.collect.Maps;

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
    private final Map<Id, Float> columnToDataSize = Maps.newHashMap();
    // The ndv of the corresponding column in the operator
    // The actual key is slotId
    private final Map<Id, Long> columnToNdv = Maps.newHashMap();

    public StatsDeriveResult(long rowCount, Map<Id, Float> columnToDataSize, Map<Id, Long> columnToNdv) {
        this.rowCount = rowCount;
        this.columnToDataSize.putAll(columnToDataSize);
        this.columnToNdv.putAll(columnToNdv);
    }

    public float computeSize() {
        return Math.max(1, columnToDataSize.values().stream().reduce((float) 0, Float::sum)) * rowCount;
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

        for (Entry<Id, Float> entry : columnToDataSize.entrySet()) {
            if (slotIds.contains(entry.getKey())) {
                count += entry.getValue();
                exist = true;
            }
        }
        if (!exist) {
            return 1;
        }
        return count * rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getRowCount() {
        return rowCount;
    }

    public Map<Id, Long> getColumnToNdv() {
        return columnToNdv;
    }

    public Map<Id, Float> getColumnToDataSize() {
        return columnToDataSize;
    }
}
