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

import com.google.common.collect.Maps;
import org.apache.doris.analysis.SlotId;

import java.util.Map;

// This structure is maintained in each operator to store the statistical information results obtained by the operator.
public class StatsDeriveResult {
    private long rowCount = -1;
    // The data size of the corresponding column in the operator
    // The actual key is slotId
    private final Map<SlotId, Float> columnToDataSize = Maps.newHashMap();
    // The ndv of the corresponding column in the operator
    // The actual key is slotId
    private final Map<SlotId, Long> columnToNdv = Maps.newHashMap();

    public StatsDeriveResult(long rowCount, Map<SlotId, Float> columnToDataSize, Map<SlotId, Long> columnToNdv) {
        this.rowCount = rowCount;
        this.columnToDataSize.putAll(columnToDataSize);
        this.columnToNdv.putAll(columnToNdv);
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    public long getRowCount() {
        return rowCount;
    }

    public Map<SlotId, Long> getColumnToNdv() {
        return columnToNdv;
    }

    public Map<SlotId, Float> getColumnToDataSize() {
        return columnToDataSize;
    }
}
