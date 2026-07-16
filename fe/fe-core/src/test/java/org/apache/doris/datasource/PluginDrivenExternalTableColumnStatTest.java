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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.connector.api.ConnectorColumnStatistics;
import org.apache.doris.statistics.ColumnStatistic;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Tests {@link PluginDrivenExternalTable#toColumnStatistic}, the Doris-type-dependent column-stat math that
 * fe-core does on the connector's raw facts (HMS cutover §4.2a).
 *
 * <p>WHY: this is the byte-parity half of legacy {@code HMSExternalTable.setStatData} the connector cannot do
 * (it must not import fe-type). A string column's data size uses the source {@code avgColLen}
 * ({@code round(avgColLen * count)}); every other type uses the Doris fixed slot width
 * ({@code count * slotSize}); {@code avgSizeByte = dataSize / count}; min/max stay unconstrained. Getting the
 * string-vs-fixed-width split or the rounding wrong would skew every cardinality estimate that reads the
 * column's stats.</p>
 */
public class PluginDrivenExternalTableColumnStatTest {

    @Test
    public void stringColumnUsesAvgColLen() {
        ConnectorColumnStatistics stats = new ConnectorColumnStatistics(100, 10, 2, 5.0);
        ColumnStatistic cs = PluginDrivenExternalTable
                .toColumnStatistic(stats, new Column("c", Type.STRING)).get();
        Assertions.assertEquals(100, cs.count, 0.0);
        Assertions.assertEquals(10, cs.ndv, 0.0);
        Assertions.assertEquals(2, cs.numNulls, 0.0);
        // round(5.0 * 100) = 500; avgSizeByte = 500 / 100 = 5.0
        Assertions.assertEquals(500, cs.dataSize, 0.0);
        Assertions.assertEquals(5.0, cs.avgSizeByte, 0.0);
        Assertions.assertEquals(Double.NEGATIVE_INFINITY, cs.minValue, 0.0);
        Assertions.assertEquals(Double.POSITIVE_INFINITY, cs.maxValue, 0.0);
    }

    @Test
    public void nonStringColumnUsesSlotWidth() {
        Column column = new Column("c", Type.INT);
        long count = 100;
        long slotSize = column.getType().getSlotSize();
        // avgSizeBytes = -1 => non-string => data size from the Doris slot width.
        ConnectorColumnStatistics stats = new ConnectorColumnStatistics(count, 7, 1, -1);
        ColumnStatistic cs = PluginDrivenExternalTable.toColumnStatistic(stats, column).get();
        Assertions.assertEquals(7, cs.ndv, 0.0);
        Assertions.assertEquals(1, cs.numNulls, 0.0);
        Assertions.assertEquals((double) count * slotSize, cs.dataSize, 0.0);
        Assertions.assertEquals((double) slotSize, cs.avgSizeByte, 0.0);
    }

    @Test
    public void nonPositiveCountIsEmpty() {
        Assertions.assertFalse(PluginDrivenExternalTable
                .toColumnStatistic(new ConnectorColumnStatistics(0, 1, 0, -1), new Column("c", Type.INT))
                .isPresent());
    }

    @Test
    public void nullColumnIsEmpty() {
        Optional<ColumnStatistic> result = PluginDrivenExternalTable
                .toColumnStatistic(new ConnectorColumnStatistics(100, 1, 0, -1), null);
        Assertions.assertFalse(result.isPresent());
    }
}
