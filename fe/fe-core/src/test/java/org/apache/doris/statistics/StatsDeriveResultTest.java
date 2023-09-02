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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Date;

public class StatsDeriveResultTest {
    @Test
    public void testUpdateRowCountByLimit() {
        StatsDeriveResult stats = new StatsDeriveResult(100);
        ColumnStatistic a = new ColumnStatistic(100, 10,  null, 1, 5, 10,
                1, 100, null, null, false, null,
                new Date().toString(), null);
        Id id = new Id(1);
        stats.addColumnStats(id, a);
        StatsDeriveResult res = stats.updateByLimit(0);
        Assertions.assertEquals(0, res.getRowCount());
        Assertions.assertEquals(1, res.getSlotIdToColumnStats().size());
        ColumnStatistic resColStats = res.getColumnStatsBySlotId(id);
        Assertions.assertEquals(0, resColStats.ndv);
        Assertions.assertEquals(1, resColStats.avgSizeByte);
        Assertions.assertEquals(0, resColStats.numNulls);
        Assertions.assertEquals(1, resColStats.dataSize);
        Assertions.assertEquals(1, resColStats.minValue);
        Assertions.assertEquals(100, resColStats.maxValue);
        Assertions.assertEquals(false, resColStats.isUnKnown);

        res = stats.updateByLimit(1);
        resColStats = res.getColumnStatsBySlotId(id);
        Assertions.assertEquals(1, resColStats.ndv);
        Assertions.assertEquals(1, resColStats.avgSizeByte);
        Assertions.assertEquals(1, resColStats.numNulls);
        Assertions.assertEquals(1, resColStats.dataSize);
        Assertions.assertEquals(1, resColStats.minValue);
        Assertions.assertEquals(100, resColStats.maxValue);
        Assertions.assertEquals(false, resColStats.isUnKnown);
    }
}
