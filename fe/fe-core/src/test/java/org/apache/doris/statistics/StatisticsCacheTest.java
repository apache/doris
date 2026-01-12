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

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StatisticsCacheTest {

    private ConnectContext ctx;

    @BeforeEach
    public void setUp() throws Exception {
        if (ConnectContext.get() == null) {
            ctx = UtFrameUtils.createDefaultCtx();
        } else {
            ctx = ConnectContext.get();
        }
    }

    @Test
    public void testGetColumnStatistics_withPlanWithUnknownColumnStats() {
        Assumptions.assumeTrue(ConnectContext.get() != null, "ConnectContext not available");

        boolean prevFlag = ConnectContext.get().getState().isPlanWithUnKnownColumnStats();
        ConnectContext.get().getState().setPlanWithUnKnownColumnStats(true);
        try {
            StatisticsCache cache = new StatisticsCache();
            ColumnStatistic stat = cache.getColumnStatistics(
                    1L, 1L, 1L, -1L, "col", ConnectContext.get());
            Assertions.assertEquals(ColumnStatistic.UNKNOWN, stat,
                    "Expect UNKNOWN when plan has unknown column stats");
        } finally {
            ConnectContext.get().getState().setPlanWithUnKnownColumnStats(prevFlag);
        }
    }

    @Test
    public void testGetHistogram_withPlanWithUnknownColumnStats() {
        Assumptions.assumeTrue(ConnectContext.get() != null, "ConnectContext not available");

        boolean prevFlag = ConnectContext.get().getState().isPlanWithUnKnownColumnStats();
        ConnectContext.get().getState().setPlanWithUnKnownColumnStats(true);
        try {
            StatisticsCache cache = new StatisticsCache();
            // public getHistogram returns null when underlying optional is empty
            Histogram hist = cache.getHistogram(1L, 1L, 1L, "col");
            Assertions.assertNull(hist, "Expect null histogram when plan has unknown column stats");
        } finally {
            ConnectContext.get().getState().setPlanWithUnKnownColumnStats(prevFlag);
        }
    }

    @Test
    public void testGetPartitionColumnStatistics_withPlanWithUnknownColumnStats() {
        Assumptions.assumeTrue(ConnectContext.get() != null, "ConnectContext not available");

        boolean prevFlag = ConnectContext.get().getState().isPlanWithUnKnownColumnStats();
        ConnectContext.get().getState().setPlanWithUnKnownColumnStats(true);
        try {
            StatisticsCache cache = new StatisticsCache();
            PartitionColumnStatistic pstat = cache.getPartitionColumnStatistics(
                    1L, 1L, 1L, -1L, "p", "col", ConnectContext.get());
            Assertions.assertEquals(PartitionColumnStatistic.UNKNOWN, pstat,
                    "Expect UNKNOWN partition col stat when plan has unknown column stats");
        } finally {
            ConnectContext.get().getState().setPlanWithUnKnownColumnStats(prevFlag);
        }
    }
}
