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

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.nereids.trees.plans.algebra.OlapScan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.utframe.UtFrameUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

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

    @Test
    public void testOlapTableStatisticsSkipSystemTable() {
        try (MockedConstruction<ColumnStatisticsCacheLoader> columnLoader = Mockito.mockConstruction(
                ColumnStatisticsCacheLoader.class);
                MockedConstruction<PartitionColumnStatisticCacheLoader> partitionLoader = Mockito.mockConstruction(
                        PartitionColumnStatisticCacheLoader.class)) {
            StatisticsCache cache = new StatisticsCache();
            StatisticsCache.OlapTableStatistics olapTableStats =
                    cache.getOlapTableStats(mockSystemOlapScan(FeConstants.INTERNAL_DB_NAME));

            Assertions.assertEquals(ColumnStatistic.UNKNOWN,
                    olapTableStats.getColumnStatistics("col", ConnectContext.get()));
            Assertions.assertEquals(PartitionColumnStatistic.UNKNOWN,
                    olapTableStats.getPartitionColumnStatistics("p1", "col", ConnectContext.get()));

            Mockito.verifyNoInteractions(columnLoader.constructed().get(0));
            Mockito.verifyNoInteractions(partitionLoader.constructed().get(0));
        }
    }

    private OlapScan mockSystemOlapScan(String dbName) {
        CatalogIf catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getId()).thenReturn(1L);
        DatabaseIf database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getId()).thenReturn(2L);
        Mockito.when(database.getCatalog()).thenReturn(catalog);

        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getQualifiedDbName()).thenReturn(dbName);
        Mockito.when(table.getDatabase()).thenReturn(database);
        Mockito.when(table.getId()).thenReturn(3L);
        Mockito.when(table.getBaseIndexId()).thenReturn(4L);

        OlapScan scan = Mockito.mock(OlapScan.class);
        Mockito.when(scan.getTable()).thenReturn(table);
        Mockito.when(scan.getSelectedIndexId()).thenReturn(4L);
        return scan;
    }
}
