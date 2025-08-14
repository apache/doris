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

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.statistics.StatisticsCleaner.ExpiredStats;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

class StatisticsCleanerTest {

    @Test
    void testFindExpiredStats() {
        StatisticsCleaner cleaner = new StatisticsCleaner();
        ExpiredStats stat = new ExpiredStats();
        OlapTable olapTable = new OlapTable();
        for (int i = 0; i <= Config.max_allowed_in_element_num_of_delete; i++) {
            stat.expiredCatalog.add((long) i);
        }
        long expiredStats = cleaner.findExpiredStats(null, stat, 1, true);
        Assertions.assertEquals(expiredStats, 1);

        try (MockedStatic<StatisticsRepository> mockedRep = Mockito.mockStatic(StatisticsRepository.class)) {
            mockedRep.when(() -> StatisticsRepository.fetchStatsFullName(
                    ArgumentMatchers.anyLong(), ArgumentMatchers.anyLong(), ArgumentMatchers.anyBoolean()))
                    .thenReturn(Lists.newArrayList());
            stat.expiredCatalog.clear();
            expiredStats = cleaner.findExpiredStats(olapTable, stat, 0, true);
            Assertions.assertEquals(expiredStats, StatisticConstants.FETCH_LIMIT);
        }
    }
}
