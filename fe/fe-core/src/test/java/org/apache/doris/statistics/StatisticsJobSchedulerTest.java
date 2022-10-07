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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.HashDistributionInfo;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StatisticsJobSchedulerTest {
    private StatisticsJob statisticsJob;

    private StatisticsJobScheduler statisticsJobSchedulerUnderTest;

    @Before
    public void setUp() throws Exception {
        HashSet<Long> tblIds = Sets.newHashSet();
        tblIds.add(0L);
        tblIds.add(1L);

        Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        tableIdToColumnName.put(0L, Arrays.asList("c1", "c2"));
        tableIdToColumnName.put(1L, Arrays.asList("c1", "c2"));
        Map<Long, List<String>> tblIdToPartitionName = Maps.newHashMap();

        statisticsJob = new StatisticsJob(0L, tblIds, tblIdToPartitionName,
                tableIdToColumnName, null);
        statisticsJobSchedulerUnderTest = new StatisticsJobScheduler();
        statisticsJobSchedulerUnderTest.addPendingJob(statisticsJob);
    }

    @Test
    public void testRunAfterCatalogReady() {
        // Setup
        Column col1 = new Column("c1", PrimitiveType.STRING);
        Column col2 = new Column("c2", PrimitiveType.INT);

        OlapTable tbl1 = new OlapTable(0L, "tbl1", Arrays.asList(col1, col2),
                KeysType.AGG_KEYS, new PartitionInfo(), new HashDistributionInfo());
        OlapTable tbl2 = new OlapTable(1L, "tbl2", Arrays.asList(col1, col2),
                KeysType.DUP_KEYS, new PartitionInfo(), new HashDistributionInfo());

        Database database = new Database(0L, "db");
        database.createTable(tbl1);
        database.createTable(tbl2);

        InternalCatalog catalog = Env.getCurrentInternalCatalog();
        ConcurrentHashMap<String, Database> fullNameToDb = new ConcurrentHashMap<>();
        fullNameToDb.put("cluster:db", database);
        Deencapsulation.setField(catalog, "fullNameToDb", fullNameToDb);

        ConcurrentHashMap<Long, Database> idToDb = new ConcurrentHashMap<>();
        idToDb.put(0L, database);
        Deencapsulation.setField(catalog, "idToDb", idToDb);

        new MockUp<SystemInfoService>(SystemInfoService.class) {
            @Mock
            public List<Long> getBackendIds(boolean needAlive) {
                return Collections.singletonList(1L);
            }
        };

        new MockUp<OlapTable>(OlapTable.class) {
            @Mock
            public long getDataSize() {
                return 1L;
            }
        };

        // Run the test
        statisticsJobSchedulerUnderTest.runAfterCatalogReady();

        /*
         * expected results:
         * mateTask(2):
         *  - tbl1:
         *    - task1:
         *      - data_size
         *      - max_size(c2)
         *      - avg_size(c2)
         *  - tbl2:
         *    - task:
         *      - row_count
         *      - data_size
         *      - max_size(c2)
         *      - avg_size(c2)
         *
         * sqlTask(11):
         *  - tbl1:
         *    - task:
         *      - ndv(c1)
         *      - min_value(c1)
         *      - max_value(c1)
         *    - task:
         *      - ndv(c2)
         *      - min_value(c2)
         *      - max_value(c2)
         *    - task:
         *      - max_size(c1)
         *      - avg_size(c1)
         *    - task:
         *      - num_nulls(c1)
         *    - task:
         *      - num_nulls(c2)
         *    - task
         *      - row_count
         *  - tbl2:
         *    - task:
         *      - ndv(c1)
         *      - min_value(c1)
         *      - max_value(c1)
         *    - task:
         *      - ndv(c2)
         *      - min_value(c2)
         *      - max_value(c2)
         *    - task:
         *      - max_size(c1)
         *      - avg_size(c1)
         *    - task:
         *      - num_nulls(c1)
         *    - task:
         *      - num_nulls(c2)
         */

        // Verify the results
        List<StatisticsTask> tasks = statisticsJob.getTasks();
        Assert.assertEquals(13, tasks.size());

        int sqlTaskCount = 0;
        int metaTaskCount = 0;

        for (StatisticsTask task : tasks) {
            if (task instanceof SQLStatisticsTask) {
                sqlTaskCount++;
            } else if (task instanceof MetaStatisticsTask) {
                metaTaskCount++;
            } else {
                Assert.fail("Unknown task type.");
            }
        }

        Assert.assertEquals(2, metaTaskCount);
        Assert.assertEquals(11, sqlTaskCount);
    }
}
