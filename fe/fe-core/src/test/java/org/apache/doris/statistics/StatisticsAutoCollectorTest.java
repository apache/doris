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

import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HMSExternalTable.DLAType;
import org.apache.doris.datasource.jdbc.JdbcExternalTable;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

public class StatisticsAutoCollectorTest {

    @Test
    public void testFetchJob() {
        AnalysisManager manager = new AnalysisManager();
        TableName high1 = new TableName("catalog", "db", "high1");
        TableName high2 = new TableName("catalog", "db", "high2");
        TableName mid1 = new TableName("catalog", "db", "mid1");
        TableName mid2 = new TableName("catalog", "db", "mid2");
        TableName low1 = new TableName("catalog", "db", "low1");

        manager.highPriorityJobs.put(high1, new HashSet<>());
        manager.highPriorityJobs.get(high1).add(Pair.of("index1", "col1"));
        manager.highPriorityJobs.get(high1).add(Pair.of("index1", "col2"));
        manager.highPriorityJobs.put(high2, new HashSet<>());
        manager.highPriorityJobs.get(high2).add(Pair.of("index1", "col3"));
        manager.midPriorityJobs.put(mid1, new HashSet<>());
        manager.midPriorityJobs.get(mid1).add(Pair.of("index1", "col4"));
        manager.midPriorityJobs.put(mid2, new HashSet<>());
        manager.midPriorityJobs.get(mid2).add(Pair.of("index1", "col5"));
        manager.lowPriorityJobs.put(low1, new HashSet<>());
        manager.lowPriorityJobs.get(low1).add(Pair.of("index1", "col6"));
        manager.lowPriorityJobs.get(low1).add(Pair.of("index1", "col7"));


        new MockUp<Env>() {
            @Mock
            public AnalysisManager getAnalysisManager() {
                return manager;
            }
        };
        StatisticsAutoCollector collector = new StatisticsAutoCollector();
        Pair<Entry<TableName, Set<Pair<String, String>>>, JobPriority> job = collector.getJob();
        Assertions.assertEquals(high1, job.first.getKey());
        Assertions.assertEquals(2, job.first.getValue().size());
        Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col1")));
        Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col2")));
        Assertions.assertEquals(JobPriority.HIGH, job.second);

        job = collector.getJob();
        Assertions.assertEquals(high2, job.first.getKey());
        Assertions.assertEquals(1, job.first.getValue().size());
        Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col3")));
        Assertions.assertEquals(JobPriority.HIGH, job.second);

        job = collector.getJob();
        Assertions.assertEquals(mid1, job.first.getKey());
        Assertions.assertEquals(1, job.first.getValue().size());
        Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col4")));
        Assertions.assertEquals(JobPriority.MID, job.second);

        job = collector.getJob();
        Assertions.assertEquals(mid2, job.first.getKey());
        Assertions.assertEquals(1, job.first.getValue().size());
        Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col5")));
        Assertions.assertEquals(JobPriority.MID, job.second);

        job = collector.getJob();
        Assertions.assertEquals(low1, job.first.getKey());
        Assertions.assertEquals(2, job.first.getValue().size());
        Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col6")));
        Assertions.assertTrue(job.first.getValue().contains(Pair.of("index1", "col7")));
        Assertions.assertEquals(JobPriority.LOW, job.second);

        job = collector.getJob();
        Assertions.assertNull(job);
    }

    @Test
    public void testSupportAutoAnalyze() {
        StatisticsAutoCollector collector = new StatisticsAutoCollector();
        Assertions.assertFalse(collector.supportAutoAnalyze(null));
        Column column1 = new Column("placeholder", PrimitiveType.INT);
        List<Column> schema = new ArrayList<>();
        schema.add(column1);
        OlapTable table1 = new OlapTable(200, "testTable", schema, null, null, null);
        Assertions.assertTrue(collector.supportAutoAnalyze(table1));

        ExternalTable externalTable = new JdbcExternalTable(1, "jdbctable", "jdbcdb", null);
        Assertions.assertFalse(collector.supportAutoAnalyze(externalTable));

        new MockUp<HMSExternalTable>() {
            @Mock
            public DLAType getDlaType() {
                return DLAType.ICEBERG;
            }
        };
        ExternalTable icebergExternalTable = new HMSExternalTable(1, "hmsTable", "hmsDb", null);
        Assertions.assertFalse(collector.supportAutoAnalyze(icebergExternalTable));

        new MockUp<HMSExternalTable>() {
            @Mock
            public DLAType getDlaType() {
                return DLAType.HIVE;
            }
        };
        ExternalTable hiveExternalTable = new HMSExternalTable(1, "hmsTable", "hmsDb", null);
        Assertions.assertTrue(collector.supportAutoAnalyze(hiveExternalTable));
    }
}
