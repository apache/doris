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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Tested;
import org.junit.jupiter.api.Test;

public class MVStatisticsTest extends TestWithFeService {

    @Injectable
    StatisticsCache statisticsCache;

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test");
        connectContext.setDatabase("test");
        createTable("CREATE TABLE t1 (col1 int not null, col2 int not null, col3 int not null)\n"
                + "DISTRIBUTED BY HASH(col3)\n"
                + "BUCKETS 1\n"
                + "PROPERTIES(\n"
                + "    \"replication_num\"=\"1\"\n"
                + ");\n");
        createMv("CREATE MATERIALIZED VIEW mv1 AS SELECT col3 , SUM(COL2) FROM t1 group by col3");
    }

    @Tested

    @Test
    public void testCreate() throws Exception {
        new MockUp<StatisticsRepository>() {
        };
        new MockUp<StatisticsUtil>() {

            @Mock
            public void execUpdate(String sql) throws Exception {}
        };
        new MockUp<OlapAnalysisTask>(OlapAnalysisTask.class) {

            @Mock
            public void execSQL(String sql) throws Exception {}
        };
        new MockUp<Env>() {

            @Mock
            public StatisticsCache getStatisticsCache() {
                return statisticsCache;
            }
        };
        AnalysisManager analysisManager = Env.getCurrentEnv().getAnalysisManager();
        Deencapsulation.setField(analysisManager, "statisticsCache", statisticsCache);
        getSqlStmtExecutor("analyze table t1");
        Thread.sleep(3000);
    }
}
