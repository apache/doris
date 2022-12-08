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
import org.apache.doris.catalog.InternalSchemaInitializer;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisType;
import org.apache.doris.statistics.AnalysisTaskInfo.JobType;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class AnalysisJobTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        try {
            InternalSchemaInitializer.createDB();
            createDatabase("analysis_job_test");
            connectContext.setDatabase("default_cluster:analysis_job_test");
            createTable("CREATE TABLE t1 (col1 int not null, col2 int not null, col3 int not null)\n"
                    + "DISTRIBUTED BY HASH(col3)\n" + "BUCKETS 1\n"
                    + "PROPERTIES(\n" + "    \"replication_num\"=\"1\"\n"
                    + ");");
            InternalSchemaInitializer storageInitializer = new InternalSchemaInitializer();
            Env.getCurrentEnv().createTable(storageInitializer.buildAnalysisJobTblStmt());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCreateAnalysisJob(@Mocked AnalysisTaskScheduler scheduler) throws Exception {
        new Expectations() {
            {
                scheduler.schedule((AnalysisTaskInfo) any);
                times = 3;
            }
        };

        new MockUp<StatisticsUtil>() {

            @Mock
            public AutoCloseConnectContext buildConnectContext() {
                return new AutoCloseConnectContext(connectContext);
            }

            @Mock
            public void execUpdate(String sql) throws Exception {
            }
        };

        new MockUp<ConnectContext>() {

            @Mock
            public ConnectContext get() {
                return connectContext;
            }
        };
        String sql = "ANALYZE t1";
        Assertions.assertNotNull(getSqlStmtExecutor(sql));
    }

    @Test
    public void testJobExecution(@Mocked AnalysisTaskScheduler scheduler, @Mocked StmtExecutor stmtExecutor)
            throws Exception {
        new MockUp<StatisticsUtil>() {

            @Mock
            public ConnectContext buildConnectContext() {
                return connectContext;
            }

            @Mock
            public void execUpdate(String sql) throws Exception {
            }
        };
        new Expectations() {
            {
                stmtExecutor.execute();
                times = 2;
            }
        };
        AnalysisTaskInfo analysisJobInfo = new AnalysisTaskInfoBuilder().setJobId(0).setTaskId(0)
                .setCatalogName("internal").setDbName("default_cluster:analysis_job_test").setTblName("t1")
                .setColName("col1").setJobType(JobType.MANUAL).setAnalysisMethod(AnalysisMethod.FULL).setAnalysisType(
                        AnalysisType.COLUMN)
                .build();
        new OlapAnalysisTask(scheduler, analysisJobInfo).execute();
    }

}
