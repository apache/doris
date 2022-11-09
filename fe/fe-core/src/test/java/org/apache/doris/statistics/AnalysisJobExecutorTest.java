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
import org.apache.doris.persist.AnalysisJobScheduler;
import org.apache.doris.statistics.AnalysisJobInfo.JobType;
import org.apache.doris.statistics.AnalysisJobInfo.ScheduleType;
import org.apache.doris.statistics.util.BlockingCounter;
import org.apache.doris.utframe.TestWithFeService;

import mockit.Expectations;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;

public class AnalysisJobExecutorTest extends TestWithFeService {

    @Mocked
    AnalysisJobScheduler analysisJobScheduler;

    @Override
    protected void runBeforeAll() throws Exception {
        try {
            StatisticStorageInitializer.createDB();
            createDatabase("analysis_job_test");
            connectContext.setDatabase("default_cluster:analysis_job_test");
            createTable("CREATE TABLE t1 (col1 int not null, col2 int not null, col3 int not null)\n"

                    + "DISTRIBUTED BY HASH(col3)\n" + "BUCKETS 1\n"
                    + "PROPERTIES(\n" + "    \"replication_num\"=\"1\"\n"
                    + ");");
            StatisticStorageInitializer storageInitializer = new StatisticStorageInitializer();
            Env.getCurrentEnv().createTable(storageInitializer.buildAnalysisJobTblStmt());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testExpiredJobCancellation() throws Exception {
        AnalysisJobExecutor analysisJobExecutor = new AnalysisJobExecutor(analysisJobScheduler);
        BlockingQueue<AnalysisJobWrapper> b = Deencapsulation.getField(analysisJobExecutor, "jobQueue");
        AnalysisJobInfo analysisJobInfo = new AnalysisJobInfo(0,
                "internal",
                "default_cluster:analysis_job_test",
                "t1",
                "col1", JobType.MANUAL,
                ScheduleType.ONCE);
        AnalysisJob analysisJob = new AnalysisJob(analysisJobScheduler, analysisJobInfo);
        AnalysisJobWrapper analysisJobWrapper = new AnalysisJobWrapper(analysisJobExecutor, analysisJob);
        Deencapsulation.setField(analysisJobWrapper, "startTime", 5);
        b.put(analysisJobWrapper);
        new Expectations() {
            {
                analysisJobWrapper.cancel();
                times = 1;
            }
        };
        analysisJobExecutor.start();
        BlockingCounter counter = Deencapsulation.getField(analysisJobExecutor, "blockingCounter");
        Assertions.assertEquals(0, counter.getVal());
    }

    @Test
    public void testJobExecution() throws Exception {
        AnalysisJobExecutor analysisJobExecutor = new AnalysisJobExecutor(analysisJobScheduler);
        AnalysisJobInfo analysisJobInfo = new AnalysisJobInfo(0,
                "internal",
                "default_cluster:analysis_job_test",
                "t1",
                "col1", JobType.MANUAL,
                ScheduleType.ONCE);
        AnalysisJob job = new AnalysisJob(analysisJobScheduler, analysisJobInfo);
        new Expectations() {
            {
                analysisJobScheduler.getPendingJobs();
                result = job;
                job.execute();
                times = 1;
            }
        };
        Deencapsulation.invoke(analysisJobExecutor, "doFetchAndExecute");
    }
}
