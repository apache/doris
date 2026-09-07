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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportJobState;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class ExportMgrTest {
    private final ExportMgr exportMgr = new ExportMgr();

    private AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);

    @BeforeEach
    public void setUp() {
        MockedAuth.mockedAccess(accessManager);
    }

    @Test
    public void testShowExport() throws Exception {
        Env env = Mockito.mock(Env.class);
        ConnectContext connectContext = Mockito.mock(ConnectContext.class);

        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> connectContextStatic = Mockito.mockStatic(ConnectContext.class)) {
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            connectContextStatic.when(ConnectContext::get).thenReturn(connectContext);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(connectContext.getSessionVariable()).thenReturn(new SessionVariable());

            ExportJob job1 = makeExportJob(1, "aabbcc");
            ExportJob job2 = makeExportJob(2, "aabbdd");
            ExportJob job3 = makeExportJob(3, "eebbcc");

            exportMgr.unprotectAddJob(job1);
            exportMgr.unprotectAddJob(job2);
            exportMgr.unprotectAddJob(job3);

            List<List<String>> r1 = exportMgr.getExportJobInfosByIdOrState(-1, 3, "", true, null, null, -1);
            Assertions.assertEquals(r1.size(), 1);

            List<List<String>> r2 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "", false, null, null, -1);
            Assertions.assertEquals(r2.size(), 3);

            List<List<String>> r3 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "aabbcc", false, null, null, -1);
            Assertions.assertEquals(r3.size(), 1);

            List<List<String>> r4 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "%bb%", true, null, null, -1);
            Assertions.assertEquals(r4.size(), 3);

            List<List<String>> r5 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "aabb%", true, null, null, -1);
            Assertions.assertEquals(r5.size(), 2);

            List<List<String>> r6 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "%dd", true, null, null, -1);
            Assertions.assertEquals(r6.size(), 1);
        }
    }

    @Test
    public void testRemoveOldExportJobs() {
        // Setup: Create jobs with different creation times
        long currentTime = System.currentTimeMillis();
        for (int i = 1; i <= 10; i++) {
            ExportJob job = makeExportJob(i, "label" + i);
            // Jobs created 1, 2...10 days ago
            Deencapsulation.setField(job, "createTimeMs", currentTime - (i * 24 * 3600 * 1000));
            Deencapsulation.setField(job, "state", ExportJobState.FINISHED);
            exportMgr.unprotectAddJob(job);
        }

        // Invoke the method
        exportMgr.removeOldExportJobs();

        // Assertions: Check the number of jobs remaining
        List<ExportJob> remainingJobs = exportMgr.getJobs();
        Assertions.assertTrue(remainingJobs.size() <= Config.history_job_keep_max_second);
        Assertions.assertEquals(7, remainingJobs.size()); // Expecting 8 jobs to remain


        for (int i = 11; i <= 1010; i++) {
            ExportJob job = makeExportJob(i, "label" + i);
            // Jobs created 0, 1, 2, 3, 4...1000 seconds ago
            Deencapsulation.setField(job, "createTimeMs", currentTime - (i * 1000));
            Deencapsulation.setField(job, "state", ExportJobState.FINISHED);
            exportMgr.unprotectAddJob(job);
        }

        // Invoke the method
        exportMgr.removeOldExportJobs();
        // Assertions: Check the number of jobs remaining
        remainingJobs = exportMgr.getJobs();
        Assertions.assertTrue(remainingJobs.size() <= Config.history_job_keep_max_second);
        Assertions.assertEquals(1000, remainingJobs.size()); // Expecting 1000 jobs to remain

        // check the created time
        remainingJobs.sort(Comparator.comparingLong(entry -> entry.getCreateTimeMs()));
        for (int i = 0; i < remainingJobs.size(); ++i) {
            Assertions.assertEquals(1010 - i, remainingJobs.get(i).getId());
        }
    }

    @Test
    public void testRemoveOldExportJobsKeepsRunningJobs() {
        ExportMgr isolatedExportMgr = new ExportMgr();
        int originalMaxHistoryJobNum = Config.max_export_history_job_num;
        Config.max_export_history_job_num = 2;
        try {
            long currentTime = System.currentTimeMillis();
            ExportJob pendingJob = makeExportJob(1001, "pending");
            Deencapsulation.setField(pendingJob, "createTimeMs", currentTime - 4000);
            Deencapsulation.setField(pendingJob, "state", ExportJobState.PENDING);
            isolatedExportMgr.unprotectAddJob(pendingJob);

            ExportJob exportingJob = makeExportJob(1002, "exporting");
            Deencapsulation.setField(exportingJob, "createTimeMs", currentTime - 3000);
            Deencapsulation.setField(exportingJob, "state", ExportJobState.EXPORTING);
            isolatedExportMgr.unprotectAddJob(exportingJob);

            ExportJob finishedJob = makeExportJob(1003, "finished");
            Deencapsulation.setField(finishedJob, "createTimeMs", currentTime - 2000);
            Deencapsulation.setField(finishedJob, "state", ExportJobState.FINISHED);
            isolatedExportMgr.unprotectAddJob(finishedJob);

            ExportJob cancelledJob = makeExportJob(1004, "cancelled");
            Deencapsulation.setField(cancelledJob, "createTimeMs", currentTime - 1000);
            Deencapsulation.setField(cancelledJob, "state", ExportJobState.CANCELLED);
            isolatedExportMgr.unprotectAddJob(cancelledJob);

            isolatedExportMgr.removeOldExportJobs();

            Assert.assertEquals(2, isolatedExportMgr.getJobs().size());
            Assert.assertNotNull(isolatedExportMgr.getJob(pendingJob.getId()));
            Assert.assertNotNull(isolatedExportMgr.getJob(exportingJob.getId()));
            Assert.assertNull(isolatedExportMgr.getJob(finishedJob.getId()));
            Assert.assertNull(isolatedExportMgr.getJob(cancelledJob.getId()));
        } finally {
            Config.max_export_history_job_num = originalMaxHistoryJobNum;
        }
    }

    @Test
    public void testRemoveOldExportJobsKeepsRunningJobsWhenOverLimit() {
        ExportMgr isolatedExportMgr = new ExportMgr();
        int originalMaxHistoryJobNum = Config.max_export_history_job_num;
        Config.max_export_history_job_num = 1;
        try {
            long currentTime = System.currentTimeMillis();
            ExportJob pendingJob = makeExportJob(2001, "pending-over-limit");
            Deencapsulation.setField(pendingJob, "createTimeMs", currentTime - 3000);
            Deencapsulation.setField(pendingJob, "state", ExportJobState.PENDING);
            isolatedExportMgr.unprotectAddJob(pendingJob);

            ExportJob exportingJob = makeExportJob(2002, "exporting-over-limit");
            Deencapsulation.setField(exportingJob, "createTimeMs", currentTime - 2000);
            Deencapsulation.setField(exportingJob, "state", ExportJobState.EXPORTING);
            isolatedExportMgr.unprotectAddJob(exportingJob);

            ExportJob inQueueJob = makeExportJob(2003, "in-queue-over-limit");
            Deencapsulation.setField(inQueueJob, "createTimeMs", currentTime - 1000);
            Deencapsulation.setField(inQueueJob, "state", ExportJobState.IN_QUEUE);
            isolatedExportMgr.unprotectAddJob(inQueueJob);

            isolatedExportMgr.removeOldExportJobs();

            Assert.assertEquals(3, isolatedExportMgr.getJobs().size());
            Assert.assertEquals(ExportJobState.PENDING, isolatedExportMgr.getJob(pendingJob.getId()).getState());
            Assert.assertEquals(ExportJobState.EXPORTING, isolatedExportMgr.getJob(exportingJob.getId()).getState());
            Assert.assertEquals(ExportJobState.IN_QUEUE, isolatedExportMgr.getJob(inQueueJob.getId()).getState());
        } finally {
            Config.max_export_history_job_num = originalMaxHistoryJobNum;
        }
    }

    private ExportJob makeExportJob(long id, String label) {
        ExportJob job1 = new ExportJob(id);
        Deencapsulation.setField(job1, "label", label);

        TableNameInfo tbl1 = new TableNameInfo(InternalCatalog.INTERNAL_CATALOG_NAME, "testCluster", "testDb");
        Deencapsulation.setField(job1, "tableName", tbl1);

        BrokerDesc bd = new BrokerDesc("broker", new HashMap<>());
        Deencapsulation.setField(job1, "brokerDesc", bd);

        Deencapsulation.setField(job1, "timeoutSecond", -1);
        return job1;
    }

}
