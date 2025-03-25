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
import org.apache.doris.analysis.TableName;
import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.ExportJob;
import org.apache.doris.load.ExportJobState;
import org.apache.doris.load.ExportMgr;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.MockedAuth;

import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

public class ExportMgrTest {
    private final ExportMgr exportMgr = new ExportMgr();

    @Mocked
    private AccessControllerManager accessManager;

    @Before
    public void setUp() {
        MockedAuth.mockedAccess(accessManager);
    }

    @Test
    public void testShowExport() throws Exception {

        ExportJob job1 = makeExportJob(1, "aabbcc");
        ExportJob job2 = makeExportJob(2, "aabbdd");
        ExportJob job3 = makeExportJob(3, "eebbcc");

        exportMgr.unprotectAddJob(job1);
        exportMgr.unprotectAddJob(job2);
        exportMgr.unprotectAddJob(job3);

        List<List<String>> r1 = exportMgr.getExportJobInfosByIdOrState(-1, 3, "", true, null, null, -1);
        Assert.assertEquals(r1.size(), 1);

        List<List<String>> r2 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "", false, null, null, -1);
        Assert.assertEquals(r2.size(), 3);

        List<List<String>> r3 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "aabbcc", false, null, null, -1);
        Assert.assertEquals(r3.size(), 1);

        List<List<String>> r4 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "%bb%", true, null, null, -1);
        Assert.assertEquals(r4.size(), 3);

        List<List<String>> r5 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "aabb%", true, null, null, -1);
        Assert.assertEquals(r5.size(), 2);

        List<List<String>> r6 = exportMgr.getExportJobInfosByIdOrState(-1, 0, "%dd", true, null, null, -1);
        Assert.assertEquals(r6.size(), 1);

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
        Assert.assertTrue(remainingJobs.size() <= Config.history_job_keep_max_second);
        Assert.assertEquals(7, remainingJobs.size()); // Expecting 8 jobs to remain


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
        Assert.assertTrue(remainingJobs.size() <= Config.history_job_keep_max_second);
        Assert.assertEquals(1000, remainingJobs.size()); // Expecting 1000 jobs to remain

        // check the created time
        remainingJobs.sort(Comparator.comparingLong(entry -> entry.getCreateTimeMs()));
        for (int i = 0; i < remainingJobs.size(); ++i) {
            Assert.assertEquals(1010 - i, remainingJobs.get(i).getId());
        }
    }

    private ExportJob makeExportJob(long id, String label) {
        ExportJob job1 = new ExportJob(id);
        Deencapsulation.setField(job1, "label", label);

        TableName tbl1 = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, "testCluster", "testDb");
        Deencapsulation.setField(job1, "tableName", tbl1);

        BrokerDesc bd = new BrokerDesc("broker", new HashMap<>());
        Deencapsulation.setField(job1, "brokerDesc", bd);

        Deencapsulation.setField(job1, "timeoutSecond", -1);
        return job1;
    }

}
