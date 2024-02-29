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

package org.apache.doris.load.sync;

import org.apache.doris.analysis.CreateDataSyncJobStmt;
import org.apache.doris.analysis.PauseSyncJobStmt;
import org.apache.doris.analysis.ResumeSyncJobStmt;
import org.apache.doris.analysis.StopSyncJobStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.sync.SyncFailMsg.MsgType;
import org.apache.doris.load.sync.SyncJob.JobState;
import org.apache.doris.load.sync.SyncJob.SyncJobUpdateStateInfo;
import org.apache.doris.load.sync.canal.CanalDestination;
import org.apache.doris.load.sync.canal.CanalSyncJob;
import org.apache.doris.load.sync.canal.SyncCanalClient;
import org.apache.doris.persist.EditLog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SyncJobManagerTest {
    private static final Logger LOG = LogManager.getLogger(SyncJobManagerTest.class);

    private long jobId = 10000L;
    private String jobName = "testJob";
    private long dbId = 50000L;

    @Mocked
    EditLog editLog;
    @Mocked
    Env env;
    @Mocked
    InternalCatalog catalog;
    @Mocked
    Database database;
    @Mocked
    SyncCanalClient client;

    @Before
    public void setUp() throws DdlException {
        new Expectations() {
            {
                env.getEditLog();
                minTimes = 0;
                result = editLog;
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;
                catalog.getDbNullable(anyString);
                minTimes = 0;
                result = database;
                database.getId();
                minTimes = 0;
                result = dbId;
                Env.getCurrentEnv();
                minTimes = 0;
                result = env;
            }
        };
    }

    @Test
    public void testAddSyncJob(@Injectable CreateDataSyncJobStmt stmt,
                               @Mocked SyncJob syncJob) throws DdlException {
        CanalSyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
        new Expectations() {
            {
                SyncJob.fromStmt(anyLong, (CreateDataSyncJobStmt) any);
                result = canalSyncJob;
            }
        };

        SyncJobManager manager = new SyncJobManager();
        manager.addDataSyncJob(stmt);

        Map<Long, SyncJob> idToSyncJobs = Deencapsulation.getField(manager, "idToSyncJob");
        Assert.assertEquals(1, idToSyncJobs.size());
        SyncJob syncJob1 = idToSyncJobs.values().iterator().next();
        Assert.assertEquals(10000L, syncJob1.getId());
        Assert.assertEquals("testJob", syncJob1.getJobName());
        Assert.assertEquals(50000L, syncJob1.getDbId());
        Assert.assertEquals(JobState.PENDING, syncJob1.getJobState());
        Assert.assertEquals(DataSyncJobType.CANAL, syncJob1.getJobType());
        Assert.assertTrue(syncJob1 instanceof CanalSyncJob);

        Map<Long, Map<String, List<SyncJob>>> dbIdToJobNameToSyncJobs =
                Deencapsulation.getField(manager, "dbIdToJobNameToSyncJobs");
        Assert.assertEquals(1, dbIdToJobNameToSyncJobs.size());
        Map<String, List<SyncJob>> jobNameToSyncJobs = dbIdToJobNameToSyncJobs.values().iterator().next();
        Assert.assertEquals(1, jobNameToSyncJobs.size());
        Assert.assertTrue(jobNameToSyncJobs.containsKey("testJob"));
        List<SyncJob> syncJobs = jobNameToSyncJobs.get("testJob");
        Assert.assertEquals(1, syncJobs.size());
        SyncJob syncJob2 = syncJobs.get(0);
        Assert.assertEquals(syncJob1, syncJob2);
    }

    @Test(expected = DdlException.class)
    public void testCreateDuplicateRemote(@Injectable CreateDataSyncJobStmt stmt,
                                          @Mocked SyncJob syncJob) throws DdlException {
        // create two canal jobs
        CanalSyncJob canalSyncJob1 = new CanalSyncJob(jobId, jobName + "_1", dbId);
        CanalSyncJob canalSyncJob2 = new CanalSyncJob(jobId + 1, jobName + "_2", dbId);
        new Expectations() {
            {
                SyncJob.fromStmt(anyLong, (CreateDataSyncJobStmt) any);
                returns(canalSyncJob1, canalSyncJob2);
            }
        };

        // set same remote destination to two jobs
        CanalDestination duplicateDes1 = new CanalDestination("dupIp", 1, "dupDestination");
        CanalDestination duplicateDes2 = new CanalDestination("dupIp", 1, "dupDestination");
        Deencapsulation.setField(canalSyncJob1, "remote", duplicateDes1);
        Deencapsulation.setField(canalSyncJob2, "remote", duplicateDes2);

        SyncJobManager manager = new SyncJobManager();
        manager.addDataSyncJob(stmt);

        Map<Long, SyncJob> idToSyncJobs = Deencapsulation.getField(manager, "idToSyncJob");
        Assert.assertEquals(1, idToSyncJobs.size());
        SyncJob syncJob1 = idToSyncJobs.values().iterator().next();
        Assert.assertEquals(DataSyncJobType.CANAL, syncJob1.getJobType());
        Assert.assertEquals(duplicateDes1, ((CanalSyncJob) syncJob1).getRemote());

        // should throw exception
        manager.addDataSyncJob(stmt);
        Assert.fail();
    }

    @Test
    public void testPauseSyncJob(@Injectable PauseSyncJobStmt stmt) {
        CanalSyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
        new Expectations() {
            {
                stmt.getJobName();
                result = "testJob";

                stmt.getDbFullName();
                result = "testDb";
            }
        };

        SyncJobManager manager = new SyncJobManager();
        try {
            manager.pauseSyncJob(stmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // add a sync job to manager
        Map<Long, Map<String, List<SyncJob>>> dbIdToJobNameToSyncJobs = Maps.newHashMap();
        Map<String, List<SyncJob>> jobNameToSyncJobs = Maps.newHashMap();
        jobNameToSyncJobs.put("testJob", Lists.newArrayList(canalSyncJob));
        dbIdToJobNameToSyncJobs.put(50000L, jobNameToSyncJobs);
        Deencapsulation.setField(manager, "dbIdToJobNameToSyncJobs", dbIdToJobNameToSyncJobs);

        // a new sync job state is pending
        Assert.assertEquals(JobState.PENDING, canalSyncJob.getJobState());
        try {
            manager.pauseSyncJob(stmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // change sync job state to paused
        canalSyncJob.unprotectedUpdateState(JobState.PAUSED, false);
        Assert.assertEquals(JobState.PAUSED, canalSyncJob.getJobState());
        try {
            manager.pauseSyncJob(stmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // change sync job state to cancelled
        canalSyncJob.unprotectedUpdateState(JobState.CANCELLED, false);
        Assert.assertEquals(JobState.CANCELLED, canalSyncJob.getJobState());
        try {
            manager.pauseSyncJob(stmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // change sync job state to running
        canalSyncJob.unprotectedUpdateState(JobState.RUNNING, false);
        Assert.assertEquals(JobState.RUNNING, canalSyncJob.getJobState());
        try {
            manager.pauseSyncJob(stmt);
            Assert.assertEquals(JobState.PAUSED, canalSyncJob.getJobState());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testResumeSyncJob(@Injectable ResumeSyncJobStmt stmt) {
        CanalSyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
        new Expectations() {
            {
                stmt.getJobName();
                result = "testJob";

                stmt.getDbFullName();
                result = "testDb";
            }
        };

        Deencapsulation.setField(canalSyncJob, "client", client);

        SyncJobManager manager = new SyncJobManager();
        try {
            manager.resumeSyncJob(stmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // add a sync job to manager
        Map<Long, Map<String, List<SyncJob>>> dbIdToJobNameToSyncJobs = Maps.newHashMap();
        Map<String, List<SyncJob>> jobNameToSyncJobs = Maps.newHashMap();
        jobNameToSyncJobs.put("testJob", Lists.newArrayList(canalSyncJob));
        dbIdToJobNameToSyncJobs.put(50000L, jobNameToSyncJobs);
        Deencapsulation.setField(manager, "dbIdToJobNameToSyncJobs", dbIdToJobNameToSyncJobs);

        // a new sync job state is pending
        Assert.assertEquals(JobState.PENDING, canalSyncJob.getJobState());
        try {
            manager.resumeSyncJob(stmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // change sync job state to running
        canalSyncJob.unprotectedUpdateState(JobState.RUNNING, false);
        Assert.assertEquals(JobState.RUNNING, canalSyncJob.getJobState());
        try {
            manager.resumeSyncJob(stmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // change sync job state to cancelled
        canalSyncJob.unprotectedUpdateState(JobState.CANCELLED, false);
        Assert.assertEquals(JobState.CANCELLED, canalSyncJob.getJobState());
        try {
            manager.resumeSyncJob(stmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // change sync job state to paused
        canalSyncJob.unprotectedUpdateState(JobState.PAUSED, false);
        Assert.assertEquals(JobState.PAUSED, canalSyncJob.getJobState());
        try {
            manager.resumeSyncJob(stmt);
            Assert.assertEquals(JobState.PENDING, canalSyncJob.getJobState());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testStopSyncJob(@Injectable StopSyncJobStmt stmt) {
        CanalSyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
        new Expectations() {
            {
                stmt.getJobName();
                result = "testJob";

                stmt.getDbFullName();
                result = "testDb";
            }
        };

        Deencapsulation.setField(canalSyncJob, "client", client);

        SyncJobManager manager = new SyncJobManager();
        try {
            manager.stopSyncJob(stmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // add a sync job to manager
        Map<Long, Map<String, List<SyncJob>>> dbIdToJobNameToSyncJobs = Maps.newHashMap();
        Map<String, List<SyncJob>> jobNameToSyncJobs = Maps.newHashMap();
        jobNameToSyncJobs.put("testJob", Lists.newArrayList(canalSyncJob));
        dbIdToJobNameToSyncJobs.put(50000L, jobNameToSyncJobs);
        Deencapsulation.setField(manager, "dbIdToJobNameToSyncJobs", dbIdToJobNameToSyncJobs);

        // a new sync job state is pending
        Assert.assertEquals(JobState.PENDING, canalSyncJob.getJobState());
        try {
            manager.stopSyncJob(stmt);
            Assert.assertEquals(JobState.CANCELLED, canalSyncJob.getJobState());
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // change sync job state to paused
        canalSyncJob.unprotectedUpdateState(JobState.PAUSED, false);
        Assert.assertEquals(JobState.PAUSED, canalSyncJob.getJobState());
        try {
            manager.stopSyncJob(stmt);
            Assert.assertEquals(JobState.CANCELLED, canalSyncJob.getJobState());
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }

        // change sync job state to running
        canalSyncJob.unprotectedUpdateState(JobState.RUNNING, false);
        Assert.assertEquals(JobState.RUNNING, canalSyncJob.getJobState());
        try {
            manager.stopSyncJob(stmt);
            Assert.assertEquals(JobState.CANCELLED, canalSyncJob.getJobState());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }

        // change sync job state to cancelled
        canalSyncJob.unprotectedUpdateState(JobState.CANCELLED, false);
        Assert.assertEquals(JobState.CANCELLED, canalSyncJob.getJobState());
        try {
            manager.stopSyncJob(stmt);
            Assert.fail();
        } catch (UserException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testJobNameExist() throws DdlException {
        CanalSyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
        SyncJobManager manager = new SyncJobManager();
        Assert.assertFalse(manager.isJobNameExist("testDb", "testJob"));

        // add a sync job to manager
        Map<Long, Map<String, List<SyncJob>>> dbIdToJobNameToSyncJobs = Maps.newHashMap();
        Map<String, List<SyncJob>> jobNameToSyncJobs = Maps.newHashMap();
        jobNameToSyncJobs.put("testJob", Lists.newArrayList(canalSyncJob));
        dbIdToJobNameToSyncJobs.put(50000L, jobNameToSyncJobs);
        Deencapsulation.setField(manager, "dbIdToJobNameToSyncJobs", dbIdToJobNameToSyncJobs);
        Assert.assertTrue(manager.isJobNameExist("testDb", "testJob"));
    }

    @Test
    public void testReplayUpdateSyncJobState() {
        CanalSyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
        // change sync job state to running
        try {
            canalSyncJob.updateState(JobState.RUNNING, false);
        } catch (UserException e) {
            Assert.fail();
        }
        Assert.assertEquals(JobState.RUNNING, canalSyncJob.getJobState());

        Deencapsulation.setField(canalSyncJob, "client", client);
        Deencapsulation.setField(canalSyncJob, "channels", Lists.newArrayList());

        SyncJobUpdateStateInfo info = new SyncJobUpdateStateInfo(jobId,
                JobState.CANCELLED, -1L, -1L, -1L,
                new SyncFailMsg(MsgType.USER_CANCEL, "user cancel"));
        SyncJobManager manager = new SyncJobManager();

        // add a sync job to manager
        Map<Long, SyncJob> idToSyncJob = Maps.newHashMap();
        idToSyncJob.put(jobId, canalSyncJob);
        Deencapsulation.setField(manager, "idToSyncJob", idToSyncJob);
        manager.replayUpdateSyncJobState(info);
        Assert.assertEquals(JobState.CANCELLED, canalSyncJob.getJobState());
        Assert.assertEquals(MsgType.USER_CANCEL, canalSyncJob.getFailMsg().getMsgType());
    }

    @Test
    public void testCleanOldSyncJobs() {
        SyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
        // change sync job state to cancelled
        try {
            canalSyncJob.updateState(JobState.CANCELLED, false);
        } catch (UserException e) {
            Assert.fail();
        }
        Assert.assertEquals(JobState.CANCELLED, canalSyncJob.getJobState());

        SyncJobManager manager = new SyncJobManager();

        // add a sync job to manager
        Map<Long, SyncJob> idToSyncJob = Maps.newHashMap();
        idToSyncJob.put(jobId, canalSyncJob);
        Map<Long, Map<String, List<SyncJob>>> dbIdToJobNameToSyncJobs = Maps.newHashMap();
        Map<String, List<SyncJob>> jobNameToSyncJobs = Maps.newHashMap();
        jobNameToSyncJobs.put(jobName, Lists.newArrayList(canalSyncJob));
        dbIdToJobNameToSyncJobs.put(dbId, jobNameToSyncJobs);

        Deencapsulation.setField(manager, "idToSyncJob", idToSyncJob);
        Deencapsulation.setField(manager, "dbIdToJobNameToSyncJobs", dbIdToJobNameToSyncJobs);

        new Expectations(canalSyncJob) {
            {
                canalSyncJob.isExpired(anyLong);
                result = true;
            }
        };
        manager.cleanOldSyncJobs();

        Assert.assertEquals(0, idToSyncJob.size());
        Assert.assertEquals(0, dbIdToJobNameToSyncJobs.size());
    }

    @Test
    public void testCleanOverLimitJobs() {
        SyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
        // change sync job state to cancelled
        try {
            canalSyncJob.updateState(JobState.CANCELLED, false);
        } catch (UserException e) {
            Assert.fail();
        }
        Assert.assertEquals(JobState.CANCELLED, canalSyncJob.getJobState());

        SyncJobManager manager = new SyncJobManager();

        // add a sync job to manager
        Map<Long, SyncJob> idToSyncJob = Maps.newHashMap();
        idToSyncJob.put(jobId, canalSyncJob);
        Map<Long, Map<String, List<SyncJob>>> dbIdToJobNameToSyncJobs = Maps.newHashMap();
        Map<String, List<SyncJob>> jobNameToSyncJobs = Maps.newHashMap();
        jobNameToSyncJobs.put(jobName, Lists.newArrayList(canalSyncJob));
        dbIdToJobNameToSyncJobs.put(dbId, jobNameToSyncJobs);

        Deencapsulation.setField(manager, "idToSyncJob", idToSyncJob);
        Deencapsulation.setField(manager, "dbIdToJobNameToSyncJobs", dbIdToJobNameToSyncJobs);

        new Expectations(canalSyncJob) {
            {
                canalSyncJob.isCompleted();
                result = true;
            }
        };
        Config.label_num_threshold = 0;
        manager.cleanOverLimitSyncJobs();

        Assert.assertEquals(0, idToSyncJob.size());
        Assert.assertEquals(0, dbIdToJobNameToSyncJobs.size());
    }
}
