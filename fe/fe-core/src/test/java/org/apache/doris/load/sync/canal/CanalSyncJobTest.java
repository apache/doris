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

package org.apache.doris.load.sync.canal;

import org.apache.doris.analysis.BinlogDesc;
import org.apache.doris.analysis.ChannelDescription;
import org.apache.doris.analysis.CreateDataSyncJobStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.load.sync.DataSyncJobType;
import org.apache.doris.load.sync.SyncChannel;
import org.apache.doris.load.sync.SyncFailMsg;
import org.apache.doris.load.sync.SyncFailMsg.MsgType;
import org.apache.doris.load.sync.SyncJob;
import org.apache.doris.load.sync.SyncJob.JobState;
import org.apache.doris.load.sync.SyncJob.SyncJobUpdateStateInfo;
import org.apache.doris.persist.EditLog;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class CanalSyncJobTest {
    private static final Logger LOG = LogManager.getLogger(CanalSyncJobTest.class);

    private long jobId;
    private long dbId;
    private String dbName;
    private String tblName;
    private String jobName;
    private Env env;
    private InternalCatalog catalog;
    private Map<String, String> properties;

    @Mocked
    EditLog editLog;

    @Injectable
    Database database;

    @Injectable
    OlapTable table;

    @Before
    public void setUp() {
        jobId = 1L;
        dbId = 10000L;
        dbName = "testDb";
        tblName = "testTbl";
        jobName = "testJob";
        properties = Maps.newHashMap();
        properties.put(CanalSyncJob.CANAL_SERVER_IP, "127.0.0.1");
        properties.put(CanalSyncJob.CANAL_SERVER_PORT, "11111");
        properties.put(CanalSyncJob.CANAL_DESTINATION, "test");
        properties.put(CanalSyncJob.CANAL_USERNAME, "test_user");
        properties.put(CanalSyncJob.CANAL_PASSWORD, "test_password");

        catalog = Deencapsulation.newInstance(InternalCatalog.class);
        new Expectations(catalog) {
            {
                catalog.getDbNullable(10000L);
                minTimes = 0;
                result = database;

                catalog.getDbNullable("testDb");
                minTimes = 0;
                result = database;
            }
        };

        env = Deencapsulation.newInstance(Env.class);
        new Expectations(env) {
            {
                env.getInternalCatalog();
                minTimes = 0;
                result = catalog;

                env.getEditLog();
                minTimes = 0;
                result = editLog;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;
            }
        };

        new Expectations(database) {
            {
                database.getId();
                minTimes = 0;
                result = dbId;

                database.getTableNullable("testTbl");
                minTimes = 0;
                result = table;
            }
        };

        new Expectations(table) {
            {
                table.getName();
                minTimes = 0;
                result = tblName;

                table.getKeysType();
                minTimes = 0;
                result = KeysType.UNIQUE_KEYS;

                table.hasDeleteSign();
                minTimes = 0;
                result = true;
            }
        };

        new MockUp<SyncCanalClient>() {
            @Mock
            public void startup() {
            }

            @Mock
            public void shutdown(boolean needCleanUp) {
            }

            @Mock
            public void registerChannels(List<SyncChannel> channels) {
            }
        };
    }

    @Test
    public void testCreateFromStmtWithNoDatabase(@Injectable CreateDataSyncJobStmt stmt) {
        new Expectations() {
            {
                stmt.getDbName();
                result = "";
            }
        };

        try {
            SyncJob.fromStmt(jobId, stmt);
            Assert.fail();
        } catch (DdlException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testCreateFromStmtWithoutBinlog(@Injectable CreateDataSyncJobStmt stmt,
                                                @Injectable ChannelDescription channelDescription,
                                                @Injectable BinlogDesc binlogDesc) {
        List<ChannelDescription> channelDescriptions = Lists.newArrayList();
        channelDescriptions.add(channelDescription);
        new Expectations() {
            {
                stmt.getJobName();
                result = jobName;

                stmt.getDbName();
                result = dbName;

                stmt.getDataSyncJobType();
                result = DataSyncJobType.CANAL;

                stmt.getBinlogDesc();
                result = binlogDesc;

                stmt.getChannelDescriptions();
                result = channelDescriptions;

                binlogDesc.getProperties();
                result = Maps.newHashMap();
            }
        };

        try {
            SyncJob.fromStmt(jobId, stmt);
            Assert.fail();
        } catch (DdlException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testCreateFromStmt(@Injectable CreateDataSyncJobStmt stmt,
                                   @Injectable ChannelDescription channelDescription,
                                   @Injectable BinlogDesc binlogDesc) {

        List<ChannelDescription> channelDescriptions = Lists.newArrayList();
        channelDescriptions.add(channelDescription);

        new Expectations() {
            {
                stmt.getJobName();
                result = jobName;

                stmt.getDbName();
                result = dbName;

                stmt.getDataSyncJobType();
                result = DataSyncJobType.CANAL;

                stmt.getBinlogDesc();
                result = binlogDesc;

                stmt.getChannelDescriptions();
                result = channelDescriptions;

                binlogDesc.getProperties();
                result = properties;
            }
        };

        try {
            SyncJob syncJob = SyncJob.fromStmt(jobId, stmt);
            CanalSyncJob canalSyncJob = (CanalSyncJob) syncJob;
            Assert.assertEquals(jobId, canalSyncJob.getId());
            Assert.assertEquals(jobName, canalSyncJob.getJobName());
            Assert.assertEquals(dbId, canalSyncJob.getDbId());
            Assert.assertEquals(JobState.PENDING, canalSyncJob.getJobState());
            Assert.assertEquals(DataSyncJobType.CANAL, canalSyncJob.getJobType());
            Assert.assertNull(canalSyncJob.getFailMsg());

        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testExecute(@Injectable ChannelDescription channelDescription,
                            @Injectable BinlogDesc binlogDesc) {
        List<ChannelDescription> channelDescriptions = Lists.newArrayList();
        channelDescriptions.add(channelDescription);

        new Expectations() {
            {
                binlogDesc.getProperties();
                result = properties;

                channelDescription.getTargetTable();
                result = tblName;

                channelDescription.getSrcDatabase();
                result = "mysqlDb";

                channelDescription.getSrcTableName();
                result = "mysqlTbl";

                channelDescription.getColNames();
                result = null;

                channelDescription.getPartitionNames();
                result = null;
            }
        };

        try {
            CanalSyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
            canalSyncJob.setChannelDescriptions(channelDescriptions);
            canalSyncJob.checkAndSetBinlogInfo(binlogDesc);
            Assert.assertEquals(jobId, canalSyncJob.getId());
            Assert.assertEquals(jobName, canalSyncJob.getJobName());
            Assert.assertEquals(dbId, canalSyncJob.getDbId());
            Assert.assertEquals(DataSyncJobType.CANAL, canalSyncJob.getJobType());
            Assert.assertEquals(JobState.PENDING, canalSyncJob.getJobState());
            Assert.assertNull(canalSyncJob.getFailMsg());
            // execute job
            canalSyncJob.execute();
            Assert.assertTrue(canalSyncJob.isInit());
            Assert.assertTrue(canalSyncJob.isRunning());
            Assert.assertEquals(JobState.RUNNING, canalSyncJob.getJobState());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPauseAndResumeJob(@Injectable BinlogDesc binlogDesc) {
        new MockUp<SyncCanalClient>() {
            @Mock
            public void startup() {
            }

            @Mock
            public void shutdown(boolean needCleanUp) {
            }

            @Mock
            public void registerChannels(List<SyncChannel> channels) {
            }
        };

        new MockUp<CanalSyncJob>() {
            @Mock
            public void initChannels() {
            }
        };

        new Expectations() {
            {
                binlogDesc.getProperties();
                result = properties;
            }
        };

        try {
            CanalSyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
            canalSyncJob.checkAndSetBinlogInfo(binlogDesc);
            Assert.assertEquals(JobState.PENDING, canalSyncJob.getJobState());
            // run job
            canalSyncJob.execute();
            Assert.assertTrue(canalSyncJob.isRunning());
            // pause job
            canalSyncJob.pause();
            Assert.assertTrue(canalSyncJob.isPaused());
            // resume job
            canalSyncJob.resume();
            Assert.assertSame(canalSyncJob.getJobState(), JobState.PENDING);
        } catch (UserException e) {
            Assert.fail();
        }
    }

    @Test
    public void testCancelJob(@Injectable BinlogDesc binlogDesc) {

        new MockUp<CanalSyncJob>() {
            @Mock
            public void initChannels() {
            }
        };

        new Expectations() {
            {
                binlogDesc.getProperties();
                result = properties;
            }
        };

        try {
            CanalSyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
            canalSyncJob.checkAndSetBinlogInfo(binlogDesc);
            Assert.assertEquals(JobState.PENDING, canalSyncJob.getJobState());
            // run job
            canalSyncJob.execute();
            Assert.assertTrue(canalSyncJob.isRunning());
            // cancel job
            canalSyncJob.cancel(MsgType.USER_CANCEL, "user cancel");
            Assert.assertTrue(canalSyncJob.isCancelled());
            Assert.assertTrue(canalSyncJob.isCompleted());
            Assert.assertEquals(MsgType.USER_CANCEL, canalSyncJob.getFailMsg().getMsgType());
            Assert.assertEquals("user cancel", canalSyncJob.getFailMsg().getMsg());
        } catch (UserException e) {
            Assert.fail();
        }
    }

    @Test
    public void testReplayUpdateState(@Injectable ChannelDescription channelDescription,
                                      @Injectable BinlogDesc binlogDesc) {
        List<ChannelDescription> channelDescriptions = Lists.newArrayList();
        channelDescriptions.add(channelDescription);

        new Expectations() {
            {
                binlogDesc.getProperties();
                result = properties;
                channelDescription.getTargetTable();
                result = tblName;
                channelDescription.getSrcDatabase();
                result = "mysqlDb";
                channelDescription.getSrcTableName();
                result = "mysqlTbl";
            }
        };

        try {
            CanalSyncJob canalSyncJob = new CanalSyncJob(jobId, jobName, dbId);
            canalSyncJob.setChannelDescriptions(channelDescriptions);
            canalSyncJob.checkAndSetBinlogInfo(binlogDesc);
            Assert.assertEquals(JobState.PENDING, canalSyncJob.getJobState());
            SyncJobUpdateStateInfo info = new SyncJobUpdateStateInfo(
                    jobId, JobState.CANCELLED, 1622469769L, -1L, -1L,
                    new SyncFailMsg(MsgType.USER_CANCEL, "user cancel"));
            canalSyncJob.replayUpdateSyncJobState(info);
            Assert.assertTrue(canalSyncJob.isCancelled());
            Assert.assertEquals(JobState.CANCELLED, canalSyncJob.getJobState());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }
}
