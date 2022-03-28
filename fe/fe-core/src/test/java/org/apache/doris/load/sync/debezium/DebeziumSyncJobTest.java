package org.apache.doris.load.sync.debezium;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.*;
import org.apache.doris.analysis.BinlogDesc;
import org.apache.doris.analysis.ChannelDescription;
import org.apache.doris.analysis.CreateDataSyncJobStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.sync.DataSyncJobType;
import org.apache.doris.load.sync.SyncChannel;
import org.apache.doris.load.sync.SyncFailMsg;
import org.apache.doris.load.sync.SyncJob;
import org.apache.doris.persist.EditLog;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DebeziumSyncJobTest {
    private static final Logger LOG = LogManager.getLogger(DebeziumSyncJobTest.class);

    private long jobId;
    private long dbId;
    private String dbName;
    private String tblName;
    private String jobName;
    private Catalog catalog;
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
        properties.put(DebeziumSyncJob.MYSQL_SERVER_IP, "127.0.0.1");
        properties.put(DebeziumSyncJob.MYSQL_SERVER_PORT, "11111");
        properties.put(DebeziumSyncJob.MYSQL_USERNAME, "test_user");
        properties.put(DebeziumSyncJob.MYSQL_PASSWORD, "test_password");

        catalog = Deencapsulation.newInstance(Catalog.class);
        new Expectations(catalog) {
            {
                catalog.getDbNullable(10000L);
                minTimes = 0;
                result = database;

                catalog.getDbNullable("testDb");
                minTimes = 0;
                result = database;

                catalog.getEditLog();
                minTimes = 0;
                result = editLog;

                Catalog.getCurrentCatalog();
                minTimes = 0;
                result = catalog;
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

        new MockUp<SyncMySQLClient>() {
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
                result = DataSyncJobType.DEBEZIUM;

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
                result = DataSyncJobType.DEBEZIUM;

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
            DebeziumSyncJob debeziumSyncJob = (DebeziumSyncJob) syncJob;
            Assert.assertEquals(jobId, debeziumSyncJob.getId());
            Assert.assertEquals(jobName, debeziumSyncJob.getJobName());
            Assert.assertEquals(dbId, debeziumSyncJob.getDbId());
            Assert.assertEquals(SyncJob.JobState.PENDING, debeziumSyncJob.getJobState());
            Assert.assertEquals(DataSyncJobType.DEBEZIUM, debeziumSyncJob.getJobType());
            Assert.assertNull(debeziumSyncJob.getFailMsg());

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
            DebeziumSyncJob debeziumSyncJob = new DebeziumSyncJob(jobId, jobName, dbId);
            debeziumSyncJob.setChannelDescriptions(channelDescriptions);
            debeziumSyncJob.checkAndSetBinlogInfo(binlogDesc);
            Assert.assertEquals(jobId, debeziumSyncJob.getId());
            Assert.assertEquals(jobName, debeziumSyncJob.getJobName());
            Assert.assertEquals(dbId, debeziumSyncJob.getDbId());
            Assert.assertEquals(DataSyncJobType.DEBEZIUM, debeziumSyncJob.getJobType());
            Assert.assertEquals(SyncJob.JobState.PENDING, debeziumSyncJob.getJobState());
            Assert.assertNull(debeziumSyncJob.getFailMsg());
            // execute job
            debeziumSyncJob.execute();
            Assert.assertTrue(debeziumSyncJob.isInit());
            Assert.assertTrue(debeziumSyncJob.isRunning());
            Assert.assertEquals(SyncJob.JobState.RUNNING, debeziumSyncJob.getJobState());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testPauseAndResumeJob(@Injectable BinlogDesc binlogDesc) {
        new MockUp<SyncMySQLClient>() {
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

        new MockUp<DebeziumSyncJob>() {
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
            DebeziumSyncJob debeziumSyncJob = new DebeziumSyncJob(jobId, jobName, dbId);
            debeziumSyncJob.checkAndSetBinlogInfo(binlogDesc);
            Assert.assertEquals(SyncJob.JobState.PENDING, debeziumSyncJob.getJobState());
            // run job
            debeziumSyncJob.execute();
            Assert.assertTrue(debeziumSyncJob.isRunning());
            // pause job
            debeziumSyncJob.pause();
            Assert.assertTrue(debeziumSyncJob.isPaused());
            // resume job
            debeziumSyncJob.resume();
            Assert.assertSame(debeziumSyncJob.getJobState(), SyncJob.JobState.PENDING);
        } catch (UserException e) {
            Assert.fail();
        }
    }

    @Test
    public void testCancelJob(@Injectable BinlogDesc binlogDesc) {

        new MockUp<DebeziumSyncJob>() {
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
            DebeziumSyncJob debeziumSyncJob = new DebeziumSyncJob(jobId, jobName, dbId);
            debeziumSyncJob.checkAndSetBinlogInfo(binlogDesc);
            Assert.assertEquals(SyncJob.JobState.PENDING, debeziumSyncJob.getJobState());
            // run job
            debeziumSyncJob.execute();
            Assert.assertTrue(debeziumSyncJob.isRunning());
            // cancel job
            debeziumSyncJob.cancel(SyncFailMsg.MsgType.USER_CANCEL, "user cancel");
            Assert.assertTrue(debeziumSyncJob.isCancelled());
            Assert.assertTrue(debeziumSyncJob.isCompleted());
            Assert.assertEquals(SyncFailMsg.MsgType.USER_CANCEL, debeziumSyncJob.getFailMsg().getMsgType());
            Assert.assertEquals("user cancel", debeziumSyncJob.getFailMsg().getMsg());
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
            DebeziumSyncJob debeziumSyncJob = new DebeziumSyncJob(jobId, jobName, dbId);
            debeziumSyncJob.setChannelDescriptions(channelDescriptions);
            debeziumSyncJob.checkAndSetBinlogInfo(binlogDesc);
            Assert.assertEquals(SyncJob.JobState.PENDING, debeziumSyncJob.getJobState());
            SyncJob.SyncJobUpdateStateInfo info = new SyncJob.SyncJobUpdateStateInfo(
                    jobId, SyncJob.JobState.CANCELLED, 1622469769L, -1L, -1L,
                    new SyncFailMsg(SyncFailMsg.MsgType.USER_CANCEL, "user cancel"));
            debeziumSyncJob.replayUpdateSyncJobState(info);
            Assert.assertTrue(debeziumSyncJob.isCancelled());
            Assert.assertEquals(SyncJob.JobState.CANCELLED, debeziumSyncJob.getJobState());
        } catch (UserException e) {
            Assert.fail(e.getMessage());
        }
    }
}
