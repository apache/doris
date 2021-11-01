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
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.sync.DataSyncJobType;
import org.apache.doris.load.sync.SyncFailMsg;
import org.apache.doris.load.sync.SyncFailMsg.MsgType;
import org.apache.doris.load.sync.SyncJob;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;

public class CanalSyncJob extends SyncJob {
    private static final Logger LOG = LogManager.getLogger(CanalSyncJob.class);

    protected final static String CANAL_SERVER_IP = "canal.server.ip";
    protected final static String CANAL_SERVER_PORT = "canal.server.port";
    protected final static String CANAL_DESTINATION = "canal.destination";
    protected final static String CANAL_USERNAME = "canal.username";
    protected final static String CANAL_PASSWORD = "canal.password";
    protected final static String CANAL_BATCH_SIZE = "canal.batchSize";
    protected final static String CANAL_DEBUG = "canal.debug";

    @SerializedName(value = "remote")
    private final CanalDestination remote;
    @SerializedName(value = "username")
    private String username;
    @SerializedName(value = "password")
    private String password;
    @SerializedName(value = "batchSize")
    private int batchSize = 8192;
    @SerializedName(value = "debug")
    private boolean debug = false;

    private transient SyncCanalClient client;

    public CanalSyncJob(long id, String jobName, long dbId) {
        super(id, jobName, dbId);
        this.dataSyncJobType = DataSyncJobType.CANAL;
        this.remote = new CanalDestination("", 0, "");
    }

    private void init() throws DdlException {
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(remote.getIp(), remote.getPort()), remote.getDestination(), username, password);
        // create channels
        initChannels();
        // create client
        client = new SyncCanalClient(this, remote.getDestination(), connector, batchSize, debug);
        // register channels into client
        client.registerChannels(channels);
    }

    public void initChannels() throws DdlException {
        if (channels == null) {
            channels = Lists.newArrayList();
        }
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);
        db.writeLock();
        try {
            for (ChannelDescription channelDescription : channelDescriptions) {
                String tableName = channelDescription.getTargetTable();
                OlapTable olapTable = db.getOlapTableOrDdlException(tableName);
                if (olapTable.getKeysType() != KeysType.UNIQUE_KEYS || !olapTable.hasDeleteSign()) {
                    throw new DdlException("Table[" + tableName + "] don't support batch delete.");
                }
                List<String> colNames = channelDescription.getColNames();
                if (colNames == null) {
                    colNames = Lists.newArrayList();
                    for (Column column : olapTable.getBaseSchema(false)) {
                        colNames.add(column.getName());
                    }
                }
                CanalSyncChannel syncChannel = new CanalSyncChannel(channelDescription.getChannelId(), this, db,
                        olapTable, colNames, channelDescription.getSrcDatabase(), channelDescription.getSrcTableName());
                if (channelDescription.getPartitionNames() != null) {
                    syncChannel.setPartitions(channelDescription.getPartitionNames());
                }
                channels.add(syncChannel);
            }
        } finally {
            db.writeUnlock();
        }
    }

    @Override
    public void checkAndSetBinlogInfo(BinlogDesc binlogDesc) throws DdlException {
        super.checkAndSetBinlogInfo(binlogDesc);
        Map<String, String> properties = binlogDesc.getProperties();

        remote.parse(properties);
        if (!properties.containsKey(CANAL_USERNAME)) {
            throw new DdlException("Missing " + CANAL_USERNAME + " property in binlog properties");
        } else {
            username = properties.get(CANAL_USERNAME);
        }

        if (!properties.containsKey(CANAL_PASSWORD)) {
            throw new DdlException("Missing " + CANAL_PASSWORD + " property in binlog properties");
        } else {
            password = properties.get(CANAL_PASSWORD);
        }

        // optional
        if (properties.containsKey(CANAL_BATCH_SIZE)) {
            try {
                batchSize = Integer.parseInt(properties.get(CANAL_BATCH_SIZE));
            } catch (NumberFormatException e) {
                throw new DdlException("Property " + CANAL_BATCH_SIZE + " is not int");
            }
        }

        // optional
        if (properties.containsKey(CANAL_DEBUG)) {
            debug = Boolean.parseBoolean(properties.get(CANAL_DEBUG));
        }
    }

    public boolean isInit() {
        return client != null && channels != null;
    }

    public boolean isNeedReschedule() {
        return jobState == JobState.RUNNING && !isInit();
    }

    @Override
    public void execute() throws UserException {
        LOG.info(new LogBuilder(LogKey.SYNC_JOB, id)
                .add("remote ip", remote.getIp())
                .add("remote port", remote.getPort())
                .add("msg", "Try to start canal client.")
                .add("debug", debug)
                .build());

        // init
        if (!isInit()) {
            init();
        }
        // start client
        unprotectedStartClient();
    }

    @Override
    public void cancel(MsgType msgType, String errMsg) {
        try {
            switch (msgType) {
                case SUBMIT_FAIL:
                case RUN_FAIL:
                case UNKNOWN:
                    unprotectedStopClient(JobState.PAUSED);
                    break;
                case SCHEDULE_FAIL:
                case USER_CANCEL:
                    unprotectedStopClient(JobState.CANCELLED);
                    break;
                default:
                    Preconditions.checkState(false, "unknown msg type: " + msgType.name());
                    break;
            }
            failMsg = new SyncFailMsg(msgType, errMsg);
            LOG.info(new LogBuilder(LogKey.SYNC_JOB, id)
                    .add("MsgType", msgType.name())
                    .add("msg", "Cancel canal sync job.")
                    .add("errMsg", errMsg)
                    .build());
        } catch (UserException e) {
            LOG.warn(new LogBuilder(LogKey.SYNC_JOB, id)
                    .add("msg", "Failed to cancel canal sync job.")
                    .build(), e);
        }
    }

    @Override
    public void pause() throws UserException {
        unprotectedStopClient(JobState.PAUSED);
        LOG.info(new LogBuilder(LogKey.SYNC_JOB, id)
                .add("remote ip", remote.getIp())
                .add("remote port", remote.getPort())
                .add("msg", "Pause canal sync job.")
                .add("debug", debug)
                .build());
    }

    @Override
    public void resume() throws UserException {
        updateState(JobState.PENDING, false);
        LOG.info(new LogBuilder(LogKey.SYNC_JOB, id)
                .add("remote ip", remote.getIp())
                .add("remote port", remote.getPort())
                .add("msg", "Resume canal sync job.")
                .add("debug", debug)
                .build());
    }

    public void unprotectedStartClient() throws UserException {
        client.startup();
        updateState(JobState.RUNNING, false);
        LOG.info(new LogBuilder(LogKey.SYNC_JOB, id)
                .add("name", jobName)
                .add("msg", "Client has been started.")
                .build());
    }

    public void unprotectedStopClient(JobState jobState) throws UserException {
        if (jobState != JobState.CANCELLED && jobState != JobState.PAUSED) {
            return;
        }
        if (client != null) {
            client.shutdown(true);
        }
        updateState(jobState, false);
        LOG.info(new LogBuilder(LogKey.SYNC_JOB, id)
                .add("name", jobName)
                .add("msg", "Client has been stopped.")
                .build());
    }

    @Override
    public void replayUpdateSyncJobState(SyncJobUpdateStateInfo info) {
        lastStartTimeMs = info.getLastStartTimeMs();
        lastStopTimeMs = info.getLastStopTimeMs();
        finishTimeMs = info.getFinishTimeMs();
        failMsg = info.getFailMsg();
        try {
            JobState jobState = info.getJobState();
            switch (jobState) {
                case PENDING:
                    updateState(JobState.PENDING, true);
                    break;
                case RUNNING:
                    updateState(JobState.RUNNING, true);
                    break;
                case PAUSED:
                    updateState(JobState.PAUSED, true);
                    break;
                case CANCELLED:
                    updateState(JobState.CANCELLED, true);
                    break;
            }
        } catch (UserException e) {
            LOG.error(new LogBuilder(LogKey.SYNC_JOB, id)
                    .add("desired_state", info.getJobState())
                    .add("msg", "replay update state error.")
                    .add("reason", e.getMessage())
                    .build(), e);
        }
        LOG.info(new LogBuilder(LogKey.SYNC_JOB, info.getId())
                .add("desired_state:", info.getJobState())
                .add("msg", "replay update sync job state")
                .build());
    }

    @Override
    public String getStatus() {
        if (client != null) {
            return client.getPositionInfo();
        }
        return "\\N";
    }

    @Override
    public String getJobConfig() {
        StringBuilder sb = new StringBuilder();
        sb.append("address:").append(remote.getIp()).append(":").append(remote.getPort()).append(",")
                .append("destination:").append(remote.getDestination()).append(",")
                .append("batchSize:").append(batchSize);
        return sb.toString();
    }

    public CanalDestination getRemote() {
        return remote;
    }

    @Override
    public String toString() {
        return "SyncJob [jobId=" + id
                + ", jobName=" +jobName
                + ", dbId=" + dbId
                + ", state=" + jobState
                + ", createTimeMs=" + TimeUtils.longToTimeString(createTimeMs)
                + ", lastStartTimeMs=" + TimeUtils.longToTimeString(lastStartTimeMs)
                + ", lastStopTimeMs=" + TimeUtils.longToTimeString(lastStopTimeMs)
                + ", finishTimeMs=" + TimeUtils.longToTimeString(finishTimeMs)
                + "]";
    }
}
