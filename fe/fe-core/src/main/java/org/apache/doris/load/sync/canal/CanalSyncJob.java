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
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
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

    @SerializedName(value = "ip")
    private String ip;
    @SerializedName(value = "port")
    private int port;
    @SerializedName(value = "destination")
    private String destination;
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
    }

    private void init() throws UserException {
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(ip, port), destination, username, password);
        client = new SyncCanalClient(this, destination, connector, batchSize, debug);
        // create channels
        initChannels();
        // register channels into client
        client.registerChannels(channels);
    }

    public void initChannels() throws DdlException {
        if (channels == null) {
            channels = Lists.newArrayList();
        }
        Database db = Catalog.getCurrentCatalog().getDb(dbId);
        if (db == null) {
            throw new DdlException("Database[" + dbId + "] does not exist");
        }
        db.writeLock();
        try {
            for (ChannelDescription channelDescription : channelDescriptions) {
                String tableName = channelDescription.getTargetTable();
                Table table = db.getTable(tableName);
                if (!(table instanceof OlapTable)) {
                    throw new DdlException("Table[" + tableName + "] is invalid.");
                }
                if (((OlapTable) table).getKeysType() != KeysType.UNIQUE_KEYS || !((OlapTable) table).hasDeleteSign()) {
                    throw new DdlException("Table[" + tableName + "] don't support batch delete.");
                }
                List<String> colNames = channelDescription.getColNames();
                if (colNames == null) {
                    colNames = Lists.newArrayList();
                    for (Column column : table.getBaseSchema(false)) {
                        colNames.add(column.getName());
                    }
                }
                CanalSyncChannel syncChannel = new CanalSyncChannel(this, db, (OlapTable) table, colNames,
                        channelDescription.getSrcDatabase(), channelDescription.getSrcTableName());
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

        // required binlog properties
        if (!properties.containsKey(CANAL_SERVER_IP)) {
            throw new DdlException("Missing " + CANAL_SERVER_IP + " property in binlog properties");
        } else {
            ip = properties.get(CANAL_SERVER_IP);
        }

        if (!properties.containsKey(CANAL_SERVER_PORT)) {
            throw new DdlException("Missing " + CANAL_SERVER_PORT + " property in binlog properties");
        } else {
            try {
                port = Integer.parseInt(properties.get(CANAL_SERVER_PORT));
            } catch (NumberFormatException e) {
                throw new DdlException("canal port is not int");
            }
        }

        if (!properties.containsKey(CANAL_DESTINATION)) {
            throw new DdlException("Missing " + CANAL_DESTINATION + " property in binlog properties");
        } else {
            destination = properties.get(CANAL_DESTINATION);
        }

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

        // optional binlog properties
        if (properties.containsKey(CANAL_BATCH_SIZE)) {
            try {
                batchSize = Integer.parseInt(properties.get(CANAL_BATCH_SIZE));
            } catch (NumberFormatException e) {
                throw new DdlException("Property " + CANAL_BATCH_SIZE + " is not int");
            }
        }

        if (properties.containsKey(CANAL_DEBUG)) {
            debug = Boolean.parseBoolean(properties.get(CANAL_DEBUG));
        }
    }

    public boolean isInit() {
        return client != null && channels != null;
    }

    @Override
    public void execute() throws UserException {
        LOG.info("try to start canal client. Remote ip: {}, remote port: {}, debug: {}", ip, port, debug);
        // init
        init();
        // start client
        unprotectedStartClient();
    }

    @Override
    public void cancel(MsgType msgType, String errMsg) {
        LOG.info("Cancel canal sync job {}. MsgType: {}, errMsg: {}", id, msgType.name(), errMsg);
        failMsg = new SyncFailMsg(msgType, errMsg);
        switch (msgType) {
            case USER_CANCEL:
            case SUBMIT_FAIL:
            case RUN_FAIL:
            case UNKNOWN:
                unprotectedStopClient(JobState.CANCELLED);
                break;
            default:
                Preconditions.checkState(false, "unknown msg type: " + msgType.name());
                break;
        }
    }

    @Override
    public void pause() {
        LOG.info("Pause canal sync job {}. Client remote ip: {}, remote port: {}, debug: {}", id, ip, port, debug);
        unprotectedStopClient(JobState.PAUSED);
    }

    @Override
    public void resume() {
        LOG.info("Resume canal sync job {}. Client remote ip: {}, remote port: {}, debug: {}", id, ip, port, debug);
        unprotectedStartClient();
    }

    public void unprotectedStartClient() {
        client.startup();
        updateState(JobState.RUNNING, false);
        LOG.info("client has been started. id: {}, jobName: {}", id, jobName);
    }

    public void unprotectedStopClient(JobState jobState) {
        if (jobState != JobState.CANCELLED && jobState != JobState.PAUSED) {
            return;
        }
        if (client != null) {
            if (jobState == JobState.CANCELLED) {
                client.shutdown(true);
            } else {
                client.shutdown(false);
            }
        }
        updateState(jobState, false);
        LOG.info("client has been stopped. id: {}, jobName: {}" , id, jobName);
    }

    @Override
    public void replayUpdateSyncJobState(SyncJobUpdateStateInfo info) {
        lastStartTimeMs = info.getLastStartTimeMs();
        lastStopTimeMs = info.getLastStopTimeMs();
        finishTimeMs = info.getFinishTimeMs();
        failMsg = info.getFailMsg();
        try {
            if (!isInit()) {
                init();
            }
            JobState jobState = info.getJobState();
            switch (jobState) {
                case RUNNING:
                    client.startup();
                    updateState(JobState.RUNNING, true);
                    break;
                case PAUSED:
                    client.shutdown(false);
                    updateState(JobState.PAUSED, true);
                    break;
                case CANCELLED:
                    client.shutdown(true);
                    updateState(JobState.CANCELLED, true);
                    break;
            }
        } catch (UserException e) {
            LOG.warn("encounter an error when replay update sync job state. id: {}, newState: {}, reason: {}",
                    info.getId(), info.getJobState(), e.getMessage());
            cancel(MsgType.UNKNOWN, e.getMessage());
        }
        LOG.info("replay update sync job state: {}", info);
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
        sb.append("adress:").append(ip).append(":").append(port).append(",")
                .append("destination:").append(destination).append(",")
                .append("batchSize:").append(batchSize);
        return sb.toString();
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