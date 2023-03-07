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

package org.apache.doris.system;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.ha.BDBHA;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.HeartbeatResponse.HbStatus;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Frontend implements Writable {
    @SerializedName("role")
    private FrontendNodeType role;
    // nodeName = ip:port_timestamp
    @SerializedName("nodeName")
    private String nodeName;
    @SerializedName("ip")
    private volatile String ip;
    // used for getIpByHostname
    @SerializedName("hostName")
    private String hostName;
    @SerializedName("editLogPort")
    private int editLogPort;
    private String version;

    private int queryPort;
    private int rpcPort;

    private long replayedJournalId;
    private long lastUpdateTime;
    private String heartbeatErrMsg = "";

    private boolean isAlive = false;

    public Frontend() {}

    public Frontend(FrontendNodeType role, String nodeName, String ip, int editLogPort) {
        this(role, nodeName, ip, "", editLogPort);
    }

    public Frontend(FrontendNodeType role, String nodeName, String ip, String hostName, int editLogPort) {
        this.role = role;
        this.nodeName = nodeName;
        this.ip = ip;
        this.hostName = hostName;
        this.editLogPort = editLogPort;
    }

    public FrontendNodeType getRole() {
        return this.role;
    }

    public String getIp() {
        return this.ip;
    }

    public String getVersion() {
        return version;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getNodeName() {
        return nodeName;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public int getEditLogPort() {
        return this.editLogPort;
    }

    public long getReplayedJournalId() {
        return replayedJournalId;
    }

    public String getHeartbeatErrMsg() {
        return heartbeatErrMsg;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    /**
     * handle Frontend's heartbeat response. Because the replayed journal id is very likely to be
     * changed at each heartbeat response, so we simple return true if the heartbeat status is OK.
     * But if heartbeat status is BAD, only return true if it is the first time to transfer from
     * alive to dead.
     */
    public boolean handleHbResponse(FrontendHbResponse hbResponse, boolean isReplay) {
        boolean isChanged = false;
        if (hbResponse.getStatus() == HbStatus.OK) {
            if (!isAlive && !isReplay && Config.edit_log_type.equalsIgnoreCase("bdb")) {
                BDBHA bdbha = (BDBHA) Env.getCurrentEnv().getHaProtocol();
                bdbha.removeUnReadyElectableNode(nodeName, Env.getCurrentEnv().getFollowerCount());
            }
            isAlive = true;
            version = hbResponse.getVersion();
            queryPort = hbResponse.getQueryPort();
            rpcPort = hbResponse.getRpcPort();
            replayedJournalId = hbResponse.getReplayedJournalId();
            lastUpdateTime = hbResponse.getHbTime();
            heartbeatErrMsg = "";
            isChanged = true;
        } else {
            if (isAlive) {
                isAlive = false;
                isChanged = true;
            }
            heartbeatErrMsg = hbResponse.getMsg() == null ? "Unknown error" : hbResponse.getMsg();
        }
        return isChanged;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Deprecated
    private void readFields(DataInput in) throws IOException {
        role = FrontendNodeType.valueOf(Text.readString(in));
        if (role == FrontendNodeType.REPLICA) {
            // this is for compatibility.
            // we changed REPLICA to FOLLOWER
            role = FrontendNodeType.FOLLOWER;
        }
        ip = Text.readString(in);
        editLogPort = in.readInt();
        nodeName = Text.readString(in);
    }

    public static Frontend read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_118) {
            Frontend frontend = new Frontend();
            frontend.readFields(in);
            return frontend;
        }
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Frontend.class);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("name: ").append(nodeName).append(", role: ").append(role.name());
        sb.append(", hostname: ").append(hostName);
        sb.append(", ").append(ip).append(":").append(editLogPort);
        return sb.toString();
    }

    public void setIp(String ip) {
        this.ip = ip;
    }
}
