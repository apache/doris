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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.system.HeartbeatResponse.HbStatus;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Frontend implements Writable {
    private FrontendNodeType role;
    private String nodeName;
    private String host;
    private int editLogPort;
    private String version;
    
    private int queryPort;
    private int rpcPort;

    private long replayedJournalId;
    private long lastUpdateTime;
    private String heartbeatErrMsg = "";

    private boolean isAlive = false;

    public Frontend() {
    }
    
    public Frontend(FrontendNodeType role, String nodeName, String host, int editLogPort) {
        this.role = role;
        this.nodeName = nodeName;
        this.host = host;
        this.editLogPort = editLogPort;
    }

    public FrontendNodeType getRole() {
        return this.role;
    }
    
    public String getHost() {
        return this.host;
    }

    public String getVersion() {
        return version;
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
     * handle Frontend's heartbeat response.
     * Because the replayed journal id is very likely to be changed at each heartbeat response,
     * so we simple return true if the heartbeat status is OK.
     * But if heartbeat status is BAD, only return true if it is the first time to transfer from alive to dead.
     */
    public boolean handleHbResponse(FrontendHbResponse hbResponse) {
        boolean isChanged = false;
        if (hbResponse.getStatus() == HbStatus.OK) {
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
        Text.writeString(out, role.name());
        Text.writeString(out, host);
        out.writeInt(editLogPort);
        Text.writeString(out, nodeName);
    }

    public void readFields(DataInput in) throws IOException {
        role = FrontendNodeType.valueOf(Text.readString(in));
        if (role == FrontendNodeType.REPLICA) {
            // this is for compatibility.
            // we changed REPLICA to FOLLOWER
            role = FrontendNodeType.FOLLOWER;
        }
        host = Text.readString(in);
        editLogPort = in.readInt();
        nodeName = Text.readString(in);
    }
    
    public static Frontend read(DataInput in) throws IOException {
        Frontend frontend = new Frontend();
        frontend.readFields(in);
        return frontend;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("name: ").append(nodeName).append(", role: ").append(role.name());
        sb.append(", ").append(host).append(":").append(editLogPort);
        return sb.toString();
    }
}
