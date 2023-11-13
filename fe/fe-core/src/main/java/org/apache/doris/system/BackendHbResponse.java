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

import org.apache.doris.common.io.Writable;
import org.apache.doris.resource.Tag;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.IOException;

/**
 * Backend heartbeat response contains Backend's be port, http port and brpc port
 */
public class BackendHbResponse extends HeartbeatResponse implements Writable {
    @SerializedName(value = "beId")
    private long beId;
    @SerializedName(value = "bePort")
    private int bePort;
    @SerializedName(value = "httpPort")
    private int httpPort;
    @SerializedName(value = "brpcPort")
    private int brpcPort;
    @SerializedName(value = "arrowFlightSqlPort")
    private int arrowFlightSqlPort;
    @SerializedName(value = "nodeRole")
    private String nodeRole = Tag.VALUE_MIX;

    // We need to broadcast be start time to all frontends,
    // it will be used to check if query on this backend should be canceled.
    @SerializedName(value = "beStartTime")
    private long beStartTime = 0;
    private String host;
    private String version = "";
    @SerializedName(value = "isShutDown")
    private boolean isShutDown = false;

    public BackendHbResponse() {
        super(HeartbeatResponse.Type.BACKEND);
    }

    public BackendHbResponse(long beId, int bePort, int httpPort, int brpcPort, long hbTime, long beStartTime,
            String version, String nodeRole, boolean isShutDown, int arrowFlightSqlPort) {
        super(HeartbeatResponse.Type.BACKEND);
        this.beId = beId;
        this.status = HbStatus.OK;
        this.bePort = bePort;
        this.httpPort = httpPort;
        this.brpcPort = brpcPort;
        this.hbTime = hbTime;
        this.beStartTime = beStartTime;
        this.version = version;
        this.nodeRole = nodeRole;
        this.isShutDown = isShutDown;
        this.arrowFlightSqlPort = arrowFlightSqlPort;
    }

    public BackendHbResponse(long beId, String errMsg) {
        super(HeartbeatResponse.Type.BACKEND);
        this.status = HbStatus.BAD;
        this.beId = beId;
        this.msg = errMsg;
    }

    public BackendHbResponse(long beId, String host, String errMsg) {
        super(HeartbeatResponse.Type.BACKEND);
        this.status = HbStatus.BAD;
        this.beId = beId;
        this.host = host;
        this.msg = errMsg;
    }

    public long getBeId() {
        return beId;
    }

    public int getBePort() {
        return bePort;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public int getBrpcPort() {
        return brpcPort;
    }

    public int getArrowFlightSqlPort() {
        return arrowFlightSqlPort;
    }

    public long getBeStartTime() {
        return beStartTime;
    }

    public String getVersion() {
        return version;
    }

    public String getNodeRole() {
        return nodeRole;
    }

    public boolean isShutDown() {
        return isShutDown;
    }

    @Override
    protected void readFields(DataInput in) throws IOException {
        super.readFields(in);
        beId = in.readLong();
        bePort = in.readInt();
        httpPort = in.readInt();
        brpcPort = in.readInt();
        arrowFlightSqlPort = in.readInt();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(", beId: ").append(beId);
        sb.append(", beHost: ").append(host);
        sb.append(", bePort: ").append(bePort);
        sb.append(", httpPort: ").append(httpPort);
        sb.append(", brpcPort: ").append(brpcPort);
        sb.append(", arrowFlightSqlPort: ").append(arrowFlightSqlPort);
        return sb.toString();
    }

}
