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

import com.google.gson.annotations.SerializedName;
import org.apache.doris.service.ExecuteEnv;

import java.io.DataInput;
import java.io.IOException;
import java.util.List;

/**
 * Frontend heartbeat response contains Frontend's query port, rpc port and current replayed journal id.
 * (http port is supposed to the same, so no need to be carried on heartbeat response)
 */
public class FrontendHbResponse extends HeartbeatResponse implements Writable {
    @SerializedName(value = "name")
    private String name;
    @SerializedName(value = "queryPort")
    private int queryPort;
    @SerializedName(value = "rpcPort")
    private int rpcPort;
    @SerializedName(value = "replayedJournalId")
    private long replayedJournalId;
    private String version;
    private long feStartTime;
    private List<ExecuteEnv.DiskInfo> diskInfos;

    public FrontendHbResponse() {
        super(HeartbeatResponse.Type.FRONTEND);
    }

    public FrontendHbResponse(String name, int queryPort, int rpcPort,
            long replayedJournalId, long hbTime, String version, long feStartTime, List<ExecuteEnv.DiskInfo> diskInfos) {
        super(HeartbeatResponse.Type.FRONTEND);
        this.status = HbStatus.OK;
        this.name = name;
        this.queryPort = queryPort;
        this.rpcPort = rpcPort;
        this.replayedJournalId = replayedJournalId;
        this.hbTime = hbTime;
        this.version = version;
        this.feStartTime = feStartTime;
        this.diskInfos = diskInfos;
    }

    public FrontendHbResponse(String name, String errMsg) {
        super(HeartbeatResponse.Type.FRONTEND);
        this.status = HbStatus.BAD;
        this.name = name;
        this.msg = errMsg;
    }

    public String getName() {
        return name;
    }

    public int getQueryPort() {
        return queryPort;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public long getReplayedJournalId() {
        return replayedJournalId;
    }

    public String getVersion() {
        return version;
    }

    public long getFeStartTime() {
        return feStartTime;
    }

    public List<ExecuteEnv.DiskInfo> getDiskInfos() {
        return diskInfos;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        name = Text.readString(in);
        queryPort = in.readInt();
        rpcPort = in.readInt();
        replayedJournalId = in.readLong();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(", name: ").append(name);
        sb.append(", version: ").append(version);
        sb.append(", queryPort: ").append(queryPort);
        sb.append(", rpcPort: ").append(rpcPort);
        sb.append(", replayedJournalId: ").append(replayedJournalId);
        sb.append(", festartTime: ").append(feStartTime);
        return sb.toString();
    }

}
