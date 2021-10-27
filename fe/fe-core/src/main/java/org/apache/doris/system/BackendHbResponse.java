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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Backend heartbeat response contains Backend's be port, http port and brpc port
 */
public class BackendHbResponse extends HeartbeatResponse implements Writable {
    private long beId;
    private int bePort;
    private int httpPort;
    private int brpcPort;
    private long beStartTime;
    private String version = "";

    public BackendHbResponse() {
        super(HeartbeatResponse.Type.BACKEND);
    }

    public BackendHbResponse(long beId, int bePort, int httpPort, int brpcPort, long hbTime, long beStartTime, String version) {
        super(HeartbeatResponse.Type.BACKEND);
        this.beId = beId;
        this.status = HbStatus.OK;
        this.bePort = bePort;
        this.httpPort = httpPort;
        this.brpcPort = brpcPort;
        this.hbTime = hbTime;
        this.beStartTime = beStartTime;
        this.version = version;
    }

    public BackendHbResponse(long beId, String errMsg) {
        super(HeartbeatResponse.Type.BACKEND);
        this.status = HbStatus.BAD;
        this.beId = beId;
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

    public long getBeStartTime() {
        return beStartTime;
    }

    public String getVersion() {
        return version;
    }

    public static BackendHbResponse read(DataInput in) throws IOException {
        BackendHbResponse result = new BackendHbResponse();
        result.readFields(in);
        return result;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeLong(beId);
        out.writeInt(bePort);
        out.writeInt(httpPort);
        out.writeInt(brpcPort);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        beId = in.readLong();
        bePort = in.readInt();
        httpPort = in.readInt();
        brpcPort = in.readInt();
    }
	
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.toString());
        sb.append(", beId: ").append(beId);
        sb.append(", bePort: ").append(bePort);
        sb.append(", httpPort: ").append(httpPort);
        sb.append(", brpcPort: ").append(brpcPort);
        return sb.toString();
    }

}
