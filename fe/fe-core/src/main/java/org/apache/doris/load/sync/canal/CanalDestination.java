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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

public class CanalDestination implements Writable {

    @SerializedName(value = "ip")
    private String ip;
    @SerializedName(value = "port")
    private int port;
    @SerializedName(value = "destination")
    private String destination;

    public void parse(Map<String, String> properties) throws DdlException {
        // required binlog properties
        if (!properties.containsKey(CanalSyncJob.CANAL_SERVER_IP)) {
            throw new DdlException("Missing " + CanalSyncJob.CANAL_SERVER_IP + " property in binlog properties");
        } else {
            ip = properties.get(CanalSyncJob.CANAL_SERVER_IP);
        }
        if (!properties.containsKey(CanalSyncJob.CANAL_SERVER_PORT)) {
            throw new DdlException("Missing " + CanalSyncJob.CANAL_SERVER_PORT + " property in binlog properties");
        } else {
            try {
                port = Integer.parseInt(properties.get(CanalSyncJob.CANAL_SERVER_PORT));
            } catch (NumberFormatException e) {
                throw new DdlException("canal port is not int");
            }
        }
        if (!properties.containsKey(CanalSyncJob.CANAL_DESTINATION)) {
            throw new DdlException("Missing " + CanalSyncJob.CANAL_DESTINATION + " property in binlog properties");
        } else {
            destination = properties.get(CanalSyncJob.CANAL_DESTINATION);
        }
    }

    public CanalDestination(String ip, int port, String destination) {
        this.ip = ip;
        this.port = port;
        this.destination = destination;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port, destination);
    }

    @Override
    public boolean equals(Object other) {
        if (other == this) return true;
        if (!(other instanceof CanalDestination)) {
            return false;
        }
        CanalDestination otherDestination = (CanalDestination) other;
        return ip.equalsIgnoreCase(otherDestination.getIp()) && port == otherDestination.getPort() &&
                destination.equalsIgnoreCase(otherDestination.getDestination());
    }

    @Override
    public String toString() {
        return "CanalDestination [ip=" + ip + ", port=" + port +
                ", destination=" + destination + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static CanalDestination read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CanalDestination.class);
    }
}
