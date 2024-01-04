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

package org.apache.doris.catalog;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.NetUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.system.BrokerHbResponse;
import org.apache.doris.system.HeartbeatResponse.HbStatus;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FsBroker implements Writable, Comparable<FsBroker> {
    @SerializedName(value = "host", alternate = {"ip"})
    public String host;
    @SerializedName(value = "port")
    public int port;
    // msg for ping result
    public String heartbeatErrMsg = "";
    public long lastUpdateTime = -1;

    @SerializedName(value = "lastStartTime")
    public long lastStartTime = -1;
    @SerializedName(value = "isAlive")
    public boolean isAlive;

    public FsBroker() {
    }

    public FsBroker(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /*
     * handle Broker's heartbeat response.
     * return true if alive state is changed.
     */
    public boolean handleHbResponse(BrokerHbResponse hbResponse) {
        boolean isChanged = false;
        if (hbResponse.getStatus() == HbStatus.OK) {
            if (!isAlive) {
                isAlive = true;
                isChanged = true;
                lastStartTime = hbResponse.getHbTime();
            } else if (lastStartTime <= 0) {
                lastStartTime = hbResponse.getHbTime();
            }
            lastUpdateTime = hbResponse.getHbTime();
            heartbeatErrMsg = "";
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FsBroker)) {
            return false;
        }

        FsBroker other = (FsBroker) o;

        return port == other.port
                && host.equals(other.host);
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }

    @Override
    public int compareTo(FsBroker o) {
        int ret = host.compareTo(o.host);
        if (ret != 0) {
            return ret;
        }
        return port - o.port;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Deprecated
    private void readFields(DataInput in) throws IOException {
        host = Text.readString(in);
        port = in.readInt();
    }

    @Override
    public String toString() {
        return NetUtils.getHostPortInAccessibleFormat(host, port);
    }

    public static FsBroker readIn(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, FsBroker.class);
    }
}
