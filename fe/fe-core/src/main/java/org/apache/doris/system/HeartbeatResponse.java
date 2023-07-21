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
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This the superclass of all kinds of heartbeat response
 */
public class HeartbeatResponse implements Writable {
    public enum Type {
        FRONTEND,
        BACKEND,
        BROKER
    }

    public enum HbStatus {
        OK, BAD
    }

    @SerializedName(value = "type")
    protected Type type;
    @SerializedName(value = "status")
    protected HbStatus status;
    @Deprecated
    protected boolean isTypeRead = false;

    /**
     * msg and hbTime are no need to be synchronized to other Frontends,
     * and only Master Frontend has these info
     */
    protected String msg;
    protected long hbTime;

    public HeartbeatResponse(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    public HbStatus getStatus() {
        return status;
    }

    public String getMsg() {
        return msg;
    }

    public long getHbTime() {
        return hbTime;
    }

    public void setTypeRead(boolean isTypeRead) {
        this.isTypeRead = isTypeRead;
    }

    public static HeartbeatResponse read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_121) {
            HeartbeatResponse result = null;
            Type type = Type.valueOf(Text.readString(in));
            if (type == Type.FRONTEND) {
                result = new FrontendHbResponse();
            } else if (type == Type.BACKEND) {
                result = new BackendHbResponse();
            } else if (type == Type.BROKER) {
                result = new BrokerHbResponse();
            } else {
                throw new IOException("Unknown job type: " + type.name());
            }

            result.setTypeRead(true);
            result.readFields(in);
            return result;
        } else {
            return GsonUtils.GSON.fromJson(Text.readString(in), HeartbeatResponse.class);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    @Deprecated
    protected void readFields(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() < FeMetaVersion.VERSION_121) {
            if (!isTypeRead) {
                type = Type.valueOf(Text.readString(in));
                isTypeRead = true;
            }
            status = HbStatus.valueOf(Text.readString(in));
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("type: ").append(type.name());
        sb.append(", status: ").append(status.name());
        sb.append(", msg: ").append(msg);
        return sb.toString();
    }
}
