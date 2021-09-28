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

package org.apache.doris.load.sync;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SyncFailMsg implements Writable {
    public enum MsgType {
        USER_CANCEL,
        SUBMIT_FAIL,
        RUN_FAIL,
        SCHEDULE_FAIL,
        UNKNOWN
    }

    @SerializedName(value = "msgType")
    private SyncFailMsg.MsgType msgType;
    @SerializedName(value = "msg")
    private String msg;

    public SyncFailMsg(MsgType msgType, String msg) {
        this.msgType = msgType;
        this.msg = msg;
    }

    public MsgType getMsgType() {
        return msgType;
    }

    public void setMsgType(MsgType msgType) {
        this.msgType = msgType;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "SyncFailMsg [type=" + msgType + ", msg=" + msg + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this, SyncFailMsg.class));
    }

    public static SyncFailMsg read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, SyncFailMsg.class);
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof SyncFailMsg)) {
            return false;
        }

        SyncFailMsg other = (SyncFailMsg) obj;

        return msgType.equals(other.msgType)
                && msg.equals(other.msg);
    }
}