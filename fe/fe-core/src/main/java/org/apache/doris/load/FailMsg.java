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

package org.apache.doris.load;

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FailMsg implements Writable {
    public enum CancelType {
        USER_CANCEL,
        ETL_SUBMIT_FAIL,
        ETL_RUN_FAIL,
        ETL_QUALITY_UNSATISFIED,
        LOAD_RUN_FAIL,
        TIMEOUT,
        UNKNOWN,
        TXN_UNKNOWN // cancelled because txn status is unknown
    }

    @SerializedName(value = "ct")
    private CancelType cancelType;
    @SerializedName(value = "m")
    private String msg = "";

    public FailMsg() {
        this.cancelType = CancelType.UNKNOWN;
    }

    public FailMsg(CancelType cancelType) {
        this.cancelType = cancelType;
    }

    public FailMsg(CancelType cancelType, String msg) {
        this.cancelType = cancelType;
        this.msg = msg;
    }

    public CancelType getCancelType() {
        return cancelType;
    }

    public void setCancelType(CancelType cancelType) {
        this.cancelType = cancelType;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "FailMsg [cancelType=" + cancelType + ", msg=" + msg + "]";
    }

    public void write(DataOutput out) throws IOException {
        Text.writeString(out, cancelType.name());
        Text.writeString(out, msg);
    }

    public void readFields(DataInput in) throws IOException {
        cancelType = CancelType.valueOf(Text.readString(in));
        msg = Text.readString(in);
    }

    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof FailMsg)) {
            return false;
        }

        FailMsg failMsg = (FailMsg) obj;

        return cancelType.equals(failMsg.cancelType)
                && msg.equals(failMsg.msg);
    }

}
