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

import org.apache.doris.common.io.Writable;

import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class MetaObject implements Writable {

    @SerializedName(value = "signature")
    protected long signature;
    @SerializedName(value = "lastCheckTime")
    protected long lastCheckTime; // last check consistency time

    protected MetaObject() {
        signature = -1L;
        lastCheckTime = -1L;
    }

    // implement this in derived class
    public String getSignature(int signatureVersion) {
        String md5 = DigestUtils.md5Hex(Integer.toString(signatureVersion));
        return md5;
    }

    public long getLastCheckTime() {
        return lastCheckTime;
    }

    public void setLastCheckTime(long lastCheckTime) {
        this.lastCheckTime = lastCheckTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(signature);
        out.writeLong(lastCheckTime);
    }

    public void readFields(DataInput in) throws IOException {
        this.signature = in.readLong();
        this.lastCheckTime = in.readLong();
    }

}
