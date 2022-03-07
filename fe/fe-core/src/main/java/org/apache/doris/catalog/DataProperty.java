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

import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.thrift.TStorageMedium;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataProperty implements Writable {
    public static final DataProperty DEFAULT_DATA_PROPERTY = new DataProperty(
            "SSD".equalsIgnoreCase(Config.default_storage_medium) ? TStorageMedium.SSD : TStorageMedium.HDD);
    public static final long MAX_COOLDOWN_TIME_MS = 253402271999000L; // 9999-12-31 23:59:59

    @SerializedName(value =  "storageMedium")
    private TStorageMedium storageMedium;
    @SerializedName(value =  "cooldownTimeMs")
    private long cooldownTimeMs;

    private DataProperty() {
        // for persist
    }

    public DataProperty(TStorageMedium medium) {
        this.storageMedium = medium;
        if (medium == TStorageMedium.SSD) {
            long currentTimeMs = System.currentTimeMillis();
            this.cooldownTimeMs = currentTimeMs + Config.storage_cooldown_second * 1000L;
        } else {
            this.cooldownTimeMs = MAX_COOLDOWN_TIME_MS;
        }
    }

    public DataProperty(TStorageMedium medium, long cooldown) {
        this.storageMedium = medium;
        this.cooldownTimeMs = cooldown;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public long getCooldownTimeMs() {
        return cooldownTimeMs;
    }

    public static DataProperty read(DataInput in) throws IOException {
        DataProperty dataProperty = new DataProperty();
        dataProperty.readFields(in);
        return dataProperty;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, storageMedium.name());
        out.writeLong(cooldownTimeMs);
    }

    public void readFields(DataInput in) throws IOException {
        storageMedium = TStorageMedium.valueOf(Text.readString(in));
        cooldownTimeMs = in.readLong();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof DataProperty)) {
            return false;
        }

        DataProperty other = (DataProperty) obj;

        return this.storageMedium == other.storageMedium
                && this.cooldownTimeMs == other.cooldownTimeMs;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Storage medium[").append(this.storageMedium).append("]. ");
        sb.append("cool down[").append(TimeUtils.longToTimeString(cooldownTimeMs)).append("].");
        return sb.toString();
    }
}
