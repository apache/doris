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

import com.google.common.base.Strings;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageMedium;

import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DataProperty implements Writable {
    public static final DataProperty DEFAULT_DATA_PROPERTY = new DataProperty(
            "SSD".equalsIgnoreCase(Config.default_storage_medium) ? TStorageMedium.SSD : TStorageMedium.HDD
    );
    public static final long MAX_COOL_DOWN_TIME_MS = 253402271999000L; // 9999-12-31 23:59:59

    @SerializedName(value =  "storageMedium")
    private TStorageMedium storageMedium;
    @SerializedName(value =  "cooldownTimeMs")
    private long coolDownTimeMs;
    @SerializedName(value = "remoteStorageResourceName")
    private String remoteStorageResourceName;
    @SerializedName(value = "remoteCoolDownTimeMs")
    private long remoteCoolDownTimeMs;
    @SerializedName(value = "remoteStorageMedium")
    private TStorageMedium remoteStorageMedium;

    private DataProperty() {
        // for persist
    }

    public DataProperty(TStorageMedium medium) {
        this.storageMedium = medium;
        if (medium == TStorageMedium.SSD) {
            long currentTimeMs = System.currentTimeMillis();
            this.coolDownTimeMs = currentTimeMs + Config.storage_cooldown_second * 1000L;
        } else {
            this.coolDownTimeMs = MAX_COOL_DOWN_TIME_MS;
        }
        this.remoteStorageResourceName = "";
        this.remoteCoolDownTimeMs = MAX_COOL_DOWN_TIME_MS;
        this.remoteStorageMedium = TStorageMedium.S3;
    }

    public DataProperty(TStorageMedium medium, long coolDown,
                        String remoteStorageResourceName, long remoteCoolDownTimeMs) {
        this.storageMedium = medium;
        this.coolDownTimeMs = coolDown;
        this.remoteStorageResourceName = remoteStorageResourceName;
        this.remoteCoolDownTimeMs = remoteCoolDownTimeMs;
        if (Strings.isNullOrEmpty(remoteStorageResourceName)) {
            this.remoteStorageMedium = TStorageMedium.S3;
        } else {
            this.remoteStorageMedium = TStorageMedium.valueOf(
                    Catalog.getCurrentCatalog().getResourceMgr().getResource(remoteStorageResourceName).getType().name()
            );
        }
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public long getCoolDownTimeMs() {
        return coolDownTimeMs;
    }

    public long getRemoteCoolDownTimeMs() {
        return remoteCoolDownTimeMs;
    }

    public String getRemoteStorageResourceName() {
        return remoteStorageResourceName;
    }

    public TStorageMedium getRemoteStorageMedium() {
        return remoteStorageMedium;
    }

    public static DataProperty read(DataInput in) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_108) {
            String json = Text.readString(in);
            return GsonUtils.GSON.fromJson(json, DataProperty.class);
        }
        DataProperty dataProperty = new DataProperty();
        dataProperty.readFields(in);
        return dataProperty;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public void readFields(DataInput in) throws IOException {
        storageMedium = TStorageMedium.valueOf(Text.readString(in));
        coolDownTimeMs = in.readLong();
        remoteStorageResourceName = "";
        remoteCoolDownTimeMs = MAX_COOL_DOWN_TIME_MS;
        remoteStorageMedium = TStorageMedium.S3;
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
                && this.coolDownTimeMs == other.coolDownTimeMs
                && this.remoteCoolDownTimeMs == other.remoteCoolDownTimeMs
                && this.remoteStorageResourceName.equals(other.remoteStorageResourceName)
                && this.remoteStorageMedium == other.remoteStorageMedium;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Storage medium[").append(this.storageMedium).append("]. ");
        sb.append("cool down[").append(TimeUtils.longToTimeString(coolDownTimeMs)).append("]. ");
        sb.append("remote storage resource name[").append(this.remoteStorageResourceName).append("]. ");
        sb.append("remote cool down[").append(TimeUtils.longToTimeString(remoteCoolDownTimeMs)).append("]. ");
        sb.append("remote storage medium[").append(this.remoteStorageMedium).append("].");
        return sb.toString();
    }
}
