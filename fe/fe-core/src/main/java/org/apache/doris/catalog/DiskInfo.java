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
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TStorageMedium;

import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DiskInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(DiskInfo.class);

    public enum DiskState {
        ONLINE,
        OFFLINE
    }

    private static final long DEFAULT_CAPACITY_B = 1024 * 1024 * 1024 * 1024L; // 1T

    @SerializedName("rootPath")
    private String rootPath;
    @SerializedName("totalCapacityB")
    private long totalCapacityB;
    @SerializedName("dataUsedCapacityB")
    private long dataUsedCapacityB;
    @SerializedName("trashUsedCapacityB")
    private long trashUsedCapacityB;
    @SerializedName("remoteUsedCapacity")
    private long remoteUsedCapacity = 0;
    @SerializedName("diskAvailableCapacityB")
    private long diskAvailableCapacityB;
    @SerializedName("state")
    private DiskState state;
    @SerializedName("dirType")
    private String dirType;
    // path hash and storage medium are reported from Backend and no need to persist
    private long pathHash = 0;
    private TStorageMedium storageMedium;

    private DiskInfo() {
        // for persist
    }

    public DiskInfo(String rootPath) {
        this.rootPath = rootPath;
        this.totalCapacityB = DEFAULT_CAPACITY_B;
        this.dataUsedCapacityB = 0;
        this.trashUsedCapacityB = 0;
        this.diskAvailableCapacityB = DEFAULT_CAPACITY_B;
        this.state = DiskState.ONLINE;
        this.dirType = "STORAGE";
        this.pathHash = 0;
        this.storageMedium = TStorageMedium.HDD;
    }

    public String getRootPath() {
        return rootPath;
    }

    public long getTotalCapacityB() {
        return totalCapacityB;
    }

    public void setTotalCapacityB(long totalCapacityB) {
        this.totalCapacityB = totalCapacityB;
    }

    public long getDataUsedCapacityB() {
        return dataUsedCapacityB;
    }

    public void setDataUsedCapacityB(long dataUsedCapacityB) {
        this.dataUsedCapacityB = dataUsedCapacityB;
    }

    public long getRemoteUsedCapacity() {
        return remoteUsedCapacity;
    }

    public void setRemoteUsedCapacity(long remoteUsedCapacity) {
        this.remoteUsedCapacity = remoteUsedCapacity;
    }

    public long getTrashUsedCapacityB() {
        return trashUsedCapacityB;
    }

    public void setTrashUsedCapacityB(long trashUsedCapacityB) {
        this.trashUsedCapacityB = trashUsedCapacityB;
    }

    public long getDiskUsedCapacityB() {
        return totalCapacityB - diskAvailableCapacityB;
    }

    public long getAvailableCapacityB() {
        return diskAvailableCapacityB;
    }

    public void setAvailableCapacityB(long availableCapacityB) {
        this.diskAvailableCapacityB = availableCapacityB;
    }

    public double getUsedPct() {
        return this.getDiskUsedCapacityB() / (double) (totalCapacityB <= 0 ? 1 : totalCapacityB);
    }

    public DiskState getState() {
        return state;
    }

    public String getDirType() {
        return dirType;
    }

    // return true if changed
    public boolean setState(DiskState state) {
        if (this.state != state) {
            this.state = state;
            return true;
        }
        return false;
    }

    public void setDirType(String dirType) {
        this.dirType = dirType;
    }

    public long getPathHash() {
        return pathHash;
    }

    public void setPathHash(long pathHash) {
        this.pathHash = pathHash;
    }

    public boolean hasPathHash() {
        return pathHash != 0;
    }

    public boolean isStorageMediumMatch(TStorageMedium storageMedium) {
        return this.storageMedium == storageMedium;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public void setStorageMedium(TStorageMedium storageMedium) {
        this.storageMedium = storageMedium;
    }

    /*
     * Check if this disk's capacity reach the limit. Return true if yes.
     * if floodStage is true, use floodStage threshold to check.
     *      floodStage threshold means a loosely limit, and we use 'AND' to give a more loosely limit.
     */
    public boolean exceedLimit(boolean floodStage) {
        LOG.debug("flood stage: {}, diskAvailableCapacityB: {}, totalCapacityB: {}",
                floodStage, diskAvailableCapacityB, totalCapacityB);
        if (floodStage) {
            return diskAvailableCapacityB < Config.storage_flood_stage_left_capacity_bytes
                && this.getUsedPct() > (Config.storage_flood_stage_usage_percent / 100.0);
        } else {
            return diskAvailableCapacityB < Config.storage_min_left_capacity_bytes
                || this.getUsedPct() > (Config.storage_high_watermark_usage_percent / 100.0);
        }
    }

    @Override
    public String toString() {
        return "DiskInfo [rootPath=" + rootPath + "(" + pathHash + "), totalCapacityB=" + totalCapacityB
                + ", dataUsedCapacityB=" + dataUsedCapacityB + ", trashUsedCapacityB=" + trashUsedCapacityB
                + ", diskAvailableCapacityB=" + diskAvailableCapacityB + ", state=" + state
                + ", dirType=" + dirType + ", medium: " + storageMedium + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static DiskInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DiskInfo.class);
    }
}
