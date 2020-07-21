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
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.thrift.TStorageMedium;

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

    private String rootPath;
    private long totalCapacityB;
    private long dataUsedCapacityB;
    private long diskAvailableCapacityB;
    private DiskState state;

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
        this.diskAvailableCapacityB = DEFAULT_CAPACITY_B;
        this.state = DiskState.ONLINE;
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

    public long getAvailableCapacityB() {
        return diskAvailableCapacityB;
    }

    public void setAvailableCapacityB(long availableCapacityB) {
        this.diskAvailableCapacityB = availableCapacityB;
    }

    public double getUsedPct() {
        return (totalCapacityB - diskAvailableCapacityB) / (double) (totalCapacityB <= 0 ? 1 : totalCapacityB);
    }

    public DiskState getState() {
        return state;
    }

    // return true if changed
    public boolean setState(DiskState state) {
        if (this.state != state) {
            this.state = state;
            return true;
        }
        return false;
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
            return diskAvailableCapacityB < Config.storage_flood_stage_left_capacity_bytes &&
                    (double) (totalCapacityB - diskAvailableCapacityB) / totalCapacityB > (Config.storage_flood_stage_usage_percent / 100.0);
        } else {
            return diskAvailableCapacityB < Config.storage_min_left_capacity_bytes ||
                    (double) (totalCapacityB - diskAvailableCapacityB) / totalCapacityB > (Config.storage_high_watermark_usage_percent / 100.0);
        }
    }

    @Override
    public String toString() {
        return "DiskInfo [rootPath=" + rootPath + "(" + pathHash + "), totalCapacityB=" + totalCapacityB
                + ", dataUsedCapacityB=" + dataUsedCapacityB + ", diskAvailableCapacityB="
                + diskAvailableCapacityB + ", state=" + state + ", medium: " + storageMedium + "]";
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, rootPath);
        out.writeLong(totalCapacityB);
        out.writeLong(dataUsedCapacityB);
        out.writeLong(diskAvailableCapacityB);
        Text.writeString(out, state.name());
    }

    public void readFields(DataInput in) throws IOException {
        this.rootPath = Text.readString(in);
        this.totalCapacityB = in.readLong();
        if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_36) {
            this.dataUsedCapacityB = in.readLong();
            this.diskAvailableCapacityB = in.readLong();
        } else {
            long availableCapacityB = in.readLong();
            this.dataUsedCapacityB = this.totalCapacityB - availableCapacityB;
            this.diskAvailableCapacityB = availableCapacityB;
        }
        this.state = DiskState.valueOf(Text.readString(in));
    }

    public static DiskInfo read(DataInput in) throws IOException {
        DiskInfo diskInfo = new DiskInfo();
        diskInfo.readFields(in);
        return diskInfo;
    }
}
