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

package org.apache.doris.clone;

import org.apache.doris.catalog.DiskInfo.DiskState;
import org.apache.doris.clone.BackendLoadStatistic.Classification;
import org.apache.doris.clone.BalanceStatus.ErrCode;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TStorageMedium;

public class RootPathLoadStatistic implements Comparable<RootPathLoadStatistic> {

    private long beId;
    private String path;
    private Long pathHash;
    private TStorageMedium storageMedium;
    private long capacityB;
    private long usedCapacityB;
    private long copingSizeB;
    private DiskState diskState;

    private Classification clazz = Classification.INIT;

    public RootPathLoadStatistic(long beId, String path, Long pathHash, TStorageMedium storageMedium,
            long capacityB, long usedCapacityB, DiskState diskState) {
        this.beId = beId;
        this.path = path;
        this.pathHash = pathHash;
        this.storageMedium = storageMedium;
        this.capacityB = capacityB <= 0 ? 1 : capacityB;
        this.usedCapacityB = usedCapacityB;
        this.copingSizeB = 0;
        this.diskState = diskState;
    }

    public long getBeId() {
        return beId;
    }

    public String getPath() {
        return path;
    }

    public long getPathHash() {
        return pathHash;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public long getCapacityB() {
        return capacityB;
    }

    public long getUsedCapacityB() {
        return usedCapacityB;
    }

    public double getUsedPercent() {
        return capacityB <= 0 ? 0.0 : (usedCapacityB + copingSizeB) / (double) capacityB;
    }

    public void incrCopingSizeB(long size) {
        copingSizeB += size;
    }

    public void setClazz(Classification clazz) {
        this.clazz = clazz;
    }

    public Classification getClazz() {
        return clazz;
    }

    public DiskState getDiskState() {
        return diskState;
    }

    public BalanceStatus isFit(long tabletSize, boolean isSupplement) {
        if (diskState == DiskState.OFFLINE) {
            return new BalanceStatus(ErrCode.COMMON_ERROR,
                    toString() + " does not fit tablet with size: " + tabletSize + ", offline");
        }

        double newUsagePerc = (usedCapacityB + tabletSize) / (double) capacityB;
        long newLeftCapacity = capacityB - usedCapacityB - tabletSize;
        if (isSupplement) {
            if (newUsagePerc > (Config.storage_flood_stage_usage_percent / 100.0)
                    || newLeftCapacity < Config.storage_flood_stage_left_capacity_bytes) {
                return new BalanceStatus(ErrCode.COMMON_ERROR,
                        toString() + " does not fit tablet with size: " + tabletSize + ", limitation reached");
            } else {
                return BalanceStatus.OK;
            }
        }

        if (newUsagePerc > (Config.storage_high_watermark_usage_percent / 100.0)
                || newLeftCapacity < Config.storage_min_left_capacity_bytes) {
            return new BalanceStatus(ErrCode.COMMON_ERROR,
                    toString() + " does not fit tablet with size: " + tabletSize);
        }
        return BalanceStatus.OK;
    }

    // path with lower usage percent rank ahead
    @Override
    public int compareTo(RootPathLoadStatistic o) {
        double myPercent = getUsedPercent();
        double otherPercent = o.getUsedPercent();
        return Double.compare(myPercent, otherPercent);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("path: ").append(path).append(", be: ").append(beId);
        sb.append(", used: ").append(usedCapacityB).append(", total: ").append(capacityB);
        return sb.toString();
    }
}
