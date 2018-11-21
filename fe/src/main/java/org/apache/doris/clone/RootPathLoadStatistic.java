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

import org.apache.doris.clone.BalanceStatus.ErrCode;
import org.apache.doris.common.Config;

public class RootPathLoadStatistic implements Comparable<RootPathLoadStatistic> {
    // Even if for tablet recovery, we can not exceed these 2 limitations.
    public static final double MAX_USAGE_PERCENT_LIMIT = 0.95;
    public static final double MIN_LEFT_CAPACITY_BYTES_LIMIT = 100 * 1024 * 1024; // 100MB

    private long beId;
    private String path;
    private Long pathHash;
    private long capacityB;
    private long usedCapacityB;

    private boolean hasTask = false;

    public RootPathLoadStatistic(long beId, String path, Long pathHash, long capacityB, long usedCapacityB) {
        this.beId = beId;
        this.path = path;
        this.pathHash = pathHash;
        this.capacityB = capacityB <= 0 ? 1 : capacityB;
        this.usedCapacityB = usedCapacityB;
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

    public long getCapacityB() {
        return capacityB;
    }

    public long getUsedCapacityB() {
        return usedCapacityB;
    }

    public void setHasTask(boolean hasTask) {
        this.hasTask = hasTask;
    }

    public boolean hasTask() {
        return hasTask;
    }

    public BalanceStatus isFit(long tabletSize, boolean isSupplement) {
        if (isSupplement) {
            if ((usedCapacityB + tabletSize) / (double) capacityB > MAX_USAGE_PERCENT_LIMIT
                    && capacityB - usedCapacityB - tabletSize < MIN_LEFT_CAPACITY_BYTES_LIMIT) {
                return new BalanceStatus(ErrCode.COMMON_ERROR,
                        toString() + " does not fit tablet with size: " + tabletSize + ", limitation reached");
            } else {
                return BalanceStatus.OK;
            }
        }

        if ((usedCapacityB + tabletSize) / (double) capacityB > Config.storage_high_watermark_usage_percent
                || capacityB - usedCapacityB - tabletSize < Config.storage_min_left_capacity_bytes) {
            return new BalanceStatus(ErrCode.COMMON_ERROR,
                    toString() + " does not fit tablet with size: " + tabletSize);
        }
        return BalanceStatus.OK;
    }

    @Override
    public int compareTo(RootPathLoadStatistic o) {
        double myPercent = usedCapacityB / (double) capacityB;
        double otherPercent = o.usedCapacityB / (double) capacityB;
        if (myPercent < otherPercent) {
            return 1;
        } else if (myPercent > otherPercent) {
            return -1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("path: ").append(path).append(", be: ").append(beId);
        sb.append(", used: ").append(usedCapacityB).append(", total: ").append(capacityB);
        return sb.toString();
    }

}
