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
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.thrift.TStorageMedium;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.Objects;

public class DataProperty implements GsonPostProcessable {
    public static final TStorageMedium DEFAULT_STORAGE_MEDIUM = "SSD".equalsIgnoreCase(Config.default_storage_medium)
            ? TStorageMedium.SSD : TStorageMedium.HDD;
    public static final long MAX_COOLDOWN_TIME_MS = 253402271999000L; // 9999-12-31 23:59:59

    public static final DataProperty DEFAULT_HDD_DATA_PROPERTY = new DataProperty(TStorageMedium.HDD);

    public enum AllocationPolicy {
        STRICT("strict"),
        ADAPTIVE("adaptive");

        private final String value;

        AllocationPolicy(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static AllocationPolicy fromString(String value) {
            for (AllocationPolicy policy : values()) {
                if (policy.value.equalsIgnoreCase(value)) {
                    return policy;
                }
            }
            throw new IllegalArgumentException("Invalid allocation_policy values: " + value);
        }

        public boolean isStrict() {
            return this == STRICT;
        }

        public boolean isAdaptive() {
            return this == ADAPTIVE;
        }
    }

    @SerializedName(value = "storageMedium")
    private TStorageMedium storageMedium;
    @SerializedName(value = "cooldownTimeMs")
    private long cooldownTimeMs;
    @SerializedName(value = "storagePolicy")
    private String storagePolicy;
    @SerializedName(value = "isMutable")
    private boolean isMutable = true;
    @SerializedName(value = "allocationPolicy")
    private AllocationPolicy allocationPolicy = AllocationPolicy.ADAPTIVE;

    private DataProperty() {
        // for persist
    }

    public DataProperty(TStorageMedium medium) {
        this.storageMedium = medium;
        this.cooldownTimeMs = MAX_COOLDOWN_TIME_MS;
        this.storagePolicy = "";
        this.allocationPolicy = AllocationPolicy.ADAPTIVE;
    }

    public DataProperty(DataProperty other) {
        this.storageMedium = other.storageMedium;
        this.cooldownTimeMs = other.cooldownTimeMs;
        this.storagePolicy = other.storagePolicy;
        this.isMutable = other.isMutable;
        this.allocationPolicy = other.allocationPolicy;
    }

    /**
     * DataProperty construction.
     *
     * @param medium storage medium for the init storage of the table
     * @param cooldown cool down time for SSD->HDD
     * @param storagePolicy remote storage policy for remote storage
     */
    public DataProperty(TStorageMedium medium, long cooldown, String storagePolicy) {
        this(medium, cooldown, storagePolicy, true, AllocationPolicy.ADAPTIVE);
    }

    public DataProperty(TStorageMedium medium, long cooldown, String storagePolicy, boolean isMutable,
            AllocationPolicy allocationPolicy) {
        this.storageMedium = medium;
        this.cooldownTimeMs = cooldown;
        this.storagePolicy = storagePolicy;
        this.isMutable = isMutable;
        this.allocationPolicy = allocationPolicy;
    }

    public TStorageMedium getStorageMedium() {
        return storageMedium;
    }

    public long getCooldownTimeMs() {
        return cooldownTimeMs;
    }

    public String getStoragePolicy() {
        return storagePolicy;
    }

    public void setStoragePolicy(String storagePolicy) {
        this.storagePolicy = storagePolicy;
    }

    public AllocationPolicy getAllocationPolicy() {
        return allocationPolicy;
    }

    public void setAllocationPolicy(AllocationPolicy allocationPolicy) {
        this.allocationPolicy = allocationPolicy;
    }

    public boolean isMutable() {
        return isMutable;
    }

    public void setMutable(boolean mutable) {
        isMutable = mutable;
    }

    public void setStorageMedium(TStorageMedium medium) {
        this.storageMedium = medium;
    }

    @Override
    public int hashCode() {
        return Objects.hash(storageMedium, cooldownTimeMs, storagePolicy, allocationPolicy);
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
                && this.cooldownTimeMs == other.cooldownTimeMs
                && Strings.nullToEmpty(this.storagePolicy).equals(Strings.nullToEmpty(other.storagePolicy))
                && this.isMutable == other.isMutable
                && this.allocationPolicy == other.allocationPolicy;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Storage medium[").append(this.storageMedium).append("]. ");
        sb.append("cool down[").append(TimeUtils.longToTimeString(cooldownTimeMs)).append("]. ");
        sb.append("remote storage policy[").append(this.storagePolicy).append("]. ");
        sb.append("allocation policy[").append(this.allocationPolicy).append("]. ");
        return sb.toString();
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // storagePolicy is a newly added field, it may be null when replaying from old version.
        this.storagePolicy = Strings.nullToEmpty(this.storagePolicy);
        if (this.allocationPolicy == null) {
            this.allocationPolicy = AllocationPolicy.ADAPTIVE;
        }
    }
}
