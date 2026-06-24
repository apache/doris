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

package org.apache.doris.datasource.hudi;

import org.apache.doris.datasource.NameMapping;

import java.util.Objects;

/**
 * Cache key for Hudi partition metadata by table, snapshot timestamp and load mode.
 */
public final class HudiPartitionCacheKey {
    private final NameMapping nameMapping;
    private final long timestamp;
    private final boolean useHiveSyncPartition;

    private HudiPartitionCacheKey(NameMapping nameMapping, long timestamp, boolean useHiveSyncPartition) {
        this.nameMapping = Objects.requireNonNull(nameMapping, "nameMapping can not be null");
        this.timestamp = timestamp;
        this.useHiveSyncPartition = useHiveSyncPartition;
    }

    public static HudiPartitionCacheKey of(NameMapping nameMapping, long timestamp, boolean useHiveSyncPartition) {
        return new HudiPartitionCacheKey(nameMapping, timestamp, useHiveSyncPartition);
    }

    public NameMapping getNameMapping() {
        return nameMapping;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isUseHiveSyncPartition() {
        return useHiveSyncPartition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HudiPartitionCacheKey)) {
            return false;
        }
        HudiPartitionCacheKey that = (HudiPartitionCacheKey) o;
        return timestamp == that.timestamp
                && useHiveSyncPartition == that.useHiveSyncPartition
                && Objects.equals(nameMapping, that.nameMapping);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nameMapping, timestamp, useHiveSyncPartition);
    }
}
