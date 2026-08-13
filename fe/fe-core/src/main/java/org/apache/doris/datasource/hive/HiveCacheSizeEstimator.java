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

package org.apache.doris.datasource.hive;

import org.apache.doris.datasource.hive.HiveExternalMetaCache.HivePartitionValues;
import org.apache.doris.datasource.hive.HiveExternalMetaCache.PartitionValueCacheKey;
import org.apache.doris.datasource.metacache.MetaCacheSizeEstimate;
import org.apache.doris.datasource.metacache.MetaCacheWeightUtils;

/** Constant-time retained-weight formula for Hive partition-value cache entries. */
final class HiveCacheSizeEstimator {
    // Calibrated against complete 4.1 object graphs. The per-character reserve covers the
    // partition name plus derived value/literal strings and therefore remains skew-sensitive.
    private static final long ENTRY_BASE_BYTES = 2L * 1024L;
    private static final long PARTITION_BASE_BYTES = 4L * 1024L;
    private static final long PARTITION_COLUMN_BYTES = 384L;
    private static final long PARTITION_NAME_CHARACTER_BYTES = 8L;

    private HiveCacheSizeEstimator() {
    }

    static MetaCacheSizeEstimate estimatePartitionValuesEntry(
            PartitionValueCacheKey key, HivePartitionValues value) {
        long partitionCount = value.getIdToPartitionItem() == null
                ? 0L : value.getIdToPartitionItem().size();
        long perPartitionBytes = MetaCacheWeightUtils.saturatedAdd(
                PARTITION_BASE_BYTES,
                MetaCacheWeightUtils.saturatedMultiply(
                        value.getPartitionColumnCount(), PARTITION_COLUMN_BYTES));
        long bytes = MetaCacheWeightUtils.saturatedAdd(
                ENTRY_BASE_BYTES, MetaCacheWeightUtils.estimatedNameMappingBytes(key.getNameMapping()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.saturatedMultiply(partitionCount, perPartitionBytes));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.saturatedMultiply(
                        value.getPartitionNameCharacterCount(), PARTITION_NAME_CHARACTER_BYTES));
        return MetaCacheSizeEstimate.complete(bytes);
    }
}
