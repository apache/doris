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
    // Calibrated against complete 4.1 object graphs. The payload reserve covers the partition
    // name plus derived value/literal strings and therefore remains skew-sensitive.
    private static final long ENTRY_BASE_BYTES = objectBytes(2L * 1024L);
    private static final long PARTITION_BASE_BYTES = objectBytes(896L);
    // PartitionValueCacheKey retains an ImmutableList over the partition column types; the Type
    // instances themselves are shared catalog singletons and are not charged. A single-element
    // list is Guava's SingletonImmutableList (one reference, no array); wider lists add a
    // backing array with one slot per column.
    private static final long KEY_TYPE_LIST_BYTES =
            MetaCacheWeightUtils.estimatedObjectLayoutBytes(1L, 0L);
    private static final long PARTITION_COLUMN_BYTES = objectBytes(256L);
    // One copy is retained as the partition name and another in the decoded partition values.
    private static final long PARTITION_NAME_PAYLOAD_COPIES = 2L;

    private HiveCacheSizeEstimator() {
    }

    private static long objectBytes(long bytes) {
        return MetaCacheWeightUtils.estimatedObjectBytes(bytes);
    }

    static MetaCacheSizeEstimate estimatePartitionValuesEntry(
            PartitionValueCacheKey key, HivePartitionValues value) {
        if (!MetaCacheWeightUtils.isSupportedJvmObjectLayout()) {
            return MetaCacheSizeEstimate.incomplete("unsupported_jvm_object_alignment");
        }
        long partitionCount = value.getIdToPartitionItem() == null
                ? 0L : value.getIdToPartitionItem().size();
        long perPartitionBytes = MetaCacheWeightUtils.saturatedAdd(
                PARTITION_BASE_BYTES,
                MetaCacheWeightUtils.saturatedMultiply(
                        value.getPartitionColumnCount(), PARTITION_COLUMN_BYTES));
        long bytes = MetaCacheWeightUtils.saturatedAdd(
                ENTRY_BASE_BYTES, MetaCacheWeightUtils.estimatedNameMappingBytes(key.getNameMapping()));
        // The retained key width does not depend on how many partitions the table has today;
        // an empty partitioned table still retains one type list slot per partition column.
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes, keyTypeListBytes(key.retainedTypeCount()));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.saturatedMultiply(partitionCount, perPartitionBytes));
        bytes = MetaCacheWeightUtils.saturatedAdd(bytes,
                MetaCacheWeightUtils.saturatedMultiply(
                        value.getPartitionNamePayloadBytes(), PARTITION_NAME_PAYLOAD_COPIES));
        return MetaCacheSizeEstimate.complete(bytes);
    }

    private static long keyTypeListBytes(int typeCount) {
        if (typeCount <= 0) {
            // ImmutableList.of() is a shared singleton.
            return 0L;
        }
        long bytes = KEY_TYPE_LIST_BYTES;
        if (typeCount > 1) {
            bytes = MetaCacheWeightUtils.saturatedAdd(
                    bytes, MetaCacheWeightUtils.estimatedObjectArrayBytes(typeCount));
        }
        return bytes;
    }
}
