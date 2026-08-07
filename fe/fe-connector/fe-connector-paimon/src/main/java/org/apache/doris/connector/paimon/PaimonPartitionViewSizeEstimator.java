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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.cache.ConnectorTableKey;
import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

/** Type-specific retained-heap estimator for the Paimon derived partition-view cache. */
final class PaimonPartitionViewSizeEstimator {
    private static final long KEY_SHALLOW_BYTES = JvmSizeUtils.instanceSize(ConnectorTableKey.class);
    private static final long VIEW_SHALLOW_BYTES = JvmSizeUtils.instanceSize(PaimonPartitionView.class);
    private static final long PARTITION_SHALLOW_BYTES = JvmSizeUtils.instanceSize(ConnectorPartitionInfo.class);
    private static final long UNMODIFIABLE_LIST_SHALLOW_BYTES = JvmSizeUtils.instanceSize(
            Collections.unmodifiableList(Collections.emptyList()).getClass());
    private static final long UNMODIFIABLE_MAP_SHALLOW_BYTES = JvmSizeUtils.instanceSize(
            Collections.unmodifiableMap(Collections.emptyMap()).getClass());
    private static final long LINKED_HASH_MAP_SHALLOW_BYTES = JvmSizeUtils.instanceSize(LinkedHashMap.class);
    private static final long LINKED_HASH_MAP_ENTRY_SHALLOW_BYTES = classSize("java.util.LinkedHashMap$Entry");

    private PaimonPartitionViewSizeEstimator() {
    }

    /** Caffeine callback: the complete key/value weight was computed when the immutable view was built. */
    static long estimateEntry(ConnectorTableKey key, List<ConnectorPartitionInfo> value) {
        return ((PaimonPartitionView) value).getEstimatedBytes();
    }

    static long estimateEntryOnConstruction(ConnectorTableKey key, PaimonPartitionView value) {
        long bytes = KEY_SHALLOW_BYTES;
        bytes = add(bytes, JvmSizeUtils.stringSize(key.getDb()));
        bytes = add(bytes, JvmSizeUtils.stringSize(key.getTable()));
        bytes = add(bytes, VIEW_SHALLOW_BYTES);
        bytes = add(bytes, UNMODIFIABLE_LIST_SHALLOW_BYTES);
        bytes = add(bytes, JvmSizeUtils.arrayListSize(value.size()));
        for (ConnectorPartitionInfo partition : value) {
            bytes = add(bytes, estimatePartition(partition));
        }
        return bytes;
    }

    private static long estimatePartition(ConnectorPartitionInfo partition) {
        long bytes = PARTITION_SHALLOW_BYTES;
        bytes = add(bytes, JvmSizeUtils.stringSize(partition.getPartitionName()));

        int valueCount = partition.getPartitionValues().size();
        if (valueCount > 0) {
            bytes = add(bytes, UNMODIFIABLE_MAP_SHALLOW_BYTES);
            bytes = add(bytes, LINKED_HASH_MAP_SHALLOW_BYTES);
            bytes = add(bytes, JvmSizeUtils.objectArraySize(hashCapacity(valueCount)));
            bytes = add(bytes, multiply(valueCount, LINKED_HASH_MAP_ENTRY_SHALLOW_BYTES));
        }

        List<String> orderedValues = partition.getOrderedPartitionValues();
        bytes = add(bytes, estimateCopiedList(orderedValues));
        for (String value : orderedValues) {
            // The same rendered String is retained by both partitionValues and orderedPartitionValues.
            bytes = add(bytes, JvmSizeUtils.stringSize(value));
        }
        return add(bytes, estimateCopiedList(partition.getPartitionValueNullFlags()));
    }

    private static long estimateCopiedList(List<?> values) {
        if (values.isEmpty()) {
            return 0L;
        }
        return add(JvmSizeUtils.instanceSize(values.getClass()), JvmSizeUtils.arrayListSize(values.size()));
    }

    private static int hashCapacity(int size) {
        long needed = (size * 4L + 2L) / 3L;
        int capacity = 16;
        while (capacity < needed && capacity < 1 << 30) {
            capacity <<= 1;
        }
        return capacity;
    }

    private static long classSize(String className) {
        try {
            return JvmSizeUtils.instanceSize(Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Required JVM collection class is missing: " + className, e);
        }
    }

    private static long multiply(long left, long right) {
        return JvmSizeUtils.saturatedMultiply(left, right);
    }

    private static long add(long left, long right) {
        return JvmSizeUtils.saturatedAdd(left, right);
    }
}
