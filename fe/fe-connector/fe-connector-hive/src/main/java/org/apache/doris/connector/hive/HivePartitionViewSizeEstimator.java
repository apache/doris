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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.cache.ConnectorTableKey;
import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.spi.ConnectorPartitionInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Publication-time estimate of Hive's derived partition view, including every variable-length value. */
final class HivePartitionViewSizeEstimator {
    private static final long KEY_SHALLOW_BYTES = JvmSizeUtils.instanceSize(ConnectorTableKey.class);
    private static final long PARTITION_SHALLOW_BYTES = JvmSizeUtils.instanceSize(ConnectorPartitionInfo.class);
    private static final long ARRAY_LIST_SHALLOW_BYTES = JvmSizeUtils.instanceSize(ArrayList.class);
    private static final long UNMODIFIABLE_LIST_SHALLOW_BYTES = JvmSizeUtils.instanceSize(
            Collections.unmodifiableList(Collections.emptyList()).getClass());
    private static final long UNMODIFIABLE_MAP_SHALLOW_BYTES = JvmSizeUtils.instanceSize(
            Collections.unmodifiableMap(Collections.emptyMap()).getClass());
    private static final long LINKED_HASH_MAP_SHALLOW_BYTES = JvmSizeUtils.instanceSize(LinkedHashMap.class);
    private static final long LINKED_HASH_MAP_ENTRY_SHALLOW_BYTES = classSize("java.util.LinkedHashMap$Entry");

    private HivePartitionViewSizeEstimator() {
    }

    static MetaCacheSizeEstimate estimateEntry(ConnectorTableKey key, List<ConnectorPartitionInfo> partitions) {
        long bytes = KEY_SHALLOW_BYTES;
        bytes = add(bytes, JvmSizeUtils.stringSize(key.getDb()));
        bytes = add(bytes, JvmSizeUtils.stringSize(key.getTable()));
        bytes = add(bytes, ARRAY_LIST_SHALLOW_BYTES);
        bytes = add(bytes, JvmSizeUtils.objectArraySize(partitions.size()));
        ConnectorPartitionInfo previous = null;
        long structureBytes = 0L;
        boolean columnsCounted = false;
        for (ConnectorPartitionInfo partition : partitions) {
            // The collector uses the same wrappers and empty properties for every partition. Reuse the
            // fixed layout while its arity is unchanged; malformed names can have a different arity.
            if (previous == null
                    || previous.getPartitionValues().size() != partition.getPartitionValues().size()
                    || previous.getOrderedPartitionValues().size() != partition.getOrderedPartitionValues().size()
                    || previous.getPartitionValueNullFlags().size() != partition.getPartitionValueNullFlags().size()) {
                structureBytes = estimatePartitionStructure(partition);
                previous = partition;
            }
            bytes = add(bytes, structureBytes);
            bytes = add(bytes, JvmSizeUtils.stringSize(partition.getPartitionName()));
            // Hive parses map and ordered values separately: these are distinct retained Strings.
            for (String value : partition.getPartitionValues().values()) {
                bytes = add(bytes, JvmSizeUtils.stringSize(value));
            }
            for (String value : partition.getOrderedPartitionValues()) {
                bytes = add(bytes, JvmSizeUtils.stringSize(value));
            }
            if (!columnsCounted && !partition.getPartitionValues().isEmpty()) {
                for (String column : partition.getPartitionValues().keySet()) {
                    bytes = add(bytes, JvmSizeUtils.stringSize(column));
                }
                columnsCounted = true;
            }
        }
        return MetaCacheSizeEstimate.complete(bytes);
    }

    private static long estimatePartitionStructure(ConnectorPartitionInfo partition) {
        long bytes = PARTITION_SHALLOW_BYTES;
        bytes = add(bytes, estimateMapStructure(partition.getPartitionValues()));
        bytes = add(bytes, UNMODIFIABLE_MAP_SHALLOW_BYTES);
        bytes = add(bytes, estimateReferenceList(partition.getOrderedPartitionValues()));
        return add(bytes, estimateReferenceList(partition.getPartitionValueNullFlags()));
    }

    private static long estimateMapStructure(Map<String, String> values) {
        long bytes = UNMODIFIABLE_MAP_SHALLOW_BYTES;
        if (values.isEmpty()) {
            return bytes;
        }
        bytes = add(bytes, LINKED_HASH_MAP_SHALLOW_BYTES);
        bytes = add(bytes, JvmSizeUtils.objectArraySize(hashCapacity(values.size())));
        bytes = add(bytes, JvmSizeUtils.saturatedMultiply(
                values.size(), LINKED_HASH_MAP_ENTRY_SHALLOW_BYTES));
        return bytes;
    }

    private static long estimateReferenceList(List<?> values) {
        return add(UNMODIFIABLE_LIST_SHALLOW_BYTES, JvmSizeUtils.arrayListSize(values.size()));
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

    private static long add(long left, long right) {
        return JvmSizeUtils.saturatedAdd(left, right);
    }
}
