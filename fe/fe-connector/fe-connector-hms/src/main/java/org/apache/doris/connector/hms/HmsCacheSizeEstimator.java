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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.cache.MetaCacheSizeEstimate;
import org.apache.doris.connector.cache.ReflectiveObjectSizeEstimator;

import java.util.List;

/** Construction-time retained-size formulas for HMS cache entries. */
final class HmsCacheSizeEstimator {
    private static final long COLUMN_STATS_SHALLOW_BYTES =
            JvmSizeUtils.instanceSize(HmsColumnStatistics.class);

    private HmsCacheSizeEstimator() {
    }

    static MetaCacheSizeEstimate estimateTable(Object key, HmsTableInfo value) {
        return complete(key, value);
    }

    static MetaCacheSizeEstimate estimatePartition(Object key, HmsPartitionInfo value) {
        return complete(key, value);
    }

    static MetaCacheSizeEstimate estimatePartitionNames(Object key, List<String> value) {
        return MetaCacheSizeEstimate.complete(add(
                ReflectiveObjectSizeEstimator.estimateComplete(key), estimateStringList(value)));
    }

    static MetaCacheSizeEstimate estimateColumnStats(Object key, List<HmsColumnStatistics> value) {
        long bytes = add(JvmSizeUtils.instanceSize(value.getClass()),
                JvmSizeUtils.objectArraySize(value.size()));
        for (HmsColumnStatistics stats : value) {
            bytes = add(bytes, COLUMN_STATS_SHALLOW_BYTES);
            bytes = add(bytes, JvmSizeUtils.stringSize(stats.getColumnName()));
        }
        return MetaCacheSizeEstimate.complete(add(
                ReflectiveObjectSizeEstimator.estimateComplete(key), bytes));
    }

    private static MetaCacheSizeEstimate complete(Object key, Object value) {
        return MetaCacheSizeEstimate.complete(add(
                ReflectiveObjectSizeEstimator.estimateComplete(key),
                ReflectiveObjectSizeEstimator.estimateComplete(value)));
    }

    private static long estimateStringList(List<String> values) {
        long bytes = add(JvmSizeUtils.instanceSize(values.getClass()),
                JvmSizeUtils.objectArraySize(values.size()));
        for (String value : values) {
            bytes = add(bytes, JvmSizeUtils.stringSize(value));
        }
        return bytes;
    }

    private static long add(long left, long right) {
        return JvmSizeUtils.saturatedAdd(left, right);
    }
}
