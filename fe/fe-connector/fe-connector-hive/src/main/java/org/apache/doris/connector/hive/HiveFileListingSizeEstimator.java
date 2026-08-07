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

import org.apache.doris.connector.cache.JvmSizeUtils;
import org.apache.doris.connector.hive.HiveFileListingCache.FileListingKey;
import org.apache.doris.connector.hive.HiveFileListingCache.FileListingValue;

/** Type-specific retained-heap estimator for one Hive directory-listing cache entry. */
final class HiveFileListingSizeEstimator {
    private static final long KEY_SHALLOW_BYTES = JvmSizeUtils.instanceSize(FileListingKey.class);
    private static final long VALUE_SHALLOW_BYTES = JvmSizeUtils.instanceSize(FileListingValue.class);
    private static final long FILE_STATUS_SHALLOW_BYTES = JvmSizeUtils.instanceSize(HiveFileStatus.class);

    private HiveFileListingSizeEstimator() {
    }

    /** Caffeine callback: both key and value sizes were computed once during construction. */
    static long estimateEntry(FileListingKey key, FileListingValue value) {
        return add(key.estimatedBytes, value.estimatedBytes);
    }

    static long estimateKey(FileListingKey key) {
        long bytes = KEY_SHALLOW_BYTES;
        bytes = add(bytes, JvmSizeUtils.stringSize(key.dbName));
        bytes = add(bytes, JvmSizeUtils.stringSize(key.tableName));
        bytes = add(bytes, JvmSizeUtils.stringSize(key.location));
        bytes = add(bytes, estimateOwnedStringList(key.partitionValues));
        return bytes;
    }

    static long estimateValue(FileListingValue value) {
        long bytes = VALUE_SHALLOW_BYTES;
        bytes = add(bytes, estimateArrayBackedList(value.files));
        for (HiveFileStatus file : value.files) {
            bytes = add(bytes, FILE_STATUS_SHALLOW_BYTES);
            bytes = add(bytes, JvmSizeUtils.stringSize(file.getPath()));
        }
        return bytes;
    }

    private static long estimateOwnedStringList(java.util.List<String> values) {
        long bytes = estimateArrayBackedList(values);
        for (String value : values) {
            bytes = add(bytes, JvmSizeUtils.stringSize(value));
        }
        return bytes;
    }

    private static long estimateArrayBackedList(java.util.List<?> values) {
        long bytes = JvmSizeUtils.instanceSize(values.getClass());
        return add(bytes, JvmSizeUtils.arrayListSize(values.size()));
    }

    private static long add(long left, long right) {
        return JvmSizeUtils.saturatedAdd(left, right);
    }
}
