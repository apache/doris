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

package org.apache.doris.connector.delta;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Metadata for one data file produced by a connector file sink. */
public final class DeltaFileCommitInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String filePath;
    private final long rowCount;
    private final long fileSize;
    private final long modificationTime;
    private final Map<String, String> partitionValues;
    private final Set<String> nullPartitionColumns;

    public DeltaFileCommitInfo(String filePath, long rowCount, long fileSize,
            long modificationTime, Map<String, String> partitionValues) {
        this(filePath, rowCount, fileSize, modificationTime, partitionValues, Set.of());
    }

    public DeltaFileCommitInfo(String filePath, long rowCount, long fileSize,
            long modificationTime, Map<String, String> partitionValues,
            Set<String> nullPartitionColumns) {
        this.filePath = Objects.requireNonNull(filePath, "filePath");
        if (rowCount < 0) {
            throw new IllegalArgumentException("rowCount must be non-negative");
        }
        if (fileSize <= 0) {
            throw new IllegalArgumentException("fileSize must be positive");
        }
        if (modificationTime < 0) {
            throw new IllegalArgumentException("modificationTime must be non-negative");
        }
        this.rowCount = rowCount;
        this.fileSize = fileSize;
        this.modificationTime = modificationTime;
        this.partitionValues = Collections.unmodifiableMap(
                new LinkedHashMap<>(Objects.requireNonNull(partitionValues, "partitionValues")));
        this.nullPartitionColumns = Set.copyOf(
                Objects.requireNonNull(nullPartitionColumns, "nullPartitionColumns"));
        if (!this.partitionValues.keySet().containsAll(this.nullPartitionColumns)) {
            throw new IllegalArgumentException(
                    "nullPartitionColumns must be a subset of partitionValues keys");
        }
    }

    public String getFilePath() {
        return filePath;
    }

    public long getRowCount() {
        return rowCount;
    }

    public long getFileSize() {
        return fileSize;
    }

    public long getModificationTime() {
        return modificationTime;
    }

    public Map<String, String> getPartitionValues() {
        return partitionValues;
    }

    public Set<String> getNullPartitionColumns() {
        return nullPartitionColumns;
    }
}
