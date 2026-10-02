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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** A Delta add-file entry with the partition values needed by a file scan. */
public final class DeltaScanFile {
    private final String path;
    private final long size;
    private final long modificationTime;
    private final Map<String, String> partitionValues;
    private final Set<String> nullPartitionColumns;
    private final DeltaDeletionVector deletionVector;
    private final String tablePath;

    public DeltaScanFile(String path, long size, long modificationTime,
            Map<String, String> partitionValues) {
        this(path, size, modificationTime, partitionValues, null, null);
    }

    public DeltaScanFile(String path, long size, long modificationTime,
            Map<String, String> partitionValues, DeltaDeletionVector deletionVector,
            String tablePath) {
        this.path = Objects.requireNonNull(path, "path");
        this.size = size;
        this.modificationTime = modificationTime;
        this.partitionValues = Collections.unmodifiableMap(
                new LinkedHashMap<>(partitionValues));
        this.nullPartitionColumns = Collections.unmodifiableSet(partitionValues.entrySet().stream()
                .filter(entry -> entry.getValue() == null)
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet()));
        this.deletionVector = deletionVector;
        this.tablePath = tablePath;
    }

    public String getPath() {
        return path;
    }

    public long getSize() {
        return size;
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

    public DeltaDeletionVector getDeletionVector() {
        return deletionVector;
    }

    public String getTablePath() {
        return tablePath;
    }
}
