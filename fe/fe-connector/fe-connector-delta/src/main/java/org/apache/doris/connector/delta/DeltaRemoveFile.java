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

import io.delta.kernel.data.Row;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.data.GenericRow;
import io.delta.kernel.internal.util.VectorUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Materialized metadata needed to tombstone one active Delta add-file. */
final class DeltaRemoveFile {
    private final String path;
    private final long size;
    private final Map<String, String> partitionValues;
    private final DeletionVectorDescriptor deletionVector;
    private final Optional<Long> baseRowId;
    private final Optional<Long> defaultRowCommitVersion;
    private final Optional<Long> recordCount;

    DeltaRemoveFile(String path, long size, Map<String, String> partitionValues,
            DeletionVectorDescriptor deletionVector, Optional<Long> baseRowId,
            Optional<Long> defaultRowCommitVersion, Optional<Long> recordCount) {
        this.path = path;
        this.size = size;
        this.partitionValues = Collections.unmodifiableMap(
                new LinkedHashMap<>(partitionValues));
        this.deletionVector = deletionVector;
        this.baseRowId = baseRowId;
        this.defaultRowCommitVersion = defaultRowCommitVersion;
        this.recordCount = recordCount;
    }

    String getPath() {
        return path;
    }

    OptionalLong getLiveRowCount() {
        if (!recordCount.isPresent()) {
            return OptionalLong.empty();
        }
        long deletedRows = deletionVector == null ? 0 : deletionVector.getCardinality();
        long liveRows = recordCount.get() - deletedRows;
        if (liveRows < 0) {
            throw new IllegalArgumentException(
                    "Delta deletion vector cardinality exceeds file record count: " + path);
        }
        return OptionalLong.of(liveRows);
    }

    Row toSingleAction(long deletionTimestamp) {
        Map<Integer, Object> values = new HashMap<>();
        values.put(RemoveFile.FULL_SCHEMA.indexOf("path"), path);
        values.put(RemoveFile.FULL_SCHEMA.indexOf("deletionTimestamp"), deletionTimestamp);
        values.put(RemoveFile.FULL_SCHEMA.indexOf("dataChange"), true);
        values.put(RemoveFile.FULL_SCHEMA.indexOf("extendedFileMetadata"), true);
        values.put(RemoveFile.FULL_SCHEMA.indexOf("partitionValues"),
                VectorUtils.stringStringMapValue(partitionValues));
        values.put(RemoveFile.FULL_SCHEMA.indexOf("size"), size);
        if (deletionVector != null) {
            values.put(RemoveFile.FULL_SCHEMA.indexOf("deletionVector"),
                    deletionVector.toRow());
        }
        baseRowId.ifPresent(value -> values.put(
                RemoveFile.FULL_SCHEMA.indexOf("baseRowId"), value));
        defaultRowCommitVersion.ifPresent(value -> values.put(
                RemoveFile.FULL_SCHEMA.indexOf("defaultRowCommitVersion"), value));
        return SingleAction.createRemoveFileSingleAction(
                new GenericRow(RemoveFile.FULL_SCHEMA, values));
    }
}
