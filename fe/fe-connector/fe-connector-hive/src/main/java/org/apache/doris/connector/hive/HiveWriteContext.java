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

import org.apache.doris.connector.api.handle.WriteOperation;
import org.apache.doris.thrift.TFileType;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Immutable op context for a single hive write, threaded into
 * {@link HiveConnectorTransaction#beginWrite}. The connector-internal equivalent of the fe-core
 * {@code HiveInsertCommandContext} (which the connector cannot import): it carries the write operation,
 * the overwrite mode, the static partition spec (for {@code INSERT OVERWRITE ... PARTITION}), the query
 * id (replaces {@code ConnectContext}, per design D4), and the BE-facing file type / staging write path
 * (which decides an in-place object-store write from a staged HDFS/local write).
 *
 * <p>Peer of {@code IcebergWriteContext}; drops iceberg's {@code branchName}/{@code readSnapshotId} and
 * adds hive's {@code queryId}/{@code fileType}/{@code writePath}. INC-4's {@code HiveWritePlanProvider}
 * constructs it in {@code buildWriteContext}; INC-3 only consumes it via {@link
 * HiveConnectorTransaction#beginWrite}.</p>
 */
final class HiveWriteContext {

    private final WriteOperation writeOperation;
    private final boolean overwrite;
    private final Map<String, String> staticPartitionValues;
    private final String queryId;
    private final TFileType fileType;
    private final String writePath;

    HiveWriteContext(WriteOperation writeOperation, boolean overwrite,
            Map<String, String> staticPartitionValues, String queryId,
            TFileType fileType, String writePath) {
        this.writeOperation = writeOperation;
        this.overwrite = overwrite;
        this.staticPartitionValues = staticPartitionValues == null
                ? Collections.emptyMap() : new HashMap<>(staticPartitionValues);
        this.queryId = queryId;
        this.fileType = fileType;
        this.writePath = writePath;
    }

    WriteOperation getWriteOperation() {
        return writeOperation;
    }

    boolean isOverwrite() {
        return overwrite;
    }

    Map<String, String> getStaticPartitionValues() {
        return staticPartitionValues;
    }

    /** An {@code INSERT OVERWRITE ... PARTITION(col=val, ...)} (a non-empty static partition spec). */
    boolean isStaticPartitionOverwrite() {
        return overwrite && !staticPartitionValues.isEmpty();
    }

    String getQueryId() {
        return queryId;
    }

    TFileType getFileType() {
        return fileType;
    }

    String getWritePath() {
        return writePath;
    }
}
