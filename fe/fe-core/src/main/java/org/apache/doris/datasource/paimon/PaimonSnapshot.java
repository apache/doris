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

package org.apache.doris.datasource.paimon;

import org.apache.paimon.Snapshot;
import org.apache.paimon.table.Table;

public class PaimonSnapshot {
    public static long INVALID_SNAPSHOT_ID = -1;
    private final long snapshotId;
    private final long schemaId;
    private final long timestamp;
    private final Snapshot.CommitKind commitKind;
    private final Long deltaRecordCount;
    private final Table table;

    public PaimonSnapshot(long snapshotId, long schemaId, long timestamp,
            Snapshot.CommitKind commitKind, Long deltaRecordCount, Table table) {
        this.snapshotId = snapshotId;
        this.schemaId = schemaId;
        this.table = table;
        this.timestamp = timestamp;
        this.commitKind = commitKind;
        this.deltaRecordCount = deltaRecordCount;
    }

    public PaimonSnapshot(long snapshotId, Snapshot.CommitKind commitKind, Long deltaRecordCount) {
        this(snapshotId, 0, 0, commitKind, deltaRecordCount, null);
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public long getSchemaId() {
        return schemaId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Snapshot.CommitKind getCommitKind() {
        return commitKind;
    }

    public Long getDeltaRecordCount() {
        return deltaRecordCount;
    }

    public Table getTable() {
        return table;
    }
}
