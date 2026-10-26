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

package org.apache.doris.transaction;

import org.apache.doris.thrift.TTxnWriteSchemaSnapshot;

import com.google.common.base.Preconditions;

import java.util.TreeMap;

/** FE-to-FE transport for the durable part of a prepared write schema. */
public final class TxnWriteSchemaSnapshotCodec {
    private TxnWriteSchemaSnapshotCodec() {
    }

    public static TTxnWriteSchemaSnapshot toThrift(TxnWriteSchemaSnapshot snapshot) {
        return new TTxnWriteSchemaSnapshot().setTableId(snapshot.getTableId())
                .setIndexSchemaVersions(new TreeMap<>(snapshot.getIndexSchemaVersions()))
                .setChangelogMappings(ChangelogWriteMapping.toThrift(snapshot.getChangelogMappings()))
                .setPartialUpdate(snapshot.isPartialUpdate());
    }

    public static TxnWriteSchemaSnapshot fromThrift(TTxnWriteSchemaSnapshot snapshot) {
        Preconditions.checkArgument(snapshot != null && snapshot.isSetTableId()
                        && snapshot.isSetIndexSchemaVersions() && snapshot.isSetChangelogMappings()
                        && snapshot.isSetPartialUpdate(), "Incomplete remote write schema");
        return new TxnWriteSchemaSnapshot(snapshot.getTableId(), snapshot.getIndexSchemaVersions(),
                ChangelogWriteMapping.fromThrift(snapshot.getChangelogMappings()), snapshot.isPartialUpdate());
    }

    public static TxnWriteSchemaSnapshot fromThrift(TTxnWriteSchemaSnapshot snapshot, long tableId, long baseIndexId) {
        TxnWriteSchemaSnapshot decoded = fromThrift(snapshot);
        Preconditions.checkArgument(decoded.getTableId() == tableId && decoded.getIndexIds().contains(baseIndexId),
                "Remote write schema does not include the target table's base index");
        return decoded;
    }
}
