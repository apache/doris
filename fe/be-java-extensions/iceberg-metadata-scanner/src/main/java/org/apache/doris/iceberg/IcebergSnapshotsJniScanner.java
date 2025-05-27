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

package org.apache.doris.iceberg;

import org.apache.iceberg.Snapshot;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class IcebergSnapshotsJniScanner extends IcebergMetadataJniScanner {
    private static final String NAME = "snapshots";
    private static final Map<String, String> SNAPSHOTS_SCHEMA = new HashMap<>();

    static {
        SNAPSHOTS_SCHEMA.put("committed_at", "datetime");
        SNAPSHOTS_SCHEMA.put("snapshot_id", "bigint");
        SNAPSHOTS_SCHEMA.put("parent_id", "bigint");
        SNAPSHOTS_SCHEMA.put("operation", "string");
        SNAPSHOTS_SCHEMA.put("manifest_list", "string");
        SNAPSHOTS_SCHEMA.put("summary", "map<string, string>");
    }

    public IcebergSnapshotsJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
    }

    @Override
    protected void initReader() throws IOException {
        reader = table.snapshots().iterator();
    }

    @Override
    protected Map<String, String> getMetadataSchema() {
        return SNAPSHOTS_SCHEMA;
    }

    @Override
    protected Object getColumnValue(String columnName, Object row) {
        Snapshot snapshot = (Snapshot) row;
        switch (columnName) {
            case "committed_at":
                return snapshot.timestampMillis();
            case "snapshot_id":
                return snapshot.snapshotId();
            case "parent_id":
                return snapshot.parentId();
            case "operation":
                return snapshot.operation();
            case "manifest_list":
                return snapshot.manifestListLocation();
            case "summary":
                return snapshot.summary();
            default:
                throw new IllegalArgumentException(
                        "Unrecognized column name " + columnName + " in Iceberg " + NAME + " metadata table");
        }
    }
}
