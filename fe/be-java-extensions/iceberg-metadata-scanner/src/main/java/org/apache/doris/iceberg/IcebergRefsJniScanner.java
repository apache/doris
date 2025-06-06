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

import org.apache.iceberg.SnapshotRef;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

class IcebergRefsJniScanner extends IcebergMetadataJniScanner {
    private static final String NAME = "refs";
    private static final Map<String, String> REF_SCHEMA = new HashMap<>();

    static {
        REF_SCHEMA.put("name", "string");
        REF_SCHEMA.put("type", "string");
        REF_SCHEMA.put("snapshot_id", "bigint");
        REF_SCHEMA.put("max_reference_age_in_ms", "bigint");
        REF_SCHEMA.put("min_snapshots_to_keep", "int");
        REF_SCHEMA.put("max_snapshot_age_in_ms", "bigint");
    }

    public IcebergRefsJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
    }

    @Override
    protected void initReader() throws IOException {
        reader = table.refs().entrySet().iterator();
    }

    @Override
    protected Map<String, String> getMetadataSchema() {
        return REF_SCHEMA;
    }

    @Override
    protected Object getColumnValue(String columnName, Object row) {
        Entry<String, SnapshotRef> entry = (Entry<String, SnapshotRef>) row;
        switch (columnName) {
            case "name":
                return entry.getKey();
            case "type":
                return entry.getValue().isBranch() ? "BRANCH" : "TAG";
            case "snapshot_id":
                return entry.getValue().snapshotId();
            case "max_reference_age_in_ms":
                return entry.getValue().maxRefAgeMs();
            case "min_snapshots_to_keep":
                return entry.getValue().minSnapshotsToKeep();
            case "max_snapshot_age_in_ms":
                return entry.getValue().maxSnapshotAgeMs();
            default:
                throw new IllegalArgumentException(
                        "Unrecognized column name " + columnName + " in Iceberg " + NAME + " metadata table");
        }
    }
}
