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

import org.apache.doris.common.jni.vec.ColumnValue;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class IcebergSnapshotsJniScanner extends IcebergMetadataJniScanner {

    private static final Map<String, String> SNAPSHOTS_SCHEMA = new HashMap<>();
    static {
        SNAPSHOTS_SCHEMA.put("committed_at", "datetime");
        SNAPSHOTS_SCHEMA.put("snapshot_id", "long");
        SNAPSHOTS_SCHEMA.put("parent_id", "long");
        SNAPSHOTS_SCHEMA.put("operation", "string");
        SNAPSHOTS_SCHEMA.put("manifest_list", "string");
        SNAPSHOTS_SCHEMA.put("summary", "string");
    }

    private Iterator<Snapshot> reader;

    public IcebergSnapshotsJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
    }

    @Override
    protected void loadTable(Table table) throws IOException {
        reader = table.snapshots().iterator();
    }

    @Override
    protected int getNext() throws IOException {
        if (reader == null) {
            return 0;
        }
        int rows = 0;
        while (reader.hasNext() && rows < getBatchSize()) {
            Snapshot snapshot = reader.next();
            for (int i = 0; i < requiredFields.length; i++) {
                String columnName = requiredFields[i];
                Object value = getValue(columnName, snapshot);
                if (value == null) {
                    appendData(i, null);
                } else {
                    ColumnValue columnValue = new IcebergMetadataColumnValue(value, timezone);
                    appendData(i, columnValue);
                }
            }
            rows++;
        }
        return rows;
    }

    @Override
    public void close() throws IOException {
        // TODO: move this to base class
        if (reader != null) {
            reader = null; // Clear the iterator to release resources
        }
    }

    @Override
    protected Map<String, String> getMetadataSchema() {
        return SNAPSHOTS_SCHEMA;
    }

    private Object getValue(String columnName, Snapshot snapshot) {
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
                        "Unrecognized column name " + columnName + " in Iceberg snapshot metadata table");
        }
    }
}
