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

import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.util.SnapshotUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

class IcebergHistoryJniScanner extends IcebergMetadataJniScanner {
    private static final String NAME = "history";
    private static final Map<String, String> HISTORY_SCHEMA = new HashMap<>();
    static {
        HISTORY_SCHEMA.put("made_current_at", "datetime");
        HISTORY_SCHEMA.put("snapshot_id", "bigint");
        HISTORY_SCHEMA.put("parent_id", "bigint");
        HISTORY_SCHEMA.put("is_current_ancestor", "boolean");

    }
    Set<Long> ancestorIds;

    public IcebergHistoryJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
    }

    @Override
    protected void initReader() throws IOException {
        ancestorIds = new HashSet<>(SnapshotUtil.currentAncestorIds(table));
        reader = table.history().iterator();
    }

    @Override
    protected Map<String, String> getMetadataSchema() {
        return HISTORY_SCHEMA;
    }

    @Override
    protected Object getColumnValue(String columnName, Object row) {
        HistoryEntry entry = (HistoryEntry) row;
        switch (columnName) {
            case "made_current_at":
                return entry.timestampMillis();
            case "snapshot_id":
                return entry.snapshotId();
            case "parent_id":
                return table.snapshot(entry.snapshotId()).parentId();
            case "is_current_ancestor":
                return ancestorIds.contains(entry.snapshotId());
            default:
                throw new IllegalArgumentException(
                        "Unrecognized column name " + columnName + " in Iceberg " + NAME + " metadata table");
        }
    }
}
