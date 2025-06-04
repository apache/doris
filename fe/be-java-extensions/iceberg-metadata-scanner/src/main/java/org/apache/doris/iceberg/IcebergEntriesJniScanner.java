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

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableScan;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

class IcebergEntriesJniScanner extends IcebergMetadataJniScanner {
    private static final String NAME = "entries";
    private static final Map<String, String> ENTRIES_SCHEMA = new HashMap<>();

    static {
        ENTRIES_SCHEMA.put("status", "int");
        ENTRIES_SCHEMA.put("snapshot_id", "bigint");
        ENTRIES_SCHEMA.put("sequence_number", "bigint");
        ENTRIES_SCHEMA.put("file_sequence_number", "bigint");
        ENTRIES_SCHEMA.put("data_file", "string"); // Placeholder for data file, can be extended later
        ENTRIES_SCHEMA.put("readable_metrics", "string"); // Placeholder for readable metrics, can be extended later
    }

    public IcebergEntriesJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
    }

    @Override
    protected void initReader() throws IOException {
        TableScan tableScan = MetadataTableUtils
                .createMetadataTableInstance(table, MetadataTableType.ENTRIES).newScan();
        Iterator<FileScanTask> fileScanTasks = tableScan.planFiles().iterator();
        if (!fileScanTasks.hasNext()) {
            throw new IOException("No entries found for table: " + table.name());
        }
        reader = fileScanTasks.next().asDataTask().rows().iterator();
    }

    @Override
    protected Map<String, String> getMetadataSchema() {
        return ENTRIES_SCHEMA;
    }

    @Override
    protected Object getColumnValue(String columnName, Object row) {
        StructLike entry = (StructLike) row;
        switch (columnName) {
            case "status":
                return entry.get(0, Integer.class);
            case "snapshot_id":
                return entry.get(1, Long.class);
            case "sequence_number":
                return entry.get(2, Long.class);
            case "file_sequence_number":
                return entry.get(3, Long.class);
            case "data_file":
                // TODO: implement data_file
                return null;
            case "readable_metrics":
                // TODO: support readable_metrics
                return null;
            default:
                throw new IllegalArgumentException(
                        "Unrecognized column name " + columnName + " in Iceberg " + NAME + " metadata table");
        }
    }
}
