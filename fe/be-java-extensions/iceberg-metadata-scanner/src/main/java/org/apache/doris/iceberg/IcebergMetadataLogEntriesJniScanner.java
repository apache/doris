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

class IcebergMetadataLogEntriesJniScanner extends IcebergMetadataJniScanner {
    private static final String NAME = "metadata_log_entries";
    private static final Map<String, String> METADATA_LOG_ENTRIES_SCHEMA = new HashMap<>();

    static {
        METADATA_LOG_ENTRIES_SCHEMA.put("timestamp", "datetime");
        METADATA_LOG_ENTRIES_SCHEMA.put("file", "string");
        METADATA_LOG_ENTRIES_SCHEMA.put("latest_snapshot_id", "bigint");
        METADATA_LOG_ENTRIES_SCHEMA.put("latest_schema_id", "int");
        METADATA_LOG_ENTRIES_SCHEMA.put("latest_sequence_number", "bigint");
    }

    public IcebergMetadataLogEntriesJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
    }

    @Override
    protected void initReader() throws IOException {
        TableScan tableScan = MetadataTableUtils
                .createMetadataTableInstance(table, MetadataTableType.METADATA_LOG_ENTRIES).newScan();
        Iterator<FileScanTask> fileScanTasks = tableScan.planFiles().iterator();
        if (!fileScanTasks.hasNext()) {
            throw new IOException("No metadata log entries found for table: " + table.name());
        }
        reader = fileScanTasks.next().asDataTask().rows().iterator();
    }

    @Override
    protected Map<String, String> getMetadataSchema() {
        return METADATA_LOG_ENTRIES_SCHEMA;
    }

    @Override
    protected Object getColumnValue(String columnName, Object row) {
        StructLike entry = (StructLike) row;
        switch (columnName) {
            case "timestamp":
                return entry.get(0, Long.class) / 1000;
            case "file":
                return entry.get(1, String.class);
            case "latest_snapshot_id":
                return entry.get(2, Long.class);
            case "latest_schema_id":
                return entry.get(3, Integer.class);
            case "latest_sequence_number":
                return entry.get(4, Long.class);
            default:
                throw new IllegalArgumentException(
                        "Unrecognized column name " + columnName + " in Iceberg " + NAME + " metadata table");
        }
    }
}
