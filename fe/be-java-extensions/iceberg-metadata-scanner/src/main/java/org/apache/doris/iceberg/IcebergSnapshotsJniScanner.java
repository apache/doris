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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class IcebergSnapshotsJniScanner extends IcebergMetadataJniScanner {

    private CloseableIterator<StructLike> reader;
    private Map<String, Integer> columnNameToPosition = new HashMap<>();

    public IcebergSnapshotsJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
    }

    @Override
    protected void loadTable(Table table) throws IOException {
        TableScan tableScan = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.SNAPSHOTS)
                .newScan();
        this.columnNameToPosition = Streams.mapWithIndex(tableScan.schema().columns().stream(),
                (column, position) -> Maps.immutableEntry(column.name(), Long.valueOf(position).intValue()))
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        for (String requiredField : requiredFields) {
            if (!columnNameToPosition.containsKey(requiredField)) {
                throw new IOException("Invalid required field: " + requiredField);
            }
        }
        this.reader = tableScan.planFiles().iterator().next().asDataTask().rows().iterator();
    }

    @Override
    protected int getNext() throws IOException {
        if (reader == null) {
            return 0;
        }
        int rows = 0;
        while (reader.hasNext() && rows < getBatchSize()) {
            StructLike dataRow = reader.next();
            for (int i = 0; i < requiredFields.length; i++) {
                String columnName = requiredFields[i];
                Object value = getValue(columnName, dataRow);
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
        if (reader != null) {
            reader.close();
        }
    }

    @Override
    protected HashMap<String, String> getMetadataSchema() {
        HashMap<String, String> metadataSchema = new HashMap<>();
        metadataSchema.put("committed_at", "long");
        metadataSchema.put("snapshot_id", "long");
        metadataSchema.put("parent_id", "long");
        metadataSchema.put("operation", "string");
        metadataSchema.put("manifest_list", "string");
        metadataSchema.put("summary", "string");
        return metadataSchema;
    }

    private Object getValue(String columnName, StructLike dataRow) {
        switch (columnName) {
            case "committed_at":
                return dataRow.get(columnNameToPosition.get(columnName), Long.class) / 1000;
            case "snapshot_id":
                return dataRow.get(columnNameToPosition.get(columnName), Long.class);
            case "parent_id":
                return dataRow.get(columnNameToPosition.get(columnName), Long.class);
            case "operation":
                return dataRow.get(columnNameToPosition.get(columnName), String.class);
            case "manifest_list":
                return dataRow.get(columnNameToPosition.get(columnName), String.class);
            case "summary":
                return dataRow.get(columnNameToPosition.get(columnName), String.class);
            default:
                throw new IllegalArgumentException("Unrecognized column name " + columnName);
        }
    }
}
