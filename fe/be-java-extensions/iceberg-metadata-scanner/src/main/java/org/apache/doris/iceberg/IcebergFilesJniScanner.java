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

import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.util.SerializationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

class IcebergFilesJniScanner extends IcebergMetadataJniScanner {
    private static final String NAME = "files";
    private static final Map<String, String> FILES_SCHEMA = new HashMap<>();

    static {
        FILES_SCHEMA.put("content", "int");
        FILES_SCHEMA.put("file_path", "string");
        FILES_SCHEMA.put("file_format", "string");
        FILES_SCHEMA.put("spec_id", "int");
        FILES_SCHEMA.put("record_count", "bigint");
        FILES_SCHEMA.put("file_size_in_bytes", "bigint");
        FILES_SCHEMA.put("column_sizes", "map<int,bigint>");
        FILES_SCHEMA.put("value_counts", "map<int,bigint>");
        FILES_SCHEMA.put("null_value_counts", "map<int,bigint>");
        FILES_SCHEMA.put("nan_value_counts", "map<int,bigint>");
        FILES_SCHEMA.put("lower_bounds", "map<int,string>");
        FILES_SCHEMA.put("upper_bounds", "map<int,string>");
        FILES_SCHEMA.put("key_metadata", "string");
        FILES_SCHEMA.put("split_offsets", "array<bigint>");
        FILES_SCHEMA.put("equality_ids", "array<int>");
        FILES_SCHEMA.put("sort_order_id", "int");
        FILES_SCHEMA.put("readable_metrics", "string"); // This can be extended to include actual metrics if needed
    }

    // A serializable bean that contains a bare minimum to read a manifest
    private final ManifestFile manifestBean;

    public IcebergFilesJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
        // TODO: use IcebergMetaSplit to pass the manifest file information
        manifestBean = SerializationUtil.deserializeFromBase64(params.get("serialized_split"));
    }

    @Override
    protected void initReader() throws IOException {
        reader = ManifestFiles.read(manifestBean, table.io(), table.specs()).iterator();
    }

    @Override
    protected Map<String, String> getMetadataSchema() {
        return FILES_SCHEMA;
    }

    protected Object getColumnValue(String columnName, Object row) {
        DataFile dataFile = (DataFile) row;
        switch (columnName) {
            case "content":
                return dataFile.content().ordinal();
            case "file_path":
                return dataFile.path().toString();
            case "file_format":
                return dataFile.format().name();
            case "spec_id":
                return dataFile.specId();
            case "record_count":
                return dataFile.recordCount();
            case "file_size_in_bytes":
                return dataFile.fileSizeInBytes();
            case "column_sizes":
                return dataFile.columnSizes();
            case "value_counts":
                return dataFile.valueCounts();
            case "null_value_counts":
                return dataFile.nullValueCounts();
            case "nan_value_counts":
                return dataFile.nanValueCounts();
            case "lower_bounds":
                return convertKeyTypeToString(dataFile.lowerBounds());
            case "upper_bounds":
                return convertKeyTypeToString(dataFile.upperBounds());
            case "key_metadata":
                // The key metadata is stored as a ByteBuffer, so we convert it to a string.
                // TODO: how to parse this
                return dataFile.keyMetadata() != null ? dataFile.keyMetadata().toString() : null;
            case "split_offsets":
                return dataFile.splitOffsets();
            case "equality_ids":
                return dataFile.equalityFieldIds();
            case "sort_order_id":
                return dataFile.sortOrderId();
            case "readable_metrics":
                // TODO: support this
                // The readable metrics are not directly available in DataFile, so we return
                // null.
                // This can be extended to include actual metrics if needed.
                return null;
            default:
                throw new IllegalArgumentException(
                        "Unrecognized column name " + columnName + " in Iceberg " + NAME + " metadata table");
        }
    }

    private static Map<Integer, String> convertKeyTypeToString(Map<Integer, ByteBuffer> map) {
        if (map == null) {
            return null;
        }
        return map.entrySet().stream()
                .collect(HashMap::new,
                        (m, e) -> m.put(e.getKey(), e.getValue() != null ? e.getValue().toString() : null),
                        HashMap::putAll);
    }
}
