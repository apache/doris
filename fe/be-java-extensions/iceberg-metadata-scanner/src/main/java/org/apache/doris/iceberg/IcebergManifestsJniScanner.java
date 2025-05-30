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

import org.apache.iceberg.ManifestFile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class IcebergManifestsJniScanner extends IcebergMetadataJniScanner {
    private static final String NAME = "manifests";
    private static final Map<String, String> MANIFESTS_SCHEMA = new HashMap<>();

    static {
        MANIFESTS_SCHEMA.put("content", "string");
        MANIFESTS_SCHEMA.put("path", "string");
        MANIFESTS_SCHEMA.put("length", "bigint");
        MANIFESTS_SCHEMA.put("partition_spec_id", "int");
        MANIFESTS_SCHEMA.put("added_snapshot_id", "bigint");
        MANIFESTS_SCHEMA.put("added_data_files_count", "bigint");
        MANIFESTS_SCHEMA.put("existing_data_files_count", "bigint");
        MANIFESTS_SCHEMA.put("deleted_data_files_count", "bigint");
        MANIFESTS_SCHEMA.put("deleted_rows_count", "bigint");
        MANIFESTS_SCHEMA.put("partitions", "string"); // TODO: support partitions
    }

    public IcebergManifestsJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
    }

    @Override
    protected void initReader() throws IOException {
        reader = table.currentSnapshot().allManifests(table.io()).iterator();
    }

    @Override
    protected Map<String, String> getMetadataSchema() {
        return MANIFESTS_SCHEMA;
    }

    @Override
    protected Object getColumnValue(String columnName, Object row) {
        ManifestFile manifestFile = (ManifestFile) row;
        switch (columnName) {
            case "content":
                return manifestFile.content().name();
            case "path":
                return manifestFile.path();
            case "length":
                return manifestFile.length();
            case "partition_spec_id":
                return manifestFile.partitionSpecId();
            case "added_snapshot_id":
                return manifestFile.snapshotId();
            case "added_data_files_count":
                return manifestFile.addedFilesCount();
            case "existing_data_files_count":
                return manifestFile.existingFilesCount();
            case "deleted_data_files_count":
                return manifestFile.deletedFilesCount();
            case "deleted_rows_count":
                return manifestFile.deletedRowsCount();
            case "partitions":
                // TODO: support partitions
                return null;
            default:
                throw new IllegalArgumentException(
                        "Unrecognized column name " + columnName + " in Iceberg " + NAME + " metadata table");
        }
    }
}
