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

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.util.SerializationUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class IcebergPositionDeletesJniScanner extends IcebergMetadataJniScanner {
    private static final String NAME = "position_deletes";
    private static final Map<String, String> POSITION_DELETES_SCHEMA = new HashMap<>();

    static {
        POSITION_DELETES_SCHEMA.put("file_path", "string");
        POSITION_DELETES_SCHEMA.put("pos", "bigint");
        POSITION_DELETES_SCHEMA.put("row", "bigint");
        POSITION_DELETES_SCHEMA.put("partition", "string"); // TODO: fix this
        POSITION_DELETES_SCHEMA.put("spec_id", "int");
        POSITION_DELETES_SCHEMA.put("delete_file_path", "string"); // TODO: fix this
    }

    // A serializable bean that contains a bare minimum to read a manifest
    private final ManifestFile manifestBean;

    public IcebergPositionDeletesJniScanner(int batchSize, Map<String, String> params) {
        super(batchSize, params);
        manifestBean = SerializationUtil.deserializeFromBase64(params.get("serialized_split"));
    }

    @Override
    protected void initReader() throws IOException {
        reader = ManifestFiles.readDeleteManifest(manifestBean, table.io(), table.specs()).iterator();
    }

    @Override
    protected Map<String, String> getMetadataSchema() {
        return POSITION_DELETES_SCHEMA;
    }

    @Override
    protected Object getColumnValue(String columnName, Object row) {
        DeleteFile file = (DeleteFile) row;
        switch (columnName) {
            case "file_path":
                return file.path().toString();
            case "pos":
                return file.pos();
            case "row":
                return file.recordCount();
            case "partition":
                return file.partition();
            case "spec_id":
                return file.specId();
            case "delete_file_path":
                // TODO: figure out file_path and delete_file_path
                return file.path().toString();
            default:
                throw new IllegalArgumentException(
                        "Unrecognized column name " + columnName + " in Iceberg " + NAME + " metadata table");
        }
    }
}
