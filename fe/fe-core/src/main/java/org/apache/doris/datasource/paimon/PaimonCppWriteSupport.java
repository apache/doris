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

package org.apache.doris.datasource.paimon;

import org.apache.doris.thrift.TPaimonCppColumn;
import org.apache.doris.thrift.TPaimonCppWriteDescriptor;
import org.apache.doris.thrift.TPaimonWriteMode;

import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Pure, pre-writer capability decision. Never retry a failed native writer through JNI. */
public final class PaimonCppWriteSupport {
    private static final Set<String> OPTIONS = new HashSet<>(Arrays.asList(
            "bucket", "file.format", "manifest.format", "write-only", "path",
            "file.compression", "target-file-size", "write-buffer-size",
            "page-size", "commit.force-create-snapshot"));

    private PaimonCppWriteSupport() {
    }

    public static String unsupportedReason(FileStoreTable table, List<String> columns,
            TPaimonWriteMode mode) {
        if (mode != TPaimonWriteMode.APPEND) {
            return "v1 only supports APPEND";
        }
        if (!table.schema().primaryKeys().isEmpty() || !table.schema().partitionKeys().isEmpty()) {
            return "v1 requires an unpartitioned append table";
        }
        Map<String, String> options = table.options();
        if (!"-1".equals(options.getOrDefault("bucket", "-1"))) {
            return "v1 requires unaware bucket (-1)";
        }
        if (!"parquet".equalsIgnoreCase(options.getOrDefault("file.format", "orc"))) {
            return "v1 requires Parquet data files";
        }
        if (!"avro".equalsIgnoreCase(options.getOrDefault("manifest.format", "avro"))) {
            return "v1 requires Avro manifests";
        }
        // Do not silently change the table's compaction policy to qualify for native.
        if (!"true".equalsIgnoreCase(options.getOrDefault("write-only", "false"))) {
            return "v1 requires an explicitly configured write-only table";
        }
        URI location = table.location().toUri();
        if ((location.getScheme() != null && !"file".equalsIgnoreCase(location.getScheme()))
                || location.getAuthority() != null || location.getQuery() != null
                || location.getFragment() != null || location.getPath() == null
                || !location.getPath().startsWith("/")) {
            return "v1 only supports local/shared POSIX paths without URI authority";
        }
        List<DataField> fields = table.schema().fields();
        if (columns.size() != fields.size()) {
            return "v1 requires all columns in table order";
        }
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            if (!columns.get(i).equals(field.name()) || !supportedType(field.type().getTypeRoot().name())) {
                return "v1 requires ordered primitive columns";
            }
        }
        for (String option : options.keySet()) {
            if (!OPTIONS.contains(option)) {
                return "unvalidated table option: " + option;
            }
        }
        return null;
    }

    private static boolean supportedType(String type) {
        switch (type) {
            case "BOOLEAN":
            case "TINYINT":
            case "SMALLINT":
            case "INTEGER":
            case "BIGINT":
            case "FLOAT":
            case "DOUBLE":
            case "VARCHAR":
            case "VARBINARY":
                return true;
            default:
                return false;
        }
    }

    public static TPaimonCppWriteDescriptor describe(FileStoreTable table) {
        TPaimonCppWriteDescriptor descriptor = new TPaimonCppWriteDescriptor();
        descriptor.setVersion(1);
        descriptor.setRootPath(table.location().toString());
        descriptor.setSchemaId(table.schema().id());
        List<TPaimonCppColumn> columns = new ArrayList<>();
        for (DataField field : table.schema().fields()) {
            TPaimonCppColumn column = new TPaimonCppColumn();
            column.setName(field.name());
            column.setType(field.type().getTypeRoot().name());
            column.setNullable(field.type().isNullable());
            columns.add(column);
        }
        descriptor.setColumns(columns);
        Map<String, String> options = new HashMap<>(table.options());
        options.put("file.format", "parquet");
        options.put("manifest.format", "avro");
        options.put("write-only", "true");
        descriptor.setOptions(options);
        return descriptor;
    }
}
