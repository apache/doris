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

import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPaimonStorageDescriptor;
import org.apache.doris.thrift.TPaimonWriteMode;

import com.google.common.collect.ImmutableSet;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Pure, pre-writer capability decision. Never retry a failed native writer through JNI. */
public final class PaimonCppWriteSupport {
    private static final Set<String> OPTIONS = ImmutableSet.of(
            "bucket", "file.format", "manifest.format", "write-only", "path", "owner",
            "file.compression", "target-file-size", "write-buffer-size",
            "page-size", "commit.force-create-snapshot");
    private static final Set<String> TYPES = ImmutableSet.of(
            "BOOLEAN", "TINYINT", "SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "VARCHAR", "VARBINARY");

    private PaimonCppWriteSupport() {
    }

    @Getter
    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    public static final class Decision {
        private final TPaimonStorageDescriptor storage;
        private final String fallbackReason;

        public boolean isSupported() {
            return storage != null;
        }

        public TPaimonStorageDescriptor getStorage() {
            if (!isSupported()) {
                throw new IllegalStateException("No paimon-cpp storage: " + fallbackReason);
            }
            return storage;
        }
    }

    public static Decision decide(FileStoreTable table, List<String> columns,
            TPaimonWriteMode mode, Map<StorageProperties.Type, StorageProperties> storageProperties) {
        String reason = unsupportedFormatReason(table, columns, mode);
        if (reason != null) {
            return new Decision(null, reason);
        }
        TPaimonStorageDescriptor storage;
        try {
            storage = describeStorage(table, storageProperties);
        } catch (RuntimeException e) {
            // Provider exception text can contain credentials/configuration.
            return new Decision(null,
                    "storage is not supported by the Doris native Paimon filesystem adapter");
        }
        return new Decision(storage, null);
    }

    private static String unsupportedFormatReason(FileStoreTable table, List<String> columns,
            TPaimonWriteMode mode) {
        if (mode != TPaimonWriteMode.APPEND) {
            return "v1 only supports APPEND";
        }
        if (table.fileIO() instanceof RESTTokenFileIO || table.catalogEnvironment().supportsVersionManagement()) {
            return "catalog-managed tokens and snapshots require JNI";
        }
        if (!table.schema().primaryKeys().isEmpty() || !table.schema().partitionKeys().isEmpty()) {
            return "v1 requires an unpartitioned append table";
        }
        Map<String, String> options = table.options();
        if (!"-1".equals(options.getOrDefault("bucket", "-1"))) {
            return "v1 requires unaware bucket (-1)";
        }
        String fileFormat = options.getOrDefault("file.format", "orc");
        if (!"parquet".equalsIgnoreCase(fileFormat) && !"orc".equalsIgnoreCase(fileFormat)
                && !"avro".equalsIgnoreCase(fileFormat)) {
            return "paimon-cpp supports Parquet, ORC and Avro data files";
        }
        if (!"avro".equalsIgnoreCase(options.getOrDefault("manifest.format", "avro"))) {
            return "v1 requires Avro manifests";
        }
        // Do not silently change the table's compaction policy to qualify for native.
        if (!"true".equalsIgnoreCase(options.getOrDefault("write-only", "false"))) {
            return "v1 requires an explicitly configured write-only table";
        }
        List<DataField> fields = table.schema().fields();
        if (columns.size() != fields.size()) {
            return "v1 requires all columns in table order";
        }
        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            if (!columns.get(i).equals(field.name()) || !TYPES.contains(field.type().getTypeRoot().name())) {
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

    private static TPaimonStorageDescriptor describeStorage(FileStoreTable table,
            Map<StorageProperties.Type, StorageProperties> storageProperties) {
        URI location = table.location().toUri();
        String path = location.getPath();
        if (location.getQuery() != null || location.getFragment() != null || path == null
                || !path.startsWith("/") || path.startsWith("//") || path.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Invalid Paimon storage location");
        }
        if (location.getScheme() == null || "file".equalsIgnoreCase(location.getScheme())) {
            if (location.getAuthority() != null) {
                throw new IllegalArgumentException("Local Paimon paths cannot have an authority");
            }
            return new TPaimonStorageDescriptor(TFileType.FILE_LOCAL, path, Collections.emptyMap());
        }
        // S3URI currently interprets '?' and '#' as URI components, not object-key bytes.
        // Reject ambiguous keys rather than writing to a different object.
        if (path.indexOf('?') >= 0 || path.indexOf('#') >= 0) {
            throw new IllegalArgumentException("Ambiguous object storage key");
        }
        LocationPath resolved = LocationPath.of(table.location().toString(), storageProperties);
        if (resolved.getTFileTypeForBE() != TFileType.FILE_S3 || resolved.getStorageProperties() == null) {
            throw new IllegalArgumentException("Native Paimon supports Doris object storage and local files");
        }
        Map<String, String> backend = resolved.getStorageProperties().getBackendConfigProperties();
        if (!backend.containsKey("AWS_ENDPOINT") || !backend.containsKey("AWS_REGION")) {
            throw new IllegalArgumentException(
                    "Storage configuration is not a Doris native object-store configuration");
        }
        return new TPaimonStorageDescriptor(TFileType.FILE_S3, resolved.toStorageLocation().toString(), backend);
    }
}
