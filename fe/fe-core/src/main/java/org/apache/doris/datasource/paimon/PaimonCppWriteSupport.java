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

import org.apache.doris.common.util.JsonUtil;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TPaimonStorageDescriptor;
import org.apache.doris.thrift.TPaimonWriteMode;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.utils.JsonSerdeUtil;

import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Pure, pre-writer capability decision. Never retry a failed native writer through JNI. */
public final class PaimonCppWriteSupport {
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
        String reason = fallbackReason(table, columns, mode);
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

    private static String fallbackReason(FileStoreTable table, List<String> columns,
            TPaimonWriteMode mode) {
        if (table.fileIO() instanceof RESTTokenFileIO) {
            return "REST data tokens require JNI";
        }
        if (mode == TPaimonWriteMode.CHANGELOG) {
            return "changelog writes require JNI";
        }
        if (!table.schema().partitionKeys().isEmpty()) {
            return "native partition routing is not implemented yet";
        }
        if (!table.schema().primaryKeys().isEmpty()) {
            return "primary-key tables require JNI";
        }
        Map<String, String> options = table.options();
        CoreOptions coreOptions = new CoreOptions(options);
        int bucket = coreOptions.bucket();
        if (bucket != -1) {
            return "bucketed tables require JNI";
        }
        if (coreOptions.changelogProducer() != CoreOptions.ChangelogProducer.NONE) {
            return "paimon-cpp does not support changelog producers";
        }
        if (options.containsKey("data-file.external-paths")
                || options.containsKey("global-index.external-path")) {
            return "external file routing requires JNI";
        }
        if (options.containsKey("file.format.per.level")
                || !supportedCppDataFormat(coreOptions.fileFormatString())) {
            return "data file format requires JNI";
        }
        List<DataField> fields = table.schema().fields();
        Set<String> fieldNames = new HashSet<>();
        for (DataField field : fields) {
            fieldNames.add(field.name());
            if (!supportedCppType(field.type())) {
                return "paimon-cpp does not support field '" + field.name()
                        + "' with type " + field.type().asSQLString();
            }
        }
        Set<String> writeColumns = new HashSet<>();
        for (String column : columns) {
            if (!fieldNames.contains(column) || !writeColumns.add(column)) {
                return "native write schema contains an unknown or duplicate column: " + column;
            }
        }
        String configuredShreddingSchema = options.containsKey("variant.shreddingSchema")
                ? options.get("variant.shreddingSchema")
                : options.get("parquet.variant.shreddingSchema");
        boolean hasVariant = false;
        for (DataField field : fields) {
            hasVariant |= containsVariant(field.type());
        }
        if (hasVariant && configuredShreddingSchema != null
                && isValidShreddingSchemaWithoutIds(configuredShreddingSchema)) {
            return "native VARIANT shredding schema requires explicit field IDs";
        }
        return null;
    }

    private static boolean supportedCppDataFormat(String format) {
        return "parquet".equalsIgnoreCase(format) || "orc".equalsIgnoreCase(format)
                || "avro".equalsIgnoreCase(format) || "blob".equalsIgnoreCase(format);
    }

    private static boolean isValidShreddingSchemaWithoutIds(String schema) {
        try {
            DataType type = JsonSerdeUtil.fromJson(schema, DataType.class);
            if (!(type instanceof RowType)) {
                return false;
            }
            FieldIdStats stats = new FieldIdStats();
            collectFieldIds(JsonUtil.readTree(schema), stats);
            return stats.total > 0 && stats.present == 0;
        } catch (RuntimeException e) {
            // Let the selected SDK parse invalid schemas so callers receive its diagnostic.
            return false;
        }
    }

    private static void collectFieldIds(JsonNode type, FieldIdStats stats) {
        if (type == null || !type.isObject() || !type.path("type").isTextual()) {
            return;
        }
        String typeName = type.path("type").asText();
        if (typeName.startsWith("ROW")) {
            JsonNode fields = type.get("fields");
            for (JsonNode field : fields) {
                stats.total++;
                JsonNode id = field.get("id");
                if (id != null) {
                    stats.present++;
                }
                collectFieldIds(field.get("type"), stats);
            }
        } else if (typeName.startsWith("ARRAY") || typeName.startsWith("MULTISET")) {
            collectFieldIds(type.get("element"), stats);
        } else if (typeName.startsWith("MAP")) {
            collectFieldIds(type.get("key"), stats);
            collectFieldIds(type.get("value"), stats);
        }
    }

    private static final class FieldIdStats {
        private int total;
        private int present;
    }

    private static boolean containsVariant(DataType type) {
        if ("VARIANT".equals(type.getTypeRoot().name())) {
            return true;
        }
        if (type instanceof ArrayType) {
            return containsVariant(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            return containsVariant(((MapType) type).getKeyType())
                    || containsVariant(((MapType) type).getValueType());
        }
        return type instanceof RowType && ((RowType) type).getFields().stream()
                .anyMatch(field -> containsVariant(field.type()));
    }

    private static boolean supportedCppType(DataType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BOOLEAN:
            case BINARY:
            case VARBINARY:
            case DECIMAL:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case VARIANT:
                return true;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return supportedTimestampPrecision(((TimestampType) type).getPrecision());
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return supportedTimestampPrecision(((LocalZonedTimestampType) type).getPrecision());
            case ARRAY:
                return supportedCppType(((ArrayType) type).getElementType());
            case MAP:
                MapType map = (MapType) type;
                return supportedCppType(map.getKeyType()) && supportedCppType(map.getValueType());
            case ROW:
                return ((RowType) type).getFields().stream()
                        .allMatch(field -> supportedCppType(field.type()));
            case TIME_WITHOUT_TIME_ZONE:
            case BLOB:
            case VECTOR:
            case MULTISET:
            default:
                return false;
        }
    }

    private static boolean supportedTimestampPrecision(int precision) {
        return precision == 0 || precision == 3 || precision == 6 || precision == 9;
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
        TFileType fileType = resolved.getTFileTypeForBE();
        if ((fileType != TFileType.FILE_S3 && fileType != TFileType.FILE_HDFS)
                || resolved.getStorageProperties() == null) {
            throw new IllegalArgumentException(
                    "Native Paimon supports Doris object storage, HDFS and local files");
        }
        Map<String, String> backend = resolved.getStorageProperties().getBackendConfigProperties();
        if (fileType == TFileType.FILE_S3
                && (!backend.containsKey("AWS_ENDPOINT") || !backend.containsKey("AWS_REGION"))) {
            throw new IllegalArgumentException(
                    "Storage configuration is not a Doris native object-store configuration");
        }
        return new TPaimonStorageDescriptor(fileType, resolved.toStorageLocation().toString(), backend);
    }
}
