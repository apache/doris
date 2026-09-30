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

package org.apache.doris.connector.paimon;

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;

import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypeRoot;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.MultisetType;
import org.apache.paimon.types.RowType;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Chooses the Rust reader only for table, split, storage and backend shapes it can read safely. */
final class PaimonRustReaderSelector {
    static final String BACKEND_CAPABILITY = "paimon-rust-reader";
    private static final String ENABLE_PAIMON_RUST_READER = "enable_paimon_rust_reader";
    private static final String ENABLE_FILE_SCANNER_V2 = "enable_file_scanner_v2";
    private static final String DELETION_VECTORS_MERGE_ON_READ = "deletion-vectors.merge-on-read";
    private static final Set<String> VERIFIED_LOCATION_SCHEMES =
            new HashSet<>(Arrays.asList("s3", "s3a", "oss", "hdfs", "file"));
    private static final Set<String> VERIFIED_HDFS_OPTION_KEYS = new HashSet<>(Arrays.asList(
            "fs.defaultFS",
            "hadoop.security.authentication",
            "hdfs.security.authentication",
            "ipc.client.fallback-to-simple-auth-allowed"));

    private final FileStoreTable table;
    private final PaimonRustReaderCapabilities capabilities;
    private final Map<String, String> backendStorageProperties;
    private final boolean scanCompatible;
    private final boolean partialUpdateOrAggregateWithDeletionVectors;
    private final boolean deletionVectorsMergeOnRead;

    PaimonRustReaderSelector(ConnectorSession session, boolean backendsSupportRust,
            FileStoreTable table, List<ConnectorColumnHandle> columns,
            Map<String, String> backendStorageProperties, boolean usesFallbackRead,
            boolean incrementalRead, boolean projectedVariant) {
        this.table = table;
        this.backendStorageProperties = backendStorageProperties;
        CoreOptions options = table.coreOptions();
        CoreOptions.MergeEngine mergeEngine = options.mergeEngine();
        partialUpdateOrAggregateWithDeletionVectors = options.deletionVectorsEnabled()
                && (mergeEngine == CoreOptions.MergeEngine.PARTIAL_UPDATE
                        || mergeEngine == CoreOptions.MergeEngine.AGGREGATE);
        TableSchema schema = table.schema();
        String mergeOnRead = schema.options().get(DELETION_VECTORS_MERGE_ON_READ);
        deletionVectorsMergeOnRead = "true".equalsIgnoreCase(mergeOnRead);

        String location = table.location();
        boolean enabled = session != null && "true".equalsIgnoreCase(
                session.getSessionProperties().get(ENABLE_PAIMON_RUST_READER));
        boolean fileScannerV2 = session != null && "true".equalsIgnoreCase(
                session.getSessionProperties().get(ENABLE_FILE_SCANNER_V2));
        boolean restTokenTable = table.fileIO() instanceof RESTTokenFileIO;
        boolean unsupportedMergeOption = (mergeEngine == CoreOptions.MergeEngine.PARTIAL_UPDATE
                || mergeEngine == CoreOptions.MergeEngine.AGGREGATE)
                && hasRustUnsupportedMergeOption(schema.options(), mergeEngine);
        boolean deduplicateIgnoreDelete = mergeEngine == CoreOptions.MergeEngine.DEDUPLICATE
                && options.ignoreDelete();
        String scanMode = PaimonScanParams.withoutTimeTravelSelectors(schema)
                .options().get(CoreOptions.SCAN_MODE.key());
        boolean scanModeSupported = scanMode == null || "default".equalsIgnoreCase(scanMode);

        scanCompatible = enabled && fileScannerV2
                && backendsSupportRust
                && !usesFallbackRead && !incrementalRead && !projectedVariant
                && !options.queryAuthEnabled() && !restTokenTable
                && !deletionVectorsMergeOnRead && !deduplicateIgnoreDelete
                && !unsupportedMergeOption && scanModeSupported
                && isRustVerifiedLocationScheme(location)
                && (!isHdfsLocationScheme(location)
                        || isRustVerifiedHdfsBackend(backendStorageProperties))
                && isCredentialProviderTranslatable(location, backendStorageProperties);
        capabilities = new PaimonRustReaderCapabilities(table, columns);
    }

    boolean canRead(DataSplit split) {
        if (!scanCompatible || splitHasExternalFiles(split)) {
            return false;
        }
        if (splitHasOrcFile(split) && table.schema().fields().stream()
                .anyMatch(field -> containsTimestampLtz(field.type()))) {
            return false;
        }
        if (partialUpdateOrAggregateWithDeletionVectors && !deletionVectorsMergeOnRead
                && !isFullyMaterializedPkDvSplit(split)) {
            return false;
        }
        return capabilities.canRead(split);
    }

    static boolean isRustVerifiedLocationScheme(String location) {
        if (location == null) {
            return false;
        }
        int separator = location.indexOf("://");
        return separator <= 0 || VERIFIED_LOCATION_SCHEMES.contains(location.substring(0, separator));
    }

    private static boolean isHdfsLocationScheme(String location) {
        if (location == null) {
            return false;
        }
        int separator = location.indexOf("://");
        return separator > 0 && "hdfs".equalsIgnoreCase(location.substring(0, separator));
    }

    static boolean splitHasOrcFile(DataSplit split) {
        for (DataFileMeta file : split.dataFiles()) {
            if ("orc".equalsIgnoreCase(file.fileFormat())) {
                return true;
            }
        }
        return false;
    }

    static boolean splitHasExternalFiles(DataSplit split) {
        return split.dataFiles().stream().anyMatch(file -> file.externalPath().isPresent());
    }

    static boolean containsTimestampLtz(DataType type) {
        if (type == null) {
            return false;
        }
        if (type.getTypeRoot() == DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return true;
        }
        if (type instanceof ArrayType) {
            return containsTimestampLtz(((ArrayType) type).getElementType());
        }
        if (type instanceof MapType) {
            MapType map = (MapType) type;
            return containsTimestampLtz(map.getKeyType()) || containsTimestampLtz(map.getValueType());
        }
        if (type instanceof MultisetType) {
            return containsTimestampLtz(((MultisetType) type).getElementType());
        }
        if (type instanceof RowType) {
            return ((RowType) type).getFields().stream()
                    .anyMatch(field -> containsTimestampLtz(field.type()));
        }
        return false;
    }

    static boolean isRustVerifiedHdfsBackend(Map<String, String> properties) {
        if (properties == null) {
            return true;
        }
        for (String key : new String[] {"hadoop.security.authentication", "hdfs.security.authentication"}) {
            String value = properties.get(key);
            if (value != null && !"simple".equalsIgnoreCase(value.trim())) {
                return false;
            }
        }
        for (String key : new String[] {"hadoop.kerberos.principal", "hadoop.kerberos.keytab",
                "hadoop.username"}) {
            String value = properties.get(key);
            if (StringUtils.isNotBlank(value)) {
                return false;
            }
        }
        if (StringUtils.isNotBlank(properties.get("dfs.nameservices"))) {
            return false;
        }
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key == null || StringUtils.isBlank(value)) {
                continue;
            }
            if (key.startsWith("dfs.ha.")) {
                return false;
            }
            if ((key.startsWith("dfs.") || key.startsWith("hadoop.") || key.startsWith("fs."))
                    && !VERIFIED_HDFS_OPTION_KEYS.contains(key)) {
                return false;
            }
        }
        return true;
    }

    static boolean isFullyMaterializedPkDvSplit(DataSplit split) {
        if (!split.rawConvertible()) {
            return false;
        }
        for (DataFileMeta file : split.dataFiles()) {
            Optional<Long> deleteRowCount = file.deleteRowCount();
            if (file.level() == 0 || !deleteRowCount.isPresent() || deleteRowCount.get() != 0L) {
                return false;
            }
        }
        return true;
    }

    static boolean hasRustUnsupportedMergeOption(Map<String, String> options,
            CoreOptions.MergeEngine mergeEngine) {
        boolean partialUpdate = mergeEngine == CoreOptions.MergeEngine.PARTIAL_UPDATE;
        for (String key : options.keySet()) {
            if (key != null && (partialUpdate ? isUnsupportedPartialUpdateOption(key)
                    : isUnsupportedAggregationOption(key))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isUnsupportedPartialUpdateOption(String key) {
        return (key.endsWith(".ignore-delete")
                && !"ignore-delete".equals(key)
                && !"partial-update.ignore-delete".equals(key))
                || "partial-update.remove-record-on-delete".equals(key)
                || "partial-update.remove-record-on-sequence-group".equals(key)
                || hasFieldOptionSuffix(key, ".ignore-retract")
                || hasFieldOptionSuffix(key, ".distinct")
                || hasFieldOptionSuffix(key, ".nested-key")
                || hasFieldOptionSuffix(key, ".count-limit");
    }

    private static boolean isUnsupportedAggregationOption(String key) {
        return "ignore-delete".equals(key)
                || key.endsWith(".ignore-delete")
                || "aggregation.remove-record-on-delete".equals(key)
                || hasFieldOptionSuffix(key, ".sequence-group")
                || hasFieldOptionSuffix(key, ".ignore-retract")
                || hasFieldOptionSuffix(key, ".distinct")
                || hasFieldOptionSuffix(key, ".nested-key")
                || hasFieldOptionSuffix(key, ".count-limit");
    }

    private static boolean hasFieldOptionSuffix(String key, String suffix) {
        return key.startsWith("fields.") && key.endsWith(suffix);
    }

    private static boolean isCredentialProviderTranslatable(
            String location, Map<String, String> properties) {
        String provider = properties.get("AWS_CREDENTIALS_PROVIDER_TYPE");
        String mode = provider == null ? "DEFAULT" : provider.trim().toUpperCase(Locale.ROOT);
        if (mode.isEmpty()) {
            mode = "DEFAULT";
        }
        if (location != null && (location.startsWith("s3://") || location.startsWith("s3a://"))) {
            boolean staticKeys = StringUtils.isNotBlank(properties.get("AWS_ACCESS_KEY"))
                    && StringUtils.isNotBlank(properties.get("AWS_SECRET_KEY"));
            boolean conflictingSettings = staticKeys && ("ANONYMOUS".equals(mode)
                    || StringUtils.isNotEmpty(properties.get("AWS_ROLE_ARN"))
                    || StringUtils.isNotEmpty(properties.get("AWS_TOKEN")));
            return !conflictingSettings
                    && ("ANONYMOUS".equals(mode) || ("DEFAULT".equals(mode) && staticKeys));
        }
        if (provider == null) {
            return true;
        }
        if ("ANONYMOUS".equals(mode) && location != null && location.startsWith("oss://")) {
            return false;
        }
        return "DEFAULT".equals(mode) || "ANONYMOUS".equals(mode);
    }
}
