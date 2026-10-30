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

import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.TimestampType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/** Selects the append subset whose write and commit semantics the Rust backend implements. */
public final class PaimonRustWriteSupport {
    private PaimonRustWriteSupport() {
    }

    public static boolean isSupported(FileStoreTable table, List<String> columns,
            Map<String, String> storageOptions) {
        return !unsupportedReason(table, columns, storageOptions).isPresent();
    }

    public static Optional<String> unsupportedReason(FileStoreTable table, List<String> columns,
            Map<String, String> storageOptions) {
        Map<String, String> options = table.options();
        CoreOptions coreOptions = new CoreOptions(options);
        if (new CommitMessageSerializer().getVersion() != 11) {
            return Optional.of("commit_protocol_version");
        }
        if (!table.primaryKeys().isEmpty()) {
            return Optional.of("primary_key_table");
        }
        if (!(table instanceof AppendOnlyFileStoreTable)) {
            return Optional.of("table_implementation");
        }
        if (!table.partitionKeys().isEmpty()) {
            return Optional.of("partitioned_table");
        }
        if (coreOptions.bucket() != -1) {
            return Optional.of("bucket_mode");
        }
        if (!"main".equals(CoreOptions.branch(options))) {
            return Optional.of("non_main_branch");
        }
        if (!"parquet".equalsIgnoreCase(coreOptions.formatType())) {
            return Optional.of("file_format");
        }
        if (!columns.equals(table.schema().fields().stream().map(DataField::name).collect(Collectors.toList()))) {
            return Optional.of("column_mapping");
        }
        for (Map.Entry<String, String> option : options.entrySet()) {
            String key = option.getKey();
            if (key.startsWith("file-index.") || key.startsWith("global-index.")
                    || key.startsWith("blob.") || key.startsWith("scan.snapshot")
                    || key.startsWith("scan.tag") || key.startsWith("scan.timestamp")
                    || key.startsWith("incremental-")
                    || (key.equals("changelog-producer") && !option.getValue().equals("none"))
                    || ((key.equals("data-evolution.enabled") || key.equals("row-tracking.enabled")
                            || key.equals("deletion-vectors.enabled"))
                            && Boolean.parseBoolean(option.getValue()))
                    || (key.equals("type") && !option.getValue().equals("paimon"))) {
                return Optional.of("table_option:" + key);
            }
        }
        for (DataField field : table.schema().fields()) {
            // Java replaces nullable input with schema defaults. The Rust file-store
            // writer does not implement this behavior yet, including explicit NULLs.
            if (field.defaultValue() != null) {
                return Optional.of("schema_defaults");
            }
            switch (field.type().getTypeRoot()) {
                case BOOLEAN:
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                case FLOAT:
                case DOUBLE:
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                case DECIMAL:
                case DATE:
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    if (((TimestampType) field.type()).getPrecision() > 6) {
                        return Optional.of("timestamp_precision");
                    }
                    break;
                default:
                    return Optional.of("column_type:" + field.type().getTypeRoot().name());
            }
        }
        String scheme = table.location().toUri().getScheme();
        if (scheme == null || scheme.equals("file")) {
            return Optional.empty();
        }
        if (!scheme.equals("s3")) {
            return Optional.of("unsupported_filesystem");
        }
        Map<String, String> normalized = PaimonTableDescriptor.normalizeStorage(storageOptions);
        // Refreshable credentials and ambient SDK provider chains require separate integration.
        boolean supported = !normalized.containsKey("AWS_ROLE_ARN")
                && !normalized.containsKey("s3.assumed.role.arn")
                && !normalized.containsKey("fs.s3a.assumed.role.arn")
                && StringUtils.isNotBlank(normalized.get("s3.access-key"))
                && StringUtils.isNotBlank(normalized.get("s3.secret-key"))
                && StringUtils.isNotBlank(normalized.get("s3.region"));
        return supported ? Optional.empty() : Optional.of("s3_credentials_or_region");
    }
}
