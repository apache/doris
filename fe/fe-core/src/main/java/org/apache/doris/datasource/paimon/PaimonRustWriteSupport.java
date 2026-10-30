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

import org.apache.doris.thrift.TPaimonTableDesc;

import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.table.AppendOnlyFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.CommitMessageSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.TimestampType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Selects the append subset whose write and commit semantics the Rust backend implements. */
public final class PaimonRustWriteSupport {
    private PaimonRustWriteSupport() {
    }

    public static boolean isSupported(FileStoreTable table, List<String> columns,
            Map<String, String> storageOptions) {
        Map<String, String> options = table.options();
        CoreOptions coreOptions = new CoreOptions(options);
        if (new CommitMessageSerializer().getVersion() != 11
                || !(table instanceof AppendOnlyFileStoreTable)
                || !table.partitionKeys().isEmpty() || !table.primaryKeys().isEmpty()
                || coreOptions.bucket() != -1
                || !"main".equals(CoreOptions.branch(options))
                || !"parquet".equalsIgnoreCase(coreOptions.formatType())
                || !columns.equals(table.schema().fields().stream()
                        .map(DataField::name).collect(Collectors.toList()))) {
            return false;
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
                return false;
            }
        }
        for (DataField field : table.schema().fields()) {
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
                        return false;
                    }
                    break;
                default:
                    return false;
            }
        }
        String scheme = table.location().toUri().getScheme();
        if (scheme == null || scheme.equals("file")) {
            return true;
        }
        if (!scheme.equals("s3")) {
            return false;
        }
        Map<String, String> normalized = normalizeStorage(storageOptions);
        // Refreshable credentials and ambient SDK provider chains require separate integration.
        return !normalized.containsKey("AWS_ROLE_ARN")
                && !normalized.containsKey("s3.assumed.role.arn")
                && !normalized.containsKey("fs.s3a.assumed.role.arn")
                && StringUtils.isNotBlank(normalized.get("s3.access-key"))
                && StringUtils.isNotBlank(normalized.get("s3.secret-key"))
                && StringUtils.isNotBlank(normalized.get("s3.region"));
    }

    public static TPaimonTableDesc describe(FileStoreTable table, String database, String name,
            Map<String, String> storageOptions) {
        TPaimonTableDesc result = new TPaimonTableDesc();
        result.setTablePath(table.location().toString());
        Map<String, String> writerOptions = new HashMap<>(table.schema().options());
        // Bound the native writer's row-group flushing threshold independently of file size.
        writerOptions.put("write-buffer-size", "8 mb");
        result.setSchemaJson(PaimonUtil.encodeTableSchemaToJson(table.schema().copy(writerOptions)));
        result.setDatabaseName(database);
        result.setTableName(name);
        result.setBranch(CoreOptions.branch(table.options()));
        result.setStorageOptions(normalizeStorage(storageOptions));
        result.setCommitMessageVersion(new CommitMessageSerializer().getVersion());
        return result;
    }

    private static Map<String, String> normalizeStorage(Map<String, String> original) {
        Map<String, String> result = new HashMap<>(original);
        String[][] aliases = {
                {"AWS_ACCESS_KEY", "s3.access-key"}, {"AWS_SECRET_KEY", "s3.secret-key"},
                {"AWS_TOKEN", "s3.session.token"}, {"AWS_ENDPOINT", "s3.endpoint"},
                {"AWS_REGION", "s3.region"}, {"use_path_style", "s3.path-style-access"},
                {"fs.s3a.access.key", "s3.access-key"}, {"fs.s3a.secret.key", "s3.secret-key"},
                {"fs.s3a.endpoint.region", "s3.region"}
        };
        for (String[] alias : aliases) {
            if (original.containsKey(alias[0])) {
                result.putIfAbsent(alias[1], original.get(alias[0]));
            }
        }
        return result;
    }
}
