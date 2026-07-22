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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.mvcc.MvccUtil;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.commands.insert.PaimonInsertCommandContext;

import com.google.common.base.Preconditions;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.FileStoreTable;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Transaction-scoped binding between one Doris insert and one concrete Paimon table handle.
 *
 * <p>The table and writer configuration are captured exactly once while the sink is finalized.
 * BE preparation, FE commit, abort and outcome reconciliation therefore all refer to the same
 * Paimon table handle.
 */
public class PaimonWriteBinding {
    private final PaimonExternalTable dorisTable;
    private final FileStoreTable table;
    private final String serializedTable;
    private final Map<String, String> hadoopConfig;
    private final boolean overwrite;
    private final Map<String, String> staticPartition;

    private PaimonWriteBinding(PaimonExternalTable dorisTable, FileStoreTable table,
            Map<String, String> hadoopConfig, boolean overwrite,
            Map<String, String> staticPartition) {
        this.dorisTable = dorisTable;
        this.table = table;
        this.serializedTable = PaimonUtil.encodeObjectToString(table);
        this.hadoopConfig = Collections.unmodifiableMap(new HashMap<>(hadoopConfig));
        this.overwrite = overwrite;
        this.staticPartition = Collections.unmodifiableMap(new LinkedHashMap<>(staticPartition));
    }

    public static PaimonWriteBinding create(PaimonExternalTable dorisTable,
            PaimonInsertCommandContext context) throws UserException {
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) dorisTable.getCatalog();
        FileStoreTable table;
        try {
            table = catalog.getExecutionAuthenticator().execute(() -> requireFileStoreTable(
                    dorisTable.getPaimonTable(MvccUtil.getSnapshotFromContext(dorisTable))));
        } catch (Exception e) {
            throw new UserException("Failed to bind Paimon table for write", e);
        }
        if (table.bucketMode() == BucketMode.HASH_DYNAMIC) {
            throw new AnalysisException("Paimon dynamic-bucket tables are not supported for writes");
        }

        Map<String, Expression> typedStaticPartition = context.getStaticPartition();
        Map<String, String> staticPartition = resolveStaticPartition(table, typedStaticPartition);
        return new PaimonWriteBinding(
                dorisTable,
                table,
                buildHadoopConfig(catalog),
                context.isOverwrite(),
                staticPartition);
    }

    public FileStoreTable getTable() {
        return table;
    }

    public String getSerializedTable() {
        return serializedTable;
    }

    public Map<String, String> getHadoopConfig() {
        return hadoopConfig;
    }

    public boolean isOverwrite() {
        return overwrite;
    }

    public Map<String, String> getStaticPartition() {
        return staticPartition;
    }

    public String tableName() {
        return dorisTable.getDbName() + "." + dorisTable.getName();
    }

    private static FileStoreTable requireFileStoreTable(org.apache.paimon.table.Table table) {
        Preconditions.checkState(table instanceof FileStoreTable,
                "Paimon write requires a file store table");
        return (FileStoreTable) table;
    }

    private static Map<String, String> resolveStaticPartition(FileStoreTable table,
            Map<String, Expression> typedStaticPartition) throws AnalysisException {
        Map<String, String> canonicalNames = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        for (String partitionKey : table.partitionKeys()) {
            canonicalNames.put(partitionKey, partitionKey);
        }

        String defaultPartitionName = CoreOptions.fromMap(table.options()).partitionDefaultName();
        Map<String, String> resolved = new LinkedHashMap<>();
        for (Map.Entry<String, Expression> entry : typedStaticPartition.entrySet()) {
            String canonicalName = canonicalNames.get(entry.getKey());
            if (canonicalName == null) {
                throw new AnalysisException("Column '" + entry.getKey()
                        + "' is not a partition column of Paimon table");
            }
            Expression value = entry.getValue();
            if (!(value instanceof Literal)) {
                throw new AnalysisException("Static partition value must be a literal, but got: "
                        + value);
            }
            String partitionValue = value instanceof NullLiteral
                    ? defaultPartitionName : ((Literal) value).getStringValue();
            resolved.put(canonicalName, partitionValue);
        }
        return resolved;
    }

    private static Map<String, String> buildHadoopConfig(PaimonExternalCatalog catalog) {
        Map<String, String> hadoopConfig = new HashMap<>(
                catalog.getCatalogProperty().getHadoopProperties());
        String warehouse = catalog.getPaimonOptionsMap().get(CatalogOptions.WAREHOUSE.key());
        String defaultFs = resolveDefaultFsName(warehouse);
        if (defaultFs != null && !defaultFs.isEmpty()) {
            String current = hadoopConfig.get("fs.defaultFS");
            if (current == null || current.isEmpty() || current.startsWith("file:/")) {
                hadoopConfig.put("fs.defaultFS", defaultFs);
            }
        }

        String hadoopUser = hadoopConfig.get("hadoop.username");
        if (hadoopUser == null || hadoopUser.isEmpty()) {
            hadoopUser = hadoopConfig.get("hadoop.user.name");
        }
        if (hadoopUser == null || hadoopUser.isEmpty()) {
            hadoopUser = "hadoop";
        }
        hadoopConfig.put("hadoop.username", hadoopUser);
        hadoopConfig.put("hadoop.user.name", hadoopUser);
        return hadoopConfig;
    }

    private static String resolveDefaultFsName(String warehouse) {
        if (warehouse == null || warehouse.isEmpty()) {
            return null;
        }
        try {
            java.net.URI uri = java.net.URI.create(warehouse);
            String scheme = uri.getScheme();
            String authority = uri.getAuthority();
            if (scheme != null && !scheme.isEmpty() && authority != null && !authority.isEmpty()) {
                return scheme + "://" + authority;
            }
        } catch (IllegalArgumentException ignored) {
            return null;
        }
        return null;
    }

}
