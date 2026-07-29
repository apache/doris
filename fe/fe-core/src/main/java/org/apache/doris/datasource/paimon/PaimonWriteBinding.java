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
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.commands.insert.PaimonInsertCommandContext;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeRoot;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Transaction-scoped binding between one Doris insert and one pinned Paimon write target.
 *
 * <p>Schema analysis and writer distribution have already used the target's table handle.
 * Finalizing the transaction adds statement-specific overwrite and authentication state without
 * reloading remote metadata.
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

    public static PaimonWriteBinding create(PaimonWriteTarget writeTarget,
            PaimonInsertCommandContext context) throws UserException {
        PaimonExternalTable dorisTable = writeTarget.getDorisTable();
        PaimonExternalCatalog catalog = (PaimonExternalCatalog) dorisTable.getCatalog();
        FileStoreTable table = writeTarget.getTable();
        Map<String, Expression> typedStaticPartition = context.getStaticPartition();
        Map<String, String> staticPartition = resolveStaticPartition(
                table,
                writeTarget.getColumnTypes(),
                typedStaticPartition,
                context.isOverwrite());
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

    static Map<String, String> resolveStaticPartition(FileStoreTable table,
            Map<String, org.apache.doris.catalog.Type> writeColumnTypes,
            Map<String, Expression> typedStaticPartition,
            boolean overwrite) throws AnalysisException {
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
            DataField partitionField = table.rowType().getField(canonicalName);
            org.apache.doris.catalog.Type writeType = writeColumnTypes.get(canonicalName);
            Preconditions.checkNotNull(writeType,
                    "Paimon partition column is missing from the write schema: " + canonicalName);
            Literal castValue = castPartitionValue((Literal) value, writeType);
            boolean isNull = castValue instanceof NullLiteral;
            String partitionValue = isNull
                    ? defaultPartitionName
                    : canonicalPartitionValue(castValue, partitionField);
            if (overwrite && !isNull
                    && defaultPartitionName.equals(partitionValue)) {
                // Paimon 1.3's public static-overwrite API uses this string as the
                // NULL marker for every partition type, so the corresponding literal
                // value cannot be represented without changing its typed identity.
                throw new AnalysisException("Static partition value for column '" + canonicalName
                        + "' equals Paimon partition.default-name '" + defaultPartitionName
                        + "' and cannot be represented in a static overwrite");
            }
            resolved.put(canonicalName, partitionValue);
        }
        return resolved;
    }

    private static Literal castPartitionValue(
            Literal literal, org.apache.doris.catalog.Type writeType) throws AnalysisException {
        Expression castValue = literal.checkedCastTo(DataType.fromCatalogType(writeType));
        Preconditions.checkState(castValue instanceof Literal,
                "Static Paimon partition cast must produce a literal");
        return (Literal) castValue;
    }

    private static String canonicalPartitionValue(
            Literal literal, DataField partitionField) {
        String value = literal.getStringValue();
        if (partitionField.type().getTypeRoot()
                != DataTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
            return value;
        }

        // Doris writes an LTZ literal as civil time in the session zone. Paimon 1.3
        // parses the string accepted by withOverwrite in the FE JVM default zone.
        // Translate the same instant into that zone so the overwrite predicate and
        // the row written by the JNI writer identify the same typed partition.
        LocalDateTime sessionValue = LocalDateTime.parse(
                value.replace(' ', 'T'), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        return sessionValue.atZone(TimeUtils.getDorisZoneId())
                .withZoneSameInstant(ZoneId.systemDefault())
                .toLocalDateTime()
                // Paimon 1.3's timestamp parser accepts a space, but not ISO's
                // 'T', between the date and time components.
                .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .replace('T', ' ');
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
