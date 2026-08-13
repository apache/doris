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

package org.apache.doris.catalog;

import org.apache.doris.catalog.constraint.Constraint;
import org.apache.doris.catalog.constraint.ForeignKeyConstraint;
import org.apache.doris.catalog.constraint.PrimaryKeyConstraint;
import org.apache.doris.catalog.constraint.UniqueConstraint;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.info.TableNameInfoUtils;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Describes the keys and indexes of a table the way MySQL does, as one row per
 * (index, column) pair.
 *
 * <p>MySQL clients discover the primary key of a table through {@code SHOW KEYS} and
 * {@code information_schema.STATISTICS}, both of which are shaped like this. The MySQL
 * ODBC driver, for instance, answers {@code SQLPrimaryKeys} and {@code SQLStatistics}
 * by running {@code SHOW KEYS FROM `db`.`tbl`} and looking for rows whose key name is
 * exactly {@code PRIMARY}. Producing those rows in one place keeps every such surface
 * telling the same story.
 *
 * <p>What counts as the primary key, in priority order:
 * <ol>
 *   <li>A user declared {@code PRIMARY KEY} constraint, if the table has one.</li>
 *   <li>The key columns of a UNIQUE KEY or AGGREGATE KEY table. Both models make the
 *       key columns unique, so they are a faithful primary key.</li>
 * </ol>
 * The key columns of a DUPLICATE KEY table are only a sort prefix and are <em>not</em>
 * unique, so they are reported as a non-unique index instead. Reporting them as a
 * primary key would let a client such as Access believe it can address a single row by
 * them, which silently corrupts edits.
 */
public class TableKeyMeta {
    /** MySQL's reserved name for the primary key. Clients match on this exact string. */
    public static final String PRIMARY_KEY_NAME = "PRIMARY";
    /** Name reported for the sort prefix of a DUPLICATE KEY table. */
    public static final String DUPLICATE_KEY_NAME = "DUPLICATE";

    private static final String BTREE = "BTREE";
    private static final String ASCENDING = "A";

    private TableKeyMeta() {}

    /** One (index, column) pair, i.e. one row of SHOW KEYS or information_schema.STATISTICS. */
    public static class KeyRow {
        private final String tableName;
        private final boolean nonUnique;
        private final String indexName;
        private final int seqInIndex;
        private final String columnName;
        private final String collation;
        private final Long cardinality;
        private final boolean nullable;
        private final String indexType;
        private final String comment;
        private final String properties;

        public KeyRow(String tableName, boolean nonUnique, String indexName, int seqInIndex, String columnName,
                String collation, Long cardinality, boolean nullable, String indexType, String comment,
                String properties) {
            this.tableName = tableName;
            this.nonUnique = nonUnique;
            this.indexName = indexName;
            this.seqInIndex = seqInIndex;
            this.columnName = columnName;
            this.collation = collation;
            this.cardinality = cardinality;
            this.nullable = nullable;
            this.indexType = indexType;
            this.comment = comment;
            this.properties = properties;
        }

        public String getTableName() {
            return tableName;
        }

        public boolean isNonUnique() {
            return nonUnique;
        }

        public String getIndexName() {
            return indexName;
        }

        public int getSeqInIndex() {
            return seqInIndex;
        }

        public String getColumnName() {
            return columnName;
        }

        /** "A" for an ordered index, null when the order is not meaningful. */
        public String getCollation() {
            return collation;
        }

        /** Estimated distinct values, or null when unknown. */
        public Long getCardinality() {
            return cardinality;
        }

        public boolean isNullable() {
            return nullable;
        }

        public String getIndexType() {
            return indexType;
        }

        public String getComment() {
            return comment;
        }

        /** Doris specific index properties. Empty for keys derived from the table model. */
        public String getProperties() {
            return properties;
        }
    }

    /**
     * Builds every key row of a table, primary key first.
     *
     * <p>The caller is expected to hold a read lock on the table.
     */
    public static List<KeyRow> buildKeyRows(TableIf table) {
        List<KeyRow> rows = Lists.newArrayList();
        Map<String, Constraint> constraints = getConstraints(table);

        List<Column> primaryKeyColumns = findPrimaryKeyColumns(table, constraints);
        if (!primaryKeyColumns.isEmpty()) {
            addRows(rows, table, PRIMARY_KEY_NAME, primaryKeyColumns, false, tableCardinality(table), BTREE, "", "");
        }

        // Unique constraints are declared rather than enforced, but they are the user's own
        // statement about the data, so report them as unique indexes.
        for (Map.Entry<String, Constraint> entry : sortedByName(constraints).entrySet()) {
            if (!(entry.getValue() instanceof UniqueConstraint)) {
                continue;
            }
            List<Column> columns = orderBySchema(table, ((UniqueConstraint) entry.getValue()).getUniqueColumnNames());
            if (!columns.isEmpty()) {
                addRows(rows, table, entry.getKey(), columns, false, tableCardinality(table), BTREE, "", "");
            }
        }

        if (primaryKeyColumns.isEmpty() && table instanceof OlapTable
                && ((OlapTable) table).getKeysType() == KeysType.DUP_KEYS) {
            // Only a sort prefix, so not unique. Still worth reporting: it tells a client
            // that a prefix scan on these columns is cheap.
            List<Column> sortKeyColumns = keyColumnsOf(table);
            if (!sortKeyColumns.isEmpty()) {
                addRows(rows, table, DUPLICATE_KEY_NAME, sortKeyColumns, true, null, BTREE, "", "");
            }
        }

        if (table instanceof OlapTable) {
            for (Index index : ((OlapTable) table).getIndexes()) {
                List<Column> columns = Lists.newArrayList();
                for (String columnName : index.getColumns()) {
                    Column column = table.getColumn(columnName);
                    if (column != null) {
                        columns.add(column);
                    }
                }
                if (columns.isEmpty()) {
                    continue;
                }
                // A secondary index imposes no order on its columns, so MySQL reports no collation.
                addRows(rows, table, index.getIndexName(), columns, true, null,
                        index.getIndexType().name(), index.getComment(), index.getPropertiesString());
            }
        }
        return rows;
    }

    /**
     * The COLUMN_KEY value MySQL reports for each column of a table: PRI for a column of the
     * primary key, UNI for the first column of a unique index, MUL for the first column of a
     * non-unique one. Columns that are none of these are absent from the map.
     *
     * <p>Derived from the same rows as SHOW KEYS, so the two always agree.
     */
    public static Map<String, String> buildColumnKeys(TableIf table) {
        Map<String, String> columnKeys = new HashMap<>();
        for (KeyRow row : buildKeyRows(table)) {
            String value;
            if (PRIMARY_KEY_NAME.equals(row.getIndexName())) {
                value = "PRI";
            } else if (row.getSeqInIndex() != 1) {
                // Only the leading column of an index gets a marker.
                continue;
            } else {
                value = row.isNonUnique() ? "MUL" : "UNI";
            }
            String current = columnKeys.get(row.getColumnName());
            if (current == null || rank(value) > rank(current)) {
                columnKeys.put(row.getColumnName(), value);
            }
        }
        return columnKeys;
    }

    private static int rank(String columnKey) {
        switch (columnKey) {
            case "PRI":
                return 3;
            case "UNI":
                return 2;
            default:
                return 1;
        }
    }

    /** One row of information_schema.TABLE_CONSTRAINTS. */
    public static class ConstraintRow {
        private final String constraintName;
        private final String constraintType;

        public ConstraintRow(String constraintName, String constraintType) {
            this.constraintName = constraintName;
            this.constraintType = constraintType;
        }

        public String getConstraintName() {
            return constraintName;
        }

        /** One of PRIMARY KEY, UNIQUE, FOREIGN KEY. */
        public String getConstraintType() {
            return constraintType;
        }
    }

    /** One row of information_schema.KEY_COLUMN_USAGE. */
    public static class KeyColumnUsageRow {
        private final String constraintName;
        private final String columnName;
        private final int ordinalPosition;
        private final Integer positionInUniqueConstraint;
        private final String referencedTableSchema;
        private final String referencedTableName;
        private final String referencedColumnName;

        public KeyColumnUsageRow(String constraintName, String columnName, int ordinalPosition,
                Integer positionInUniqueConstraint, String referencedTableSchema, String referencedTableName,
                String referencedColumnName) {
            this.constraintName = constraintName;
            this.columnName = columnName;
            this.ordinalPosition = ordinalPosition;
            this.positionInUniqueConstraint = positionInUniqueConstraint;
            this.referencedTableSchema = referencedTableSchema;
            this.referencedTableName = referencedTableName;
            this.referencedColumnName = referencedColumnName;
        }

        public String getConstraintName() {
            return constraintName;
        }

        public String getColumnName() {
            return columnName;
        }

        public int getOrdinalPosition() {
            return ordinalPosition;
        }

        /** Null unless this row belongs to a foreign key. */
        public Integer getPositionInUniqueConstraint() {
            return positionInUniqueConstraint;
        }

        public String getReferencedTableSchema() {
            return referencedTableSchema;
        }

        public String getReferencedTableName() {
            return referencedTableName;
        }

        public String getReferencedColumnName() {
            return referencedColumnName;
        }
    }

    /**
     * Builds the TABLE_CONSTRAINTS rows of a table.
     *
     * <p>The primary key is always named PRIMARY here, even when it came from a constraint
     * the user named something else, because that is the name clients look for.
     */
    public static List<ConstraintRow> buildConstraintRows(TableIf table) {
        List<ConstraintRow> rows = Lists.newArrayList();
        Map<String, Constraint> constraints = getConstraints(table);
        if (!findPrimaryKeyColumns(table, constraints).isEmpty()) {
            rows.add(new ConstraintRow(PRIMARY_KEY_NAME, Constraint.ConstraintType.PRIMARY_KEY.getName()));
        }
        for (Map.Entry<String, Constraint> entry : sortedByName(constraints).entrySet()) {
            if (entry.getValue() instanceof UniqueConstraint) {
                rows.add(new ConstraintRow(entry.getKey(), Constraint.ConstraintType.UNIQUE.getName()));
            } else if (entry.getValue() instanceof ForeignKeyConstraint) {
                rows.add(new ConstraintRow(entry.getKey(), Constraint.ConstraintType.FOREIGN_KEY.getName()));
            }
        }
        return rows;
    }

    /** Builds the KEY_COLUMN_USAGE rows of a table. */
    public static List<KeyColumnUsageRow> buildKeyColumnUsageRows(TableIf table) {
        List<KeyColumnUsageRow> rows = Lists.newArrayList();
        Map<String, Constraint> constraints = getConstraints(table);

        int position = 1;
        for (Column column : findPrimaryKeyColumns(table, constraints)) {
            rows.add(new KeyColumnUsageRow(PRIMARY_KEY_NAME, column.getName(), position++,
                    null, null, null, null));
        }

        for (Map.Entry<String, Constraint> entry : sortedByName(constraints).entrySet()) {
            Constraint constraint = entry.getValue();
            if (constraint instanceof UniqueConstraint) {
                position = 1;
                for (Column column : orderBySchema(table, ((UniqueConstraint) constraint).getUniqueColumnNames())) {
                    rows.add(new KeyColumnUsageRow(entry.getKey(), column.getName(), position++,
                            null, null, null, null));
                }
            } else if (constraint instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint foreignKey = (ForeignKeyConstraint) constraint;
                TableNameInfo referenced = foreignKey.getReferencedTableName();
                position = 1;
                // The map keeps the order the foreign key was declared in, so the nth local
                // column pairs with the nth column of the key it references.
                for (Map.Entry<String, String> pair : foreignKey.getForeignToReference().entrySet()) {
                    rows.add(new KeyColumnUsageRow(entry.getKey(), pair.getKey(), position,
                            position, referenced == null ? null : referenced.getDb(),
                            referenced == null ? null : referenced.getTbl(), pair.getValue()));
                    position++;
                }
            }
        }
        return rows;
    }

    private static void addRows(List<KeyRow> rows, TableIf table, String indexName, List<Column> columns,
            boolean nonUnique, Long cardinality, String indexType, String comment, String properties) {
        String collation = BTREE.equals(indexType) ? ASCENDING : null;
        int seq = 1;
        for (Column column : columns) {
            rows.add(new KeyRow(table.getName(), nonUnique, indexName, seq++, column.getName(), collation,
                    cardinality, column.isAllowNull(), indexType, comment, properties));
        }
    }

    /**
     * A declared PRIMARY KEY constraint wins over the table model, so that the owner of a
     * DUPLICATE KEY table whose data really is unique can make their table usable from
     * ODBC and JDBC with a single ALTER TABLE.
     */
    private static List<Column> findPrimaryKeyColumns(TableIf table, Map<String, Constraint> constraints) {
        for (Constraint constraint : sortedByName(constraints).values()) {
            if (constraint instanceof PrimaryKeyConstraint) {
                List<Column> columns = orderBySchema(table, ((PrimaryKeyConstraint) constraint).getPrimaryKeyNames());
                if (!columns.isEmpty()) {
                    return columns;
                }
            }
        }
        if (table instanceof OlapTable) {
            KeysType keysType = ((OlapTable) table).getKeysType();
            if (keysType == KeysType.UNIQUE_KEYS || keysType == KeysType.AGG_KEYS
                    || keysType == KeysType.PRIMARY_KEYS) {
                return keyColumnsOf(table);
            }
        }
        return Lists.newArrayList();
    }

    private static List<Column> keyColumnsOf(TableIf table) {
        List<Column> columns = Lists.newArrayList();
        for (Column column : table.getBaseSchema()) {
            if (column.isKey()) {
                columns.add(column);
            }
        }
        return columns;
    }

    /**
     * Constraints hold their columns in an unordered set, but the key sequence a client
     * reads has to be stable and has to match the storage order, so resolve the order
     * from the table schema.
     */
    private static List<Column> orderBySchema(TableIf table, Set<String> columnNames) {
        List<Column> columns = Lists.newArrayList();
        for (Column column : table.getBaseSchema()) {
            for (String columnName : columnNames) {
                if (column.getName().equalsIgnoreCase(columnName)) {
                    columns.add(column);
                    break;
                }
            }
        }
        return columns;
    }

    private static Map<String, Constraint> sortedByName(Map<String, Constraint> constraints) {
        Map<String, Constraint> sorted = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        sorted.putAll(constraints);
        return sorted;
    }

    private static Map<String, Constraint> getConstraints(TableIf table) {
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo == null) {
            return Collections.emptyMap();
        }
        return Env.getCurrentEnv().getConstraintManager().getConstraints(tableNameInfo);
    }

    /**
     * A unique key is distinct on every row, so its cardinality is the row count. An
     * unreported row count is left unknown rather than reported as zero, which a client
     * would read as "this index selects nothing".
     */
    private static Long tableCardinality(TableIf table) {
        try {
            long rowCount = table.getRowCount();
            return rowCount > 0 ? rowCount : null;
        } catch (Exception e) {
            return null;
        }
    }
}
