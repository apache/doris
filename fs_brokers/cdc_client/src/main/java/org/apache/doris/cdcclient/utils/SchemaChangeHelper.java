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

package org.apache.doris.cdcclient.utils;

import org.apache.doris.cdcclient.common.DorisType;

import org.apache.flink.util.Preconditions;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import io.debezium.relational.Column;
import io.debezium.relational.history.TableChanges;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for generating Doris ALTER TABLE SQL from schema diffs. */
public class SchemaChangeHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeHelper.class);
    private static final String ADD_DDL = "ALTER TABLE %s ADD COLUMN %s %s";
    private static final String DROP_DDL = "ALTER TABLE %s DROP COLUMN %s";

    private SchemaChangeHelper() {}

    // ─── Schema diff result ────────────────────────────────────────────────────

    /**
     * Holds the result of a full schema comparison between an after-schema and stored TableChange.
     */
    public static class SchemaDiff {
        /** Fields present in afterSchema but absent from stored. */
        public final List<Field> added;

        /** Column names present in stored but absent from afterSchema. */
        public final List<String> dropped;

        /** Same-named columns whose Doris type or default value differs. */
        public final Map<String, Field> modified;

        public SchemaDiff(List<Field> added, List<String> dropped, Map<String, Field> modified) {
            this.added = added;
            this.dropped = dropped;
            this.modified = modified;
        }

        public boolean isEmpty() {
            return added.isEmpty() && dropped.isEmpty() && modified.isEmpty();
        }
    }

    // ─── Schema-diff helpers (Kafka Connect schema ↔ stored TableChange) ──────

    /**
     * Name-only schema diff: compare field names in {@code afterSchema} against the stored {@link
     * TableChanges.TableChange}, detecting added and dropped columns by name only.
     *
     * <p>Only support add and drop and not support modify and rename
     *
     * <p>When {@code stored} is null or empty, both lists are empty (no baseline to diff against).
     */
    public static SchemaDiff diffSchemaByName(Schema afterSchema, TableChanges.TableChange stored) {
        List<Field> added = new ArrayList<>();
        List<String> dropped = new ArrayList<>();

        if (afterSchema == null || stored == null || stored.getTable() == null) {
            return new SchemaDiff(added, dropped, new LinkedHashMap<>());
        }

        // Detect added: fields present in afterSchema but absent from stored
        for (Field field : afterSchema.fields()) {
            if (stored.getTable().columnWithName(field.name()) == null) {
                added.add(field);
            }
        }

        // Detect dropped: columns present in stored but absent from afterSchema
        for (Column col : stored.getTable().columns()) {
            if (afterSchema.field(col.name()) == null) {
                dropped.add(col.name());
            }
        }

        return new SchemaDiff(added, dropped, new LinkedHashMap<>());
    }

    // ─── Quoting helpers ──────────────────────────────────────────────────────

    /** Wrap a name in backticks if not already quoted. */
    public static String identifier(String name) {
        if (name.startsWith("`") && name.endsWith("`")) {
            return name;
        }
        return "`" + name + "`";
    }

    /** Return a fully-qualified {@code `db`.`table`} identifier string. */
    public static String quoteTableIdentifier(String db, String table) {
        return identifier(db) + "." + identifier(table);
    }

    /**
     * Format a default value (already a plain Java string, not a raw SQL expression) into a form
     * suitable for a Doris {@code DEFAULT} clause.
     *
     * <p>The caller is expected to pass a <em>deserialized</em> value — e.g. obtained from the
     * Kafka Connect schema via {@code field.schema().defaultValue().toString()} — rather than a raw
     * PG SQL expression. This avoids having to strip PG-specific type casts ({@code ::text}, etc.).
     *
     * <ul>
     *   <li>SQL keywords ({@code NULL}, {@code CURRENT_TIMESTAMP}, {@code TRUE}, {@code FALSE}) are
     *       returned as-is.
     *   <li>Numeric literals are returned as-is (no quotes).
     *   <li>Everything else is wrapped in single quotes.
     * </ul>
     */
    public static String quoteDefaultValue(String defaultValue) {
        if (defaultValue == null) {
            return null;
        }
        if (defaultValue.equalsIgnoreCase("current_timestamp")
                || defaultValue.equalsIgnoreCase("null")
                || defaultValue.equalsIgnoreCase("true")
                || defaultValue.equalsIgnoreCase("false")) {
            return defaultValue;
        }
        try {
            Double.parseDouble(defaultValue);
            return defaultValue;
        } catch (NumberFormatException ignored) {
            // fall through
        }
        return "'" + defaultValue.replace("'", "''") + "'";
    }

    /** Escape single quotes inside a COMMENT string. */
    public static String quoteComment(String comment) {
        if (comment == null) {
            return "";
        }
        return comment.replace("'", "''");
    }

    // ─── DDL builders ─────────────────────────────────────────────────────────

    /**
     * Build {@code ALTER TABLE ... ADD COLUMN} SQL.
     *
     * @param db target database
     * @param table target table
     * @param colName column name
     * @param colType Doris column type string (including optional NOT NULL)
     * @param defaultValue optional DEFAULT value; {@code null} = omit DEFAULT clause
     * @param comment optional COMMENT; {@code null}/empty = omit COMMENT clause
     */
    public static String buildAddColumnSql(
            String db,
            String table,
            String colName,
            String colType,
            String defaultValue,
            String comment) {
        StringBuilder sb =
                new StringBuilder(
                        String.format(
                                ADD_DDL,
                                quoteTableIdentifier(db, table),
                                identifier(colName),
                                colType));
        if (defaultValue != null) {
            sb.append(" DEFAULT ").append(quoteDefaultValue(defaultValue));
        }
        appendComment(sb, comment);
        return sb.toString();
    }

    /** Build {@code ALTER TABLE ... DROP COLUMN} SQL. */
    public static String buildDropColumnSql(String db, String table, String colName) {
        return String.format(DROP_DDL, quoteTableIdentifier(db, table), identifier(colName));
    }

    // ─── Type mapping ─────────────────────────────────────────────────────────

    /** Convert a Debezium Column to a Doris column type string (via PG type name). */
    public static String columnToDorisType(Column column) {
        return pgTypeNameToDorisType(column.typeName(), column.length(), column.scale().orElse(-1));
    }

    /** Map a PostgreSQL native type name to a Doris type string. */
    static String pgTypeNameToDorisType(String pgTypeName, int length, int scale) {
        Preconditions.checkNotNull(pgTypeName);
        // Debezium uses underscore prefix for PostgreSQL array types (_int4, _text, etc.)
        if (pgTypeName.startsWith("_")) {
            String innerDorisType = pgTypeNameToDorisType(pgTypeName.substring(1), length, scale);
            return String.format("%s<%s>", DorisType.ARRAY, innerDorisType);
        }
        switch (pgTypeName.toLowerCase()) {
            case "bool":
                return DorisType.BOOLEAN;
            case "bit":
                return length == 1 ? DorisType.BOOLEAN : DorisType.STRING;
            case "int2":
            case "smallserial":
                return DorisType.SMALLINT;
            case "int4":
            case "serial":
                return DorisType.INT;
            case "int8":
            case "bigserial":
                return DorisType.BIGINT;
            case "float4":
                return DorisType.FLOAT;
            case "float8":
                return DorisType.DOUBLE;
            case "numeric":
                {
                    int p = length > 0 ? Math.min(length, 38) : 38;
                    int s = scale >= 0 ? scale : 9;
                    return String.format("%s(%d, %d)", DorisType.DECIMAL, p, s);
                }
            case "bpchar":
                {
                    if (length <= 0) {
                        return DorisType.STRING;
                    }
                    int len = length * 3;
                    if (len > 255) {
                        return String.format("%s(%s)", DorisType.VARCHAR, len);
                    } else {
                        return String.format("%s(%s)", DorisType.CHAR, len);
                    }
                }
            case "date":
                return DorisType.DATE;
            case "timestamp":
            case "timestamptz":
                {
                    int s = (scale >= 0 && scale <= 6) ? scale : 6;
                    return String.format("%s(%d)", DorisType.DATETIME, s);
                }
                // All remaining types map to STRING (aligned with JdbcPostgreSQLClient)
            case "point":
            case "line":
            case "lseg":
            case "box":
            case "path":
            case "polygon":
            case "circle":
            case "varchar":
            case "text":
            case "time":
            case "timetz":
            case "interval":
            case "cidr":
            case "inet":
            case "macaddr":
            case "macaddr8":
            case "varbit":
            case "uuid":
            case "bytea":
            case "xml":
            case "hstore":
                return DorisType.STRING;
            case "json":
            case "jsonb":
                return DorisType.JSON;
            default:
                LOG.warn("Unrecognized PostgreSQL type '{}', defaulting to STRING", pgTypeName);
                return DorisType.STRING;
        }
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    private static void appendComment(StringBuilder sb, String comment) {
        if (comment != null && !comment.isEmpty()) {
            sb.append(" COMMENT '").append(quoteComment(comment)).append("'");
        }
    }
}
