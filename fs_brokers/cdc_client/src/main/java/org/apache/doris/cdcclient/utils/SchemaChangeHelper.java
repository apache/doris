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

import io.debezium.relational.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for generating Doris ALTER TABLE SQL from schema diffs. */
public class SchemaChangeHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeHelper.class);
    private static final String ADD_DDL = "ALTER TABLE %s ADD COLUMN %s %s";
    private static final String DROP_DDL = "ALTER TABLE %s DROP COLUMN %s";
    private static final int MAX_DECIMAL128_PRECISION = 38;
    private static final int MAX_CHAR_LENGTH = 255;
    private static final int MAX_VARCHAR_LENGTH = 65533;

    private SchemaChangeHelper() {}

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
     */
    public static String buildAddColumnSql(
            String db, String table, String colName, String colType) {
        return buildAddColumnSql(db, table, colName, colType, null);
    }

    /**
     * Build {@code ALTER TABLE ... ADD COLUMN} SQL with an optional column comment.
     *
     * @param db target database
     * @param table target table
     * @param colName column name
     * @param colType Doris column type string
     * @param comment optional COMMENT; {@code null}/empty = omit COMMENT clause
     */
    public static String buildAddColumnSql(
            String db, String table, String colName, String colType, String comment) {
        StringBuilder sb =
                new StringBuilder(
                        String.format(
                                ADD_DDL,
                                quoteTableIdentifier(db, table),
                                identifier(colName),
                                colType));
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

    /** Convert a Debezium MySQL Column to a Doris column type string. */
    public static String mysqlColumnToDorisType(Column column) {
        Preconditions.checkNotNull(column.typeName());
        String mysqlTypeName = column.typeName().toUpperCase();
        String[] typeFields = mysqlTypeName.split(" ");
        String mysqlType = typeFields[0];
        boolean unsigned = typeFields.length > 1 && "UNSIGNED".equals(typeFields[1]);
        int length = column.length();
        int scale = column.scale().orElse(-1);
        switch (mysqlType) {
            case "BOOLEAN":
            case "BOOL":
                return DorisType.BOOLEAN;
            case "TINYINT":
                return unsigned ? DorisType.SMALLINT : DorisType.TINYINT;
            case "SMALLINT":
            case "INT2":
            case "YEAR":
                return unsigned ? DorisType.INT : DorisType.SMALLINT;
            case "MEDIUMINT":
            case "INT":
            case "INTEGER":
            case "INT3":
            case "INT4":
                return unsigned ? DorisType.BIGINT : DorisType.INT;
            case "BIGINT":
            case "INT8":
                return unsigned ? DorisType.LARGEINT : DorisType.BIGINT;
            case "FLOAT":
            case "FLOAT4":
                return DorisType.FLOAT;
            case "DOUBLE":
            case "FLOAT8":
            case "REAL":
                return DorisType.DOUBLE;
            case "DECIMAL":
            case "DEC":
            case "FIXED":
            case "NUMERIC":
                {
                    int precision = length > 0 ? length : 10;
                    if (unsigned) {
                        precision++;
                    }
                    if (precision > MAX_DECIMAL128_PRECISION) {
                        return DorisType.STRING;
                    }
                    int decimalScale = scale >= 0 ? scale : 0;
                    return String.format("%s(%d, %d)", DorisType.DECIMAL, precision, decimalScale);
                }
            case "DATE":
                return DorisType.DATE;
            case "DATETIME":
            case "TIMESTAMP":
                {
                    int timeScale = mysqlTimeScale(length, scale);
                    return String.format("%s(%d)", DorisType.DATETIME, timeScale);
                }
            case "CHAR":
            case "NCHAR":
                return charToDorisType(length);
            case "VARCHAR":
            case "NVARCHAR":
                return varcharToDorisType(length);
            case "BIT":
                return length == 1 ? DorisType.BOOLEAN : DorisType.STRING;
            case "JSON":
                return DorisType.JSON;
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "BINARY":
            case "VARBINARY":
            case "TIME":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "STRING":
            case "SET":
            case "ENUM":
                return DorisType.STRING;
            default:
                LOG.warn("Unrecognized MySQL type '{}', defaulting to STRING", mysqlTypeName);
                return DorisType.STRING;
        }
    }

    private static int mysqlTimeScale(int length, int scale) {
        if (scale >= 0 && scale <= 6) {
            return scale;
        }
        if (length >= 0 && length <= 6) {
            return length;
        }
        return 0;
    }

    private static String charToDorisType(int length) {
        if (length <= 0) {
            return DorisType.STRING;
        }
        int len = length * 3;
        if (len > MAX_VARCHAR_LENGTH) {
            return DorisType.STRING;
        }
        if (len > MAX_CHAR_LENGTH) {
            return String.format("%s(%d)", DorisType.VARCHAR, len);
        }
        return String.format("%s(%d)", DorisType.CHAR, len);
    }

    private static String varcharToDorisType(int length) {
        if (length <= 0) {
            return DorisType.STRING;
        }
        int len = length * 3;
        if (len > MAX_VARCHAR_LENGTH) {
            return DorisType.STRING;
        }
        return String.format("%s(%d)", DorisType.VARCHAR, len);
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
                    if (length > MAX_DECIMAL128_PRECISION) {
                        return DorisType.STRING;
                    }
                    int p = length > 0 ? length : 38;
                    int s = scale >= 0 ? scale : 9;
                    return String.format("%s(%d, %d)", DorisType.DECIMAL, p, s);
                }
            case "bpchar":
                return charToDorisType(length);
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
