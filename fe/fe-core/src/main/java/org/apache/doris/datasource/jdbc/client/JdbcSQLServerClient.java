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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import java.sql.Types;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JdbcSQLServerClient extends JdbcClient {

    // TYPE_NAME of an IDENTITY column decorates the base type: "int identity", "decimal() identity",
    // "numeric(18, 0) identity", "decimal(18,0) IDENTITY(1,1)". IDENTITY is only allowed on these base types.
    private static final Pattern IDENTITY_TYPE_NAME = Pattern.compile(
            "^(tinyint|smallint|int|bigint|decimal|numeric)\\s*(\\([^)]*\\))?\\s+identity(\\s*\\([^)]*\\))?$",
            Pattern.CASE_INSENSITIVE);

    protected JdbcSQLServerClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    protected String getSchemaPatternForDatabaseNameList() {
        // "%" is a JDBC schemaPattern wildcard that matches all schemas. mssql-jdbc 13.4 filters
        // built-in schemas when catalog is non-empty and schemaPattern is null.
        return "%";
    }

    /**
     * The base type of an IDENTITY column's TYPE_NAME, or the name unchanged.
     * <p>
     * The decoration is trusted only when {@code DATA_TYPE} is the code of the named base type: an alias type
     * may legally be named like that ({@code CREATE TYPE dbo.[int identity] FROM varchar(10)}, or
     * {@code dbo.[int alias]}), and it then has to be resolved by its code, not by the words of its name.
     */
    static String identityBaseType(String typeName, int dataType) {
        Matcher matcher = IDENTITY_TYPE_NAME.matcher(typeName);
        if (!matcher.matches()) {
            return typeName;
        }
        String baseType = matcher.group(1).toLowerCase(Locale.ROOT);
        boolean codeMatches;
        switch (baseType) {
            case "tinyint":
                codeMatches = dataType == Types.TINYINT;
                break;
            case "smallint":
                codeMatches = dataType == Types.SMALLINT;
                break;
            case "int":
                codeMatches = dataType == Types.INTEGER;
                break;
            case "bigint":
                codeMatches = dataType == Types.BIGINT;
                break;
            default:
                codeMatches = dataType == Types.DECIMAL || dataType == Types.NUMERIC;
                break;
        }
        return codeMatches ? baseType : typeName;
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String originSqlserverType = fieldSchema.getDataTypeName().orElse("unknown");
        // An IDENTITY column is reported as "int identity" or "decimal(18,0) identity": only the base type
        // is matched below. Any other name is matched as it is: system type names are single words, and a
        // user-defined alias type may be named with spaces or parentheses ("int alias") and must not be
        // mistaken for the system type its name starts with.
        String sqlserverType = identityBaseType(originSqlserverType, fieldSchema.getDataType());

        switch (sqlserverType) {
            case "bit":
                return Type.BOOLEAN;
            case "tinyint":
            case "smallint":
                return Type.SMALLINT;
            case "int":
                return Type.INT;
            case "bigint":
                return Type.BIGINT;
            case "real":
                return Type.FLOAT;
            case "float":
                return Type.DOUBLE;
            case "money":
                return ScalarType.createDecimalV3Type(19, 4);
            case "smallmoney":
                return ScalarType.createDecimalV3Type(10, 4);
            case "decimal":
            case "numeric": {
                int precision = fieldSchema.getColumnSize().orElse(0);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                return createDecimalOrStringType(precision, scale);
            }
            case "date":
                return ScalarType.createDateV2Type();
            case "datetime":
            case "datetime2":
            case "smalldatetime": {
                // postgres can support microsecond
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "char":
            case "varchar":
            case "nchar":
            case "nvarchar":
            case "text":
            case "ntext":
            case "time":
            case "datetimeoffset":
            case "uniqueidentifier":
            case "timestamp":
                return ScalarType.createStringType();
            case "image":
            case "binary":
            case "varbinary":
                return enableMappingVarbinary ? ScalarType.createVarbinaryType(fieldSchema.requiredColumnSize())
                        : ScalarType.createStringType();
            case "xml":
            case "sql_variant":
            case "geometry":
            case "geography":
            case "hierarchyid":
            case "json":
            case "vector":
                // SQL Server system types that Doris does not support. They are listed explicitly
                // so that they never reach the JDBC type code fallback below.
                return Type.UNSUPPORTED;
            default:
                return jdbcTypeCodeToDoris(fieldSchema);
        }
    }

    /**
     * Fallback for type names that are not SQL Server system types.
     * <p>
     * User-defined alias types ({@code CREATE TYPE dbo.my_type FROM varchar(50)}) are reported by
     * {@code DatabaseMetaData.getColumns()} with {@code TYPE_NAME} set to the alias name, so they can not be
     * matched by name. {@code DATA_TYPE}, {@code COLUMN_SIZE} and {@code DECIMAL_DIGITS} still describe the
     * base type, so the standard {@link Types} code is used to resolve the Doris type. The mapping mirrors
     * the name based one above.
     * <p>
     * Binary codes are deliberately not mapped: mssql-jdbc also reports CLR user-defined types
     * (geometry, geography, hierarchyid, ...) as {@link Types#VARBINARY}, so they can not be told apart from
     * an alias over a binary type by the type code alone. Vendor specific codes stay unsupported as well.
     */
    private Type jdbcTypeCodeToDoris(JdbcFieldSchema fieldSchema) {
        switch (fieldSchema.getDataType()) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Type.BOOLEAN;
            // SQL Server tinyint is unsigned (0 to 255), so it needs SMALLINT
            case Types.TINYINT:
            case Types.SMALLINT:
                return Type.SMALLINT;
            case Types.INTEGER:
                return Type.INT;
            case Types.BIGINT:
                return Type.BIGINT;
            case Types.REAL:
                return Type.FLOAT;
            case Types.FLOAT:
            case Types.DOUBLE:
                return Type.DOUBLE;
            case Types.DECIMAL:
            case Types.NUMERIC: {
                // money and smallmoney are reported as DECIMAL(19,4) and DECIMAL(10,4)
                int precision = fieldSchema.getColumnSize().orElse(0);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                return createDecimalOrStringType(precision, scale);
            }
            case Types.DATE:
                return ScalarType.createDateV2Type();
            case Types.TIMESTAMP: {
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.TIME:
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }
}
