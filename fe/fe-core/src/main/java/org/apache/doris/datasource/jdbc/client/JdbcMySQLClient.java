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

import org.apache.doris.analysis.DefaultValueExprDef;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.Util;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class JdbcMySQLClient extends JdbcClient {

    private boolean convertDateToNull = false;
    private boolean isDoris = false;

    protected JdbcMySQLClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
        convertDateToNull = isConvertDatetimeToNull(jdbcClientConfig);
        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            conn = super.getConnection();
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SHOW VARIABLES LIKE 'version_comment'");
            if (rs.next()) {
                String versionComment = rs.getString("Value");
                isDoris = versionComment.toLowerCase().contains("doris");
            }
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to determine MySQL Version Comment", e);
        } finally {
            close(rs, stmt, conn);
        }
    }

    @Override
    protected String getDatabaseQuery() {
        return "SHOW DATABASES";
    }

    @Override
    protected List<String> getSpecifiedDatabase(Connection conn) {
        List<String> databaseNames = Lists.newArrayList();
        try {
            databaseNames.add(conn.getCatalog());
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get specified database name from jdbc", e);
        } finally {
            close(conn);
        }
        return databaseNames;
    }

    @Override
    protected void processTable(String dbName, String tableName, String[] tableTypes,
                                Consumer<ResultSet> resultSetConsumer) {
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = super.getConnection();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            rs = databaseMetaData.getTables(dbName, null, tableName, tableTypes);
            resultSetConsumer.accept(rs);
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to process table", e);
        } finally {
            close(rs, conn);
        }
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "VIEW", "SYSTEM VIEW"};
    }

    @Override
    protected ResultSet getColumns(DatabaseMetaData databaseMetaData, String catalogName, String schemaName,
                                   String tableName) throws SQLException {
        return databaseMetaData.getColumns(schemaName, null, tableName, null);
    }

    /**
     * get all columns of one table
     */
    @Override
    public List<JdbcFieldSchema> getJdbcColumnsInfo(String dbName, String tableName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<JdbcFieldSchema> tableSchema = com.google.common.collect.Lists.newArrayList();
        // if isLowerCaseTableNames == true, tableName is lower case
        // but databaseMetaData.getColumns() is case sensitive
        if (isLowerCaseTableNames) {
            dbName = lowerDBToRealDB.get(dbName);
            tableName = lowerTableToRealTable.get(tableName);
        }
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = getCatalogName(conn);
            rs = getColumns(databaseMetaData, catalogName, dbName, tableName);
            List<String> primaryKeys = getPrimaryKeys(databaseMetaData, catalogName, dbName, tableName);
            Map<String, String> mapFieldtoType = null;
            while (rs.next()) {
                JdbcFieldSchema field = new JdbcFieldSchema();
                field.setColumnName(rs.getString("COLUMN_NAME"));
                field.setDataType(rs.getInt("DATA_TYPE"));

                // in mysql-jdbc-connector-8.0.*, TYPE_NAME of the HLL column in doris will be "UNKNOWN"
                // in mysql-jdbc-connector-5.1.*, TYPE_NAME of the HLL column in doris will be "HLL"
                // in mysql-jdbc-connector-8.0.*, TYPE_NAME of BITMAP column in doris will be "BIT"
                // in mysql-jdbc-connector-5.1.*, TYPE_NAME of BITMAP column in doris will be "BITMAP"
                field.setDataTypeName(rs.getString("TYPE_NAME"));
                if (isDoris) {
                    mapFieldtoType = getColumnsDataTypeUseQuery(dbName, tableName);
                    field.setDataTypeName(mapFieldtoType.get(rs.getString("COLUMN_NAME")));
                }
                field.setKey(primaryKeys.contains(field.getColumnName()));
                field.setColumnSize(rs.getInt("COLUMN_SIZE"));
                field.setDecimalDigits(rs.getInt("DECIMAL_DIGITS"));
                field.setNumPrecRadix(rs.getInt("NUM_PREC_RADIX"));
                /*
                   Whether it is allowed to be NULL
                   0 (columnNoNulls)
                   1 (columnNullable)
                   2 (columnNullableUnknown)
                 */
                field.setAllowNull(rs.getInt("NULLABLE") != 0);
                field.setRemarks(rs.getString("REMARKS"));
                field.setCharOctetLength(rs.getInt("CHAR_OCTET_LENGTH"));
                String isAutoincrement = rs.getString("IS_AUTOINCREMENT");
                field.setAutoincrement("YES".equalsIgnoreCase(isAutoincrement));
                field.setDefaultValue(rs.getString("COLUMN_DEF"));
                tableSchema.add(field);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get table name list from jdbc for table %s:%s", e, tableName,
                Util.getRootCauseMessage(e));
        } finally {
            close(rs, conn);
        }
        return tableSchema;
    }

    @Override
    public List<Column> getColumnsFromJdbc(String dbName, String tableName) {
        List<JdbcFieldSchema> jdbcTableSchema = getJdbcColumnsInfo(dbName, tableName);
        List<Column> dorisTableSchema = Lists.newArrayListWithCapacity(jdbcTableSchema.size());
        for (JdbcFieldSchema field : jdbcTableSchema) {
            DefaultValueExprDef defaultValueExprDef = null;
            if (field.getDefaultValue() != null) {
                String colDefaultValue = field.getDefaultValue().toLowerCase();
                // current_timestamp()
                if (colDefaultValue.startsWith("current_timestamp")) {
                    long precision = 0;
                    if (colDefaultValue.contains("(")) {
                        String substring = colDefaultValue.substring(18, colDefaultValue.length() - 1).trim();
                        precision = substring.isEmpty() ? 0 : Long.parseLong(substring);
                    }
                    defaultValueExprDef = new DefaultValueExprDef("now", precision);
                }
            }
            dorisTableSchema.add(new Column(field.getColumnName(),
                    jdbcTypeToDoris(field), field.isKey(), null,
                    field.isAllowNull(), field.isAutoincrement(), field.getDefaultValue(), field.getRemarks(),
                    true, defaultValueExprDef, -1, null));
        }
        return dorisTableSchema;
    }

    protected List<String> getPrimaryKeys(DatabaseMetaData databaseMetaData, String catalogName,
                                          String dbName, String tableName) throws SQLException {
        ResultSet rs = null;
        List<String> primaryKeys = Lists.newArrayList();

        rs = databaseMetaData.getPrimaryKeys(dbName, null, tableName);
        while (rs.next()) {
            String columnName = rs.getString("COLUMN_NAME");
            primaryKeys.add(columnName);
        }
        rs.close();

        return primaryKeys;
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        // For Doris type
        if (isDoris) {
            return dorisTypeToDoris(fieldSchema.getDataTypeName().toUpperCase());
        }
        // For mysql type: "INT UNSIGNED":
        // fieldSchema.getDataTypeName().split(" ")[0] == "INT"
        // fieldSchema.getDataTypeName().split(" ")[1] == "UNSIGNED"
        String[] typeFields = fieldSchema.getDataTypeName().split(" ");
        String mysqlType = typeFields[0];
        // For unsigned int, should extend the type.
        if (typeFields.length > 1 && "UNSIGNED".equals(typeFields[1])) {
            switch (mysqlType) {
                case "TINYINT":
                    return Type.SMALLINT;
                case "SMALLINT":
                case "MEDIUMINT":
                    return Type.INT;
                case "INT":
                    return Type.BIGINT;
                case "BIGINT":
                    return Type.LARGEINT;
                case "DECIMAL": {
                    int precision = fieldSchema.getColumnSize() + 1;
                    int scale = fieldSchema.getDecimalDigits();
                    return createDecimalOrStringType(precision, scale);
                }
                case "DOUBLE":
                    // As of MySQL 8.0.17, the UNSIGNED attribute is deprecated
                    // for columns of type FLOAT, DOUBLE, and DECIMAL (and any synonyms)
                    // https://dev.mysql.com/doc/refman/8.0/en/numeric-type-syntax.html
                    // The maximum value may cause errors due to insufficient accuracy
                    return Type.DOUBLE;
                case "FLOAT":
                    return Type.FLOAT;
                default:
                    throw new JdbcClientException("Unknown UNSIGNED type of mysql, type: [" + mysqlType + "]");
            }
        }
        switch (mysqlType) {
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "TINYINT":
                return Type.TINYINT;
            case "SMALLINT":
            case "YEAR":
                return Type.SMALLINT;
            case "MEDIUMINT":
            case "INT":
                return Type.INT;
            case "BIGINT":
                return Type.BIGINT;
            case "DATE":
                if (convertDateToNull) {
                    fieldSchema.setAllowNull(true);
                }
                return ScalarType.createDateV2Type();
            case "TIMESTAMP":
            case "DATETIME": {
                // mysql can support microsecond
                // use columnSize to calculate the precision of timestamp/datetime
                int columnSize = fieldSchema.getColumnSize();
                int scale = columnSize > 19 ? columnSize - 20 : 0;
                if (scale > 6) {
                    scale = 6;
                }
                if (convertDateToNull) {
                    fieldSchema.setAllowNull(true);
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "DECIMAL": {
                int precision = fieldSchema.getColumnSize();
                int scale = fieldSchema.getDecimalDigits();
                return createDecimalOrStringType(precision, scale);
            }
            case "CHAR":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.columnSize);
                return charType;
            case "VARCHAR":
                return ScalarType.createVarcharType(fieldSchema.columnSize);
            case "BIT":
                if (fieldSchema.getColumnSize() == 1) {
                    return Type.BOOLEAN;
                } else {
                    return ScalarType.createStringType();
                }
            case "JSON":
                return ScalarType.createJsonbType();
            case "TIME":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "STRING":
            case "SET":
            case "BINARY":
            case "VARBINARY":
            case "ENUM":
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }

    private boolean isConvertDatetimeToNull(JdbcClientConfig jdbcClientConfig) {
        // Check if the JDBC URL contains "zeroDateTimeBehavior=convertToNull".
        return jdbcClientConfig.getJdbcUrl().contains("zeroDateTimeBehavior=convertToNull");
    }

    /**
     * get all columns like DatabaseMetaData.getColumns in mysql-jdbc-connector
     */
    private Map<String, String> getColumnsDataTypeUseQuery(String dbName, String tableName) {
        Connection conn = getConnection();
        ResultSet resultSet = null;
        Map<String, String> fieldtoType = Maps.newHashMap();

        StringBuilder queryBuf = new StringBuilder("SHOW FULL COLUMNS FROM ");
        queryBuf.append(tableName);
        queryBuf.append(" FROM ");
        queryBuf.append(dbName);
        try (Statement stmt = conn.createStatement()) {
            resultSet = stmt.executeQuery(queryBuf.toString());
            while (resultSet.next()) {
                // get column name
                String fieldName = resultSet.getString("Field");
                // get original type name
                String typeName = resultSet.getString("Type");
                fieldtoType.put(fieldName, typeName);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get column list from jdbc for table %s:%s", tableName,
                Util.getRootCauseMessage(e));
        } finally {
            close(resultSet, conn);
        }
        return fieldtoType;
    }

    private Type dorisTypeToDoris(String type) {
        if (type == null || type.isEmpty()) {
            return Type.UNSUPPORTED;
        }

        String upperType = type.toUpperCase();

        // For ARRAY type
        if (upperType.startsWith("ARRAY")) {
            String innerType = upperType.substring(6, upperType.length() - 1).trim();
            Type arrayInnerType = dorisTypeToDoris(innerType);
            return ArrayType.create(arrayInnerType, true);
        }

        int openParen = upperType.indexOf("(");
        String baseType = (openParen == -1) ? upperType : upperType.substring(0, openParen);

        switch (baseType) {
            case "BOOL":
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "TINYINT":
                return Type.TINYINT;
            case "INT":
                return Type.INT;
            case "SMALLINT":
                return Type.SMALLINT;
            case "BIGINT":
                return Type.BIGINT;
            case "LARGEINT":
                return Type.LARGEINT;
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "DECIMAL":
            case "DECIMALV3": {
                String[] params = upperType.substring(openParen + 1, upperType.length() - 1).split(",");
                int precision = Integer.parseInt(params[0].trim());
                int scale = Integer.parseInt(params[1].trim());
                return createDecimalOrStringType(precision, scale);
            }
            case "DATE":
            case "DATEV2":
                return ScalarType.createDateV2Type();
            case "DATETIME":
            case "DATETIMEV2": {
                int scale = (openParen == -1) ? 6
                        : Integer.parseInt(upperType.substring(openParen + 1, upperType.length() - 1));
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "CHAR":
            case "VARCHAR": {
                int length = Integer.parseInt(upperType.substring(openParen + 1, upperType.length() - 1));
                return baseType.equals("CHAR")
                    ? ScalarType.createCharType(length) : ScalarType.createVarcharType(length);
            }
            case "STRING":
            case "TEXT":
                return ScalarType.createStringType();
            case "JSON":
                return ScalarType.createJsonbType();
            case "HLL":
                return ScalarType.createHllType();
            case "BITMAP":
                return Type.BITMAP;
            default:
                return Type.UNSUPPORTED;
        }
    }
}
