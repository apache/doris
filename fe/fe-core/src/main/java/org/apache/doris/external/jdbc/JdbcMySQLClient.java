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

package org.apache.doris.external.jdbc;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import avro.shaded.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

public class JdbcMySQLClient extends JdbcClient {
    protected JdbcMySQLClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
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
    protected ResultSet getColumns(DatabaseMetaData databaseMetaData, String catalogName, String schemaName,
                                   String tableName) throws SQLException {
        return databaseMetaData.getColumns(schemaName, null, tableName, null);
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
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
                case "DECIMAL":
                    int precision = fieldSchema.getColumnSize() + 1;
                    int scale = fieldSchema.getDecimalDigits();
                    return createDecimalOrStringType(precision, scale);
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
            case "LARGEINT": // for jdbc catalog connecting Doris database
                return Type.LARGEINT;
            case "DATE":
            case "DATEV2":
                return ScalarType.createDateV2Type();
            case "TIMESTAMP":
            case "DATETIME":
            case "DATETIMEV2": // for jdbc catalog connecting Doris database
                // mysql can support microsecond
                // todo(gaoxin): Get real precision of DATETIMEV2
                return ScalarType.createDatetimeV2Type(JDBC_DATETIME_SCALE);
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "DECIMAL":
            case "DECIMALV3": // for jdbc catalog connecting Doris database
                int precision = fieldSchema.getColumnSize();
                int scale = fieldSchema.getDecimalDigits();
                return createDecimalOrStringType(precision, scale);
            case "CHAR":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.columnSize);
                return charType;
            case "VARCHAR":
                return ScalarType.createVarcharType(fieldSchema.columnSize);
            case "TIME":
            case "TINYTEXT":
            case "TEXT":
            case "MEDIUMTEXT":
            case "LONGTEXT":
            case "TINYBLOB":
            case "BLOB":
            case "MEDIUMBLOB":
            case "LONGBLOB":
            case "TINYSTRING":
            case "STRING":
            case "MEDIUMSTRING":
            case "LONGSTRING":
            case "JSON":
            case "SET":
            case "BIT":
            case "BINARY":
            case "VARBINARY":
            case "ENUM":
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }
}
