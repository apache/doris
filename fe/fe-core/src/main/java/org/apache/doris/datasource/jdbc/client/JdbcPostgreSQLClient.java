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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class JdbcPostgreSQLClient extends JdbcClient {

    private static final String[] supportedInnerType = new String[] {
            "int2", "int4", "int8", "smallserial", "serial",
            "bigserial", "float4", "float8", "timestamp", "timestamptz",
            "date", "bool", "bpchar", "varchar", "text"
    };

    protected JdbcPostgreSQLClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    public List<JdbcFieldSchema> getJdbcColumnsInfo(String localDbName, String localTableName) {
        Connection conn = null;
        ResultSet rs = null;
        List<JdbcFieldSchema> tableSchema = Lists.newArrayList();
        String remoteDbName = getRemoteDatabaseName(localDbName);
        String remoteTableName = getRemoteTableName(localDbName, localTableName);
        try {
            conn = getConnection();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = getCatalogName(conn);
            rs = getRemoteColumns(databaseMetaData, catalogName, remoteDbName, remoteTableName);
            while (rs.next()) {
                int dataType = rs.getInt("DATA_TYPE");
                int arrayDimensions = 0;
                if (dataType == Types.ARRAY) {
                    String columnName = rs.getString("COLUMN_NAME");
                    try (PreparedStatement pstmt = conn.prepareStatement(
                            String.format("SELECT array_ndims(%s) FROM %s.%s LIMIT 1",
                                    columnName, remoteDbName, remoteTableName))) {
                        try (ResultSet arrayRs = pstmt.executeQuery()) {
                            if (arrayRs.next()) {
                                arrayDimensions = arrayRs.getInt(1);
                            }
                        }
                    }
                }
                tableSchema.add(new JdbcFieldSchema(rs, arrayDimensions));
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get jdbc columns info for remote table `%s.%s`: %s",
                    remoteDbName, remoteTableName, Util.getRootCauseMessage(e));
        } finally {
            close(rs, conn);
        }
        return tableSchema;
    }

    @Override
    protected String[] getTableTypes() {
        return new String[] {"TABLE", "PARTITIONED TABLE", "VIEW", "MATERIALIZED VIEW", "FOREIGN TABLE"};
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String pgType = fieldSchema.getDataTypeName().orElse("unknown");
        switch (pgType) {
            case "int2":
            case "smallserial":
                return Type.SMALLINT;
            case "int4":
            case "serial":
                return Type.INT;
            case "int8":
            case "bigserial":
                return Type.BIGINT;
            case "numeric": {
                int precision = fieldSchema.getColumnSize().orElse(0);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                return createDecimalOrStringType(precision, scale);
            }
            case "float4":
                return Type.FLOAT;
            case "float8":
                return Type.DOUBLE;
            case "bpchar":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.getColumnSize().orElse(0));
                return charType;
            case "timestamp":
            case "timestamptz": {
                // postgres can support microsecond
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case "date":
                return ScalarType.createDateV2Type();
            case "bool":
                return Type.BOOLEAN;
            case "bit":
                if (fieldSchema.getColumnSize().orElse(0) == 1) {
                    return Type.BOOLEAN;
                } else {
                    return ScalarType.createStringType();
                }
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
            case "varbit":
            case "uuid":
            case "bytea":
            case "json":
            case "jsonb":
                return ScalarType.createStringType();
            default: {
                if (fieldSchema.getDataType() == Types.ARRAY && pgType.startsWith("_")) {
                    return convertArrayType(fieldSchema);
                } else {
                    return Type.UNSUPPORTED;
                }
            }
        }
    }

    private Type convertArrayType(JdbcFieldSchema fieldSchema) {
        int arrayDimensions = fieldSchema.getArrayDimensions().orElse(0);
        if (arrayDimensions == 0) {
            return Type.UNSUPPORTED;
        }

        String innerType = fieldSchema.getDataTypeName().orElse("unknown").substring(1);

        boolean isSupported = Arrays.asList(supportedInnerType).contains(innerType);
        if (!isSupported) {
            return Type.UNSUPPORTED;
        }
        if (innerType.equals("bpchar")) {
            innerType = "text";
        }
        JdbcFieldSchema innerFieldSchema = new JdbcFieldSchema(fieldSchema);
        innerFieldSchema.setDataTypeName(Optional.of(innerType));
        Type arrayInnerType = jdbcTypeToDoris(innerFieldSchema);
        Type arrayType = ArrayType.create(arrayInnerType, true);
        for (int i = 1; i < arrayDimensions; i++) {
            arrayType = ArrayType.create(arrayType, true);
        }
        return arrayType;
    }
}
