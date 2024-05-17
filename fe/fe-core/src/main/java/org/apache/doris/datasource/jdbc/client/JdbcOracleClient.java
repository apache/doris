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
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class JdbcOracleClient extends JdbcClient {

    protected JdbcOracleClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    protected JdbcOracleClient(JdbcClientConfig jdbcClientConfig, String dbType) {
        super(jdbcClientConfig);
        this.dbType = dbType;
    }

    @Override
    public String getTestQuery() {
        return "SELECT 1 FROM dual";
    }

    @Override
    public List<JdbcFieldSchema> getJdbcColumnsInfo(String localDbName, String localTableName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<JdbcFieldSchema> tableSchema = Lists.newArrayList();
        String remoteDbName = getRemoteDatabaseName(localDbName);
        String remoteTableName = getRemoteTableName(localDbName, localTableName);
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = getCatalogName(conn);
            String modifiedTableName;
            boolean isModify = false;
            if (remoteTableName.contains("/")) {
                modifiedTableName = modifyTableNameIfNecessary(remoteTableName);
                isModify = !modifiedTableName.equals(remoteTableName);
                if (isModify) {
                    rs = getRemoteColumns(databaseMetaData, catalogName, remoteDbName, modifiedTableName);
                } else {
                    rs = getRemoteColumns(databaseMetaData, catalogName, remoteDbName, remoteTableName);
                }
            } else {
                rs = getRemoteColumns(databaseMetaData, catalogName, remoteDbName, remoteTableName);
            }
            while (rs.next()) {
                if (isModify && isTableModified(rs.getString("TABLE_NAME"), remoteTableName)) {
                    continue;
                }
                tableSchema.add(new JdbcFieldSchema(rs));
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get table name list from jdbc for table %s:%s", e, remoteTableName,
                Util.getRootCauseMessage(e));
        } finally {
            close(rs, conn);
        }
        return tableSchema;
    }

    @Override
    protected String modifyTableNameIfNecessary(String remoteTableName) {
        return remoteTableName.replace("/", "%");
    }

    @Override
    protected boolean isTableModified(String modifiedTableName, String actualTableName) {
        return !modifiedTableName.equals(actualTableName);
    }

    @Override
    protected Set<String> getFilterInternalDatabases() {
        return ImmutableSet.<String>builder()
                .add("ctxsys")
                .add("flows_files")
                .add("mdsys")
                .add("outln")
                .add("sys")
                .add("system")
                .add("xdb")
                .add("xs$null")
                .build();
    }

    @Override
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        String oracleType = fieldSchema.getDataTypeName().orElse("unknown");
        if (oracleType.startsWith("INTERVAL")) {
            oracleType = oracleType.substring(0, 8);
        } else if (oracleType.startsWith("TIMESTAMP")) {
            if (oracleType.contains("TIME ZONE") || oracleType.contains("LOCAL TIME ZONE")) {
                return Type.UNSUPPORTED;
            }
            // oracle can support nanosecond, will lose precision
            int scale = fieldSchema.getDecimalDigits().orElse(0);
            if (scale > 6) {
                scale = 6;
            }
            return ScalarType.createDatetimeV2Type(scale);
        }
        switch (oracleType) {
            /**
             * The data type NUMBER(p,s) of oracle has some different of doris decimal type in semantics.
             * For Oracle Number(p,s) type:
             * 1. if s<0 , it means this is an Interger.
             *    This NUMBER(p,s) has (p+|s| ) significant digit, and rounding will be performed at s position.
             *    eg: if we insert 1234567 into NUMBER(5,-2) type, then the oracle will store 1234500.
             *    In this case, Doris will use INT type (TINYINT/SMALLINT/INT/.../LARGEINT).
             * 2. if s>=0 && s<p , it just like doris Decimal(p,s) behavior.
             * 3. if s>=0 && s>p, it means this is a decimal(like 0.xxxxx).
             *    p represents how many digits can be left to the left after the decimal point,
             *    the figure after the decimal point s will be rounded.
             *    eg: we can not insert 0.0123456 into NUMBER(5,7) type,
             *    because there must be two zeros on the right side of the decimal point,
             *    we can insert 0.0012345 into NUMBER(5,7) type.
             *    In this case, Doris will use DECIMAL(s,s)
             * 4. if we don't specify p and s for NUMBER(p,s), just NUMBER, the p and s of NUMBER are uncertain.
             *    In this case, doris can not determine p and s, so doris can not determine data type.
             */
            case "NUMBER":
                int precision = fieldSchema.getColumnSize().orElse(0);
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale <= 0) {
                    precision -= scale;
                    if (precision < 3) {
                        return Type.TINYINT;
                    } else if (precision < 5) {
                        return Type.SMALLINT;
                    } else if (precision < 10) {
                        return Type.INT;
                    } else if (precision < 19) {
                        return Type.BIGINT;
                    } else if (precision < 39) {
                        // LARGEINT supports up to 38 numbers.
                        return Type.LARGEINT;
                    } else {
                        return ScalarType.createStringType();
                    }
                }
                // scale > 0
                if (precision < scale) {
                    precision = scale;
                }
                return createDecimalOrStringType(precision, scale);
            case "FLOAT":
                return Type.DOUBLE;
            case "DATE":
                // can save date and time with second precision
                return ScalarType.createDatetimeV2Type(0);
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CHAR":
            case "NCHAR":
            case "LONG":
            case "RAW":
            case "LONG RAW":
            case "INTERVAL":
            case "CLOB":
                return ScalarType.createStringType();
            case "BLOB":
            case "NCLOB":
            case "BFILE":
            case "BINARY_FLOAT":
            case "BINARY_DOUBLE":
            default:
                return Type.UNSUPPORTED;
        }
    }
}
