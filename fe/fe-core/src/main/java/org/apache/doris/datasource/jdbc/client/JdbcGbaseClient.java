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

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.function.Consumer;

public class JdbcGbaseClient extends JdbcClient {

    protected JdbcGbaseClient(JdbcClientConfig jdbcClientConfig) {
        super(jdbcClientConfig);
    }

    @Override
    public List<String> getDatabaseNameList() {
        Connection conn = null;
        ResultSet rs = null;
        List<String> remoteDatabaseNames = Lists.newArrayList();
        try {
            conn = getConnection();
            if (isOnlySpecifiedDatabase && includeDatabaseMap.isEmpty() && excludeDatabaseMap.isEmpty()) {
                String currentDatabase = conn.getCatalog();
                remoteDatabaseNames.add(currentDatabase);
            } else {
                rs = conn.getMetaData().getCatalogs();
                while (rs.next()) {
                    remoteDatabaseNames.add(rs.getString("TABLE_CAT"));
                }
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get database name list from jdbc", e);
        } finally {
            close(rs, conn);
        }
        return filterDatabaseNames(remoteDatabaseNames);
    }

    @Override
    protected void processTable(String remoteDbName, String remoteTableName, String[] tableTypes,
            Consumer<ResultSet> resultSetConsumer) {
        Connection conn = null;
        ResultSet rs = null;
        try {
            conn = super.getConnection();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            rs = databaseMetaData.getTables(remoteDbName, null, remoteTableName, tableTypes);
            resultSetConsumer.accept(rs);
        } catch (SQLException e) {
            throw new JdbcClientException("Failed to process table", e);
        } finally {
            close(rs, conn);
        }
    }

    @Override
    protected ResultSet getRemoteColumns(DatabaseMetaData databaseMetaData, String catalogName, String remoteDbName,
            String remoteTableName) throws SQLException {
        return databaseMetaData.getColumns(remoteDbName, null, remoteTableName, null);
    }

    @Override
    public List<JdbcFieldSchema> getJdbcColumnsInfo(String remoteDbName, String remoteTableName) {
        Connection conn = null;
        ResultSet rs = null;
        List<JdbcFieldSchema> tableSchema = Lists.newArrayList();
        try {
            conn = getConnection();
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = getCatalogName(conn);
            rs = getRemoteColumns(databaseMetaData, catalogName, remoteDbName, remoteTableName);
            while (rs.next()) {
                JdbcFieldSchema field = new JdbcFieldSchema(rs);
                tableSchema.add(field);
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
    protected Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        switch (fieldSchema.getDataType()) {
            case Types.TINYINT:
                return Type.TINYINT;
            case Types.SMALLINT:
                return Type.SMALLINT;
            case Types.INTEGER:
                return Type.INT;
            case Types.BIGINT:
                return Type.BIGINT;
            case Types.FLOAT:
            case Types.REAL:
                return Type.FLOAT;
            case Types.DOUBLE:
                return Type.DOUBLE;
            case Types.NUMERIC:
            case Types.DECIMAL: {
                int precision = fieldSchema.getColumnSize()
                        .orElseThrow(() -> new IllegalArgumentException("Precision not present"));
                int scale = fieldSchema.getDecimalDigits()
                        .orElseThrow(() -> new JdbcClientException("Scale not present"));
                return createDecimalOrStringType(precision, scale);
            }
            case Types.DATE:
                return Type.DATEV2;
            case Types.TIMESTAMP: {
                int scale = fieldSchema.getDecimalDigits().orElse(0);
                if (scale > 6) {
                    scale = 6;
                }
                return ScalarType.createDatetimeV2Type(scale);
            }
            case Types.CHAR:
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.getColumnSize()
                        .orElseThrow(() -> new IllegalArgumentException("Length not present")));
                return charType;
            case Types.TIME:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }
}
