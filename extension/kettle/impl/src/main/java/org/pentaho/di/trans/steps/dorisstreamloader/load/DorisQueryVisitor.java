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

package org.pentaho.di.trans.steps.dorisstreamloader.load;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class DorisQueryVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(DorisQueryVisitor.class);

    private final DorisJdbcConnectionProvider jdbcConnProvider;
    private final String database;
    private final String table;

    public DorisQueryVisitor(DorisJdbcConnectionProvider jdbcConnProvider, String database, String table) {
        this.jdbcConnProvider = jdbcConnProvider;
        this.database = database;
        this.table = table;
    }

    public List<String> getAllTables() throws SQLException, ClassNotFoundException {
        final String query = "select `TABLE_NAME` from `information_schema`.`TABLES` where `TABLE_SCHEMA`=?;";
        List<String> tablenames = new ArrayList<>();
        PreparedStatement stmt = jdbcConnProvider.getConnection().prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        stmt.setString(1, this.database);
        ResultSet rs = stmt.executeQuery();
        int currRowIndex = rs.getRow();
        rs.beforeFirst();
        while (rs.next()) {
            tablenames.add(rs.getString(1));
        }
        rs.absolute(currRowIndex);
        rs.close();
        jdbcConnProvider.close();
        return tablenames;
    }

    public List<Map<String, Object>> getTableColumnsMetaData() {
        final String query = "select `COLUMN_NAME`, `ORDINAL_POSITION`, `COLUMN_KEY`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` from `information_schema`.`COLUMNS` where `TABLE_SCHEMA`=? and `TABLE_NAME`=?;";
        List<Map<String, Object>> rows;

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Executing query '%s'", query));
            }
            rows = executeQuery(query, this.database, this.table);
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException("Failed to get table schema info from Doris. " + se.getMessage(), se);
        }
        return rows;
    }

    public Map<String, DorisDataType> getFieldMapping() {
        List<Map<String, Object>> columns = getTableColumnsMetaData();

        Map<String, DorisDataType> mapping = new LinkedHashMap<>();
        for (Map<String, Object> column : columns) {
            mapping.put(column.get("COLUMN_NAME").toString(), DorisDataType.fromString(column.get("DATA_TYPE").toString()));
        }

        return mapping;
    }

    public String getStarRocksVersion() {
        final String query = "select current_version() as ver;";
        List<Map<String, Object>> rows;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Executing query '%s'", query));
            }
            rows = executeQuery(query);
            if (rows.isEmpty()) {
                return "";
            }
            String version = rows.get(0).get("ver").toString();
            LOG.info(String.format("StarRocks version: [%s].", version));
            return version;
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException("Failed to get StarRocks version. " + se.getMessage(), se);
        }
    }

    private List<Map<String, Object>> executeQuery(String query, String... args) throws ClassNotFoundException, SQLException {
        PreparedStatement stmt = jdbcConnProvider.getConnection().prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        // 传入语句参数
        for (int i = 0; i < args.length; i++) {
            stmt.setString(i + 1, args[i]);
        }
        ResultSet rs = stmt.executeQuery();
        rs.next();
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        List<Map<String, Object>> list = new ArrayList<>();
        int currRowIndex = rs.getRow();
        rs.beforeFirst();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>(columns);
            for (int i = 1; i <= columns; ++i) {
                row.put(meta.getColumnName(i), rs.getObject(i));
            }
            list.add(row);
        }
        rs.absolute(currRowIndex);
        rs.close();
        jdbcConnProvider.close();
        return list;
    }

    public Long getQueryCount(String SQL) {
        Long count = 0L;
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("Executing query '%s'", SQL));
            }
            List<Map<String, Object>> data = executeQuery(SQL);
            Object opCount = data.get(0).values().stream().findFirst().orElse(null);
            if (null == opCount) {
                throw new RuntimeException("Faild to get data count from StarRocks. ");
            }
            count = (Long) opCount;
        } catch (ClassNotFoundException se) {
            throw new IllegalArgumentException("Failed to find jdbc driver." + se.getMessage(), se);
        } catch (SQLException se) {
            throw new IllegalArgumentException("Failed to get data count from StarRocks. " + se.getMessage(), se);
        }
        return count;
    }
}
