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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.Util;

import com.google.common.collect.Lists;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

@Getter
public class JdbcClient {
    private static final Logger LOG = LogManager.getLogger(JdbcClient.class);
    private static final String MYSQL = "MYSQL";
    // private static final String ORACLE = "ORACLE";
    // private static final String SQLSERVER = "SQLSERVER";
    // private static final String POSTGRESQL = "POSTGRESQL";
    private static final int HTTP_TIMEOUT_MS = 10000;

    private String dbType;
    private String jdbcUser;
    private String jdbcPasswd;
    private String jdbcUrl;
    private String driverUrl;
    private String driverClass;
    private String checkSum;

    private URLClassLoader classLoader = null;

    private HikariDataSource dataSource = null;


    public JdbcClient(String user, String password, String jdbcUrl, String driverUrl, String driverClass) {
        this.jdbcUser = user;
        this.jdbcPasswd = password;
        this.jdbcUrl = jdbcUrl;
        this.driverUrl = driverUrl;
        this.driverClass = driverClass;
        this.dbType = parseDbType(jdbcUrl);
        this.checkSum = computeObjectChecksum();

        try {
            // TODO(ftw): The problem here is that the jar package is handled by FE
            //  and URLClassLoader may load the jar package directly into memory
            URL[] urls = {new URL(driverUrl)};
            // set parent ClassLoader to null, we can achieve class loading isolation.
            classLoader = URLClassLoader.newInstance(urls, null);
            Thread.currentThread().setContextClassLoader(classLoader);
            HikariConfig config = new HikariConfig();
            config.setDriverClassName(driverClass);
            config.setJdbcUrl(jdbcUrl);
            config.setUsername(jdbcUser);
            config.setPassword(jdbcPasswd);
            config.setMaximumPoolSize(1);
            dataSource = new HikariDataSource(config);
        } catch (MalformedURLException e) {
            throw new JdbcClientException("MalformedURLException to load class about " + driverUrl, e);
        }
    }

    public void closeClient() {
        dataSource.close();
    }

    public String parseDbType(String url) {
        if (url.startsWith("jdbc:mysql") || url.startsWith("jdbc:mariadb")) {
            return MYSQL;
        }
        // else if (url.startsWith("jdbc:oracle")) {
        //     return ORACLE;
        // }
        // else if (url.startsWith("jdbc:sqlserver")) {
        //     return SQLSERVER;
        // } else if (url.startsWith("jdbc:postgresql")) {
        //     return POSTGRESQL;
        // }
        throw new JdbcClientException("Unsupported jdbc database type, please check jdbcUrl: " + jdbcUrl);
    }

    public Connection getConnection() throws JdbcClientException {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (Exception e) {
            throw new JdbcClientException("Can not connect to jdbc", e);
        }
        return conn;
    }

    // close connection
    public void close(Object o) {
        if (o == null) {
            return;
        }
        if (o instanceof ResultSet) {
            try {
                ((ResultSet) o).close();
            } catch (SQLException e) {
                throw new JdbcClientException("Can not close ResultSet ", e);
            }
        } else if (o instanceof Statement) {
            try {
                ((Statement) o).close();
            } catch (SQLException e) {
                throw new JdbcClientException("Can not close Statement ", e);
            }
        } else if (o instanceof Connection) {
            Connection c = (Connection) o;
            try {
                if (!c.isClosed()) {
                    c.close();
                }
            } catch (SQLException e) {
                throw new JdbcClientException("Can not close Connection ", e);
            }
        }
    }

    public void close(ResultSet rs, Statement stmt, Connection conn) {
        close(rs);
        close(stmt);
        close(conn);
    }

    public void close(ResultSet rs, Connection conn) {
        close(rs);
        close(conn);
    }

    /**
     * get all database name through JDBC
     * @return list of database names
     */
    public List<String> getDatabaseNameList() {
        Connection conn =  getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        List<String> databaseNames = Lists.newArrayList();
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("SHOW DATABASES");
            while (rs.next()) {
                databaseNames.add(rs.getString(1));
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get database name list from jdbc", e);
        } finally {
            close(rs, stmt, conn);
        }
        return databaseNames;
    }

    /**
     * get all tables of one database
     */
    public List<String> getTablesNameList(String dbName) {
        Connection conn =  getConnection();
        ResultSet rs = null;
        List<String> tablesName = Lists.newArrayList();
        String[] types = { "TABLE", "VIEW" };
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            switch (dbType) {
                case MYSQL:
                    rs = databaseMetaData.getTables(dbName, null, null, types);
                    break;
                default:
                    throw new JdbcClientException("Unknown database type");
            }
            while (rs.next()) {
                tablesName.add(rs.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get all tables for db %s", e, dbName);
        } finally {
            close(rs, conn);
        }
        return tablesName;
    }

    public boolean isTableExist(String dbName, String tableName) {
        Connection conn =  getConnection();
        ResultSet rs = null;
        String[] types = { "TABLE", "VIEW" };
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            switch (dbType) {
                case MYSQL:
                    rs = databaseMetaData.getTables(dbName, null, tableName, types);
                    break;
                default:
                    throw new JdbcClientException("Unknown database type");
            }
            if (rs.next()) {
                return true;
            } else {
                return false;
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to judge if table exist for table %s in db %s", e, tableName, dbName);
        } finally {
            close(rs, conn);
        }
    }

    @Data
    private class JdbcFieldSchema {
        private String columnName;
        // The SQL type of the corresponding java.sql.types (Type ID)
        private int dataType;
        // The SQL type of the corresponding java.sql.types (Type Name)
        private String dataTypeName;
        // For CHAR/DATA, columnSize means the maximum number of chars.
        // For NUMERIC/DECIMAL, columnSize means precision.
        private int columnSize;
        private int decimalDigits;
        // Base number (usually 10 or 2)
        private int numPrecRadix;
        // column description
        private String remarks;
        // This length is the maximum number of bytes for CHAR type
        // for utf8 encoding, if columnSize=10, then charOctetLength=30
        // because for utf8 encoding, a Chinese character takes up 3 bytes
        private int charOctetLength;
        /**
         *  Whether it is allowed to be NULL
         *  0 (columnNoNulls)
         *  1 (columnNullable)
         *  2 (columnNullableUnknown)
         */
        private int nullAble;
    }

    /**
     * get all columns of one table
     */
    public List<JdbcFieldSchema> getJdbcColumnsInfo(String dbName, String tableName) {
        Connection conn =  getConnection();
        ResultSet rs = null;
        List<JdbcFieldSchema> tableSchema = Lists.newArrayList();
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            // getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            // catalog - the catalog of this table, `null` means all catalogs
            // schema - The schema of the table; corresponding to tablespace in Oracle
            //          `null` means get all schema;
            //          Can contain single-character wildcards ("_"), or multi-character wildcards ("%")
            // tableNamePattern - table name
            //                    Can contain single-character wildcards ("_"), or multi-character wildcards ("%")
            // columnNamePattern - column name, `null` means get all columns
            //                     Can contain single-character wildcards ("_"), or multi-character wildcards ("%")
            rs = databaseMetaData.getColumns(dbName, null, tableName, null);
            while (rs.next()) {
                JdbcFieldSchema field = new JdbcFieldSchema();
                field.setColumnName(rs.getString("COLUMN_NAME"));
                field.setDataType(rs.getInt("DATA_TYPE"));
                field.setDataTypeName(rs.getString("TYPE_NAME"));
                field.setColumnSize(rs.getInt("COLUMN_SIZE"));
                field.setDecimalDigits(rs.getInt("DECIMAL_DIGITS"));
                field.setNumPrecRadix(rs.getInt("NUM_PREC_RADIX"));
                field.setNullAble(rs.getInt("NULLABLE"));
                field.setRemarks(rs.getString("REMARKS"));
                field.setCharOctetLength(rs.getInt("CHAR_OCTET_LENGTH"));
                tableSchema.add(field);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get table name list from jdbc for table %s", e, tableName);
        } finally {
            close(rs, conn);
        }
        return tableSchema;
    }

    public Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        switch (dbType) {
            case MYSQL:
                return mysqlTypeToDoris(fieldSchema);
            default:
                throw new JdbcClientException("Unknown database type");
        }
    }

    public Type mysqlTypeToDoris(JdbcFieldSchema fieldSchema) {
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
                    return Type.INT;
                case "MEDIUMINT":
                    return Type.INT;
                case "INT":
                    return Type.BIGINT;
                case "BIGINT":
                    return ScalarType.createStringType();
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
                return ScalarType.getDefaultDateType(Type.DATE);
            case "TIMESTAMP":
                return ScalarType.getDefaultDateType(Type.DATETIME);
            case "DATETIME":
                return ScalarType.getDefaultDateType(Type.DATETIME);
            case "FLOAT":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "DECIMAL":
                int precision = fieldSchema.getColumnSize();
                int scale = fieldSchema.getDecimalDigits();
                return ScalarType.createDecimalType(precision, scale);
            case "CHAR":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.columnSize);
                return charType;
            case "TIME":
            case "VARCHAR":
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
                return ScalarType.createStringType();
            default:
                throw new JdbcClientException("Can not convert mysql data type to doris data type for type ["
                                                + mysqlType + "]");
        }
    }

    public List<Column> getColumnsFromJdbc(String dbName, String tableName) {
        List<JdbcFieldSchema> jdbcTableSchema = getJdbcColumnsInfo(dbName, tableName);
        List<Column> dorisTableSchema = Lists.newArrayListWithCapacity(jdbcTableSchema.size());
        for (JdbcFieldSchema field : jdbcTableSchema) {
            dorisTableSchema.add(new Column(field.getColumnName(),
                    jdbcTypeToDoris(field), true, null,
                    true, null, field.getRemarks(),
                    true, null, -1));
        }
        return dorisTableSchema;
    }

    private String computeObjectChecksum() {
        if (FeConstants.runningUnitTest) {
            // skip checking checksum when running ut
            return "";
        }

        InputStream inputStream = null;
        try {
            inputStream = Util.getInputStreamFromUrl(driverUrl, null, HTTP_TIMEOUT_MS, HTTP_TIMEOUT_MS);
            MessageDigest digest = MessageDigest.getInstance("MD5");
            byte[] buf = new byte[4096];
            int bytesRead = 0;
            do {
                bytesRead = inputStream.read(buf);
                if (bytesRead < 0) {
                    break;
                }
                digest.update(buf, 0, bytesRead);
            } while (true);
            return Hex.encodeHexString(digest.digest());
        } catch (IOException e) {
            throw new JdbcClientException("compute driver checksum from url: " + driverUrl + " meet an IOException.");
        } catch (NoSuchAlgorithmException e) {
            throw new JdbcClientException(
                    "compute driver checksum from url: " + driverUrl + " could not find algorithm.");
        }
    }
}
