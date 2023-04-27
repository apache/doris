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

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.Util;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import lombok.Data;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

@Getter
public class JdbcClient {
    private static final Logger LOG = LogManager.getLogger(JdbcClient.class);

    private static final int HTTP_TIMEOUT_MS = 10000;

    private String dbType;
    private String jdbcUser;

    private URLClassLoader classLoader = null;

    private DruidDataSource dataSource = null;
    private boolean isOnlySpecifiedDatabase = false;

    private boolean isLowerCaseTableNames = false;

    private Map<String, Boolean> specifiedDatabaseMap = Maps.newHashMap();

    // only used when isLowerCaseTableNames = true.
    private Map<String, String> lowerTableToRealTable = Maps.newHashMap();

    private String oceanbaseMode = "";

    public JdbcClient(String user, String password, String jdbcUrl, String driverUrl, String driverClass,
            String onlySpecifiedDatabase, String isLowerCaseTableNames, Map specifiedDatabaseMap,
            String oceanbaseMode) {
        this.jdbcUser = user;
        this.isOnlySpecifiedDatabase = Boolean.valueOf(onlySpecifiedDatabase).booleanValue();
        this.isLowerCaseTableNames = Boolean.valueOf(isLowerCaseTableNames).booleanValue();
        if (specifiedDatabaseMap != null) {
            this.specifiedDatabaseMap = specifiedDatabaseMap;
        }
        this.oceanbaseMode = oceanbaseMode;
        try {
            this.dbType = JdbcResource.parseDbType(jdbcUrl, oceanbaseMode);
        } catch (DdlException e) {
            throw new JdbcClientException("Failed to parse db type from jdbcUrl: " + jdbcUrl, e);
        }
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            // TODO(ftw): The problem here is that the jar package is handled by FE
            //  and URLClassLoader may load the jar package directly into memory
            URL[] urls = {new URL(JdbcResource.getFullDriverUrl(driverUrl))};
            // set parent ClassLoader to null, we can achieve class loading isolation.
            ClassLoader parent = getClass().getClassLoader();
            ClassLoader classLoader = URLClassLoader.newInstance(urls, parent);
            LOG.debug("parent ClassLoader: {}, old ClassLoader: {}, class Loader: {}.",
                    parent, oldClassLoader, classLoader);
            Thread.currentThread().setContextClassLoader(classLoader);
            dataSource = new DruidDataSource();
            dataSource.setDriverClassLoader(classLoader);
            dataSource.setDriverClassName(driverClass);
            dataSource.setUrl(jdbcUrl);
            dataSource.setUsername(jdbcUser);
            dataSource.setPassword(password);
            dataSource.setMinIdle(1);
            dataSource.setInitialSize(1);
            dataSource.setMaxActive(100);
            dataSource.setTimeBetweenEvictionRunsMillis(600000);
            dataSource.setMinEvictableIdleTimeMillis(300000);
            // set connection timeout to 5s.
            // The default is 30s, which is too long.
            // Because when querying information_schema db, BE will call thrift rpc(default timeout is 30s)
            // to FE to get schema info, and may create connection here, if we set it too long and the url is invalid,
            // it may cause the thrift rpc timeout.
            dataSource.setMaxWait(5000);
        } catch (MalformedURLException e) {
            throw new JdbcClientException("MalformedURLException to load class about " + driverUrl, e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    public void closeClient() {
        dataSource.close();
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
        Connection conn = getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        if (isOnlySpecifiedDatabase && specifiedDatabaseMap.isEmpty()) {
            return getSpecifiedDatabase(conn);
        }
        List<String> databaseNames = Lists.newArrayList();
        try {
            stmt = conn.createStatement();
            switch (dbType) {
                case JdbcResource.MYSQL:
                case JdbcResource.CLICKHOUSE:
                case JdbcResource.OCEANBASE:
                    rs = stmt.executeQuery("SHOW DATABASES");
                    break;
                case JdbcResource.POSTGRESQL:
                    rs = stmt.executeQuery("SELECT nspname FROM pg_namespace WHERE has_schema_privilege("
                            + "'" + jdbcUser + "', nspname, 'USAGE');");
                    break;
                case JdbcResource.ORACLE:
                case JdbcResource.OCEANBASE_ORACLE:
                    rs = stmt.executeQuery("SELECT DISTINCT OWNER FROM all_tables");
                    break;
                case JdbcResource.SQLSERVER:
                    rs = stmt.executeQuery("SELECT name FROM sys.schemas");
                    break;
                case JdbcResource.SAP_HANA:
                    rs = stmt.executeQuery("SELECT SCHEMA_NAME FROM SYS.SCHEMAS WHERE HAS_PRIVILEGES = 'TRUE'");
                    break;
                case JdbcResource.TRINO:
                    rs = stmt.executeQuery("SHOW SCHEMAS");
                    break;
                default:
                    throw new JdbcClientException("Not supported jdbc type");
            }
            List<String> tempDatabaseNames = Lists.newArrayList();
            while (rs.next()) {
                tempDatabaseNames.add(rs.getString(1));
            }
            if (isOnlySpecifiedDatabase && !specifiedDatabaseMap.isEmpty()) {
                for (String db : tempDatabaseNames) {
                    if (specifiedDatabaseMap.get(db) != null) {
                        databaseNames.add(db);
                    }
                }
            } else {
                databaseNames = tempDatabaseNames;
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get database name list from jdbc", e);
        } finally {
            close(rs, stmt, conn);
        }
        return databaseNames;
    }

    public List<String> getSpecifiedDatabase(Connection conn) {
        List<String> databaseNames = Lists.newArrayList();
        try {
            switch (dbType) {
                case JdbcResource.MYSQL:
                case JdbcResource.CLICKHOUSE:
                case JdbcResource.OCEANBASE:
                    databaseNames.add(conn.getCatalog());
                    break;
                case JdbcResource.POSTGRESQL:
                case JdbcResource.ORACLE:
                case JdbcResource.SQLSERVER:
                case JdbcResource.SAP_HANA:
                case JdbcResource.TRINO:
                case JdbcResource.OCEANBASE_ORACLE:
                    databaseNames.add(conn.getSchema());
                    break;
                default:
                    throw new JdbcClientException("Not supported jdbc type");
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get specified database name from jdbc", e);
        } finally {
            close(conn);
        }
        return databaseNames;
    }

    /**
     * get all tables of one database
     */
    public List<String> getTablesNameList(String dbName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<String> tablesName = Lists.newArrayList();
        String[] types = {"TABLE", "VIEW"};
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = conn.getCatalog();
            switch (dbType) {
                case JdbcResource.MYSQL:
                case JdbcResource.OCEANBASE:
                    rs = databaseMetaData.getTables(dbName, null, null, types);
                    break;
                case JdbcResource.POSTGRESQL:
                case JdbcResource.ORACLE:
                case JdbcResource.CLICKHOUSE:
                case JdbcResource.SQLSERVER:
                case JdbcResource.SAP_HANA:
                case JdbcResource.OCEANBASE_ORACLE:
                    rs = databaseMetaData.getTables(null, dbName, null, types);
                    break;
                case JdbcResource.TRINO:
                    rs = databaseMetaData.getTables(catalogName, dbName, null, types);
                    break;
                default:
                    throw new JdbcClientException("Unknown database type");
            }
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                if (isLowerCaseTableNames) {
                    lowerTableToRealTable.put(tableName.toLowerCase(), tableName);
                    tableName = tableName.toLowerCase();
                }
                tablesName.add(tableName);
            }
        } catch (SQLException e) {
            throw new JdbcClientException("failed to get all tables for db %s", e, dbName);
        } finally {
            close(rs, conn);
        }
        return tablesName;
    }

    public boolean isTableExist(String dbName, String tableName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        String[] types = {"TABLE", "VIEW"};
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = conn.getCatalog();
            switch (dbType) {
                case JdbcResource.MYSQL:
                case JdbcResource.OCEANBASE:
                    rs = databaseMetaData.getTables(dbName, null, tableName, types);
                    break;
                case JdbcResource.POSTGRESQL:
                case JdbcResource.ORACLE:
                case JdbcResource.CLICKHOUSE:
                case JdbcResource.SQLSERVER:
                case JdbcResource.SAP_HANA:
                case JdbcResource.OCEANBASE_ORACLE:
                    rs = databaseMetaData.getTables(null, dbName, null, types);
                    break;
                case JdbcResource.TRINO:
                    rs = databaseMetaData.getTables(catalogName, dbName, null, types);
                    break;
                default:
                    throw new JdbcClientException("Unknown database type: " + dbType);
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
        private boolean isAllowNull;
    }

    /**
     * get all columns of one table
     */
    public List<JdbcFieldSchema> getJdbcColumnsInfo(String dbName, String tableName) {
        Connection conn = getConnection();
        ResultSet rs = null;
        List<JdbcFieldSchema> tableSchema = Lists.newArrayList();
        // if isLowerCaseTableNames == true, tableName is lower case
        // but databaseMetaData.getColumns() is case sensitive
        if (isLowerCaseTableNames) {
            tableName = lowerTableToRealTable.get(tableName);
        }
        try {
            DatabaseMetaData databaseMetaData = conn.getMetaData();
            String catalogName = conn.getCatalog();
            // getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            // catalog - the catalog of this table, `null` means all catalogs
            // schema - The schema of the table; corresponding to tablespace in Oracle
            //          `null` means get all schema;
            //          Can contain single-character wildcards ("_"), or multi-character wildcards ("%")
            // tableNamePattern - table name
            //                    Can contain single-character wildcards ("_"), or multi-character wildcards ("%")
            // columnNamePattern - column name, `null` means get all columns
            //                     Can contain single-character wildcards ("_"), or multi-character wildcards ("%")
            switch (dbType) {
                case JdbcResource.MYSQL:
                case JdbcResource.OCEANBASE:
                    rs = databaseMetaData.getColumns(dbName, null, tableName, null);
                    break;
                case JdbcResource.POSTGRESQL:
                case JdbcResource.ORACLE:
                case JdbcResource.CLICKHOUSE:
                case JdbcResource.SQLSERVER:
                case JdbcResource.SAP_HANA:
                case JdbcResource.OCEANBASE_ORACLE:
                    rs = databaseMetaData.getColumns(null, dbName, tableName, null);
                    break;
                case JdbcResource.TRINO:
                    rs = databaseMetaData.getColumns(catalogName, dbName, tableName, null);
                    break;
                default:
                    throw new JdbcClientException("Unknown database type");
            }
            while (rs.next()) {
                JdbcFieldSchema field = new JdbcFieldSchema();
                field.setColumnName(rs.getString("COLUMN_NAME"));
                field.setDataType(rs.getInt("DATA_TYPE"));
                field.setDataTypeName(rs.getString("TYPE_NAME"));
                field.setColumnSize(rs.getInt("COLUMN_SIZE"));
                field.setDecimalDigits(rs.getInt("DECIMAL_DIGITS"));
                field.setNumPrecRadix(rs.getInt("NUM_PREC_RADIX"));
                /**
                 *  Whether it is allowed to be NULL
                 *  0 (columnNoNulls)
                 *  1 (columnNullable)
                 *  2 (columnNullableUnknown)
                 */
                field.setAllowNull(rs.getInt("NULLABLE") == 0 ? false : true);
                field.setRemarks(rs.getString("REMARKS"));
                field.setCharOctetLength(rs.getInt("CHAR_OCTET_LENGTH"));
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

    public Type jdbcTypeToDoris(JdbcFieldSchema fieldSchema) {
        switch (dbType) {
            case JdbcResource.MYSQL:
            case JdbcResource.OCEANBASE:
                return mysqlTypeToDoris(fieldSchema);
            case JdbcResource.POSTGRESQL:
                return postgresqlTypeToDoris(fieldSchema);
            case JdbcResource.CLICKHOUSE:
                return clickhouseTypeToDoris(fieldSchema);
            case JdbcResource.ORACLE:
            case JdbcResource.OCEANBASE_ORACLE:
                return oracleTypeToDoris(fieldSchema);
            case JdbcResource.SQLSERVER:
                return sqlserverTypeToDoris(fieldSchema);
            case JdbcResource.SAP_HANA:
                return saphanaTypeToDoris(fieldSchema);
            case JdbcResource.TRINO:
                return trinoTypeToDoris(fieldSchema);
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
                    return Type.LARGEINT;
                case "DECIMAL":
                    int precision = fieldSchema.getColumnSize() + 1;
                    int scale = fieldSchema.getDecimalDigits();
                    return createDecimalOrStringType(precision, scale);
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
                return ScalarType.createDatetimeV2Type(0);
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

    public Type postgresqlTypeToDoris(JdbcFieldSchema fieldSchema) {
        String pgType = fieldSchema.getDataTypeName();
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
                int precision = fieldSchema.getColumnSize();
                int scale = fieldSchema.getDecimalDigits();
                return createDecimalOrStringType(precision, scale);
            }
            case "float4":
                return Type.FLOAT;
            case "float8":
                return Type.DOUBLE;
            case "bpchar":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.columnSize);
                return charType;
            case "timestamp":
            case "timestamptz":
                return ScalarType.createDatetimeV2Type(0);
            case "date":
                return ScalarType.createDateV2Type();
            case "bool":
                return Type.BOOLEAN;
            case "bit":
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
            case "jsonb":
            case "uuid":
            case "bytea":
                return ScalarType.createStringType();
            default:
                return Type.UNSUPPORTED;
        }
    }

    public Type clickhouseTypeToDoris(JdbcFieldSchema fieldSchema) {
        String ckType = fieldSchema.getDataTypeName();
        if (ckType.startsWith("LowCardinality")) {
            ckType = ckType.substring(15, ckType.length() - 1);
            if (ckType.startsWith("Nullable")) {
                ckType = ckType.substring(9, ckType.length() - 1);
            }
        } else if (ckType.startsWith("Nullable")) {
            ckType = ckType.substring(9, ckType.length() - 1);
        }
        if (ckType.startsWith("Decimal")) {
            String[] accuracy = ckType.substring(8, ckType.length() - 1).split(", ");
            int precision = Integer.parseInt(accuracy[0]);
            int scale = Integer.parseInt(accuracy[1]);
            return createDecimalOrStringType(precision, scale);
        } else if ("String".contains(ckType) || ckType.startsWith("Enum")
                || ckType.startsWith("IPv") || "UUID".contains(ckType)
                || ckType.startsWith("FixedString")) {
            return ScalarType.createStringType();
        } else if (ckType.startsWith("DateTime")) {
            return ScalarType.createDatetimeV2Type(6);
        } else if (ckType.startsWith("Array")) {
            String cktype = ckType.substring(6, ckType.length() - 1);
            fieldSchema.setDataTypeName(cktype);
            Type type = clickhouseTypeToDoris(fieldSchema);
            return ArrayType.create(type, true);
        }
        switch (ckType) {
            case "Bool":
                return Type.BOOLEAN;
            case "Int8":
                return Type.TINYINT;
            case "Int16":
            case "UInt8":
                return Type.SMALLINT;
            case "Int32":
            case "UInt16":
                return Type.INT;
            case "Int64":
            case "UInt32":
                return Type.BIGINT;
            case "Int128":
            case "UInt64":
                return Type.LARGEINT;
            case "Int256":
            case "UInt128":
            case "UInt256":
                return ScalarType.createStringType();
            case "Float32":
                return Type.FLOAT;
            case "Float64":
                return Type.DOUBLE;
            case "Date":
            case "Date32":
                return ScalarType.createDateV2Type();
            default:
                return Type.UNSUPPORTED;
        }
    }

    public Type oracleTypeToDoris(JdbcFieldSchema fieldSchema) {
        String oracleType = fieldSchema.getDataTypeName();
        if (oracleType.startsWith("INTERVAL")) {
            oracleType = oracleType.substring(0, 8);
        } else if (oracleType.startsWith("TIMESTAMP")) {
            if (oracleType.equals("TIMESTAMPTZ") || oracleType.equals("TIMESTAMPLTZ")) {
                return Type.UNSUPPORTED;
            }
            return ScalarType.createDatetimeV2Type(0);
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
                int precision = fieldSchema.getColumnSize();
                int scale = fieldSchema.getDecimalDigits();
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
                return ScalarType.createDatetimeV2Type(0);
            case "VARCHAR2":
            case "NVARCHAR2":
            case "CHAR":
            case "NCHAR":
            case "LONG":
            case "RAW":
            case "LONG RAW":
            case "INTERVAL":
                return ScalarType.createStringType();
            case "BLOB":
            case "CLOB":
            case "NCLOB":
            case "BFILE":
            case "BINARY_FLOAT":
            case "BINARY_DOUBLE":
            default:
                return Type.UNSUPPORTED;
        }
    }

    public Type sqlserverTypeToDoris(JdbcFieldSchema fieldSchema) {
        String originSqlserverType = fieldSchema.getDataTypeName();
        // For sqlserver IDENTITY type, such as 'INT IDENTITY'
        // originSqlserverType is "int identity", so we only get "int".
        String sqlserverType = originSqlserverType.split(" ")[0];
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
            case "money":
            case "smallmoney":
                return Type.DOUBLE;
            case "decimal":
            case "numeric":
                int precision = fieldSchema.getColumnSize();
                int scale = fieldSchema.getDecimalDigits();
                return ScalarType.createDecimalV3Type(precision, scale);
            case "date":
                return ScalarType.createDateV2Type();
            case "datetime":
            case "datetime2":
            case "smalldatetime":
                return ScalarType.createDatetimeV2Type(6);
            case "char":
            case "varchar":
            case "nchar":
            case "nvarchar":
            case "text":
            case "ntext":
            case "time":
            case "datetimeoffset":
                return ScalarType.createStringType();
            case "image":
            case "binary":
            case "varbinary":
            default:
                return Type.UNSUPPORTED;
        }
    }

    public Type saphanaTypeToDoris(JdbcFieldSchema fieldSchema) {
        String hanaType = fieldSchema.getDataTypeName();
        switch (hanaType) {
            case "TINYINT":
                return Type.TINYINT;
            case "SMALLINT":
                return Type.SMALLINT;
            case "INTEGER":
                return Type.INT;
            case "BIGINT":
                return Type.BIGINT;
            case "SMALLDECIMAL":
            case "DECIMAL": {
                int precision = fieldSchema.getColumnSize();
                int scale = fieldSchema.getDecimalDigits();
                return createDecimalOrStringType(precision, scale);
            }
            case "REAL":
                return Type.FLOAT;
            case "DOUBLE":
                return Type.DOUBLE;
            case "TIMESTAMP":
            case "SECONDDATE":
                return ScalarType.createDatetimeV2Type(6);
            case "DATE":
                return ScalarType.createDateV2Type();
            case "BOOLEAN":
                return Type.BOOLEAN;
            case "CHAR":
            case "NCHAR":
                ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
                charType.setLength(fieldSchema.columnSize);
                return charType;
            case "TIME":
            case "VARCHAR":
            case "NVARCHAR":
            case "ALPHANUM":
            case "SHORTTEXT":
                return ScalarType.createStringType();
            case "BINARY":
            case "VARBINARY":
            case "BLOB":
            case "CLOB":
            case "NCLOB":
            case "TEXT":
            case "BINTEXT":
            case "ST_GEOMETRY":
            case "ST_POINT":
            default:
                return Type.UNSUPPORTED;
        }
    }

    public Type trinoTypeToDoris(JdbcFieldSchema fieldSchema) {
        String trinoType = fieldSchema.getDataTypeName();
        if (trinoType.startsWith("decimal")) {
            String[] split = trinoType.split("\\(");
            String[] precisionAndScale = split[1].split(",");
            int precision = Integer.parseInt(precisionAndScale[0]);
            int scale = Integer.parseInt(precisionAndScale[1].substring(0, precisionAndScale[1].length() - 1));
            return createDecimalOrStringType(precision, scale);
        } else if (trinoType.startsWith("char")) {
            ScalarType charType = ScalarType.createType(PrimitiveType.CHAR);
            charType.setLength(fieldSchema.columnSize);
            return charType;
        } else if (trinoType.startsWith("timestamp")) {
            return ScalarType.createDatetimeV2Type(6);
        } else if (trinoType.startsWith("array")) {
            String trinoArrType = trinoType.substring(6, trinoType.length() - 1);
            fieldSchema.setDataTypeName(trinoArrType);
            Type type = trinoTypeToDoris(fieldSchema);
            return ArrayType.create(type, true);
        }
        switch (trinoType) {
            case "integer":
                return Type.INT;
            case "bigint":
                return Type.BIGINT;
            case "smallint":
                return Type.SMALLINT;
            case "tinyint":
                return Type.TINYINT;
            case "double":
                return Type.DOUBLE;
            case "real":
                return Type.FLOAT;
            case "boolean":
                return Type.BOOLEAN;
            case "varchar":
                return ScalarType.createStringType();
            case "date":
                return ScalarType.createDateV2Type();
            default:
                return Type.UNSUPPORTED;
        }
    }

    private Type createDecimalOrStringType(int precision, int scale) {
        if (precision <= ScalarType.MAX_DECIMAL128_PRECISION) {
            return ScalarType.createDecimalV3Type(precision, scale);
        }
        return ScalarType.createStringType();
    }


    public List<Column> getColumnsFromJdbc(String dbName, String tableName) {
        List<JdbcFieldSchema> jdbcTableSchema = getJdbcColumnsInfo(dbName, tableName);
        List<Column> dorisTableSchema = Lists.newArrayListWithCapacity(jdbcTableSchema.size());
        for (JdbcFieldSchema field : jdbcTableSchema) {
            dorisTableSchema.add(new Column(field.getColumnName(),
                    jdbcTypeToDoris(field), true, null,
                    field.isAllowNull(), field.getRemarks(),
                    true, -1));
        }
        return dorisTableSchema;
    }
}
