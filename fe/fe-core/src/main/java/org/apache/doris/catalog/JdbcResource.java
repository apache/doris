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

package org.apache.doris.catalog;


import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * External JDBC Catalog resource for external table query.
 * <p>
 * create external resource jdbc_mysql
 * properties (
 * "type"="jdbc",
 * "user"="root",
 * "password"="123456",
 * "jdbc_url"="jdbc:mysql://127.0.0.1:3306/test",
 * "driver_url"="http://127.0.0.1:8888/mysql-connector-java-5.1.47.jar",
 * "driver_class"="com.mysql.jdbc.Driver"
 * );
 * <p>
 * DROP RESOURCE "jdbc_mysql";
 */
public class JdbcResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(JdbcResource.class);

    public static final String JDBC_MYSQL = "jdbc:mysql";
    public static final String JDBC_MARIADB = "jdbc:mariadb";
    public static final String JDBC_POSTGRESQL = "jdbc:postgresql";
    public static final String JDBC_ORACLE = "jdbc:oracle";
    public static final String JDBC_SQLSERVER = "jdbc:sqlserver";
    public static final String JDBC_CLICKHOUSE = "jdbc:clickhouse";
    public static final String JDBC_SAP_HANA = "jdbc:sap";
    public static final String JDBC_TRINO = "jdbc:trino";
    public static final String JDBC_PRESTO = "jdbc:presto";
    public static final String JDBC_OCEANBASE = "jdbc:oceanbase";
    public static final String JDBC_DB2 = "jdbc:db2";

    public static final String MYSQL = "MYSQL";
    public static final String POSTGRESQL = "POSTGRESQL";
    public static final String ORACLE = "ORACLE";
    public static final String SQLSERVER = "SQLSERVER";
    public static final String CLICKHOUSE = "CLICKHOUSE";
    public static final String SAP_HANA = "SAP_HANA";
    public static final String TRINO = "TRINO";
    public static final String PRESTO = "PRESTO";
    public static final String OCEANBASE = "OCEANBASE";
    public static final String OCEANBASE_ORACLE = "OCEANBASE_ORACLE";
    public static final String DB2 = "DB2";

    public static final String JDBC_PROPERTIES_PREFIX = "jdbc.";
    public static final String JDBC_URL = "jdbc_url";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String DRIVER_CLASS = "driver_class";
    public static final String DRIVER_URL = "driver_url";
    public static final String TYPE = "type";
    public static final String ONLY_SPECIFIED_DATABASE = "only_specified_database";
    public static final String CONNECTION_POOL_MIN_SIZE = "connection_pool_min_size";
    public static final String CONNECTION_POOL_MAX_SIZE = "connection_pool_max_size";
    public static final String CONNECTION_POOL_MAX_WAIT_TIME = "connection_pool_max_wait_time";
    public static final String CONNECTION_POOL_MAX_LIFE_TIME = "connection_pool_max_life_time";
    public static final String CONNECTION_POOL_KEEP_ALIVE = "connection_pool_keep_alive";
    public static final String CHECK_SUM = "checksum";
    public static final String CREATE_TIME = "create_time";
    public static final String TEST_CONNECTION = "test_connection";

    private static final ImmutableList<String> ALL_PROPERTIES = new ImmutableList.Builder<String>().add(
            JDBC_URL,
            USER,
            PASSWORD,
            DRIVER_CLASS,
            DRIVER_URL,
            TYPE,
            CREATE_TIME,
            ONLY_SPECIFIED_DATABASE,
            LOWER_CASE_META_NAMES,
            META_NAMES_MAPPING,
            INCLUDE_DATABASE_LIST,
            EXCLUDE_DATABASE_LIST,
            CONNECTION_POOL_MIN_SIZE,
            CONNECTION_POOL_MAX_SIZE,
            CONNECTION_POOL_MAX_LIFE_TIME,
            CONNECTION_POOL_MAX_WAIT_TIME,
            CONNECTION_POOL_KEEP_ALIVE,
            TEST_CONNECTION,
            ExternalCatalog.USE_META_CACHE
    ).build();

    // The default value of optional properties
    // if one optional property is not specified, will use default value
    private static final Map<String, String> OPTIONAL_PROPERTIES_DEFAULT_VALUE = Maps.newHashMap();

    static {
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(ONLY_SPECIFIED_DATABASE, "false");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(LOWER_CASE_META_NAMES, "false");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(META_NAMES_MAPPING, "");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(INCLUDE_DATABASE_LIST, "");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(EXCLUDE_DATABASE_LIST, "");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CONNECTION_POOL_MIN_SIZE, "1");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CONNECTION_POOL_MAX_SIZE, "30");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CONNECTION_POOL_MAX_LIFE_TIME, "1800000");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CONNECTION_POOL_MAX_WAIT_TIME, "5000");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(CONNECTION_POOL_KEEP_ALIVE, "false");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(TEST_CONNECTION, "true");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(ExternalCatalog.USE_META_CACHE,
                String.valueOf(ExternalCatalog.DEFAULT_USE_META_CACHE));
    }

    // timeout for both connection and read. 10 seconds is long enough.
    private static final int HTTP_TIMEOUT_MS = 10000;
    @SerializedName(value = "configs")
    private Map<String, String> configs;

    public JdbcResource() {
        super();
    }

    public JdbcResource(String name) {
        this(name, Maps.newHashMap());
    }

    public JdbcResource(String name, Map<String, String> configs) {
        super(name, ResourceType.JDBC);
        this.configs = configs;
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        // modify properties
        for (String propertyKey : ALL_PROPERTIES) {
            replaceIfEffectiveValue(this.configs, propertyKey, properties.get(propertyKey));
        }
        this.configs.put(JDBC_URL, handleJdbcUrl(getProperty(JDBC_URL)));
        super.modifyProperties(properties);
    }

    @Override
    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        // check properties
        for (String propertyKey : ALL_PROPERTIES) {
            copiedProperties.remove(propertyKey);
        }
        if (!copiedProperties.isEmpty()) {
            throw new AnalysisException("Unknown JDBC catalog resource properties: " + copiedProperties);
        }
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        validateProperties(properties);
        configs = properties;
        applyDefaultProperties();
        String currentDateTime = LocalDateTime.now(ZoneId.systemDefault()).toString().replace("T", " ");
        configs.put(CREATE_TIME, currentDateTime);
        // check properties
        for (String property : ALL_PROPERTIES) {
            String value = configs.get(property);
            if (value == null) {
                throw new DdlException("JdbcResource Missing " + property + " in properties");
            }
        }
        this.configs.put(JDBC_URL, handleJdbcUrl(getProperty(JDBC_URL)));
        configs.put(CHECK_SUM, computeObjectChecksum(getProperty(DRIVER_URL)));
    }

    /**
     * This function used to handle optional arguments
     * eg: only_specified_database、lower_case_table_names
     */

    @Override
    public void applyDefaultProperties() {
        for (String s : OPTIONAL_PROPERTIES_DEFAULT_VALUE.keySet()) {
            if (!configs.containsKey(s)) {
                configs.put(s, OPTIONAL_PROPERTIES_DEFAULT_VALUE.get(s));
            }
        }
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(configs);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            // it's dangerous to show password in show jdbc resource
            // so we use empty string to replace the real password
            if (entry.getKey().equals(PASSWORD)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), ""));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
    }

    public String getProperty(String propertiesKey) {
        // check the properties key
        return configs.get(propertiesKey);
    }

    public static String computeObjectChecksum(String driverPath) throws DdlException {
        if (FeConstants.runningUnitTest) {
            // skip checking checksum when running ut
            return "";
        }
        String fullDriverUrl = getFullDriverUrl(driverPath);

        try (InputStream inputStream =
                Util.getInputStreamFromUrl(fullDriverUrl, null, HTTP_TIMEOUT_MS, HTTP_TIMEOUT_MS)) {
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
            throw new DdlException("compute driver checksum from url: " + driverPath
                    + " meet an IOException: " + e.getMessage());
        } catch (NoSuchAlgorithmException e) {
            throw new DdlException("compute driver checksum from url: " + driverPath
                    + " could not find algorithm: " + e.getMessage());
        }
    }

    private static void checkCloudWhiteList(String driverUrl) throws IllegalArgumentException {
        // For compatibility with cloud mode, we use both `jdbc_driver_url_white_list`
        // and jdbc_driver_secure_path to check whitelist
        List<String> cloudWhiteList = new ArrayList<>(Arrays.asList(Config.jdbc_driver_url_white_list));
        cloudWhiteList.removeIf(String::isEmpty);
        if (!cloudWhiteList.isEmpty() && !cloudWhiteList.contains(driverUrl)) {
            throw new IllegalArgumentException("Driver URL does not match any allowed paths" + driverUrl);
        }
    }

    public static String getFullDriverUrl(String driverUrl) throws IllegalArgumentException {
        try {
            URI uri = new URI(driverUrl);
            String schema = uri.getScheme();
            checkCloudWhiteList(driverUrl);
            if (schema == null && !driverUrl.startsWith("/")) {
                return "file://" + Config.jdbc_drivers_dir + "/" + driverUrl;
            }

            if ("*".equals(Config.jdbc_driver_secure_path)) {
                return driverUrl;
            }

            boolean isAllowed = Arrays.stream(Config.jdbc_driver_secure_path.split(";"))
                            .anyMatch(allowedPath -> driverUrl.startsWith(allowedPath.trim()));
            if (!isAllowed) {
                throw new IllegalArgumentException("Driver URL does not match any allowed paths: " + driverUrl);
            }
            return driverUrl;
        } catch (URISyntaxException e) {
            LOG.warn("invalid jdbc driver url: " + driverUrl);
            return driverUrl;
        }
    }

    public static String parseDbType(String url) throws DdlException {
        if (url.startsWith(JDBC_MYSQL) || url.startsWith(JDBC_MARIADB)) {
            return MYSQL;
        } else if (url.startsWith(JDBC_POSTGRESQL)) {
            return POSTGRESQL;
        } else if (url.startsWith(JDBC_ORACLE)) {
            return ORACLE;
        } else if (url.startsWith(JDBC_SQLSERVER)) {
            return SQLSERVER;
        } else if (url.startsWith(JDBC_CLICKHOUSE)) {
            return CLICKHOUSE;
        } else if (url.startsWith(JDBC_SAP_HANA)) {
            return SAP_HANA;
        } else if (url.startsWith(JDBC_TRINO)) {
            return TRINO;
        } else if (url.startsWith(JDBC_PRESTO)) {
            return PRESTO;
        } else if (url.startsWith(JDBC_OCEANBASE)) {
            return OCEANBASE;
        } else if (url.startsWith(JDBC_DB2)) {
            return DB2;
        }
        throw new DdlException("Unsupported jdbc database type, please check jdbcUrl: " + url);
    }

    public static String handleJdbcUrl(String jdbcUrl) throws DdlException {
        // delete all space in jdbcUrl
        String newJdbcUrl = jdbcUrl.replaceAll(" ", "");
        String dbType = parseDbType(newJdbcUrl);
        if (dbType.equals(MYSQL) || dbType.equals(OCEANBASE)) {
            // `yearIsDateType` is a parameter of JDBC, and the default is true.
            // We force the use of `yearIsDateType=false`
            newJdbcUrl = checkAndSetJdbcBoolParam(dbType, newJdbcUrl, "yearIsDateType", "true", "false");
            // MySQL Types and Return Values for GetColumnTypeName and GetColumnClassName
            // are presented in https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
            // When mysql's tinyint stores non-0 or 1, we need to read the data correctly,
            // so we need tinyInt1isBit=false
            newJdbcUrl = checkAndSetJdbcBoolParam(dbType, newJdbcUrl, "tinyInt1isBit", "true", "false");
            // set useUnicode and characterEncoding to false and utf-8
            newJdbcUrl = checkAndSetJdbcBoolParam(dbType, newJdbcUrl, "useUnicode", "false", "true");
            newJdbcUrl = checkAndSetJdbcBoolParam(dbType, newJdbcUrl, "rewriteBatchedStatements", "false", "true");
            newJdbcUrl = checkAndSetJdbcParam(dbType, newJdbcUrl, "characterEncoding", "utf-8");
            if (dbType.equals(OCEANBASE)) {
                // set useCursorFetch to true
                newJdbcUrl = checkAndSetJdbcBoolParam(dbType, newJdbcUrl, "useCursorFetch", "false", "true");
            }
        }
        if (dbType.equals(POSTGRESQL)) {
            newJdbcUrl = checkAndSetJdbcBoolParam(dbType, newJdbcUrl, "reWriteBatchedInserts", "false", "true");
        }
        if (dbType.equals(SQLSERVER)) {
            if (Config.force_sqlserver_jdbc_encrypt_false) {
                newJdbcUrl = checkAndSetJdbcBoolParam(dbType, newJdbcUrl, "encrypt", "true", "false");
            }
            newJdbcUrl = checkAndSetJdbcBoolParam(dbType, newJdbcUrl, "useBulkCopyForBatchInsert", "false", "true");
        }
        return newJdbcUrl;
    }

    /**
     * Check jdbcUrl param, if the param is not set, set it to the expected value.
     * If the param is set to an unexpected value, replace it with the expected value.
     * If the param is set to the expected value, do nothing.
     *
     * @param jdbcUrl
     * @param params
     * @param unexpectedVal
     * @param expectedVal
     * @return
     */
    private static String checkAndSetJdbcBoolParam(String dbType, String jdbcUrl, String params, String unexpectedVal,
            String expectedVal) {
        String delimiter = getDelimiter(jdbcUrl, dbType);
        String unexpectedParams = params + "=" + unexpectedVal;
        String expectedParams = params + "=" + expectedVal;

        if (jdbcUrl.contains(expectedParams)) {
            return jdbcUrl;
        } else if (jdbcUrl.contains(unexpectedParams)) {
            jdbcUrl = jdbcUrl.replaceAll(unexpectedParams, expectedParams);
        } else {
            if (!jdbcUrl.endsWith(delimiter)) {
                jdbcUrl += delimiter;
            }
            jdbcUrl += expectedParams;
        }
        return jdbcUrl;
    }

    /**
     * Check jdbcUrl param, if the param is set, do thing.
     * If the param is not set, set it to expected value.
     *
     * @param jdbcUrl
     * @param params
     * @return
     */
    private static String checkAndSetJdbcParam(String dbType, String jdbcUrl, String params, String expectedVal) {
        String delimiter = getDelimiter(jdbcUrl, dbType);
        String expectedParams = params + "=" + expectedVal;

        if (jdbcUrl.contains(expectedParams)) {
            return jdbcUrl;
        } else {
            if (!jdbcUrl.endsWith(delimiter)) {
                jdbcUrl += delimiter;
            }
            jdbcUrl += expectedParams;
        }
        return jdbcUrl;
    }

    private static String getDelimiter(String jdbcUrl, String dbType) {
        if (dbType.equals(SQLSERVER) || dbType.equals(DB2)) {
            return ";";
        } else if (jdbcUrl.contains("?")) {
            return "&";
        } else {
            return "?";
        }
    }

    public static String getDefaultPropertyValue(String propertyName) {
        return OPTIONAL_PROPERTIES_DEFAULT_VALUE.getOrDefault(propertyName, "");
    }

    public static void validateProperties(Map<String, String> properties) throws DdlException {
        for (String key : properties.keySet()) {
            if (!ALL_PROPERTIES.contains(key)) {
                throw new DdlException("JDBC resource Property of " + key + " is unknown");
            }
        }
    }

    public static void checkBooleanProperty(String propertyName, String propertyValue) throws DdlException {
        if (!propertyValue.equalsIgnoreCase("true") && !propertyValue.equalsIgnoreCase("false")) {
            throw new DdlException(propertyName + " must be true or false");
        }
    }

    public static void checkDatabaseListProperties(String onlySpecifiedDatabase,
            Map<String, Boolean> includeDatabaseList, Map<String, Boolean> excludeDatabaseList) throws DdlException {
        if (!onlySpecifiedDatabase.equalsIgnoreCase("true")) {
            if ((includeDatabaseList != null && !includeDatabaseList.isEmpty()) || (excludeDatabaseList != null
                    && !excludeDatabaseList.isEmpty())) {
                throw new DdlException(
                        "include_database_list and exclude_database_list "
                                + "cannot be set when only_specified_database is false");
            }
        }
    }

    public static void checkConnectionPoolProperties(int minSize, int maxSize, int maxWaitTime, int maxLifeTime)
            throws DdlException {
        if (minSize < 0) {
            throw new DdlException("connection_pool_min_size must be greater than or equal to 0");
        }
        if (maxSize < 1) {
            throw new DdlException("connection_pool_max_size must be greater than or equal to 1");
        }
        if (maxSize < minSize) {
            throw new DdlException(
                    "connection_pool_max_size must be greater than or equal to connection_pool_min_size");
        }
        if (maxWaitTime < 0) {
            throw new DdlException("connection_pool_max_wait_time must be greater than or equal to 0");
        }
        if (maxWaitTime > 30000) {
            throw new DdlException("connection_pool_max_wait_time must be less than or equal to 30000");
        }
        if (maxLifeTime < 150000) {
            throw new DdlException("connection_pool_max_life_time must be greater than or equal to 150000");
        }
    }
}

