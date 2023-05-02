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
import java.util.Map;


/**
 * External JDBC Catalog resource for external table query.
 *
 * create external resource jdbc_mysql
 * properties (
 * "type"="jdbc",
 * "user"="root",
 * "password"="123456",
 * "jdbc_url"="jdbc:mysql://127.0.0.1:3306/test",
 * "driver_url"="http://127.0.0.1:8888/mysql-connector-java-5.1.47.jar",
 * "driver_class"="com.mysql.jdbc.Driver"
 * );
 *
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
    public static final String JDBC_OCEANBASE = "jdbc:oceanbase";

    public static final String MYSQL = "MYSQL";
    public static final String POSTGRESQL = "POSTGRESQL";
    public static final String ORACLE = "ORACLE";
    public static final String SQLSERVER = "SQLSERVER";
    public static final String CLICKHOUSE = "CLICKHOUSE";
    public static final String SAP_HANA = "SAP_HANA";
    public static final String TRINO = "TRINO";
    public static final String OCEANBASE = "OCEANBASE";
    public static final String OCEANBASE_ORACLE = "OCEANBASE_ORACLE";

    public static final String JDBC_PROPERTIES_PREFIX = "jdbc.";
    public static final String JDBC_URL = "jdbc_url";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String DRIVER_CLASS = "driver_class";
    public static final String DRIVER_URL = "driver_url";
    public static final String TYPE = "type";
    public static final String ONLY_SPECIFIED_DATABASE = "only_specified_database";
    public static final String LOWER_CASE_TABLE_NAMES = "lower_case_table_names";
    public static final String OCEANBASE_MODE = "oceanbase_mode";
    public static final String CHECK_SUM = "checksum";
    private static final ImmutableList<String> ALL_PROPERTIES = new ImmutableList.Builder<String>().add(
            JDBC_URL,
            USER,
            PASSWORD,
            DRIVER_CLASS,
            DRIVER_URL,
            TYPE,
            ONLY_SPECIFIED_DATABASE,
            LOWER_CASE_TABLE_NAMES,
            SPECIFIED_DATABASE_LIST,
            OCEANBASE_MODE
    ).build();
    private static final ImmutableList<String> OPTIONAL_PROPERTIES = new ImmutableList.Builder<String>().add(
            ONLY_SPECIFIED_DATABASE,
            LOWER_CASE_TABLE_NAMES,
            SPECIFIED_DATABASE_LIST,
            OCEANBASE_MODE
    ).build();

    // The default value of optional properties
    // if one optional property is not specified, will use default value
    private static final Map<String, String> OPTIONAL_PROPERTIES_DEFAULT_VALUE = Maps.newHashMap();

    static {
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(ONLY_SPECIFIED_DATABASE, "false");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(LOWER_CASE_TABLE_NAMES, "false");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(SPECIFIED_DATABASE_LIST, "");
        OPTIONAL_PROPERTIES_DEFAULT_VALUE.put(OCEANBASE_MODE, "");
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

    private JdbcResource(String name, Map<String, String> configs) {
        super(name, ResourceType.JDBC);
        this.configs = configs;
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        // modify properties
        for (String propertyKey : ALL_PROPERTIES) {
            replaceIfEffectiveValue(this.configs, propertyKey, properties.get(propertyKey));
        }
        this.configs.put(JDBC_URL, handleJdbcUrl(getProperty(JDBC_URL), getProperty(OCEANBASE_MODE)));
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
        for (String key : properties.keySet()) {
            if (!ALL_PROPERTIES.contains(key)) {
                throw new DdlException("JDBC resource Property of " + key + " is unknown");
            }
        }
        configs = properties;
        handleOptionalArguments();
        // check properties
        for (String property : ALL_PROPERTIES) {
            String value = configs.get(property);
            if (value == null) {
                throw new DdlException("JdbcResource Missing " + property + " in properties");
            }
        }
        this.configs.put(JDBC_URL, handleJdbcUrl(getProperty(JDBC_URL), getProperty(OCEANBASE_MODE)));
        configs.put(CHECK_SUM, computeObjectChecksum(getProperty(DRIVER_URL)));
    }

    /**
     * This function used to handle optional arguments
     * eg: only_specified_database、lower_case_table_names
     */
    private void handleOptionalArguments() {
        for (String s : OPTIONAL_PROPERTIES) {
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
        InputStream inputStream = null;
        try {
            inputStream = Util.getInputStreamFromUrl(fullDriverUrl, null, HTTP_TIMEOUT_MS, HTTP_TIMEOUT_MS);
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

    public static String getFullDriverUrl(String driverUrl) {
        try {
            URI uri = new URI(driverUrl);
            String schema = uri.getScheme();
            if (schema == null && !driverUrl.startsWith("/")) {
                return "file://" + Config.jdbc_drivers_dir + "/" + driverUrl;
            }
            return driverUrl;
        } catch (URISyntaxException e) {
            LOG.warn("invalid jdbc driver url: " + driverUrl);
            return driverUrl;
        }
    }

    public static String parseDbType(String url, String oceanbaseMode) throws DdlException {
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
        } else if (url.startsWith(JDBC_OCEANBASE)) {
            if (oceanbaseMode == null || oceanbaseMode.isEmpty()) {
                throw new DdlException("OceanBase mode must be specified for OceanBase databases"
                        + "(either 'mysql' or 'oracle')");
            }
            if (oceanbaseMode.equalsIgnoreCase("mysql")) {
                return OCEANBASE;
            } else if (oceanbaseMode.equalsIgnoreCase("oracle")) {
                return OCEANBASE_ORACLE;
            } else {
                throw new DdlException("Invalid OceanBase mode: " + oceanbaseMode + ". Must be 'mysql' or 'oracle'");
            }
        }
        throw new DdlException("Unsupported jdbc database type, please check jdbcUrl: " + url);
    }

    public static String handleJdbcUrl(String jdbcUrl, String oceanbaseMode) throws DdlException {
        // delete all space in jdbcUrl
        String newJdbcUrl = jdbcUrl.replaceAll(" ", "");
        String dbType = parseDbType(newJdbcUrl, oceanbaseMode);
        if (dbType.equals(MYSQL) || dbType.equals(OCEANBASE)) {
            // `yearIsDateType` is a parameter of JDBC, and the default is true.
            // We force the use of `yearIsDateType=false`
            newJdbcUrl = checkAndSetJdbcBoolParam(newJdbcUrl, "yearIsDateType", "true", "false");
            // `tinyInt1isBit` is a parameter of JDBC, and the default is true.
            // We force the use of `tinyInt1isBit=false`, so that for mysql type tinyint,
            // it will convert to Doris tinyint, not bit.
            newJdbcUrl = checkAndSetJdbcBoolParam(newJdbcUrl, "tinyInt1isBit", "true", "false");
            // set useUnicode and characterEncoding to false and utf-8
            newJdbcUrl = checkAndSetJdbcBoolParam(newJdbcUrl, "useUnicode", "false", "true");
            newJdbcUrl = checkAndSetJdbcParam(newJdbcUrl, "characterEncoding", "utf-8");
        }
        if (dbType.equals(POSTGRESQL)) {
            newJdbcUrl = checkAndSetJdbcBoolParam(newJdbcUrl, "useCursorFetch", "false", "true");
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
    private static String checkAndSetJdbcBoolParam(String jdbcUrl, String params, String unexpectedVal,
            String expectedVal) {
        String unexpectedParams = params + "=" + unexpectedVal;
        String expectedParams = params + "=" + expectedVal;
        if (jdbcUrl.contains(expectedParams)) {
            return jdbcUrl;
        } else if (jdbcUrl.contains(unexpectedParams)) {
            jdbcUrl = jdbcUrl.replaceAll(unexpectedParams, expectedParams);
        } else {
            if (jdbcUrl.contains("?")) {
                if (jdbcUrl.charAt(jdbcUrl.length() - 1) != '?') {
                    jdbcUrl += "&";
                }
            } else {
                jdbcUrl += "?";
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
    private static String checkAndSetJdbcParam(String jdbcUrl, String params, String expectedVal) {
        String expectedParams = params + "=" + expectedVal;
        if (jdbcUrl.contains(expectedParams)) {
            return jdbcUrl;
        } else {
            if (jdbcUrl.contains("?")) {
                if (jdbcUrl.charAt(jdbcUrl.length() - 1) != '?') {
                    jdbcUrl += "&";
                }
            } else {
                jdbcUrl += "?";
            }
            jdbcUrl += expectedParams;
        }
        return jdbcUrl;
    }
}
