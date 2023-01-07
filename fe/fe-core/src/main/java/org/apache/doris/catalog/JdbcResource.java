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
import org.apache.doris.external.jdbc.JdbcClientException;

import com.google.common.base.Preconditions;
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

    public static final String MYSQL = "MYSQL";
    public static final String POSTGRESQL = "POSTGRESQL";
    // private static final String ORACLE = "ORACLE";
    // private static final String SQLSERVER = "SQLSERVER";

    public static final String JDBC_PROPERTIES_PREFIX = "jdbc.";
    public static final String JDBC_URL = "jdbc_url";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String DRIVER_CLASS = "driver_class";
    public static final String DRIVER_URL = "driver_url";
    public static final String TYPE = "type";
    public static final String CHECK_SUM = "checksum";

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

    public JdbcResource getCopiedResource() {
        return new JdbcResource(name, Maps.newHashMap(configs));
    }

    private void checkProperties(String propertiesKey) throws DdlException {
        // check the properties key
        String value = configs.get(propertiesKey);
        if (value == null) {
            throw new DdlException("JdbcResource Missing " + propertiesKey + " in properties");
        }
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        // modify properties
        replaceIfEffectiveValue(this.configs, DRIVER_URL, properties.get(DRIVER_URL));
        replaceIfEffectiveValue(this.configs, DRIVER_CLASS, properties.get(DRIVER_CLASS));
        replaceIfEffectiveValue(this.configs, JDBC_URL, properties.get(JDBC_URL));
        replaceIfEffectiveValue(this.configs, USER, properties.get(USER));
        replaceIfEffectiveValue(this.configs, PASSWORD, properties.get(PASSWORD));
        replaceIfEffectiveValue(this.configs, TYPE, properties.get(TYPE));
        this.configs.put(JDBC_URL, handleJdbcUrl(getProperty(JDBC_URL)));
        super.modifyProperties(properties);
    }

    @Override
    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        // check properties
        copiedProperties.remove(DRIVER_URL);
        copiedProperties.remove(DRIVER_CLASS);
        copiedProperties.remove(JDBC_URL);
        copiedProperties.remove(USER);
        copiedProperties.remove(PASSWORD);
        copiedProperties.remove(TYPE);
        if (!copiedProperties.isEmpty()) {
            throw new AnalysisException("Unknown JDBC catalog resource properties: " + copiedProperties);
        }
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        for (String key : properties.keySet()) {
            if (!DRIVER_URL.equals(key) && !JDBC_URL.equals(key) && !USER.equals(key) && !PASSWORD.equals(key)
                    && !TYPE.equals(key) && !DRIVER_CLASS.equals(key)) {
                throw new DdlException("JDBC resource Property of " + key + " is unknown");
            }
        }
        configs = properties;
        checkProperties(DRIVER_URL);
        checkProperties(DRIVER_CLASS);
        checkProperties(JDBC_URL);
        checkProperties(USER);
        checkProperties(PASSWORD);
        checkProperties(TYPE);
        this.configs.put(JDBC_URL, handleJdbcUrl(getProperty(JDBC_URL)));
        configs.put(CHECK_SUM, computeObjectChecksum(getProperty(DRIVER_URL)));
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
        String fullDriverPath = getRealDriverPath(driverPath);
        InputStream inputStream = null;
        try {
            inputStream = Util.getInputStreamFromUrl(fullDriverPath, null, HTTP_TIMEOUT_MS, HTTP_TIMEOUT_MS);
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

    private static String getRealDriverPath(String driverUrl) {
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

    public static String parseDbType(String url) {
        if (url.startsWith("jdbc:mysql") || url.startsWith("jdbc:mariadb")) {
            return MYSQL;
        } else if (url.startsWith("jdbc:postgresql")) {
            return POSTGRESQL;
        }
        // else if (url.startsWith("jdbc:oracle")) {
        //     return ORACLE;
        // }
        // else if (url.startsWith("jdbc:sqlserver")) {
        //     return SQLSERVER;
        throw new JdbcClientException("Unsupported jdbc database type, please check jdbcUrl: " + url);
    }

    public static String handleJdbcUrl(String jdbcUrl) {
        // delete all space in jdbcUrl
        String newJdbcUrl = jdbcUrl.replaceAll(" ", "");
        String dbType = parseDbType(newJdbcUrl);
        if (dbType.equals(MYSQL)) {
            // 1. `yearIsDateType` is a parameter of JDBC, and the default is true.
            // We force the use of `yearIsDateType=false`
            // 2. `tinyInt1isBit` is a parameter of JDBC, and the default is true.
            // We force the use of `tinyInt1isBit=false`, so that for mysql type tinyint,
            // it will convert to Doris tinyint, not bit.
            newJdbcUrl = checkJdbcUrlParam(newJdbcUrl, "yearIsDateType", "true", "false");
            newJdbcUrl = checkJdbcUrlParam(newJdbcUrl, "tinyInt1isBit", "true", "false");
        }
        return newJdbcUrl;
    }

    private static String checkJdbcUrlParam(String jdbcUrl, String params, String unexpectedVal, String expectedVal) {
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
}
