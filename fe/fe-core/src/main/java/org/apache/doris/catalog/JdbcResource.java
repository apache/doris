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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
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
    private static final String URL = "jdbc_url";
    private static final String USER = "user";
    private static final String PASSWORD = "password";
    private static final String DRIVER_CLASS = "driver_class";
    private static final String DRIVER_URL = "driver_url";
    private static final String TYPE = "type";
    private static final String CHECK_SUM = "checksum";
    private static final Logger LOG = LogManager.getLogger(JdbcResource.class);
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
        replaceIfEffectiveValue(this.configs, URL, properties.get(URL));
        replaceIfEffectiveValue(this.configs, USER, properties.get(USER));
        replaceIfEffectiveValue(this.configs, PASSWORD, properties.get(PASSWORD));
        replaceIfEffectiveValue(this.configs, TYPE, properties.get(TYPE));
    }

    @Override
    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        // check properties
        copiedProperties.remove(DRIVER_URL);
        copiedProperties.remove(DRIVER_CLASS);
        copiedProperties.remove(URL);
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
            if (!DRIVER_URL.equals(key) && !URL.equals(key) && !USER.equals(key) && !PASSWORD.equals(key)
                    && !TYPE.equals(key) && !DRIVER_CLASS.equals(key)) {
                throw new DdlException("JDBC resource Property of " + key + " is unknown");
            }
        }
        configs = properties;
        computeObjectChecksum();
        checkProperties(DRIVER_URL);
        checkProperties(DRIVER_CLASS);
        checkProperties(URL);
        checkProperties(USER);
        checkProperties(PASSWORD);
        checkProperties(TYPE);
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        Map<String, String> copiedProperties = Maps.newHashMap(configs);
        return copiedProperties;
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
        String value = configs.get(propertiesKey);
        return value;
    }

    private void computeObjectChecksum() throws DdlException {
        if (FeConstants.runningUnitTest) {
            // skip checking checksum when running ut
            return;
        }

        InputStream inputStream = null;
        try {
            inputStream = Util.getInputStreamFromUrl(getProperty(DRIVER_URL), null, HTTP_TIMEOUT_MS, HTTP_TIMEOUT_MS);
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
            String checkSum = Hex.encodeHexString(digest.digest());
            configs.put(CHECK_SUM, checkSum);
        } catch (IOException e) {
            throw new DdlException(
                    "compute driver checksum from url: " + getProperty(DRIVER_URL) + " meet an IOException.");
        } catch (NoSuchAlgorithmException e) {
            throw new DdlException(
                    "compute driver checksum from url: " + getProperty(DRIVER_URL) + " could not find algorithm.");
        }
    }
}
