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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.zip.Adler32;

import com.google.gson.annotations.SerializedName;

/**
 * External ODBC Catalog resource for external table query.
 *
 * External ODBC Catalog resource example:
 * CREATE EXTERNAL RESOURCE "odbc_mysql"
 * PROPERTIES
 * (
 *     "type" = "external_odbc", [required]
 *     "user" = "root", [required]
 *     "password" = "root", [required]
 *     "host" = "192.168.1.1", [required]
 *     "port" = "8086", [required]
 *     "odbc_type" = "mysql", [optional, external table of ODBC should set]
 *     "driver" = "MySQL driver" [optional, external table of ODBC should set]
 * );
 *
 * DROP RESOURCE "odbc_mysql";
 */
public class OdbcCatalogResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(OdbcCatalogResource.class);
    // required
    private static final String HOST = "host";
    private static final String PORT = "port";
    private static final String USER = "user";
    private static final String PASSWORD = "password";

    // optional
    private static final String TYPE = "odbc_type";
    private static final String DRIVER = "driver";

    @SerializedName(value = "configs")
    private Map<String, String> configs;

    // only for deep copy
    public OdbcCatalogResource() {
        super();
    }

    public OdbcCatalogResource(String name) {
        this(name, Maps.newHashMap());
    }

    private OdbcCatalogResource(String name, Map<String, String> configs) {
        super(name, ResourceType.ODBC_CATALOG);
        this.configs = configs;
    }

    public OdbcCatalogResource getCopiedResource() {
        return new OdbcCatalogResource(name, Maps.newHashMap(configs));
    }

    private void checkProperties(String propertiesKey) throws DdlException {
        // check the properties key
        String value = configs.get(propertiesKey);
        if (value == null) {
            throw new DdlException("Missing " + propertiesKey + " in properties");
        }

    }

    public String getProperties(String propertiesKey)  {
        // check the properties key
        String value = configs.get(propertiesKey);
        return value;
    }

    // TODO(ml): change to md5 of string signature
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);
        final String charsetName = "UTF-8";

        try {
            // table name
            adler32.update(name.getBytes(charsetName));
            LOG.debug("signature. view name: {}", name);
            // type
            adler32.update(type.name().getBytes(charsetName));
            LOG.debug("signature. view type: {}", type.name());
            // configs
            for (Map.Entry<String, String> config: configs.entrySet()) {
                adler32.update(config.getKey().getBytes(charsetName));
                adler32.update(config.getValue().getBytes(charsetName));
                LOG.debug("signature. view config: {}", config);
            }
        } catch (UnsupportedEncodingException e) {
            LOG.error("encoding error", e);
            return -1;
        }

        LOG.debug("signature: {}", Math.abs((int) adler32.getValue()));
        return Math.abs((int) adler32.getValue());
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);

        configs = properties;

        checkProperties(HOST);
        checkProperties(PORT);
        checkProperties(USER);
        checkProperties(PASSWORD);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : configs.entrySet()) {
            // it's dangerous to show password in show odbc resource
            // so we use empty string to replace the real password
            if (entry.getKey().equals(PASSWORD)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), ""));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
    }
}

