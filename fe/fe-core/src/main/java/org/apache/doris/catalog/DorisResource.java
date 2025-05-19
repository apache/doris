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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Doris resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "remote_doris"
 * PROPERTIES
 * (
 * "type" = "doris",
 * "fe_hosts" = "http://192.168.0.1:8030,http://192.168.0.2:8030",
 * "fe_arrow_hosts" = "192.168.0.1:9090,192.168.0.2:9090",
 * "user" = "root",
 * "password" = "root"
 * );
 */
public class DorisResource extends Resource {
    public static final String DORIS_PROPERTIES_PREFIX = "doris.";

    public static final String FE_HOSTS = "fe_hosts";
    public static final String FE_ARROW_HOSTS = "fe_arrow_hosts";
    public static final String USER = "user";
    public static final String PASSWORD = "password";
    public static final String HTTP_SSL_ENABLED = "http_ssl_enabled";
    public static final String BE_HOSTS = "be_hosts";
    public static final String MAX_EXEC_BE_NUM = "max_exec_be_num";
    public static final String MAX_EXEC_BE_NUM_DEFAULT_VALUE = "-1";
    public static final String HTTP_SSL_ENABLED_DEFAULT_VALUE = "false";
    // Supports older versions of remote Doris; enabling this may introduce some inaccuracies in schema parsing.
    public static final String COMPATIBLE = "compatible";
    public static final String COMPATIBLE_DEFAULT_VALUE = "false";

    private static final ImmutableList<String> ALL_PROPERTIES = new ImmutableList.Builder<String>().add(
            FE_HOSTS,
            FE_ARROW_HOSTS,
            USER,
            PASSWORD
    ).build();

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public DorisResource() {
        super();
    }

    public DorisResource(String name) {
        super(name, ResourceType.DORIS);
        properties = Maps.newHashMap();
    }

    public static void checkBooleanProperty(String propertyName, String propertyValue) throws DdlException {
        if (!propertyValue.equalsIgnoreCase("true") && !propertyValue.equalsIgnoreCase("false")) {
            throw new DdlException(propertyName + " must be true or false");
        }
    }

    public static void checkHostsAndPortProperty(String propertyName, String hostsAndPort) throws DdlException {
        String[] hostAndPortArr = hostsAndPort.trim().split(",");
        if (hostAndPortArr.length < 1) {
            throw new DdlException(propertyName + " must be 'ip:port, ip:port'");
        }
        for (String hostAndPortStr : hostAndPortArr) {
            if (hostAndPortStr.trim().split(":").length != 2) {
                throw new DdlException(propertyName + " must be 'ip:port, ip:port'");
            }
        }
    }

    public static void fillUrlsWithSchema(String[] urls, boolean isSslEnabled) {
        for (int i = 0; i < urls.length; i++) {
            String seed = urls[i].trim();
            if (!seed.startsWith("http://") && !seed.startsWith("https://")) {
                urls[i] = (isSslEnabled ? "https://" : "http://") + seed;
            }
        }
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        valid(properties);
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
        super.modifyProperties(properties);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        valid(properties);
        this.properties = properties;
    }

    public static void valid(Map<String, String> properties) throws DdlException {
        for (String key : properties.keySet()) {
            if (!ALL_PROPERTIES.contains(key)) {
                throw new DdlException("Doris resource Property of " + key + " is unknown");
            }
        }
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals(PASSWORD)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), ""));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
    }
}
