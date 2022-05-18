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
import org.apache.doris.common.proc.BaseProcResult;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

/**
 * Policy resource for olap table.
 * Syntax:
 * CREATE RESOURCE "storage_policy_name"
 * PROPERTIES(
 *      "type"="storage_policy",
 *      "cooldown_datetime" = "2022-06-01", // time when data is transfter to medium
 *      "cooldown_ttl" = "1h"， // data is transfter to medium after 1 hour
 *      "s3_resource" = "my_s3" // point to a s3 resource
 * );
 */
public class StoragePolicyResource extends Resource {
    // required
    private static final String STORAGE_RESOURCE = "storage_resource";
    // optional
    private static final String COOLDOWN_DATETIME = "cooldown_datetime";
    private static final String COOLDOWN_TTL = "cooldown_ttl";

    private static final String DEFAULT_COOLDOWN_DATETIME = "9999-01-01 00:00:00";
    private static final String DEFAULT_COOLDOWN_TTL = "1h";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public StoragePolicyResource(String name) {
        this(name, Maps.newHashMap());
    }

    public StoragePolicyResource(String name, Map<String, String> properties) {
        super(name, ResourceType.STORAGE_POLICY);
        this.properties = properties;
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);
        this.properties = properties;
        // check properties
        // required
        checkRequiredProperty(STORAGE_RESOURCE);
        // optional
        checkOptionalProperty(COOLDOWN_DATETIME, DEFAULT_COOLDOWN_DATETIME);
        checkOptionalProperty(COOLDOWN_TTL, DEFAULT_COOLDOWN_TTL);
        if (properties.containsKey(COOLDOWN_DATETIME) && properties.containsKey(COOLDOWN_TTL)
                && !properties.get(COOLDOWN_DATETIME).isEmpty() && !properties.get(COOLDOWN_TTL).isEmpty()) {
            throw new DdlException("Only one of [" + COOLDOWN_DATETIME + "] and [" + COOLDOWN_TTL
                    + "] can be specified in properties.");
        }
    }

    private void checkRequiredProperty(String propertyKey) throws DdlException {
        String value = properties.get(propertyKey);

        if (Strings.isNullOrEmpty(value)) {
            throw new DdlException("Missing [" + propertyKey + "] in properties.");
        }
    }

    private void checkOptionalProperty(String propertyKey, String defaultValue) {
        this.properties.putIfAbsent(propertyKey, defaultValue);
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        if (properties.containsKey(COOLDOWN_DATETIME) && properties.containsKey(COOLDOWN_TTL)
                && !properties.get(COOLDOWN_DATETIME).isEmpty() && !properties.get(COOLDOWN_TTL).isEmpty()) {
            throw new DdlException("Only one of [" + COOLDOWN_DATETIME + "] and [" + COOLDOWN_TTL
                    + "] can be specified in properties.");
        }
        // modify properties
        replaceIfEffectiveValue(this.properties, STORAGE_RESOURCE, properties.get(STORAGE_RESOURCE));
        replaceIfEffectiveValue(this.properties, COOLDOWN_DATETIME, properties.get(COOLDOWN_DATETIME));
        replaceIfEffectiveValue(this.properties, COOLDOWN_TTL, properties.get(COOLDOWN_TTL));
    }

    @Override
    public void checkProperties(Map<String, String> properties) throws AnalysisException {
        // check properties
        Map<String, String> copiedProperties = Maps.newHashMap(properties);
        copiedProperties.remove(STORAGE_RESOURCE);
        copiedProperties.remove(COOLDOWN_DATETIME);
        copiedProperties.remove(COOLDOWN_TTL);

        if (!copiedProperties.isEmpty()) {
            throw new AnalysisException("Unknown policy resource properties: " + copiedProperties);
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
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }

    public String getStorageCooldownTime() {
        return properties.get(COOLDOWN_DATETIME);
    }
}
