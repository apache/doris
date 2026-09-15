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
import org.apache.doris.datasource.property.constants.AIProperties;
import org.apache.doris.thrift.TAIResource;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * AI Resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "deepseek-chat"
 * PROPERTIES
 * (
 * 'type' = 'ai',
 * 'ai.provider_type' = 'deepseek',
 * 'ai.endpoint' = 'https://api.deepseek.com/chat/completions',
 * 'ai.model_name' = 'deepseek-chat',
 * 'ai.api_key' = 'sk-xxx',
 * 'ai.temperature' = '0.7',
 * 'ai.max_token' = '1024',
 * 'ai.max_retries' = '3',
 * 'ai.retry_delay_second' = '1'
 * );
 * <p>
 */

public class AIResource extends Resource {
    private static final String LEGACY_VALIDITY_CHECK = "ai.validity_check";

    @SerializedName(value = "properties")
    private Map<String, String> properties;
    @SerializedName(value = "createdByRoot")
    private boolean createdByRoot;

    public AIResource() {
        super();
    }

    public AIResource(String name) {
        super(name, ResourceType.AI);
        properties = Maps.newHashMap();
    }

    public boolean isCreatedByRoot() {
        return createdByRoot;
    }

    void setCreatedByRoot(boolean createdByRoot) {
        this.createdByRoot = createdByRoot;
    }

    @Override
    protected void setProperties(ImmutableMap<String, String> newProperties) throws DdlException {
        Preconditions.checkState(newProperties != null);
        Map<String, String> changedProperties = Maps.newHashMap(newProperties);
        changedProperties.remove(LEGACY_VALIDITY_CHECK);

        AIProperties.requiredAIProperties(changedProperties);
        AIProperties.optionalAIProperties(changedProperties);
        this.properties = changedProperties;
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    @Override
    public void modifyProperties(Map<String, String> newProperties) throws DdlException {
        Map<String, String> changedProperties = new HashMap<>(this.properties);
        changedProperties.remove(LEGACY_VALIDITY_CHECK);
        for (Map.Entry<String, String> kv : newProperties.entrySet()) {
            if (LEGACY_VALIDITY_CHECK.equals(kv.getKey())) {
                continue;
            }
            replaceIfEffectiveValue(changedProperties, kv.getKey(), kv.getValue());
            if (AIProperties.API_KEY.equals(kv.getKey())) {
                changedProperties.put(kv.getKey(), kv.getValue());
            }
        }
        AIProperties.requiredAIProperties(changedProperties);
        AIProperties.optionalAIProperties(changedProperties);

        // modify properties
        writeLock();
        this.properties = changedProperties;
        ++version;
        writeUnlock();
        super.modifyProperties(newProperties);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();
        if (properties != null) {
            properties.remove(LEGACY_VALIDITY_CHECK);
        }
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        String lowerCaseType = type.name().toLowerCase();
        result.addRow(Lists.newArrayList(name, lowerCaseType, "id", String.valueOf(id)));
        readLock();
        result.addRow(Lists.newArrayList(name, lowerCaseType, "version", String.valueOf(version)));
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().equals(AIProperties.API_KEY)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
        readUnlock();
    }

    public TAIResource toThrift() throws NumberFormatException {
        TAIResource tAIResource = new TAIResource();
        tAIResource.setProviderType(properties.get(AIProperties.PROVIDER_TYPE));
        tAIResource.setEndpoint(properties.get(AIProperties.ENDPOINT));
        tAIResource.setApiKey(properties.get(AIProperties.API_KEY));
        tAIResource.setModelName(properties.get(AIProperties.MODEL_NAME));
        tAIResource.setAnthropicVersion(properties.get(AIProperties.ANTHROPIC_VERSION));

        try {
            tAIResource.setTemperature(Double.parseDouble(properties.get(AIProperties.TEMPERATURE)));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse temperature: "
                                            + properties.get(AIProperties.TEMPERATURE));
        }
        try {
            tAIResource.setMaxTokens(Long.parseLong(properties.get(AIProperties.MAX_TOKEN)));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse max_token: "
                                            + properties.get(AIProperties.MAX_TOKEN));
        }
        try {
            tAIResource.setMaxRetries(Integer.parseInt(properties.get(AIProperties.MAX_RETRIES)));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse max_retries: "
                                            + properties.get(AIProperties.MAX_RETRIES));
        }
        try {
            tAIResource.setRetryDelaySecond(Integer.parseInt(properties.get(AIProperties.RETRY_DELAY_SECOND)));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse retry_delay_second: "
                                            + properties.get(AIProperties.RETRY_DELAY_SECOND));
        }
        try {
            tAIResource.setDimensions(Integer.parseInt(properties.get(AIProperties.DIMENSIONS)));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse dimensions: "
                                            + properties.get(AIProperties.DIMENSIONS));
        }

        return tAIResource;
    }
}
