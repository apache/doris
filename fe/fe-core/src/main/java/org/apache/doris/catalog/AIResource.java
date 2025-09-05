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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    private static final Logger LOG = LogManager.getLogger(AIResource.class);
    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public AIResource() {
        super();
    }

    public AIResource(String name) {
        super(name, ResourceType.AI);
        properties = Maps.newHashMap();
    }

    @Override
    protected void setProperties(ImmutableMap<String, String> newProperties) throws DdlException {
        Preconditions.checkState(newProperties != null);
        this.properties = Maps.newHashMap(newProperties);

        AIProperties.requiredAIProperties(properties);

        boolean needCheck = isNeedCheck(properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("AI resource need check validity: {}", needCheck);
        }

        AIProperties.optionalAIProperties(this.properties);
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    private boolean isNeedCheck(Map<String, String> newProperties) {
        boolean needCheck = !this.properties.containsKey(AIProperties.VALIDITY_CHECK)
                || Boolean.parseBoolean(this.properties.get(AIProperties.VALIDITY_CHECK));

        if (newProperties != null && newProperties.containsKey(AIProperties.VALIDITY_CHECK)) {
            needCheck = Boolean.parseBoolean(newProperties.get(AIProperties.VALIDITY_CHECK));
        }

        if ("LOCAL".equalsIgnoreCase(this.properties.getOrDefault(AIProperties.PROVIDER_TYPE, ""))) {
            needCheck = false;
        }
        return needCheck;
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        boolean needCheck = isNeedCheck(properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("AI resource need check validity: {}", needCheck);
        }

        if (needCheck) {
            Map<String, String> changedProperties = new HashMap<>(this.properties);
            changedProperties.putAll(properties);
            AIProperties.requiredAIProperties(changedProperties);
        }

        // modify properties
        writeLock();
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
            if (kv.getKey().equals(AIProperties.API_KEY)) {
                this.properties.put(kv.getKey(), kv.getValue());
            }
        }
        ++version;
        writeUnlock();
        super.modifyProperties(properties);
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
