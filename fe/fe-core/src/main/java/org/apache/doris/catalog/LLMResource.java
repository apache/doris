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
import org.apache.doris.datasource.property.constants.LLMProperties;
import org.apache.doris.thrift.TLLMResource;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * LLM Resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "deepseek-chat"
 * PROPERTIES
 * (
 * 'type' = 'llm',
 * 'llm.provider_type' = 'deepseek',
 * 'llm.endpoint' = 'https://api.deepseek.com/chat/completions',
 * 'llm.model_name' = 'deepseek-chat',
 * 'llm.api_key' = 'sk-xxx',
 * 'llm.temperature' = '0.7',
 * 'llm.max_token' = '1024',
 * 'llm.max_retries' = '3',
 * 'llm.retry_delay_second' = '1'
 * );
 * <p>
 */

public class LLMResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(LLMResource.class);
    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public LLMResource() {
        super();
    }

    public LLMResource(String name) {
        super(name, ResourceType.LLM);
        properties = Maps.newHashMap();
    }

    @Override
    protected void setProperties(ImmutableMap<String, String> newProperties) throws DdlException {
        Preconditions.checkState(newProperties != null);
        this.properties = Maps.newHashMap(newProperties);

        LLMProperties.requiredLLMProperties(properties);

        boolean needCheck = isNeedCheck(properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("LLM resource need check validity: {}", needCheck);
        }
        if (needCheck) {
            pingLLM(properties);
        }

        LLMProperties.optionalLLMProperties(this.properties);
    }

    protected static void pingLLM(Map<String, String> properties) throws DdlException {
        String endpoint = properties.get(LLMProperties.ENDPOINT);
        if (endpoint == null || endpoint.isEmpty()) {
            throw new DdlException("LLM endpoint is not specified");
        }

        try {
            URL url = new URL(endpoint);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("HEAD");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);

            connection.connect();

            LOG.info("Successfully pinged LLM API at {}", endpoint);
        } catch (IOException e) {
            throw new DdlException("Failed to ping LLM API at " + endpoint + ": " + e.getMessage());
        }
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    private boolean isNeedCheck(Map<String, String> newProperties) {
        boolean needCheck = !this.properties.containsKey(LLMProperties.VALIDITY_CHECK)
                || Boolean.parseBoolean(this.properties.get(LLMProperties.VALIDITY_CHECK));

        if (newProperties != null && newProperties.containsKey(LLMProperties.VALIDITY_CHECK)) {
            needCheck = Boolean.parseBoolean(newProperties.get(LLMProperties.VALIDITY_CHECK));
        }

        if ("LOCAL".equalsIgnoreCase(this.properties.getOrDefault(LLMProperties.PROVIDER_TYPE, ""))) {
            needCheck = false;
        }
        return needCheck;
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        boolean needCheck = isNeedCheck(properties);
        if (LOG.isDebugEnabled()) {
            LOG.debug("LLM resource need check validity: {}", needCheck);
        }

        if (needCheck) {
            Map<String, String> changedProperties = new HashMap<>(this.properties);
            changedProperties.putAll(properties);
            LLMProperties.requiredLLMProperties(changedProperties);
            pingLLM(changedProperties);
        }

        // modify properties
        writeLock();
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
            if (kv.getKey().equals(LLMProperties.API_KEY)) {
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
            if (entry.getKey().equals(LLMProperties.API_KEY)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
        readUnlock();
    }

    public TLLMResource toThrift() throws NumberFormatException {
        TLLMResource tLLMResource = new TLLMResource();
        tLLMResource.setProviderType(properties.get(LLMProperties.PROVIDER_TYPE));
        tLLMResource.setEndpoint(properties.get(LLMProperties.ENDPOINT));
        tLLMResource.setApiKey(properties.get(LLMProperties.API_KEY));
        tLLMResource.setModelName(properties.get(LLMProperties.MODEL_NAME));
        tLLMResource.setAnthropicVersion(properties.get(LLMProperties.ANTHROPIC_VERSION));

        try {
            tLLMResource.setTemperature(Double.parseDouble(properties.get(LLMProperties.TEMPERATURE)));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse temperature: "
                                            + properties.get(LLMProperties.TEMPERATURE));
        }
        try {
            tLLMResource.setMaxTokens(Long.parseLong(properties.get(LLMProperties.MAX_TOKEN)));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse max_token: "
                                            + properties.get(LLMProperties.MAX_TOKEN));
        }
        try {
            tLLMResource.setMaxRetries(Long.parseLong(properties.get(LLMProperties.MAX_RETRIES)));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse max_retries: "
                                            + properties.get(LLMProperties.MAX_RETRIES));
        }
        try {
            tLLMResource.setRetryDelaySecond(Long.parseLong(properties.get(LLMProperties.RETRY_DELAY_SECOND)));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse retry_delay_second: "
                                            + properties.get(LLMProperties.RETRY_DELAY_SECOND));
        }
        try {
            tLLMResource.setDimensions(Integer.parseInt(properties.get(LLMProperties.DIMENSIONS)));
        } catch (NumberFormatException e) {
            throw new NumberFormatException("Failed to parse dimensions: "
                                            + properties.get(LLMProperties.DIMENSIONS));
        }

        return tLLMResource;
    }
}
