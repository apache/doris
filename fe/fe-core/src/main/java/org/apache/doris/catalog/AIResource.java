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
import org.apache.doris.datasource.property.AIResourceProperties;
import org.apache.doris.datasource.property.constants.AIProperties;
import org.apache.doris.thrift.TAIResource;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

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
    @SerializedName(value = "properties")
    private volatile Map<String, String> properties;
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
        this.properties = parseProperties(newProperties).toMap();
    }

    public String getProperty(String propertyKey) {
        return properties.get(propertyKey);
    }

    public boolean hasCompleteGeneralProperties() {
        return AIResourceProperties.hasCompleteGeneralProperties(properties);
    }

    public boolean hasCompleteEmbedProperties() {
        return AIResourceProperties.hasCompleteEmbedProperties(properties);
    }

    public boolean hasCompleteMultimodalEmbedProperties() {
        return AIResourceProperties.hasCompleteMultimodalEmbedProperties(properties);
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        writeLock();
        try {
            Map<String, String> changedProperties = new HashMap<>(this.properties);
            changedProperties.putAll(properties);
            this.properties = parseProperties(changedProperties).toMap();
            ++version;
        } finally {
            writeUnlock();
        }
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
            if (entry.getKey().equals(AIProperties.API_KEY)
                    || entry.getKey().equals(AIProperties.EMBED_API_KEY)
                    || entry.getKey().equals(AIProperties.MULTIMODAL_EMBED_API_KEY)) {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), "******"));
            } else {
                result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
            }
        }
        readUnlock();
    }

    public TAIResource toThrift() {
        return AIResourceProperties.of(properties).toThrift();
    }

    private static AIResourceProperties parseProperties(Map<String, String> properties) throws DdlException {
        try {
            return AIResourceProperties.of(properties);
        } catch (IllegalArgumentException e) {
            throw new DdlException(e.getMessage(), e);
        }
    }
}
