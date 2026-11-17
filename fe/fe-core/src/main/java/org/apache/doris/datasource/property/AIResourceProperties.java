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

package org.apache.doris.datasource.property;

import org.apache.doris.datasource.property.constants.AIProperties;
import org.apache.doris.foundation.property.ConnectorPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;
import org.apache.doris.foundation.property.ConnectorPropertyValidator;
import org.apache.doris.foundation.property.ParamRules;
import org.apache.doris.thrift.TAIResource;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class AIResourceProperties {
    private static final List<String> ALL_EFFORT_LEVELS =
            Arrays.asList("none", "minimal", "low", "medium", "high", "xhigh", "max");
    private static final List<String> ANTHROPIC_EFFORT_LEVELS =
            Arrays.asList("low", "medium", "high", "xhigh", "max");
    private static final List<String> GEMINI_EFFORT_LEVELS =
            Arrays.asList("minimal", "low", "medium", "high");

    @ConnectorProperty(names = {AIProperties.ENDPOINT}, required = false)
    private String endpoint;
    @ConnectorProperty(names = {AIProperties.PROVIDER_TYPE}, required = false)
    private String providerType;
    @ConnectorProperty(names = {AIProperties.MODEL_NAME}, required = false)
    private String modelName;
    @ConnectorProperty(names = {AIProperties.API_KEY}, required = false, sensitive = true)
    private String apiKey;
    @ConnectorProperty(names = {AIProperties.EMBED_ENDPOINT}, required = false)
    private String embedEndpoint;
    @ConnectorProperty(names = {AIProperties.EMBED_PROVIDER_TYPE}, required = false)
    private String embedProviderType;
    @ConnectorProperty(names = {AIProperties.EMBED_MODEL_NAME}, required = false)
    private String embedModelName;
    @ConnectorProperty(names = {AIProperties.EMBED_API_KEY}, required = false, sensitive = true)
    private String embedApiKey;
    @ConnectorProperty(names = {AIProperties.MULTIMODAL_EMBED_ENDPOINT}, required = false)
    private String multimodalEmbedEndpoint;
    @ConnectorProperty(names = {AIProperties.MULTIMODAL_EMBED_PROVIDER_TYPE}, required = false)
    private String multimodalEmbedProviderType;
    @ConnectorProperty(names = {AIProperties.MULTIMODAL_EMBED_MODEL_NAME}, required = false)
    private String multimodalEmbedModelName;
    @ConnectorProperty(names = {AIProperties.MULTIMODAL_EMBED_API_KEY}, required = false, sensitive = true)
    private String multimodalEmbedApiKey;
    @ConnectorProperty(names = {AIProperties.TEMPERATURE}, required = false, validator = TemperatureValidator.class)
    private double temperature;
    @ConnectorProperty(names = {AIProperties.MAX_TOKEN}, required = false, validator = MaxTokenValidator.class)
    private long maxTokens;
    @ConnectorProperty(names = {AIProperties.MAX_RETRIES}, required = false,
            validator = NonNegativeIntegerValidator.class)
    private int maxRetries;
    @ConnectorProperty(names = {AIProperties.RETRY_DELAY_SECOND}, required = false,
            validator = NonNegativeIntegerValidator.class)
    private int retryDelaySecond;
    @ConnectorProperty(names = {AIProperties.DIMENSIONS}, required = false, validator = DimensionsValidator.class)
    private int dimensions;
    @ConnectorProperty(names = {AIProperties.ANTHROPIC_VERSION}, required = false)
    private String anthropicVersion;
    @ConnectorProperty(names = {AIProperties.EFFORT}, required = false)
    private String effort;
    @ConnectorProperty(names = {AIProperties.ENABLE_STRUCTURED_OUTPUT}, required = false,
            validator = StructuredOutputValidator.class)
    private boolean enableStructuredOutput;

    private final Map<String, String> properties;

    private AIResourceProperties(Map<String, String> properties) {
        this.properties = new HashMap<>(properties);
        this.properties.putIfAbsent(AIProperties.ENABLE_STRUCTURED_OUTPUT,
                AIProperties.DEFAULT_ENABLE_STRUCTURED_OUTPUT);
        this.properties.putIfAbsent(AIProperties.TEMPERATURE, AIProperties.DEFAULT_TEMPERATURE);
        this.properties.putIfAbsent(AIProperties.MAX_TOKEN, AIProperties.DEFAULT_MAX_TOKEN);
        this.properties.putIfAbsent(AIProperties.MAX_RETRIES, AIProperties.DEFAULT_MAX_RETRIES);
        this.properties.putIfAbsent(AIProperties.RETRY_DELAY_SECOND, AIProperties.DEFAULT_RETRY_DELAY_SECOND);
        this.properties.putIfAbsent(AIProperties.DIMENSIONS, AIProperties.DEFAULT_DIMENSIONS);
        this.properties.putIfAbsent(AIProperties.ANTHROPIC_VERSION, AIProperties.DEFAULT_ANTHROPIC_VERSION);
    }

    public static AIResourceProperties of(Map<String, String> properties) {
        ParamRules rules = new ParamRules();
        properties.forEach((key, value) -> rules.require(value, "[" + key + "] must not be empty"));
        rules.validate();
        AIResourceProperties result = new AIResourceProperties(properties);
        ConnectorPropertiesUtils.bindConnectorProperties(result, result.properties);
        result.providerType = result.normalizeProvider(AIProperties.PROVIDER_TYPE, result.providerType);
        result.embedProviderType = result.normalizeProvider(AIProperties.EMBED_PROVIDER_TYPE, result.embedProviderType);
        result.multimodalEmbedProviderType = result.normalizeProvider(
                AIProperties.MULTIMODAL_EMBED_PROVIDER_TYPE, result.multimodalEmbedProviderType);
        result.validate();
        return result;
    }

    private String normalizeProvider(String key, String value) {
        if (value == null) {
            return null;
        }
        String normalized = value.toUpperCase(Locale.ROOT);
        properties.put(key, normalized);
        return normalized;
    }

    private void validate() {
        boolean hasGeneral = hasAnyProperty(AIProperties.REQUIRED_FIELDS, AIProperties.API_KEY);
        boolean hasEmbed = hasAnyProperty(AIProperties.EMBED_REQUIRED_FIELDS, AIProperties.EMBED_API_KEY);
        boolean hasMultimodal = hasAnyProperty(
                AIProperties.MULTIMODAL_EMBED_REQUIRED_FIELDS, AIProperties.MULTIMODAL_EMBED_API_KEY);
        new ParamRules().check(() -> !hasGeneral && !hasEmbed && !hasMultimodal,
                "At least one complete AI property group must be configured.").validate();
        if (hasGeneral) {
            validateGroup(AIProperties.REQUIRED_FIELDS, providerType, AIProperties.API_KEY, apiKey);
        }
        if (hasEmbed) {
            validateGroup(AIProperties.EMBED_REQUIRED_FIELDS, embedProviderType,
                    AIProperties.EMBED_API_KEY, embedApiKey);
        }
        if (hasMultimodal) {
            validateGroup(AIProperties.MULTIMODAL_EMBED_REQUIRED_FIELDS, multimodalEmbedProviderType,
                    AIProperties.MULTIMODAL_EMBED_API_KEY, multimodalEmbedApiKey);
        }
        List<String> effortLevels = "ANTHROPIC".equals(providerType) ? ANTHROPIC_EFFORT_LEVELS
                : "GEMINI".equals(providerType) ? GEMINI_EFFORT_LEVELS : ALL_EFFORT_LEVELS;
        new ParamRules().check(() -> !Strings.isNullOrEmpty(effort) && !effortLevels.contains(effort),
                "[" + AIProperties.EFFORT + "] must be one of " + effortLevels
                        + " for provider: " + providerType).validate();
    }

    private boolean hasAnyProperty(List<String> requiredFields, String apiKeyField) {
        return properties.containsKey(apiKeyField) || requiredFields.stream().anyMatch(properties::containsKey);
    }

    private void validateGroup(List<String> requiredFields, String provider, String apiKeyField, String key) {
        ParamRules rules = new ParamRules();
        for (String field : requiredFields) {
            rules.check(() -> Strings.isNullOrEmpty(properties.get(field)),
                    "Missing [" + field + "] in properties.");
        }
        rules.check(() -> !AIProperties.PROVIDERS.contains(provider),
                "Provider must be one of " + AIProperties.PROVIDERS);
        if (!"LOCAL".equals(provider)) {
            rules.check(() -> Strings.isNullOrEmpty(key),
                    "Missing [" + apiKeyField + "] in properties for provider: " + provider);
        }
        rules.validate();
    }

    public boolean hasCompleteGeneralProperties() {
        return hasCompleteGeneralProperties(properties);
    }

    public boolean hasCompleteEmbedProperties() {
        return hasCompleteEmbedProperties(properties);
    }

    public boolean hasCompleteMultimodalEmbedProperties() {
        return hasCompleteMultimodalEmbedProperties(properties);
    }

    public static boolean hasCompleteGeneralProperties(Map<String, String> properties) {
        return hasCompleteGroup(properties, AIProperties.REQUIRED_FIELDS,
                AIProperties.PROVIDER_TYPE, AIProperties.API_KEY);
    }

    public static boolean hasCompleteEmbedProperties(Map<String, String> properties) {
        return hasCompleteGroup(properties, AIProperties.EMBED_REQUIRED_FIELDS,
                AIProperties.EMBED_PROVIDER_TYPE, AIProperties.EMBED_API_KEY);
    }

    public static boolean hasCompleteMultimodalEmbedProperties(Map<String, String> properties) {
        return hasCompleteGroup(properties, AIProperties.MULTIMODAL_EMBED_REQUIRED_FIELDS,
                AIProperties.MULTIMODAL_EMBED_PROVIDER_TYPE, AIProperties.MULTIMODAL_EMBED_API_KEY);
    }

    private static boolean hasCompleteGroup(Map<String, String> properties, List<String> requiredFields,
            String providerField, String apiKeyField) {
        return requiredFields.stream()
                .allMatch(field -> StringUtils.isNotEmpty(StringUtils.trim(properties.get(field))))
                && ("LOCAL".equalsIgnoreCase(StringUtils.trim(properties.get(providerField)))
                || StringUtils.isNotEmpty(StringUtils.trim(properties.get(apiKeyField))));
    }

    public Map<String, String> toMap() {
        Map<String, String> normalized = new HashMap<>(properties);
        for (Field field : ConnectorPropertiesUtils.getConnectorProperties(AIResourceProperties.class)) {
            for (String name : field.getAnnotation(ConnectorProperty.class).names()) {
                normalized.computeIfPresent(name, (key, value) -> value.trim());
            }
        }
        return normalized;
    }

    public TAIResource toThrift() {
        TAIResource result = new TAIResource();
        result.setEnableStructuredOutput(enableStructuredOutput);
        result.setEndpoint(endpoint);
        result.setProviderType(providerType);
        result.setModelName(modelName);
        result.setApiKey(apiKey);
        result.setEmbedEndpoint(embedEndpoint);
        result.setEmbedProviderType(embedProviderType);
        result.setEmbedModelName(embedModelName);
        result.setEmbedApiKey(embedApiKey);
        result.setEmbedMmEndpoint(multimodalEmbedEndpoint);
        result.setEmbedMmProviderType(multimodalEmbedProviderType);
        result.setEmbedMmModelName(multimodalEmbedModelName);
        result.setEmbedMmApiKey(multimodalEmbedApiKey);
        result.setTemperature(temperature);
        result.setMaxTokens(maxTokens);
        result.setMaxRetries(maxRetries);
        result.setRetryDelaySecond(retryDelaySecond);
        result.setDimensions(dimensions);
        result.setAnthropicVersion(anthropicVersion);
        if (!Strings.isNullOrEmpty(effort)) {
            result.setEffort(effort);
        }
        return result;
    }

    @Override
    public String toString() {
        return ConnectorPropertiesUtils.toMaskedString(this);
    }

    public static final class StructuredOutputValidator implements ConnectorPropertyValidator {
        @Override
        public void validate(Object target, Field field, String propertyName, Object value,
                Map<String, String> properties) {
            String text = properties.get(propertyName).trim();
            new ParamRules().check(() -> !"true".equalsIgnoreCase(text) && !"false".equalsIgnoreCase(text),
                    "[" + propertyName + "] must be true or false").validate();
        }
    }

    public static final class TemperatureValidator implements ConnectorPropertyValidator {
        @Override
        public void validate(Object target, Field field, String propertyName, Object value,
                Map<String, String> properties) {
            double temperature = (Double) value;
            new ParamRules().check(() -> temperature != -1 && !(temperature >= 0 && temperature <= 1),
                    "Temperature must be -1 or a double between 0 and 1").validate();
        }
    }

    public static final class MaxTokenValidator implements ConnectorPropertyValidator {
        @Override
        public void validate(Object target, Field field, String propertyName, Object value,
                Map<String, String> properties) {
            long maxTokens = (Long) value;
            new ParamRules().check(() -> maxTokens != -1 && maxTokens <= 0,
                    "Max token must be a positive integer or -1").validate();
        }
    }

    public static final class NonNegativeIntegerValidator implements ConnectorPropertyValidator {
        @Override
        public void validate(Object target, Field field, String propertyName, Object value,
                Map<String, String> properties) {
            new ParamRules().check(() -> (Integer) value < 0,
                    "[" + propertyName + "] must be a non-negative integer").validate();
        }
    }

    public static final class DimensionsValidator implements ConnectorPropertyValidator {
        @Override
        public void validate(Object target, Field field, String propertyName, Object value,
                Map<String, String> properties) {
            int dimensions = (Integer) value;
            new ParamRules().check(() -> dimensions != -1 && dimensions <= 0,
                    "Dimensions must be a positive integer or -1").validate();
        }
    }
}
