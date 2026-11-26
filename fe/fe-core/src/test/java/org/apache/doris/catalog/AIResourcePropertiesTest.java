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
import org.apache.doris.datasource.property.AIResourceProperties;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TAIResource;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

public class AIResourcePropertiesTest {
    private Map<String, String> generalProperties() {
        return new HashMap<>(ImmutableMap.of(
                "ai.endpoint", "http://localhost:8000/v1/chat/completions",
                "ai.provider_type", "local",
                "ai.model_name", "chat-model"));
    }

    @Test
    public void testTypedDefaultsAndPropertyOwnership() {
        Map<String, String> raw = generalProperties();
        raw.put("ai.validity_check", "false");
        raw.put("custom.property", "retained");
        AIResourceProperties properties = AIResourceProperties.of(raw);
        raw.put("ai.model_name", "changed-model");

        TAIResource thrift = properties.toThrift();
        Assertions.assertEquals("LOCAL", thrift.getProviderType());
        Assertions.assertEquals("chat-model", thrift.getModelName());
        Assertions.assertFalse(thrift.isSetApiKey());
        Assertions.assertEquals(-1.0, thrift.getTemperature());
        Assertions.assertEquals(-1L, thrift.getMaxTokens());
        Assertions.assertEquals(3, thrift.getMaxRetries());
        Assertions.assertEquals(1, thrift.getRetryDelaySecond());
        Assertions.assertEquals(-1, thrift.getDimensions());
        Assertions.assertEquals("2023-06-01", thrift.getAnthropicVersion());
        Assertions.assertTrue(properties.hasCompleteGeneralProperties());
        Assertions.assertFalse(properties.hasCompleteEmbedProperties());
        Assertions.assertFalse(properties.hasCompleteMultimodalEmbedProperties());
        Assertions.assertEquals("false", properties.toMap().get("ai.validity_check"));
        Assertions.assertEquals("retained", properties.toMap().get("custom.property"));
        properties.toMap().put("ai.model_name", "another-model");
        Assertions.assertEquals("chat-model", properties.toThrift().getModelName());
    }

    @Test
    public void testIndependentEmbedGroups() {
        AIResourceProperties embed = AIResourceProperties.of(ImmutableMap.of(
                "ai.embed.endpoint", "http://localhost:8000/v1/embeddings",
                "ai.embed.provider_type", "local",
                "ai.embed.model_name", "text-model"));
        Assertions.assertFalse(embed.hasCompleteGeneralProperties());
        Assertions.assertTrue(embed.hasCompleteEmbedProperties());
        Assertions.assertFalse(embed.hasCompleteMultimodalEmbedProperties());
        Assertions.assertFalse(embed.toThrift().isSetProviderType());
        Assertions.assertEquals("LOCAL", embed.toThrift().getEmbedProviderType());
        Assertions.assertEquals("text-model", embed.toThrift().getEmbedModelName());

        AIResourceProperties multimodal = AIResourceProperties.of(ImmutableMap.of(
                "ai.embed.mm.endpoint", "http://localhost:8000/v1/embeddings",
                "ai.embed.mm.provider_type", "local",
                "ai.embed.mm.model_name", "image-model"));
        Assertions.assertFalse(multimodal.hasCompleteGeneralProperties());
        Assertions.assertFalse(multimodal.hasCompleteEmbedProperties());
        Assertions.assertTrue(multimodal.hasCompleteMultimodalEmbedProperties());
        Assertions.assertEquals("LOCAL", multimodal.toThrift().getEmbedMmProviderType());
        Assertions.assertEquals("image-model", multimodal.toThrift().getEmbedMmModelName());
    }

    @Test
    public void testExplicitValuesAndCombinedGroups() {
        Map<String, String> raw = generalProperties();
        raw.put("ai.api_key", "general-key");
        raw.put("ai.embed.endpoint", "http://localhost:8001/embeddings");
        raw.put("ai.embed.provider_type", "openai");
        raw.put("ai.embed.model_name", "text-model");
        raw.put("ai.embed.api_key", "text-key");
        raw.put("ai.embed.mm.endpoint", "http://localhost:8002/embeddings");
        raw.put("ai.embed.mm.provider_type", "gemini");
        raw.put("ai.embed.mm.model_name", "image-model");
        raw.put("ai.embed.mm.api_key", "image-key");
        raw.put("ai.temperature", "0.75");
        raw.put("ai.max_token", "4294967296");
        raw.put("ai.max_retries", "0");
        raw.put("ai.retry_delay_second", "0");
        raw.put("ai.dimensions", "768");
        raw.put("ai.anthropic_version", "2024-01-01");
        raw.put("ai.effort", "high");
        AIResourceProperties properties = AIResourceProperties.of(raw);
        TAIResource thrift = properties.toThrift();
        Assertions.assertTrue(properties.hasCompleteGeneralProperties());
        Assertions.assertTrue(properties.hasCompleteEmbedProperties());
        Assertions.assertTrue(properties.hasCompleteMultimodalEmbedProperties());
        Assertions.assertEquals("general-key", thrift.getApiKey());
        Assertions.assertEquals("text-key", thrift.getEmbedApiKey());
        Assertions.assertEquals("image-key", thrift.getEmbedMmApiKey());
        Assertions.assertEquals("http://localhost:8001/embeddings", thrift.getEmbedEndpoint());
        Assertions.assertEquals("http://localhost:8002/embeddings", thrift.getEmbedMmEndpoint());
        Assertions.assertEquals("OPENAI", thrift.getEmbedProviderType());
        Assertions.assertEquals("GEMINI", thrift.getEmbedMmProviderType());
        Assertions.assertEquals(0.75, thrift.getTemperature());
        Assertions.assertEquals(4294967296L, thrift.getMaxTokens());
        Assertions.assertEquals(0, thrift.getMaxRetries());
        Assertions.assertEquals(0, thrift.getRetryDelaySecond());
        Assertions.assertEquals(768, thrift.getDimensions());
        Assertions.assertEquals("2024-01-01", thrift.getAnthropicVersion());
        Assertions.assertEquals("high", thrift.getEffort());
        Assertions.assertFalse(properties.toString().contains("general-key"));
        Assertions.assertFalse(properties.toString().contains("text-key"));
        Assertions.assertFalse(properties.toString().contains("image-key"));
    }

    @Test
    public void testExistingGroupAndValueRules() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> AIResourceProperties.of(ImmutableMap.of()));
        Map<String, String> raw = generalProperties();
        raw.put("ai.embed.model_name", "partial-model");
        Assertions.assertThrows(IllegalArgumentException.class, () -> AIResourceProperties.of(raw));
        raw.remove("ai.embed.model_name");
        raw.put("ai.provider_type", "openai");
        Assertions.assertThrows(IllegalArgumentException.class, () -> AIResourceProperties.of(raw));
        raw.put("ai.api_key", "key");
        raw.put("ai.temperature", "1.5");
        Assertions.assertThrows(IllegalArgumentException.class, () -> AIResourceProperties.of(raw));
        raw.put("ai.temperature", "-1");
        raw.put("ai.dimensions", "0");
        Assertions.assertThrows(IllegalArgumentException.class, () -> AIResourceProperties.of(raw));
        raw.put("ai.dimensions", "-1");
        raw.put("ai.provider_type", "anthropic");
        raw.put("ai.effort", "none");
        Assertions.assertThrows(IllegalArgumentException.class, () -> AIResourceProperties.of(raw));
        raw.put("ai.effort", "high");
        Assertions.assertEquals("high", AIResourceProperties.of(raw).toThrift().getEffort());
    }

    @Test
    public void testInvalidNumericBinding() {
        for (String key : new String[] {"ai.temperature", "ai.max_token", "ai.max_retries",
                "ai.retry_delay_second", "ai.dimensions"}) {
            for (String value : new String[] {"", " ", "not-a-number"}) {
                Map<String, String> raw = generalProperties();
                raw.put(key, value);
                IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class,
                        () -> AIResourceProperties.of(raw));
                Assertions.assertTrue(failure.getMessage().contains(key));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("invalidNumericProperties")
    public void testInvalidNumericRangesOnCreateAndAlter(String key, String value) throws Exception {
        Map<String, String> invalid = generalProperties();
        invalid.put(key, value);
        AIResource created = new AIResource("invalid-numeric-resource");
        DdlException createFailure = Assertions.assertThrows(DdlException.class,
                () -> created.setProperties(ImmutableMap.copyOf(invalid)));
        Assertions.assertTrue(createFailure.getMessage().contains(key));

        AIResource altered = new AIResource("valid-numeric-resource");
        altered.setProperties(ImmutableMap.copyOf(generalProperties()));
        Map<String, String> before = altered.getCopiedProperties();
        long version = altered.version;
        DdlException alterFailure = Assertions.assertThrows(DdlException.class,
                () -> altered.modifyProperties(ImmutableMap.of(key, value)));
        Assertions.assertTrue(alterFailure.getMessage().contains(key));
        Assertions.assertEquals(before, altered.getCopiedProperties());
        Assertions.assertEquals(version, altered.version);
    }

    private static Stream<Arguments> invalidNumericProperties() {
        return Stream.of(
                Arguments.of("ai.temperature", "NaN"),
                Arguments.of("ai.temperature", "Infinity"),
                Arguments.of("ai.temperature", "-Infinity"),
                Arguments.of("ai.temperature", "1e309"),
                Arguments.of("ai.temperature", "-0.1"),
                Arguments.of("ai.temperature", "1.1"),
                Arguments.of("ai.temperature", "-2"),
                Arguments.of("ai.max_token", "0"),
                Arguments.of("ai.max_token", "-2"),
                Arguments.of("ai.max_token", "-9223372036854775808"),
                Arguments.of("ai.max_token", "9223372036854775808"),
                Arguments.of("ai.max_token", "1.5"),
                Arguments.of("ai.max_retries", "-1"),
                Arguments.of("ai.max_retries", "-2147483648"),
                Arguments.of("ai.max_retries", "2147483648"),
                Arguments.of("ai.max_retries", "1.5"),
                Arguments.of("ai.retry_delay_second", "-1"),
                Arguments.of("ai.retry_delay_second", "-2147483648"),
                Arguments.of("ai.retry_delay_second", "2147483648"),
                Arguments.of("ai.retry_delay_second", "1.5"),
                Arguments.of("ai.dimensions", "0"),
                Arguments.of("ai.dimensions", "-2"),
                Arguments.of("ai.dimensions", "-2147483648"),
                Arguments.of("ai.dimensions", "2147483648"),
                Arguments.of("ai.dimensions", "1.5"));
    }

    @Test
    public void testNumericBoundaryValues() throws Exception {
        Map<String, String> raw = generalProperties();
        raw.putAll(ImmutableMap.of("ai.temperature", "1", "ai.max_token", "9223372036854775807",
                "ai.max_retries", "2147483647", "ai.retry_delay_second", "2147483647",
                "ai.dimensions", "2147483647"));
        AIResource resource = new AIResource("numeric-boundaries");
        resource.setProperties(ImmutableMap.copyOf(raw));
        TAIResource maximum = resource.toThrift();
        Assertions.assertEquals(1.0, maximum.getTemperature());
        Assertions.assertEquals(Long.MAX_VALUE, maximum.getMaxTokens());
        Assertions.assertEquals(Integer.MAX_VALUE, maximum.getMaxRetries());
        Assertions.assertEquals(Integer.MAX_VALUE, maximum.getRetryDelaySecond());
        Assertions.assertEquals(Integer.MAX_VALUE, maximum.getDimensions());

        resource.modifyProperties(ImmutableMap.of("ai.temperature", "0", "ai.max_token", "1",
                "ai.max_retries", "0", "ai.retry_delay_second", "0", "ai.dimensions", "1"));
        TAIResource minimum = resource.toThrift();
        Assertions.assertEquals(0.0, minimum.getTemperature());
        Assertions.assertEquals(1L, minimum.getMaxTokens());
        Assertions.assertEquals(0, minimum.getMaxRetries());
        Assertions.assertEquals(0, minimum.getRetryDelaySecond());
        Assertions.assertEquals(1, minimum.getDimensions());

        resource.modifyProperties(ImmutableMap.of("ai.temperature", "-1", "ai.max_token", "-1",
                "ai.dimensions", "-1"));
        Assertions.assertEquals(-1.0, resource.toThrift().getTemperature());
        Assertions.assertEquals(-1L, resource.toThrift().getMaxTokens());
        Assertions.assertEquals(-1, resource.toThrift().getDimensions());
    }

    @Test
    public void testProviderNormalizationIsLocaleIndependent() throws Exception {
        Locale original = Locale.getDefault();
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            Map<String, String> raw = generalProperties();
            raw.put("ai.provider_type", "minimax");
            raw.put("ai.api_key", "general-key");
            raw.put("ai.embed.endpoint", "http://localhost:8001/embeddings");
            raw.put("ai.embed.provider_type", "gemini");
            raw.put("ai.embed.model_name", "text-model");
            raw.put("ai.embed.api_key", "text-key");
            raw.put("ai.embed.mm.endpoint", "http://localhost:8002/embeddings");
            raw.put("ai.embed.mm.provider_type", "jina");
            raw.put("ai.embed.mm.model_name", "image-model");
            raw.put("ai.embed.mm.api_key", "image-key");
            AIResource resource = new AIResource("locale-independent-resource");
            resource.setProperties(ImmutableMap.copyOf(raw));
            Assertions.assertEquals("MINIMAX", resource.toThrift().getProviderType());
            Assertions.assertEquals("GEMINI", resource.toThrift().getEmbedProviderType());
            Assertions.assertEquals("JINA", resource.toThrift().getEmbedMmProviderType());
            resource.modifyProperties(ImmutableMap.of("ai.provider_type", "jina",
                    "ai.embed.provider_type", "minimax", "ai.embed.mm.provider_type", "gemini"));
            Assertions.assertEquals("JINA", resource.getProperty("ai.provider_type"));
            Assertions.assertEquals("MINIMAX", resource.getProperty("ai.embed.provider_type"));
            Assertions.assertEquals("GEMINI", resource.getProperty("ai.embed.mm.provider_type"));
        } finally {
            Locale.setDefault(original);
        }
    }

    @Test
    public void testLegacyOutOfRangeMetadataLoadsAndCanBeRepaired() throws Exception {
        Map<String, String> invalid = ImmutableMap.of(
                "ai.max_token", "0", "ai.max_retries", "-1", "ai.retry_delay_second", "-1");
        for (Map.Entry<String, String> entry : invalid.entrySet()) {
            Map<String, String> raw = generalProperties();
            raw.put(entry.getKey(), entry.getValue());
            AIResource loaded = readMetadata(raw);
            Assertions.assertEquals(entry.getValue(), loaded.getProperty(entry.getKey()));
            Assertions.assertTrue(loaded.hasCompleteGeneralProperties());
            IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class,
                    loaded::toThrift);
            Assertions.assertTrue(failure.getMessage().contains(entry.getKey()));
            loaded.modifyProperties(ImmutableMap.of(entry.getKey(), "1"));
            Assertions.assertNotNull(loaded.toThrift());
        }
    }

    @Test
    public void testResourceAlterAndPersistence() throws Exception {
        AIResource resource = new AIResource("typed-resource");
        resource.setProperties(ImmutableMap.copyOf(generalProperties()));
        resource.modifyProperties(ImmutableMap.of("ai.provider_type", "openai", "ai.api_key", "key",
                "ai.effort", "high", "ai.max_token", "2048"));
        Assertions.assertEquals("OPENAI", resource.getProperty("ai.provider_type"));
        Assertions.assertEquals(2048L, resource.toThrift().getMaxTokens());
        Map<String, String> before = resource.getCopiedProperties();
        long version = resource.version;
        Assertions.assertThrows(DdlException.class,
                () -> resource.modifyProperties(ImmutableMap.of("ai.effort", "invalid")));
        Assertions.assertEquals(before, resource.getCopiedProperties());
        Assertions.assertEquals(version, resource.version);
        Assertions.assertThrows(DdlException.class,
                () -> resource.modifyProperties(ImmutableMap.of("ai.effort", "")));
        Assertions.assertEquals("high", resource.toThrift().getEffort());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            resource.write(output);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            AIResource loaded = (AIResource) Resource.read(input);
            Assertions.assertEquals(resource.getCopiedProperties(), loaded.getCopiedProperties());
            Assertions.assertEquals(resource.toThrift(), loaded.toThrift());
        }
    }

    @Test
    public void testMetadataLoadingDoesNotValidateProperties() {
        String json = "{\"clazz\":\"AIResource\",\"name\":\"legacy-ai\",\"type\":\"AI\","
                + "\"properties\":{\"ai.provider_type\":\"LOCAL\",\"ai.endpoint\":\"http://localhost:8000\","
                + "\"ai.model_name\":\"legacy-model\",\"ai.max_retries\":\"not-an-integer\","
                + "\"ai.validity_check\":\"false\"}}";
        AIResource loaded = (AIResource) GsonUtils.GSON.fromJson(json, Resource.class);
        Assertions.assertEquals("not-an-integer", loaded.getProperty("ai.max_retries"));
        Assertions.assertEquals("false", loaded.getProperty("ai.validity_check"));
        Assertions.assertTrue(loaded.hasCompleteGeneralProperties());
        Assertions.assertFalse(loaded.hasCompleteEmbedProperties());
        Assertions.assertFalse(loaded.hasCompleteMultimodalEmbedProperties());
        Assertions.assertThrows(IllegalArgumentException.class, loaded::toThrift);
    }

    @Test
    public void testStringValuesAreTrimmed() {
        Map<String, String> raw = generalProperties();
        raw.put("ai.model_name", " model ");
        raw.put("ai.endpoint", " http://localhost:8000 ");
        raw.put("ai.api_key", " key ");
        raw.put("ai.anthropic_version", " 2023-06-01 ");
        AIResourceProperties properties = AIResourceProperties.of(raw);
        TAIResource thrift = properties.toThrift();
        Assertions.assertEquals("model", thrift.getModelName());
        Assertions.assertEquals("http://localhost:8000", thrift.getEndpoint());
        Assertions.assertEquals("key", thrift.getApiKey());
        Assertions.assertEquals("2023-06-01", thrift.getAnthropicVersion());
        Assertions.assertEquals("model", properties.toMap().get("ai.model_name"));
        Assertions.assertEquals("http://localhost:8000", properties.toMap().get("ai.endpoint"));
        Assertions.assertEquals("key", properties.toMap().get("ai.api_key"));
        Assertions.assertEquals("2023-06-01", properties.toMap().get("ai.anthropic_version"));
    }

    @Test
    public void testCreateAndAlterPersistNormalizedProperties() throws Exception {
        Map<String, String> expected = new HashMap<>(ImmutableMap.<String, String>builder()
                .put("ai.provider_type", "LOCAL")
                .put("ai.endpoint", "http://localhost:8000/chat")
                .put("ai.model_name", "chat-model")
                .put("ai.api_key", "chat-key")
                .put("ai.embed.provider_type", "OPENAI")
                .put("ai.embed.endpoint", "http://localhost:8001/embeddings")
                .put("ai.embed.model_name", "text-model")
                .put("ai.embed.api_key", "text-key")
                .put("ai.embed.mm.provider_type", "GEMINI")
                .put("ai.embed.mm.endpoint", "http://localhost:8002/embeddings")
                .put("ai.embed.mm.model_name", "image-model")
                .put("ai.embed.mm.api_key", "image-key")
                .put("ai.temperature", "0.75")
                .put("ai.max_token", "4294967296")
                .put("ai.max_retries", "3")
                .put("ai.retry_delay_second", "2")
                .put("ai.dimensions", "768")
                .put("ai.anthropic_version", "2023-06-01")
                .put("ai.effort", "high")
                .put("ai.enable_structured_output", "true")
                .build());
        Map<String, String> raw = new HashMap<>();
        expected.forEach((key, value) -> raw.put(key, " \t" + value + "\t "));
        raw.put("custom.property", " retained ");
        raw.put("ai.validity_check", " false ");
        expected.put("custom.property", " retained ");
        expected.put("ai.validity_check", " false ");
        Assertions.assertEquals(expected, AIResourceProperties.of(raw).toMap());
        Assertions.assertEquals(" \t768\t ", raw.get("ai.dimensions"));

        AIResource resource = new AIResource("normalized-resource");
        resource.setProperties(ImmutableMap.copyOf(raw));
        assertNormalizedMetadata(resource, expected);

        resource.modifyProperties(ImmutableMap.of("ai.dimensions", " 1024 ",
                "ai.max_retries", " 4 ", "ai.model_name", " updated-model ",
                "ai.embed.api_key", " updated-key ", "ai.enable_structured_output", " false "));
        expected.putAll(ImmutableMap.of("ai.dimensions", "1024", "ai.max_retries", "4",
                "ai.model_name", "updated-model", "ai.embed.api_key", "updated-key",
                "ai.enable_structured_output", "false"));
        assertNormalizedMetadata(resource, expected);
    }

    private void assertNormalizedMetadata(AIResource resource, Map<String, String> expected) throws Exception {
        Assertions.assertEquals(expected, resource.getCopiedProperties());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            resource.write(output);
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            AIResource loaded = (AIResource) Resource.read(input);
            Assertions.assertEquals(expected, loaded.getCopiedProperties());
            Assertions.assertEquals(resource.toThrift(), loaded.toThrift());
            Assertions.assertDoesNotThrow(() -> Double.parseDouble(loaded.getProperty("ai.temperature")));
            Assertions.assertDoesNotThrow(() -> Long.parseLong(loaded.getProperty("ai.max_token")));
            Assertions.assertDoesNotThrow(() -> Integer.parseInt(loaded.getProperty("ai.max_retries")));
            Assertions.assertDoesNotThrow(() -> Integer.parseInt(loaded.getProperty("ai.retry_delay_second")));
            Assertions.assertDoesNotThrow(() -> Integer.parseInt(loaded.getProperty("ai.dimensions")));
            Assertions.assertEquals(resource.toThrift().isEnableStructuredOutput(),
                    Boolean.parseBoolean(loaded.getProperty("ai.enable_structured_output")));
        }
    }

    @Test
    public void testRejectBlankCreateAndAlterProperties() throws Exception {
        AIResource resource = new AIResource("non-empty-properties");
        resource.setProperties(ImmutableMap.copyOf(generalProperties()));
        Map<String, String> before = resource.getCopiedProperties();
        long version = resource.version;
        for (String key : new String[] {"ai.endpoint", "ai.provider_type", "ai.model_name", "ai.api_key",
                "ai.embed.endpoint", "ai.embed.provider_type", "ai.embed.model_name", "ai.embed.api_key",
                "ai.embed.mm.endpoint", "ai.embed.mm.provider_type", "ai.embed.mm.model_name", "ai.embed.mm.api_key",
                "ai.temperature", "ai.max_token", "ai.max_retries", "ai.retry_delay_second", "ai.dimensions",
                "ai.anthropic_version", "ai.effort"}) {
            for (String value : new String[] {"", " \t ", null}) {
                Map<String, String> raw = generalProperties();
                raw.put(key, value);
                IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class,
                        () -> AIResourceProperties.of(raw));
                Assertions.assertTrue(failure.getMessage().contains(key));
                Map<String, String> updates = new HashMap<>();
                updates.put(key, value);
                Assertions.assertThrows(DdlException.class, () -> resource.modifyProperties(updates));
                Assertions.assertEquals(before, resource.getCopiedProperties());
                Assertions.assertEquals(version, resource.version);
            }
        }
    }

    @Test
    public void testLegacyPaddedMetadata() {
        Map<String, String> raw = generalProperties();
        raw.put("ai.provider_type", " local ");
        raw.put("ai.model_name", " model ");
        raw.put("ai.endpoint", " http://localhost:8000 ");
        raw.put("ai.max_retries", " 3 ");
        AIResource loaded = readMetadata(raw);
        Assertions.assertTrue(loaded.hasCompleteGeneralProperties());
        Assertions.assertEquals("LOCAL", loaded.toThrift().getProviderType());
        Assertions.assertEquals("model", loaded.toThrift().getModelName());
        Assertions.assertEquals("http://localhost:8000", loaded.toThrift().getEndpoint());
        Assertions.assertEquals(3, loaded.toThrift().getMaxRetries());
        Assertions.assertEquals(raw, loaded.getCopiedProperties());
    }

    @Test
    public void testLegacyBlankMetadataLoadsAndCanBeRepaired() throws Exception {
        for (String key : new String[] {"ai.model_name", "ai.anthropic_version", "ai.effort", "ai.max_retries"}) {
            for (String value : new String[] {"", " \t "}) {
                Map<String, String> raw = generalProperties();
                raw.put(key, value);
                AIResource loaded = readMetadata(raw);
                Assertions.assertEquals(value, loaded.getProperty(key));
                Assertions.assertEquals(!key.equals("ai.model_name"), loaded.hasCompleteGeneralProperties());
                IllegalArgumentException failure = Assertions.assertThrows(IllegalArgumentException.class,
                        loaded::toThrift);
                Assertions.assertTrue(failure.getMessage().contains(key));
                loaded.modifyProperties(ImmutableMap.of(key, key.equals("ai.max_retries") ? "3" : "high"));
                Assertions.assertNotNull(loaded.toThrift());
            }
        }
    }

    private AIResource readMetadata(Map<String, String> properties) {
        String json = GsonUtils.GSON.toJson(ImmutableMap.of(
                "clazz", "AIResource", "name", "legacy", "type", "AI", "properties", properties));
        return (AIResource) GsonUtils.GSON.fromJson(json, Resource.class);
    }

    @Test
    public void testIncompleteMetadataReturnsGroupCompleteness() {
        AIResource loaded = (AIResource) GsonUtils.GSON.fromJson(
                "{\"clazz\":\"AIResource\",\"name\":\"partial\",\"type\":\"AI\","
                        + "\"properties\":{\"ai.endpoint\":\"http://localhost:8000\","
                        + "\"ai.embed.provider_type\":\"local\",\"ai.embed.model_name\":\"text-model\","
                        + "\"ai.embed.endpoint\":\"http://localhost:8001\"}}", Resource.class);
        Assertions.assertFalse(loaded.hasCompleteGeneralProperties());
        Assertions.assertTrue(loaded.hasCompleteEmbedProperties());
        Assertions.assertFalse(loaded.hasCompleteMultimodalEmbedProperties());
    }
}
