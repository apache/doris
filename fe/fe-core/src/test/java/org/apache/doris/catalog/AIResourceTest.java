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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.datasource.property.constants.AIProperties;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.agg.AIAgg;
import org.apache.doris.nereids.trees.expressions.functions.ai.AISentiment;
import org.apache.doris.nereids.trees.expressions.functions.ai.Embed;
import org.apache.doris.nereids.trees.expressions.literal.JsonLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class AIResourceTest {
    private static final Logger LOG = LogManager.getLogger(AIResourceTest.class);
    private String name;
    private String type;

    private String endpoint;
    private String providerType;
    private String apiKey;
    private String modelName;
    private String temperature;
    private String maxToken;
    private String maxRetries;
    private String retryDelaySecond;
    private Map<String, String> aiProperties;

    @BeforeEach
    public void setUp() {
        name = "openai-gpt";
        type = "ai";
        endpoint = "https://api.openai.com/v1/chat/completions";
        providerType = "openai";
        apiKey = "xxxxxxxxxxxxxxxxxxxxxxx";
        modelName = "gpt-3.5-turbo";
        temperature = "0.5";
        maxToken = "2048";
        maxRetries = "5";
        retryDelaySecond = "2";

        aiProperties = new HashMap<>();
        aiProperties.put("type", type);
        aiProperties.put("ai.endpoint", endpoint);
        aiProperties.put("ai.provider_type", providerType);
        aiProperties.put("ai.api_key", apiKey);
        aiProperties.put("ai.model_name", modelName);
        aiProperties.put("ai.validity_check", "false");
    }

    @Test
    public void testFromCommand() throws UserException {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkGlobalPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                    .thenReturn(true);

            // resource with default settings
            CreateResourceCommand initialCreateResourceCommand = new CreateResourceCommand(
                    new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(aiProperties)));
            initialCreateResourceCommand.getInfo().validate();

            AIResource legacyAIResource = (AIResource) Resource.fromCommand(initialCreateResourceCommand);
            Assertions.assertFalse(legacyAIResource.isCreatedByRoot());

            AIResource rootCreatedAIResource =
                    (AIResource) Resource.fromCommand(initialCreateResourceCommand, UserIdentity.ROOT);
            Assertions.assertTrue(rootCreatedAIResource.isCreatedByRoot());

            AIResource aiResource =
                    (AIResource) Resource.fromCommand(initialCreateResourceCommand, UserIdentity.ADMIN);
            Assertions.assertFalse(aiResource.isCreatedByRoot());

            Assertions.assertEquals(name, aiResource.getName());
            Assertions.assertEquals(type, aiResource.getType().name().toLowerCase());
            Assertions.assertEquals(endpoint, aiResource.getProperty(AIProperties.ENDPOINT));
            Assertions.assertEquals(providerType.toUpperCase(), aiResource.getProperty(AIProperties.PROVIDER_TYPE));
            Assertions.assertEquals(apiKey, aiResource.getProperty(AIProperties.API_KEY));
            Assertions.assertEquals(modelName, aiResource.getProperty(AIProperties.MODEL_NAME));

            Assertions.assertEquals(AIProperties.DEFAULT_TEMPERATURE,
                    aiResource.getProperty(AIProperties.TEMPERATURE));
            Assertions.assertEquals(AIProperties.DEFAULT_MAX_TOKEN,
                    aiResource.getProperty(AIProperties.MAX_TOKEN));
            Assertions.assertEquals(AIProperties.DEFAULT_MAX_RETRIES,
                    aiResource.getProperty(AIProperties.MAX_RETRIES));
            Assertions.assertEquals(AIProperties.DEFAULT_RETRY_DELAY_SECOND,
                    aiResource.getProperty(AIProperties.RETRY_DELAY_SECOND));

            // with no default settings
            aiProperties.put(AIProperties.TEMPERATURE, temperature);
            aiProperties.put(AIProperties.MAX_TOKEN, maxToken);
            aiProperties.put(AIProperties.MAX_RETRIES, maxRetries);
            aiProperties.put(AIProperties.RETRY_DELAY_SECOND, retryDelaySecond);

            CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                    new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(aiProperties)));
            createResourceCommand.getInfo().validate();

            aiResource = (AIResource) Resource.fromCommand(createResourceCommand);
            Assertions.assertEquals(name, aiResource.getName());
            Assertions.assertEquals(type, aiResource.getType().name().toLowerCase());
            Assertions.assertEquals(endpoint, aiResource.getProperty(AIProperties.ENDPOINT));
            Assertions.assertEquals(providerType.toUpperCase(), aiResource.getProperty(AIProperties.PROVIDER_TYPE));
            Assertions.assertEquals(apiKey, aiResource.getProperty(AIProperties.API_KEY));
            Assertions.assertEquals(modelName, aiResource.getProperty(AIProperties.MODEL_NAME));
            Assertions.assertEquals(temperature, aiResource.getProperty(AIProperties.TEMPERATURE));
            Assertions.assertEquals(maxToken, aiResource.getProperty(AIProperties.MAX_TOKEN));
            Assertions.assertEquals(maxRetries, aiResource.getProperty(AIProperties.MAX_RETRIES));
            Assertions.assertEquals(retryDelaySecond, aiResource.getProperty(AIProperties.RETRY_DELAY_SECOND));
        }
    }

    @Test
    public void testAnthropic() throws UserException {
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            EditLog editLog = Mockito.mock(EditLog.class);
            AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getEditLog()).thenReturn(editLog);
            Mockito.when(env.getAccessManager()).thenReturn(accessManager);
            Mockito.when(accessManager.checkGlobalPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                    .thenReturn(true);

            Map<String, String> anthropicProps = new HashMap<>(aiProperties);
            anthropicProps.put("ai.provider_type", "anthropic");
            anthropicProps.put("ai.endpoint", "https://api.anthropic.com/v1/messages");
            anthropicProps.put("ai.model_name", "claude-opus-4-20250514");
            anthropicProps.put("ai.anthropic_version", "2023-06-01");

            CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                    new CreateResourceInfo(true, false, "anthropic-claude", ImmutableMap.copyOf(anthropicProps)));
            createResourceCommand.getInfo().validate();

            AIResource aiResource = (AIResource) Resource.fromCommand(createResourceCommand);
            Assertions.assertEquals("anthropic-claude", aiResource.getName());
            Assertions.assertEquals("ANTHROPIC", aiResource.getProperty(AIProperties.PROVIDER_TYPE));
            Assertions.assertEquals("https://api.anthropic.com/v1/messages",
                    aiResource.getProperty(AIProperties.ENDPOINT));
            Assertions.assertEquals("claude-opus-4-20250514", aiResource.getProperty(AIProperties.MODEL_NAME));
            Assertions.assertEquals("2023-06-01", aiResource.getProperty(AIProperties.ANTHROPIC_VERSION));
        }
    }

    @Test
    public void testAbnormalResource() throws UserException {
        Assertions.assertThrows(DdlException.class, () -> {
            try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
                Env env = Mockito.mock(Env.class);
                EditLog editLog = Mockito.mock(EditLog.class);
                AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
                mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
                Mockito.when(env.getEditLog()).thenReturn(editLog);
                Mockito.when(env.getAccessManager()).thenReturn(accessManager);
                Mockito.when(accessManager.checkGlobalPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                        .thenReturn(true);

                aiProperties.remove("ai.endpoint");
                CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                        new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(aiProperties)));
                createResourceCommand.getInfo().validate();

                Resource.fromCommand(createResourceCommand);
            }
        });
    }

    @Test
    public void testEmbedOnlyResource() throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(AIProperties.EMBED_ENDPOINT, "https://api.example.com/v1/embeddings");
        properties.put(AIProperties.EMBED_PROVIDER_TYPE, "openai");
        properties.put(AIProperties.EMBED_MODEL_NAME, "text-embedding-model");
        properties.put(AIProperties.EMBED_API_KEY, "embed-api-key");

        AIResource aiResource = new AIResource("embed-only-resource");
        aiResource.setProperties(ImmutableMap.copyOf(properties));

        Assertions.assertEquals("OPENAI", aiResource.getProperty(AIProperties.EMBED_PROVIDER_TYPE));
        Assertions.assertEquals("https://api.example.com/v1/embeddings",
                aiResource.toThrift().getEmbedEndpoint());
        Assertions.assertEquals("OPENAI", aiResource.toThrift().getEmbedProviderType());
        Assertions.assertEquals("text-embedding-model", aiResource.toThrift().getEmbedModelName());
        Assertions.assertEquals("embed-api-key", aiResource.toThrift().getEmbedApiKey());
        Assertions.assertFalse(aiResource.toThrift().isSetEndpoint());
    }

    @Test
    public void testMultimodalEmbedOnlyResource() throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(AIProperties.MULTIMODAL_EMBED_ENDPOINT,
                "https://api.example.com/v1/multimodal-embeddings");
        properties.put(AIProperties.MULTIMODAL_EMBED_PROVIDER_TYPE, "qwen");
        properties.put(AIProperties.MULTIMODAL_EMBED_MODEL_NAME, "multimodal-embedding-model");
        properties.put(AIProperties.MULTIMODAL_EMBED_API_KEY, "multimodal-embed-api-key");

        AIResource aiResource = new AIResource("multimodal-embed-only-resource");
        aiResource.setProperties(ImmutableMap.copyOf(properties));

        Assertions.assertEquals("QWEN", aiResource.getProperty(AIProperties.MULTIMODAL_EMBED_PROVIDER_TYPE));
        Assertions.assertEquals("https://api.example.com/v1/multimodal-embeddings",
                aiResource.getProperty(AIProperties.MULTIMODAL_EMBED_ENDPOINT));
        Assertions.assertEquals("multimodal-embedding-model",
                aiResource.getProperty(AIProperties.MULTIMODAL_EMBED_MODEL_NAME));
        Assertions.assertEquals("multimodal-embed-api-key",
                aiResource.getProperty(AIProperties.MULTIMODAL_EMBED_API_KEY));
        Assertions.assertEquals("https://api.example.com/v1/multimodal-embeddings",
                aiResource.toThrift().getEmbedMmEndpoint());
        Assertions.assertEquals("QWEN", aiResource.toThrift().getEmbedMmProviderType());
        Assertions.assertEquals("multimodal-embedding-model", aiResource.toThrift().getEmbedMmModelName());
        Assertions.assertEquals("multimodal-embed-api-key", aiResource.toThrift().getEmbedMmApiKey());
        Assertions.assertFalse(aiResource.toThrift().isSetEndpoint());
        Assertions.assertFalse(aiResource.toThrift().isSetEmbedEndpoint());

        BaseProcResult result = new BaseProcResult();
        aiResource.getProcNodeData(result);
        Assertions.assertTrue(result.getRows().stream().anyMatch(row ->
                AIProperties.MULTIMODAL_EMBED_API_KEY.equals(row.get(2)) && "******".equals(row.get(3))));
        Assertions.assertFalse(result.getRows().stream().anyMatch(row -> row.contains("multimodal-embed-api-key")));
    }

    @Test
    public void testRejectPartialMultimodalEmbedProperties() {
        Map<String, String> properties = new HashMap<>(aiProperties);
        properties.put(AIProperties.MULTIMODAL_EMBED_ENDPOINT,
                "https://api.example.com/v1/multimodal-embeddings");

        AIResource aiResource = new AIResource("partial-multimodal-embed-resource");
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> aiResource.setProperties(ImmutableMap.copyOf(properties)));
        Assertions.assertTrue(exception.getMessage().contains(AIProperties.MULTIMODAL_EMBED_PROVIDER_TYPE));
    }

    @Test
    public void testRejectEmbedOnlyResourceForNonEmbedScalarFunction() throws DdlException {
        AIResource aiResource = createEmbedOnlyResource();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            ResourceMgr resourceMgr = Mockito.mock(ResourceMgr.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getResourceMgr()).thenReturn(resourceMgr);
            Mockito.when(resourceMgr.getResource("embed-only-resource")).thenReturn(aiResource);

            AISentiment function = new AISentiment(new StringLiteral("embed-only-resource"),
                    new StringLiteral("text"));
            Assertions.assertThrows(AnalysisException.class, function::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void testRejectEmbedOnlyResourceForAiAgg() throws DdlException {
        AIResource aiResource = createEmbedOnlyResource();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            ResourceMgr resourceMgr = Mockito.mock(ResourceMgr.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getResourceMgr()).thenReturn(resourceMgr);
            Mockito.when(resourceMgr.getResource("embed-only-resource")).thenReturn(aiResource);

            AIAgg function = new AIAgg(new StringLiteral("embed-only-resource"),
                    new StringLiteral("text"), new StringLiteral("task"));
            Assertions.assertThrows(AnalysisException.class, function::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void testAcceptEmbedOnlyResourceForEmbedFunction() throws DdlException {
        AIResource aiResource = createEmbedOnlyResource();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            ResourceMgr resourceMgr = Mockito.mock(ResourceMgr.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getResourceMgr()).thenReturn(resourceMgr);
            Mockito.when(resourceMgr.getResource("embed-only-resource")).thenReturn(aiResource);

            Embed function = new Embed(new StringLiteral("embed-only-resource"),
                    new StringLiteral("text"));
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void testAcceptMultimodalEmbedOnlyResourceForJsonEmbed() throws DdlException {
        AIResource aiResource = createMultimodalEmbedOnlyResource();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            ResourceMgr resourceMgr = Mockito.mock(ResourceMgr.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getResourceMgr()).thenReturn(resourceMgr);
            Mockito.when(resourceMgr.getResource("multimodal-embed-only-resource")).thenReturn(aiResource);

            Embed function = new Embed(new StringLiteral("multimodal-embed-only-resource"),
                    new JsonLiteral("{\"text\":\"hello\"}"));
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
        }
    }

    @Test
    public void testRejectMultimodalEmbedOnlyResourceForTextEmbed() throws DdlException {
        AIResource aiResource = createMultimodalEmbedOnlyResource();
        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            ResourceMgr resourceMgr = Mockito.mock(ResourceMgr.class);
            mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getResourceMgr()).thenReturn(resourceMgr);
            Mockito.when(resourceMgr.getResource("multimodal-embed-only-resource")).thenReturn(aiResource);

            Embed function = new Embed(new StringLiteral("multimodal-embed-only-resource"),
                    new StringLiteral("hello"));
            Assertions.assertThrows(AnalysisException.class, function::checkLegalityBeforeTypeCoercion);
        }
    }

    private AIResource createEmbedOnlyResource() throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(AIProperties.EMBED_ENDPOINT, "https://api.example.com/v1/embeddings");
        properties.put(AIProperties.EMBED_PROVIDER_TYPE, "openai");
        properties.put(AIProperties.EMBED_MODEL_NAME, "text-embedding-model");
        properties.put(AIProperties.EMBED_API_KEY, "embed-api-key");

        AIResource aiResource = new AIResource("embed-only-resource");
        aiResource.setProperties(ImmutableMap.copyOf(properties));
        return aiResource;
    }

    private AIResource createMultimodalEmbedOnlyResource() throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(AIProperties.MULTIMODAL_EMBED_ENDPOINT,
                "https://api.example.com/v1/multimodal-embeddings");
        properties.put(AIProperties.MULTIMODAL_EMBED_PROVIDER_TYPE, "qwen");
        properties.put(AIProperties.MULTIMODAL_EMBED_MODEL_NAME, "multimodal-embedding-model");
        properties.put(AIProperties.MULTIMODAL_EMBED_API_KEY, "multimodal-embed-api-key");

        AIResource aiResource = new AIResource("multimodal-embed-only-resource");
        aiResource.setProperties(ImmutableMap.copyOf(properties));
        return aiResource;
    }

    @Test
    public void testLocalEmbedResourceWithoutApiKey() throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(AIProperties.EMBED_ENDPOINT, "http://localhost:8000/v1/embeddings");
        properties.put(AIProperties.EMBED_PROVIDER_TYPE, "local");
        properties.put(AIProperties.EMBED_MODEL_NAME, "local-embedding-model");

        AIResource aiResource = new AIResource("local-embed-resource");
        aiResource.setProperties(ImmutableMap.copyOf(properties));

        Assertions.assertEquals("LOCAL", aiResource.getProperty(AIProperties.EMBED_PROVIDER_TYPE));
    }

    @Test
    public void testRejectEmbedResourceWithoutApiKey() {
        Map<String, String> properties = new HashMap<>();
        properties.put(AIProperties.EMBED_ENDPOINT, "https://api.example.com/v1/embeddings");
        properties.put(AIProperties.EMBED_PROVIDER_TYPE, "openai");
        properties.put(AIProperties.EMBED_MODEL_NAME, "text-embedding-model");

        AIResource aiResource = new AIResource("embed-resource-without-api-key");
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> aiResource.setProperties(ImmutableMap.copyOf(properties)));
        Assertions.assertTrue(exception.getMessage().contains("ai.embed.api_key"));
    }

    @Test
    public void testMaskEmbedApiKey() throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(AIProperties.EMBED_ENDPOINT, "https://api.example.com/v1/embeddings");
        properties.put(AIProperties.EMBED_PROVIDER_TYPE, "openai");
        properties.put(AIProperties.EMBED_MODEL_NAME, "text-embedding-model");
        properties.put(AIProperties.EMBED_API_KEY, "embed-api-key");

        AIResource aiResource = new AIResource("embed-resource");
        aiResource.setProperties(ImmutableMap.copyOf(properties));
        BaseProcResult result = new BaseProcResult();
        aiResource.getProcNodeData(result);

        Assertions.assertTrue(result.getRows().stream().anyMatch(row ->
                AIProperties.EMBED_API_KEY.equals(row.get(2)) && "******".equals(row.get(3))));
        Assertions.assertFalse(result.getRows().stream().anyMatch(row -> row.contains("embed-api-key")));
    }

    @Test
    public void testRejectPartialEmbedProperties() throws DdlException {
        Map<String, String> properties = new HashMap<>(aiProperties);
        properties.put(AIProperties.EMBED_ENDPOINT, "https://api.example.com/v1/embeddings");

        AIResource aiResource = new AIResource("partial-embed-resource");
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> aiResource.setProperties(ImmutableMap.copyOf(properties)));
        Assertions.assertTrue(exception.getMessage().contains("ai.embed.provider_type"));
    }

    @Test
    public void testRejectResourceWithoutCompletePropertyGroup() {
        Map<String, String> properties = new HashMap<>();
        properties.put("ai.validity_check", "false");

        AIResource aiResource = new AIResource("empty-ai-resource");
        Assertions.assertThrows(DdlException.class,
                () -> aiResource.setProperties(ImmutableMap.copyOf(properties)));
    }

    @Test
    public void testRejectInvalidEffort() {
        Map<String, String> properties = new HashMap<>(aiProperties);
        properties.put(AIProperties.EFFORT, "invalid");

        AIResource aiResource = new AIResource("invalid-effort-resource");
        DdlException exception = Assertions.assertThrows(DdlException.class,
                () -> aiResource.setProperties(ImmutableMap.copyOf(properties)));
        Assertions.assertTrue(exception.getMessage().contains(AIProperties.EFFORT));
    }

    @Test
    public void testOpenAiDeepSeekAndQwenAcceptAllEffortLevels() throws DdlException {
        List<String> providers = Arrays.asList("OPENAI", "DEEPSEEK", "QWEN");
        List<String> effortLevels = Arrays.asList("none", "minimal", "low", "medium", "high", "xhigh", "max");
        for (String provider : providers) {
            for (String effort : effortLevels) {
                Map<String, String> properties = new HashMap<>(aiProperties);
                properties.put(AIProperties.PROVIDER_TYPE, provider);
                properties.put(AIProperties.EFFORT, effort);

                AIResource aiResource = new AIResource("effort-resource");
                aiResource.setProperties(ImmutableMap.copyOf(properties));

                Assertions.assertEquals(effort, aiResource.toThrift().getEffort());
            }
        }
    }

    @Test
    public void testAnthropicEffortLevels() throws DdlException {
        for (String effort : Arrays.asList("low", "medium", "high", "xhigh", "max")) {
            Map<String, String> properties = new HashMap<>(aiProperties);
            properties.put(AIProperties.PROVIDER_TYPE, "ANTHROPIC");
            properties.put(AIProperties.EFFORT, effort);

            AIResource aiResource = new AIResource("effort-resource");
            aiResource.setProperties(ImmutableMap.copyOf(properties));

            Assertions.assertEquals(effort, aiResource.toThrift().getEffort());
        }

        for (String effort : Arrays.asList("none", "minimal")) {
            Map<String, String> properties = new HashMap<>(aiProperties);
            properties.put(AIProperties.PROVIDER_TYPE, "ANTHROPIC");
            properties.put(AIProperties.EFFORT, effort);

            AIResource aiResource = new AIResource("invalid-anthropic-effort-resource");
            Assertions.assertThrows(DdlException.class,
                    () -> aiResource.setProperties(ImmutableMap.copyOf(properties)));
        }
    }

    @Test
    public void testGeminiEffortLevels() throws DdlException {
        for (String effort : Arrays.asList("minimal", "low", "medium", "high")) {
            Map<String, String> properties = new HashMap<>(aiProperties);
            properties.put(AIProperties.PROVIDER_TYPE, "GEMINI");
            properties.put(AIProperties.EFFORT, effort);

            AIResource aiResource = new AIResource("effort-resource");
            aiResource.setProperties(ImmutableMap.copyOf(properties));

            Assertions.assertEquals(effort, aiResource.toThrift().getEffort());
        }

        for (String effort : Arrays.asList("none", "xhigh", "max")) {
            Map<String, String> properties = new HashMap<>(aiProperties);
            properties.put(AIProperties.PROVIDER_TYPE, "GEMINI");
            properties.put(AIProperties.EFFORT, effort);

            AIResource aiResource = new AIResource("invalid-gemini-effort-resource");
            Assertions.assertThrows(DdlException.class,
                    () -> aiResource.setProperties(ImmutableMap.copyOf(properties)));
        }
    }

    @Test
    public void testOtherProvidersAcceptAllEffortLevels() throws DdlException {
        List<String> providers = Arrays.asList("LOCAL", "MOONSHOT", "MINIMAX", "ZHIPU", "BAICHUAN", "VOYAGEAI", "JINA");
        List<String> effortLevels = Arrays.asList("none", "minimal", "low", "medium", "high", "xhigh", "max");
        for (String provider : providers) {
            for (String effort : effortLevels) {
                Map<String, String> properties = new HashMap<>(aiProperties);
                properties.put(AIProperties.PROVIDER_TYPE, provider);
                properties.put(AIProperties.EFFORT, effort);

                AIResource aiResource = new AIResource("effort-resource");
                aiResource.setProperties(ImmutableMap.copyOf(properties));

                Assertions.assertEquals(effort, aiResource.toThrift().getEffort());
            }
        }
    }

    @Test
    public void testEmptyEffortIsNotForwarded() throws DdlException {
        Map<String, String> properties = new HashMap<>(aiProperties);
        properties.put(AIProperties.EFFORT, "");

        AIResource aiResource = new AIResource("empty-effort-resource");
        aiResource.setProperties(ImmutableMap.copyOf(properties));

        Assertions.assertFalse(aiResource.toThrift().isSetEffort());
    }

    @Test
    public void testClearEffortOnModifyAndPersistence() throws Exception {
        Map<String, String> properties = new HashMap<>(aiProperties);
        properties.put(AIProperties.EFFORT, "high");

        AIResource aiResource = new AIResource("clear-effort-resource");
        aiResource.setProperties(ImmutableMap.copyOf(properties));
        aiResource.modifyProperties(ImmutableMap.of(AIProperties.EFFORT, ""));

        Assertions.assertNull(aiResource.getProperty(AIProperties.EFFORT));
        Assertions.assertFalse(aiResource.toThrift().isSetEffort());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        aiResource.write(new DataOutputStream(bytes));
        AIResource restoredResource = (AIResource) Resource.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        Assertions.assertNull(restoredResource.getProperty(AIProperties.EFFORT));
        Assertions.assertFalse(restoredResource.toThrift().isSetEffort());
    }

    @Test
    public void testInvalidProvider() throws UserException {
        Assertions.assertThrows(DdlException.class, () -> {
            try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class)) {
                Env env = Mockito.mock(Env.class);
                EditLog editLog = Mockito.mock(EditLog.class);
                AccessControllerManager accessManager = Mockito.mock(AccessControllerManager.class);
                mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
                Mockito.when(env.getEditLog()).thenReturn(editLog);
                Mockito.when(env.getAccessManager()).thenReturn(accessManager);
                Mockito.when(accessManager.checkGlobalPriv(Mockito.nullable(ConnectContext.class), Mockito.eq(PrivPredicate.ADMIN)))
                        .thenReturn(true);

                // Invalid provider type
                aiProperties.put("ai.provider_type", "invalid_provider");

                CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                        new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(aiProperties)));
                createResourceCommand.getInfo().validate();

                Resource.fromCommand(createResourceCommand);
            }
        });
    }

    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write
        Path path = Paths.get("./aiResource");
        DataOutputStream aiDos = new DataOutputStream(Files.newOutputStream(path));

        AIResource aiResource1 = new AIResource("ai_1");
        JsonObject legacyResourceJson = JsonParser.parseString(GsonUtils.GSON.toJson(aiResource1)).getAsJsonObject();
        legacyResourceJson.remove("createdByRoot");
        Text.writeString(aiDos, legacyResourceJson.toString());

        ImmutableMap<String, String> properties = ImmutableMap.of(
                "ai.endpoint", endpoint,
                "ai.provider_type", providerType,
                "ai.api_key", apiKey,
                "ai.model_name", modelName,
                "ai.validity_check", "false"
        );
        AIResource aiResource2 = new AIResource("ai_2");
        aiResource2.setCreatedByRoot(true);
        aiResource2.setProperties(properties);
        aiResource2.write(aiDos);

        aiDos.flush();
        aiDos.close();

        // 2. Read
        DataInputStream aiDis = new DataInputStream(Files.newInputStream(path));
        AIResource rAiResource1 = (AIResource) Resource.read(aiDis);
        AIResource rAiResource2 = (AIResource) Resource.read(aiDis);

        Assertions.assertEquals("ai_1", rAiResource1.getName());
        Assertions.assertEquals("ai_2", rAiResource2.getName());
        Assertions.assertFalse(rAiResource1.isCreatedByRoot());
        Assertions.assertTrue(rAiResource2.isCreatedByRoot());

        Assertions.assertEquals(rAiResource2.getProperty(AIProperties.ENDPOINT), endpoint);
        Assertions.assertEquals(rAiResource2.getProperty(AIProperties.PROVIDER_TYPE), providerType.toUpperCase());
        Assertions.assertEquals(rAiResource2.getProperty(AIProperties.API_KEY), apiKey);
        Assertions.assertEquals(rAiResource2.getProperty(AIProperties.MODEL_NAME), modelName);
        Assertions.assertEquals(rAiResource2.getProperty(AIProperties.TEMPERATURE), AIProperties.DEFAULT_TEMPERATURE);
        Assertions.assertEquals(rAiResource2.getProperty(AIProperties.MAX_TOKEN), AIProperties.DEFAULT_MAX_TOKEN);
        Assertions.assertEquals(rAiResource2.getProperty(AIProperties.MAX_RETRIES), AIProperties.DEFAULT_MAX_RETRIES);
        Assertions.assertEquals(rAiResource2.getProperty(AIProperties.RETRY_DELAY_SECOND),
                            AIProperties.DEFAULT_RETRY_DELAY_SECOND);

        // 3. delete
        aiDis.close();
        Files.deleteIfExists(path);
    }

    @Test
    public void testModifyProperties() throws Exception {
        ImmutableMap<String, String> properties = ImmutableMap.of(
                "ai.endpoint", endpoint,
                "ai.provider_type", providerType,
                "ai.api_key", apiKey,
                "ai.model_name", modelName,
                "ai.validity_check", "false"
        );
        AIResource aiResource = new AIResource("t_ai_source");
        aiResource.setProperties(properties);
        FeConstants.runningUnitTest = true;

        Map<String, String> modify = new HashMap<>();
        modify.put("ai.api_key", "new_api_key");
        modify.put("ai.temperature", "0.9");
        aiResource.modifyProperties(modify);

        Assertions.assertEquals("new_api_key", aiResource.getProperty(AIProperties.API_KEY));
        Assertions.assertEquals("0.9", aiResource.getProperty(AIProperties.TEMPERATURE));
    }

    @Test
    public void testModifyPropertiesPersistsNormalizedProviders() throws Exception {
        Map<String, String> properties = new HashMap<>(aiProperties);
        properties.put(AIProperties.VALIDITY_CHECK, "true");
        properties.put(AIProperties.EMBED_ENDPOINT, "https://api.openai.com/v1/embeddings");
        properties.put(AIProperties.EMBED_PROVIDER_TYPE, "openai");
        properties.put(AIProperties.EMBED_MODEL_NAME, "text-embedding-3-small");
        properties.put(AIProperties.EMBED_API_KEY, "embed-api-key");
        properties.put(AIProperties.DIMENSIONS, "8");

        AIResource aiResource = new AIResource("normalized-provider-resource");
        aiResource.setProperties(ImmutableMap.copyOf(properties));
        aiResource.modifyProperties(ImmutableMap.of(
                AIProperties.PROVIDER_TYPE, "openai",
                AIProperties.EMBED_PROVIDER_TYPE, "qwen"));

        Assertions.assertEquals("OPENAI", aiResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assertions.assertEquals("QWEN", aiResource.getProperty(AIProperties.EMBED_PROVIDER_TYPE));
        Assertions.assertEquals("OPENAI", aiResource.toThrift().getProviderType());
        Assertions.assertEquals("QWEN", aiResource.toThrift().getEmbedProviderType());

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        aiResource.write(new DataOutputStream(bytes));
        AIResource restoredResource = (AIResource) Resource.read(
                new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        Assertions.assertEquals("OPENAI", restoredResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assertions.assertEquals("QWEN", restoredResource.getProperty(AIProperties.EMBED_PROVIDER_TYPE));
        Assertions.assertEquals("OPENAI", restoredResource.toThrift().getProviderType());
        Assertions.assertEquals("QWEN", restoredResource.toThrift().getEmbedProviderType());
    }

    @Test
    public void testModifyPropertiesNormalizesEmbedProviderForLocalResource() throws Exception {
        AIResource aiResource = new AIResource("local-resource");
        aiResource.setProperties(ImmutableMap.of(
                AIProperties.ENDPOINT, "http://127.0.0.1:8000/v1/chat/completions",
                AIProperties.PROVIDER_TYPE, "local",
                AIProperties.MODEL_NAME, "local-model",
                AIProperties.DIMENSIONS, "8"));

        aiResource.modifyProperties(ImmutableMap.of(
                AIProperties.EMBED_ENDPOINT, "http://127.0.0.1:8000/v1/embeddings",
                AIProperties.EMBED_PROVIDER_TYPE, "openai",
                AIProperties.EMBED_MODEL_NAME, "text-embedding-3-small",
                AIProperties.EMBED_API_KEY, "embed-api-key"));

        Assertions.assertEquals("OPENAI", aiResource.getProperty(AIProperties.EMBED_PROVIDER_TYPE));
        Assertions.assertEquals("OPENAI", aiResource.toThrift().getEmbedProviderType());
    }

    @Test
    public void testModifyPropertiesNormalizesMultimodalProviderWhenValidityCheckDisabled()
            throws Exception {
        AIResource aiResource = new AIResource("validity-check-disabled-resource");
        aiProperties.put(AIProperties.DIMENSIONS, "8");
        aiResource.setProperties(ImmutableMap.copyOf(aiProperties));

        aiResource.modifyProperties(ImmutableMap.of(
                AIProperties.MULTIMODAL_EMBED_ENDPOINT, "https://example.com/multimodal-embeddings",
                AIProperties.MULTIMODAL_EMBED_PROVIDER_TYPE, "qwen",
                AIProperties.MULTIMODAL_EMBED_MODEL_NAME, "qwen3-vl-embedding",
                AIProperties.MULTIMODAL_EMBED_API_KEY, "multimodal-api-key"));

        Assertions.assertEquals("QWEN",
                aiResource.getProperty(AIProperties.MULTIMODAL_EMBED_PROVIDER_TYPE));
        Assertions.assertEquals("QWEN", aiResource.toThrift().getEmbedMmProviderType());
    }

    @Test
    public void testConcurrentModifyPropertiesPreservesIndependentUpdates() throws Exception {
        AIResource aiResource = new AIResource("concurrent-resource");
        aiProperties.put(AIProperties.DIMENSIONS, "8");
        aiResource.setProperties(ImmutableMap.copyOf(aiProperties));

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread updateTemperature = new Thread(() -> modifyPropertiesAfterStart(
                aiResource, ImmutableMap.of(AIProperties.TEMPERATURE, "0.8"), ready, start, failure));
        Thread updateMaxToken = new Thread(() -> modifyPropertiesAfterStart(
                aiResource, ImmutableMap.of(AIProperties.MAX_TOKEN, "4096"), ready, start, failure));

        boolean bothWaiting;
        aiResource.writeLock();
        try {
            updateTemperature.start();
            updateMaxToken.start();
            Assertions.assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            bothWaiting = waitUntilBlocked(updateTemperature, updateMaxToken);
        } finally {
            aiResource.writeUnlock();
        }

        updateTemperature.join(5000);
        updateMaxToken.join(5000);
        Assertions.assertTrue(bothWaiting);
        Assertions.assertFalse(updateTemperature.isAlive());
        Assertions.assertFalse(updateMaxToken.isAlive());
        Assertions.assertNull(failure.get(), () -> "Concurrent ALTER failed: " + failure.get());
        Assertions.assertEquals("0.8", aiResource.getProperty(AIProperties.TEMPERATURE));
        Assertions.assertEquals("4096", aiResource.getProperty(AIProperties.MAX_TOKEN));
    }

    private static void modifyPropertiesAfterStart(AIResource aiResource,
            Map<String, String> properties, CountDownLatch ready, CountDownLatch start,
            AtomicReference<Throwable> failure) {
        try {
            ready.countDown();
            start.await();
            aiResource.modifyProperties(properties);
        } catch (Throwable t) {
            failure.compareAndSet(null, t);
        }
    }

    private static boolean waitUntilBlocked(Thread... threads) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (Arrays.stream(threads).allMatch(AIResourceTest::isBlocked)) {
                return true;
            }
            Thread.sleep(10);
        }
        return false;
    }

    private static boolean isBlocked(Thread thread) {
        return thread.getState() == Thread.State.WAITING
                || thread.getState() == Thread.State.BLOCKED;
    }

    @Test
    public void testDifferentProviders() throws DdlException {
        // 1. OpenAI
        Map<String, String> openaiProps = new HashMap<>();
        openaiProps.put("ai.endpoint", "https://api.openai.com/v1/chat/completions");
        openaiProps.put("ai.provider_type", "openai");
        openaiProps.put("ai.api_key", "openai-key");
        openaiProps.put("ai.model_name", "gpt-4");
        openaiProps.put("ai.validity_check", "false");

        AIResource openaiResource = new AIResource("openai-resource");
        openaiResource.setProperties(ImmutableMap.copyOf(openaiProps));

        // 2. Gemini
        Map<String, String> geminiProps = new HashMap<>();
        geminiProps.put("ai.endpoint", "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent");
        geminiProps.put("ai.provider_type", "gemini");
        geminiProps.put("ai.api_key", "gemini-api-key");
        geminiProps.put("ai.model_name", "gemini-pro");
        geminiProps.put("ai.validity_check", "false");

        AIResource geminiResource = new AIResource("gemini-resource");
        geminiResource.setProperties(ImmutableMap.copyOf(geminiProps));

        // 3. Anthropic
        Map<String, String> anthropicProps = new HashMap<>();
        anthropicProps.put("ai.endpoint", "https://api.anthropic.com/v1/messages");
        anthropicProps.put("ai.provider_type", "anthropic");
        anthropicProps.put("ai.api_key", "anthropic-api-key");
        anthropicProps.put("ai.model_name", "claude-3-opus");
        anthropicProps.put("ai.anthropic_version", "2023-06-01");
        anthropicProps.put("ai.validity_check", "false");

        AIResource anthropicResource = new AIResource("anthropic-resource");
        anthropicResource.setProperties(ImmutableMap.copyOf(anthropicProps));

        // 4. Local
        Map<String, String> localProps = new HashMap<>();
        localProps.put("ai.endpoint", "http://localhost:8000/v1/chat/completions");
        localProps.put("ai.provider_type", "local");
        localProps.put("ai.api_key", "local-key");
        localProps.put("ai.model_name", "local-model");
        localProps.put("ai.validity_check", "false");

        AIResource localResource = new AIResource("local-resource");
        localResource.setProperties(ImmutableMap.copyOf(localProps));

        Assertions.assertEquals("OPENAI", openaiResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assertions.assertEquals("GEMINI", geminiResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assertions.assertEquals("ANTHROPIC", anthropicResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assertions.assertEquals("LOCAL", localResource.getProperty(AIProperties.PROVIDER_TYPE));
    }

    @Test
    public void testDimensionsValidation() throws Exception {
        Map<String, String> props = new HashMap<>(aiProperties);
        AIResource resource = new AIResource("test-dimensions");
        // default dimensions should be set
        resource.setProperties(ImmutableMap.copyOf(props));
        FeConstants.runningUnitTest = true;
        Assert.assertEquals(AIProperties.DEFAULT_DIMENSIONS, resource.getProperty(AIProperties.DIMENSIONS));
    
        // modify dimensions
        Map<String, String> modify = new HashMap<>();
        modify.put(AIProperties.DIMENSIONS, "1536");
        resource.modifyProperties(modify);
        Assert.assertEquals("1536", resource.getProperty(AIProperties.DIMENSIONS));
    
        // modify other properties without dimensions 
        modify.clear();
        modify.put(AIProperties.MAX_RETRIES, "1");
        modify.put(AIProperties.TEMPERATURE, "0.8");
        resource.modifyProperties(modify);
        Assert.assertEquals("1", resource.getProperty(AIProperties.MAX_RETRIES));
        Assert.assertEquals("0.8", resource.getProperty(AIProperties.TEMPERATURE));
        Assert.assertEquals("1536", resource.getProperty(AIProperties.DIMENSIONS));
    }
}
