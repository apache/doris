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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.property.constants.AIProperties;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

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
            CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                    new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(aiProperties)));
            createResourceCommand.getInfo().validate();

            AIResource aiResource = (AIResource) Resource.fromCommand(createResourceCommand);
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

            createResourceCommand = new CreateResourceCommand(
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
        aiResource1.write(aiDos);

        ImmutableMap<String, String> properties = ImmutableMap.of(
                "ai.endpoint", endpoint,
                "ai.provider_type", providerType,
                "ai.api_key", apiKey,
                "ai.model_name", modelName,
                "ai.validity_check", "false"
        );
        AIResource aiResource2 = new AIResource("ai_2");
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
}
