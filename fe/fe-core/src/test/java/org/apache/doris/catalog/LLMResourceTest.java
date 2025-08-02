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
import org.apache.doris.datasource.property.constants.LLMProperties;
import org.apache.doris.meta.MetaContext;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableMap;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class LLMResourceTest {
    private static final Logger LOG = LogManager.getLogger(LLMResourceTest.class);
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
    private Map<String, String> llmProperties;

    @Before
    public void setUp() {
        name = "openai-gpt";
        type = "llm";
        endpoint = "https://api.openai.com/v1/chat/completions";
        providerType = "openai";
        apiKey = "xxxxxxxxxxxxxxxxxxxxxxx";
        modelName = "gpt-3.5-turbo";
        temperature = "0.5";
        maxToken = "2048";
        maxRetries = "5";
        retryDelaySecond = "2";

        llmProperties = new HashMap<>();
        llmProperties.put("type", type);
        llmProperties.put("llm.endpoint", endpoint);
        llmProperties.put("llm.provider_type", providerType);
        llmProperties.put("llm.api_key", apiKey);
        llmProperties.put("llm.model_name", modelName);
        llmProperties.put("llm.validity_check", "false");
    }

    @Test
    public void testFromCommand(@Mocked Env env, @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // resource with default settings
        CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(llmProperties)));
        createResourceCommand.getInfo().validate();

        LLMResource llmResource = (LLMResource) Resource.fromCommand(createResourceCommand);
        Assert.assertEquals(name, llmResource.getName());
        Assert.assertEquals(type, llmResource.getType().name().toLowerCase());
        Assert.assertEquals(endpoint, llmResource.getProperty(LLMProperties.ENDPOINT));
        Assert.assertEquals(providerType.toUpperCase(), llmResource.getProperty(LLMProperties.PROVIDER_TYPE));
        Assert.assertEquals(apiKey, llmResource.getProperty(LLMProperties.API_KEY));
        Assert.assertEquals(modelName, llmResource.getProperty(LLMProperties.MODEL_NAME));

        Assert.assertEquals(LLMProperties.DEFAULT_TEMPERATURE, llmResource.getProperty(LLMProperties.TEMPERATURE));
        Assert.assertEquals(LLMProperties.DEFAULT_MAX_TOKEN, llmResource.getProperty(LLMProperties.MAX_TOKEN));
        Assert.assertEquals(LLMProperties.DEFAULT_MAX_RETRIES, llmResource.getProperty(LLMProperties.MAX_RETRIES));
        Assert.assertEquals(LLMProperties.DEFAULT_RETRY_DELAY_SECOND, llmResource.getProperty(LLMProperties.RETRY_DELAY_SECOND));

        // with no default settings
        llmProperties.put(LLMProperties.TEMPERATURE, temperature);
        llmProperties.put(LLMProperties.MAX_TOKEN, maxToken);
        llmProperties.put(LLMProperties.MAX_RETRIES, maxRetries);
        llmProperties.put(LLMProperties.RETRY_DELAY_SECOND, retryDelaySecond);

        createResourceCommand = new CreateResourceCommand(
                new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(llmProperties)));
        createResourceCommand.getInfo().validate();

        llmResource = (LLMResource) Resource.fromCommand(createResourceCommand);
        Assert.assertEquals(name, llmResource.getName());
        Assert.assertEquals(type, llmResource.getType().name().toLowerCase());
        Assert.assertEquals(endpoint, llmResource.getProperty(LLMProperties.ENDPOINT));
        Assert.assertEquals(providerType.toUpperCase(), llmResource.getProperty(LLMProperties.PROVIDER_TYPE));
        Assert.assertEquals(apiKey, llmResource.getProperty(LLMProperties.API_KEY));
        Assert.assertEquals(modelName, llmResource.getProperty(LLMProperties.MODEL_NAME));
        Assert.assertEquals(temperature, llmResource.getProperty(LLMProperties.TEMPERATURE));
        Assert.assertEquals(maxToken, llmResource.getProperty(LLMProperties.MAX_TOKEN));
        Assert.assertEquals(maxRetries, llmResource.getProperty(LLMProperties.MAX_RETRIES));
        Assert.assertEquals(retryDelaySecond, llmResource.getProperty(LLMProperties.RETRY_DELAY_SECOND));
    }

    @Test
    public void testAnthropic(@Mocked Env env, @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        Map<String, String> anthropicProps = new HashMap<>(llmProperties);
        anthropicProps.put("llm.provider_type", "anthropic");
        anthropicProps.put("llm.endpoint", "https://api.anthropic.com/v1/messages");
        anthropicProps.put("llm.model_name", "claude-opus-4-20250514");
        anthropicProps.put("llm.anthropic_version", "2023-06-01");

        CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                new CreateResourceInfo(true, false, "anthropic-claude", ImmutableMap.copyOf(anthropicProps)));
        createResourceCommand.getInfo().validate();

        LLMResource llmResource = (LLMResource) Resource.fromCommand(createResourceCommand);
        Assert.assertEquals("anthropic-claude", llmResource.getName());
        Assert.assertEquals("ANTHROPIC", llmResource.getProperty(LLMProperties.PROVIDER_TYPE));
        Assert.assertEquals("https://api.anthropic.com/v1/messages", llmResource.getProperty(LLMProperties.ENDPOINT));
        Assert.assertEquals("claude-opus-4-20250514", llmResource.getProperty(LLMProperties.MODEL_NAME));
        Assert.assertEquals("2023-06-01", llmResource.getProperty(LLMProperties.ANTHROPIC_VERSION));
    }

    @Test(expected = DdlException.class)
    public void testAbnormalResource(@Mocked Env env, @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        llmProperties.remove("llm.endpoint");
        CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(llmProperties)));
        createResourceCommand.getInfo().validate();

        Resource.fromCommand(createResourceCommand);
    }

    @Test(expected = DdlException.class)
    public void testInvalidProvider(@Mocked Env env, @Injectable AccessControllerManager accessManager)
            throws UserException {
        new Expectations() {
            {
                env.getAccessManager();
                result = accessManager;
                accessManager.checkGlobalPriv((ConnectContext) any, PrivPredicate.ADMIN);
                result = true;
            }
        };

        // Invalid provider type
        llmProperties.put("llm.provider_type", "invalid_provider");

        CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(llmProperties)));
        createResourceCommand.getInfo().validate();

        Resource.fromCommand(createResourceCommand);
    }

    @Test
    public void testSerialization() throws Exception {
        MetaContext metaContext = new MetaContext();
        metaContext.setMetaVersion(FeMetaVersion.VERSION_CURRENT);
        metaContext.setThreadLocalInfo();

        // 1. Write
        Path path = Paths.get("./llmResource");
        DataOutputStream llmDos = new DataOutputStream(Files.newOutputStream(path));

        LLMResource llmResource1 = new LLMResource("llm_1");
        llmResource1.write(llmDos);

        ImmutableMap<String, String> properties = ImmutableMap.of(
                "llm.endpoint", endpoint,
                "llm.provider_type", providerType,
                "llm.api_key", apiKey,
                "llm.model_name", modelName,
                "llm.validity_check", "false"
        );
        LLMResource llmResource2 = new LLMResource("llm_2");
        llmResource2.setProperties(properties);
        llmResource2.write(llmDos);

        llmDos.flush();
        llmDos.close();

        // 2. Read
        DataInputStream llmDis = new DataInputStream(Files.newInputStream(path));
        LLMResource rLlmResource1 = (LLMResource) Resource.read(llmDis);
        LLMResource rLlmResource2 = (LLMResource) Resource.read(llmDis);

        Assert.assertEquals("llm_1", rLlmResource1.getName());
        Assert.assertEquals("llm_2", rLlmResource2.getName());

        Assert.assertEquals(rLlmResource2.getProperty(LLMProperties.ENDPOINT), endpoint);
        Assert.assertEquals(rLlmResource2.getProperty(LLMProperties.PROVIDER_TYPE), providerType.toUpperCase());
        Assert.assertEquals(rLlmResource2.getProperty(LLMProperties.API_KEY), apiKey);
        Assert.assertEquals(rLlmResource2.getProperty(LLMProperties.MODEL_NAME), modelName);
        Assert.assertEquals(rLlmResource2.getProperty(LLMProperties.TEMPERATURE), LLMProperties.DEFAULT_TEMPERATURE);
        Assert.assertEquals(rLlmResource2.getProperty(LLMProperties.MAX_TOKEN), LLMProperties.DEFAULT_MAX_TOKEN);
        Assert.assertEquals(rLlmResource2.getProperty(LLMProperties.MAX_RETRIES), LLMProperties.DEFAULT_MAX_RETRIES);
        Assert.assertEquals(rLlmResource2.getProperty(LLMProperties.RETRY_DELAY_SECOND),
                            LLMProperties.DEFAULT_RETRY_DELAY_SECOND);

        // 3. delete
        llmDis.close();
        Files.deleteIfExists(path);
    }

    @Test
    public void testModifyProperties() throws Exception {
        ImmutableMap<String, String> properties = ImmutableMap.of(
                "llm.endpoint", endpoint,
                "llm.provider_type", providerType,
                "llm.api_key", apiKey,
                "llm.model_name", modelName,
                "llm.validity_check", "false"
        );
        LLMResource llmResource = new LLMResource("t_llm_source");
        llmResource.setProperties(properties);
        FeConstants.runningUnitTest = true;

        Map<String, String> modify = new HashMap<>();
        modify.put("llm.api_key", "new_api_key");
        modify.put("llm.temperature", "0.9");
        llmResource.modifyProperties(modify);

        Assert.assertEquals("new_api_key", llmResource.getProperty(LLMProperties.API_KEY));
        Assert.assertEquals("0.9", llmResource.getProperty(LLMProperties.TEMPERATURE));
    }

    @Test
    public void testDifferentProviders() throws DdlException {
        // 1. OpenAI
        Map<String, String> openaiProps = new HashMap<>();
        openaiProps.put("llm.endpoint", "https://api.openai.com/v1/chat/completions");
        openaiProps.put("llm.provider_type", "openai");
        openaiProps.put("llm.api_key", "openai-key");
        openaiProps.put("llm.model_name", "gpt-4");
        openaiProps.put("llm.validity_check", "false");

        LLMResource openaiResource = new LLMResource("openai-resource");
        openaiResource.setProperties(ImmutableMap.copyOf(openaiProps));

        // 2. Gemini
        Map<String, String> geminiProps = new HashMap<>();
        geminiProps.put("llm.endpoint", "https://generativelanguage.googleapis.com/v1beta/models/gemini-pro:generateContent");
        geminiProps.put("llm.provider_type", "gemini");
        geminiProps.put("llm.api_key", "gemini-api-key");
        geminiProps.put("llm.model_name", "gemini-pro");
        geminiProps.put("llm.validity_check", "false");

        LLMResource geminiResource = new LLMResource("gemini-resource");
        geminiResource.setProperties(ImmutableMap.copyOf(geminiProps));

        // 3. Anthropic
        Map<String, String> anthropicProps = new HashMap<>();
        anthropicProps.put("llm.endpoint", "https://api.anthropic.com/v1/messages");
        anthropicProps.put("llm.provider_type", "anthropic");
        anthropicProps.put("llm.api_key", "anthropic-api-key");
        anthropicProps.put("llm.model_name", "claude-3-opus");
        anthropicProps.put("llm.anthropic_version", "2023-06-01");
        anthropicProps.put("llm.validity_check", "false");

        LLMResource anthropicResource = new LLMResource("anthropic-resource");
        anthropicResource.setProperties(ImmutableMap.copyOf(anthropicProps));

        // 4. Local
        Map<String, String> localProps = new HashMap<>();
        localProps.put("llm.endpoint", "http://localhost:8000/v1/chat/completions");
        localProps.put("llm.provider_type", "local");
        localProps.put("llm.api_key", "local-key");
        localProps.put("llm.model_name", "local-model");
        localProps.put("llm.validity_check", "false");

        LLMResource localResource = new LLMResource("local-resource");
        localResource.setProperties(ImmutableMap.copyOf(localProps));

        Assert.assertEquals("OPENAI", openaiResource.getProperty(LLMProperties.PROVIDER_TYPE));
        Assert.assertEquals("GEMINI", geminiResource.getProperty(LLMProperties.PROVIDER_TYPE));
        Assert.assertEquals("ANTHROPIC", anthropicResource.getProperty(LLMProperties.PROVIDER_TYPE));
        Assert.assertEquals("LOCAL", localResource.getProperty(LLMProperties.PROVIDER_TYPE));
    }
}
