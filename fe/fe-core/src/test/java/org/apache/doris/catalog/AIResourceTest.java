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

import org.apache.doris.analysis.PasswordOptions;
import org.apache.doris.analysis.ResourcePattern;
import org.apache.doris.analysis.ResourceTypeEnum;
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
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.SqlCacheContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.analysis.ExpressionAnalyzer;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AIAgg;
import org.apache.doris.nereids.trees.expressions.functions.ai.AIGenerate;
import org.apache.doris.nereids.trees.expressions.functions.ai.AISentiment;
import org.apache.doris.nereids.trees.expressions.functions.ai.Embed;
import org.apache.doris.nereids.trees.expressions.literal.JsonLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.persist.DropResourceOperationLog;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @Before
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
                new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(aiProperties)));
        createResourceCommand.getInfo().validate();

        AIResource legacyAIResource = (AIResource) Resource.fromCommand(createResourceCommand);
        Assert.assertFalse(legacyAIResource.isCreatedByRoot());

        AIResource rootCreatedAIResource =
                (AIResource) Resource.fromCommand(createResourceCommand, UserIdentity.ROOT);
        Assert.assertTrue(rootCreatedAIResource.isCreatedByRoot());

        AIResource aiResource = (AIResource) Resource.fromCommand(createResourceCommand, UserIdentity.ADMIN);
        Assert.assertFalse(aiResource.isCreatedByRoot());
        Assert.assertEquals(name, aiResource.getName());
        Assert.assertEquals(type, aiResource.getType().name().toLowerCase());
        Assert.assertEquals(endpoint, aiResource.getProperty(AIProperties.ENDPOINT));
        Assert.assertEquals(providerType.toUpperCase(), aiResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assert.assertEquals(apiKey, aiResource.getProperty(AIProperties.API_KEY));
        Assert.assertEquals(modelName, aiResource.getProperty(AIProperties.MODEL_NAME));

        Assert.assertEquals(AIProperties.DEFAULT_TEMPERATURE, aiResource.getProperty(AIProperties.TEMPERATURE));
        Assert.assertEquals(AIProperties.DEFAULT_MAX_TOKEN, aiResource.getProperty(AIProperties.MAX_TOKEN));
        Assert.assertEquals(AIProperties.DEFAULT_MAX_RETRIES, aiResource.getProperty(AIProperties.MAX_RETRIES));
        Assert.assertEquals(AIProperties.DEFAULT_RETRY_DELAY_SECOND, aiResource.getProperty(AIProperties.RETRY_DELAY_SECOND));

        // with no default settings
        aiProperties.put(AIProperties.TEMPERATURE, temperature);
        aiProperties.put(AIProperties.MAX_TOKEN, maxToken);
        aiProperties.put(AIProperties.MAX_RETRIES, maxRetries);
        aiProperties.put(AIProperties.RETRY_DELAY_SECOND, retryDelaySecond);

        createResourceCommand = new CreateResourceCommand(
                new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(aiProperties)));
        createResourceCommand.getInfo().validate();

        aiResource = (AIResource) Resource.fromCommand(createResourceCommand);
        Assert.assertEquals(name, aiResource.getName());
        Assert.assertEquals(type, aiResource.getType().name().toLowerCase());
        Assert.assertEquals(endpoint, aiResource.getProperty(AIProperties.ENDPOINT));
        Assert.assertEquals(providerType.toUpperCase(), aiResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assert.assertEquals(apiKey, aiResource.getProperty(AIProperties.API_KEY));
        Assert.assertEquals(modelName, aiResource.getProperty(AIProperties.MODEL_NAME));
        Assert.assertEquals(temperature, aiResource.getProperty(AIProperties.TEMPERATURE));
        Assert.assertEquals(maxToken, aiResource.getProperty(AIProperties.MAX_TOKEN));
        Assert.assertEquals(maxRetries, aiResource.getProperty(AIProperties.MAX_RETRIES));
        Assert.assertEquals(retryDelaySecond, aiResource.getProperty(AIProperties.RETRY_DELAY_SECOND));
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
        withAIResource(aiResource, () -> {
            AISentiment function = new AISentiment(new StringLiteral("embed-only-resource"),
                    new StringLiteral("text"));
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    function::checkLegalityAfterRewrite);
            Assertions.assertTrue(exception.getMessage().contains("does not contain complete general AI properties"));
        });
    }

    @Test
    public void testRejectEmbedOnlyResourceForAiAgg() throws DdlException {
        AIResource aiResource = createEmbedOnlyResource();
        withAIResource(aiResource, () -> {
            AIAgg function = new AIAgg(new StringLiteral("embed-only-resource"),
                    new StringLiteral("text"), new StringLiteral("task"));
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    function::checkLegalityAfterRewrite);
            Assertions.assertTrue(exception.getMessage().contains("does not contain complete general AI properties"));
        });
    }

    @Test
    public void testAcceptEmbedOnlyResourceForEmbedFunction() throws DdlException {
        AIResource aiResource = createEmbedOnlyResource();
        withAIResource(aiResource, () -> {
            Embed function = new Embed(new StringLiteral("embed-only-resource"),
                    new StringLiteral("text"));
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
        });
    }

    @Test
    public void testAcceptMultimodalEmbedOnlyResourceForJsonEmbed() throws DdlException {
        AIResource aiResource = createMultimodalEmbedOnlyResource();
        withAIResource(aiResource, () -> {
            Embed function = new Embed(new StringLiteral("multimodal-embed-only-resource"),
                    new JsonLiteral("{\"text\":\"hello\"}"));
            Assertions.assertDoesNotThrow(function::checkLegalityBeforeTypeCoercion);
            Assertions.assertDoesNotThrow(function::checkLegalityAfterRewrite);
        });
    }

    @Test
    public void testRejectMultimodalEmbedOnlyResourceForTextEmbed() throws DdlException {
        AIResource aiResource = createMultimodalEmbedOnlyResource();
        withAIResource(aiResource, () -> {
            Embed function = new Embed(new StringLiteral("multimodal-embed-only-resource"),
                    new StringLiteral("hello"));
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    function::checkLegalityBeforeTypeCoercion);
            Assertions.assertTrue(exception.getMessage().contains("does not have properties required by EMBED"));
        });
    }

    private void withAIResource(AIResource resource, Runnable check) {
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(UserIdentity.ROOT);
        context.setRemoteIP("127.0.0.1");
        context.setThreadLocalInfo();
        ResourceMgr resourceMgr = Env.getCurrentEnv().getResourceMgr();
        resourceMgr.replayCreateResource(resource);
        try {
            check.run();
        } finally {
            resourceMgr.replayDropResource(new DropResourceOperationLog(resource.getName()));
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"generate", "aggregate", "embed", "multimodal", "default_generate",
            "default_aggregate", "default_embed", "default_multimodal", "null_input"})
    public void testAIResourceUsagePrivilege(String kind) throws Exception {
        String resourceName = "ai_resource_privilege_test";
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("ai_resource_privilege_user", "%");
        AIResource resource = new AIResource(resourceName);
        resource.setProperties(ImmutableMap.<String, String>builder()
                .put("ai.provider_type", "LOCAL")
                .put("ai.endpoint", "http://localhost:8000")
                .put("ai.model_name", "chat")
                .put("ai.embed.provider_type", "LOCAL")
                .put("ai.embed.endpoint", "http://localhost:8001")
                .put("ai.embed.model_name", "embed")
                .put("ai.embed.mm.provider_type", "LOCAL")
                .put("ai.embed.mm.endpoint", "http://localhost:8002")
                .put("ai.embed.mm.model_name", "multimodal")
                .put("ai.validity_check", "false")
                .build());
        Auth auth = Env.getCurrentEnv().getAuth();
        ResourceMgr resourceMgr = Env.getCurrentEnv().getResourceMgr();
        ConnectContext previousContext = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setCurrentUserIdentity(user);
        context.setRemoteIP("127.0.0.1");
        context.getSessionVariable().defaultAIResource = resourceName;
        context.getSessionVariable().setEnableSqlCache(true);
        context.setThreadLocalInfo();
        try {
            auth.replayCreateUser(new PrivInfo(user, null, new byte[0], null, PasswordOptions.UNSET_OPTION));
            Assertions.assertTrue(auth.doesUserExist(user));
            resourceMgr.replayCreateResource(resource);
            BoundFunction function = aiFunctionForPrivilegeCheck(kind, resourceName);
            assertResourceAccessDenied(function, resourceName);

            auth.replayGrant(usagePrivilege(user, "another_ai_resource"));
            assertResourceAccessDenied(function, resourceName);

            auth.replayGrant(usagePrivilege(user, resourceName));
            Assertions.assertTrue(Env.getCurrentEnv().getAccessManager()
                    .checkResourcePriv(context, resourceName, PrivPredicate.USAGE));
            for (Expression expression : ImmutableList.of(function,
                    new UnboundFunction(function.getName(), function.getArguments()))) {
                StatementContext statementContext = new StatementContext(context, new OriginStatement("SELECT 1", 0));
                context.setStatementContext(statementContext);
                SqlCacheContext sqlCacheContext = statementContext.getSqlCacheContext()
                        .orElseThrow(IllegalStateException::new);
                function.checkLegalityBeforeTypeCoercion();
                function.checkLegalityAfterRewrite();
                Assertions.assertTrue(sqlCacheContext.supportSqlCache());
                LogicalEmptyRelation plan = new LogicalEmptyRelation(new RelationId(0), ImmutableList.of());
                CascadesContext cascadesContext = CascadesContext.initContext(statementContext,
                        plan, PhysicalProperties.ANY);
                Expression analyzed = ExpressionAnalyzer.analyzeFunction(plan, cascadesContext, expression);
                Assertions.assertFalse(sqlCacheContext.supportSqlCache());
                Assertions.assertFalse(function.isDeterministic());
                Assertions.assertTrue(analyzed.containsNondeterministic());
            }

            auth.replayRevoke(usagePrivilege(user, resourceName));
            Assertions.assertFalse(Env.getCurrentEnv().getAccessManager()
                    .checkResourcePriv(context, resourceName, PrivPredicate.USAGE));
            assertResourceAccessDenied(function, resourceName);

            auth.replayGrant(usagePrivilege(user, "*"));
            function.checkLegalityBeforeTypeCoercion();
            function.checkLegalityAfterRewrite();
            auth.replayRevoke(usagePrivilege(user, "*"));
            assertResourceAccessDenied(function, resourceName);
        } finally {
            auth.replayDropUser(user);
            resourceMgr.replayDropResource(new DropResourceOperationLog(resourceName));
            if (previousContext == null) {
                ConnectContext.remove();
            } else {
                previousContext.setThreadLocalInfo();
            }
        }
    }

    private void assertResourceAccessDenied(BoundFunction function, String resourceName) {
        AnalysisException before = Assertions.assertThrows(AnalysisException.class,
                function::checkLegalityBeforeTypeCoercion);
        AnalysisException after = Assertions.assertThrows(AnalysisException.class,
                function::checkLegalityAfterRewrite);
        for (AnalysisException exception : new AnalysisException[] {before, after}) {
            Assertions.assertTrue(exception.getMessage().contains("Access denied"), exception.getMessage());
            Assertions.assertTrue(exception.getMessage().contains("Usage_priv"), exception.getMessage());
            Assertions.assertTrue(exception.getMessage().contains(resourceName), exception.getMessage());
        }
    }

    private PrivInfo usagePrivilege(UserIdentity user, String resourceName) throws Exception {
        ResourcePattern pattern = new ResourcePattern(resourceName, ResourceTypeEnum.GENERAL);
        pattern.analyze();
        return new PrivInfo(user, pattern, PrivBitSet.of(Privilege.USAGE_PRIV), null, null);
    }

    private BoundFunction aiFunctionForPrivilegeCheck(String kind, String resourceName) {
        StringLiteral resource = new StringLiteral(resourceName);
        StringLiteral input = new StringLiteral("input");
        StringLiteral task = new StringLiteral("summarize");
        JsonLiteral multimodal = new JsonLiteral("{\"text\":\"input\"}");
        switch (kind) {
            case "generate":
                return new AIGenerate(resource, input);
            case "aggregate":
                return new AIAgg(resource, input, task);
            case "embed":
                return new Embed(resource, input);
            case "multimodal":
                return new Embed(resource, multimodal);
            case "default_generate":
                return new AIGenerate(input);
            case "default_aggregate":
                return new AIAgg(input, task);
            case "default_embed":
                return new Embed(input);
            case "default_multimodal":
                return new Embed(multimodal);
            case "null_input":
                return new AIGenerate(resource, NullLiteral.INSTANCE);
            default:
                throw new IllegalArgumentException(kind);
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

        Map<String, String> anthropicProps = new HashMap<>(aiProperties);
        anthropicProps.put("ai.provider_type", "anthropic");
        anthropicProps.put("ai.endpoint", "https://api.anthropic.com/v1/messages");
        anthropicProps.put("ai.model_name", "claude-opus-4-20250514");
        anthropicProps.put("ai.anthropic_version", "2023-06-01");

        CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                new CreateResourceInfo(true, false, "anthropic-claude", ImmutableMap.copyOf(anthropicProps)));
        createResourceCommand.getInfo().validate();

        AIResource aiResource = (AIResource) Resource.fromCommand(createResourceCommand);
        Assert.assertEquals("anthropic-claude", aiResource.getName());
        Assert.assertEquals("ANTHROPIC", aiResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assert.assertEquals("https://api.anthropic.com/v1/messages", aiResource.getProperty(AIProperties.ENDPOINT));
        Assert.assertEquals("claude-opus-4-20250514", aiResource.getProperty(AIProperties.MODEL_NAME));
        Assert.assertEquals("2023-06-01", aiResource.getProperty(AIProperties.ANTHROPIC_VERSION));
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

        aiProperties.remove("ai.endpoint");
        CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(aiProperties)));
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
        aiProperties.put("ai.provider_type", "invalid_provider");

        CreateResourceCommand createResourceCommand = new CreateResourceCommand(
                new CreateResourceInfo(true, false, name, ImmutableMap.copyOf(aiProperties)));
        createResourceCommand.getInfo().validate();

        Resource.fromCommand(createResourceCommand);
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

        Assert.assertEquals("ai_1", rAiResource1.getName());
        Assert.assertEquals("ai_2", rAiResource2.getName());
        Assert.assertFalse(rAiResource1.isCreatedByRoot());
        Assert.assertTrue(rAiResource2.isCreatedByRoot());

        Assert.assertEquals(rAiResource2.getProperty(AIProperties.ENDPOINT), endpoint);
        Assert.assertEquals(rAiResource2.getProperty(AIProperties.PROVIDER_TYPE), providerType.toUpperCase());
        Assert.assertEquals(rAiResource2.getProperty(AIProperties.API_KEY), apiKey);
        Assert.assertEquals(rAiResource2.getProperty(AIProperties.MODEL_NAME), modelName);
        Assert.assertEquals(rAiResource2.getProperty(AIProperties.TEMPERATURE), AIProperties.DEFAULT_TEMPERATURE);
        Assert.assertEquals(rAiResource2.getProperty(AIProperties.MAX_TOKEN), AIProperties.DEFAULT_MAX_TOKEN);
        Assert.assertEquals(rAiResource2.getProperty(AIProperties.MAX_RETRIES), AIProperties.DEFAULT_MAX_RETRIES);
        Assert.assertEquals(rAiResource2.getProperty(AIProperties.RETRY_DELAY_SECOND),
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

        Assert.assertEquals("new_api_key", aiResource.getProperty(AIProperties.API_KEY));
        Assert.assertEquals("0.9", aiResource.getProperty(AIProperties.TEMPERATURE));
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

        Assert.assertEquals("OPENAI", openaiResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assert.assertEquals("GEMINI", geminiResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assert.assertEquals("ANTHROPIC", anthropicResource.getProperty(AIProperties.PROVIDER_TYPE));
        Assert.assertEquals("LOCAL", localResource.getProperty(AIProperties.PROVIDER_TYPE));
    }
}
