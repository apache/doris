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
import org.apache.doris.nereids.trees.expressions.functions.ai.Embed;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.persist.DropResourceOperationLog;
import org.apache.doris.persist.PrivInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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

    @ParameterizedTest
    @ValueSource(strings = {"generate", "aggregate", "embed", "default_generate",
            "default_aggregate", "default_embed", "null_input"})
    public void testAIResourceUsagePrivilege(String kind) throws Exception {
        String resourceName = "ai_resource_privilege_test";
        UserIdentity user = UserIdentity.createAnalyzedUserIdentWithIp("ai_resource_privilege_user", "%");
        AIResource resource = new AIResource(resourceName);
        resource.setProperties(ImmutableMap.of(
                "ai.provider_type", "LOCAL",
                "ai.endpoint", "http://localhost:8000",
                "ai.model_name", "chat",
                "ai.validity_check", "false"));
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
        switch (kind) {
            case "generate":
                return new AIGenerate(resource, input);
            case "aggregate":
                return new AIAgg(resource, input, task);
            case "embed":
                return new Embed(resource, input);
            case "default_generate":
                return new AIGenerate(input);
            case "default_aggregate":
                return new AIAgg(input, task);
            case "default_embed":
                return new Embed(input);
            case "null_input":
                return new AIGenerate(resource, NullLiteral.INSTANCE);
            default:
                throw new IllegalArgumentException(kind);
        }
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

        AIResource aiResource = (AIResource) Resource.fromCommand(createResourceCommand);
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

        Assert.assertEquals("ai_1", rAiResource1.getName());
        Assert.assertEquals("ai_2", rAiResource2.getName());

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
