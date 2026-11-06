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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprToThriftVisitor;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionToThriftConverter;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.catalog.Type;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToFile;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.runtime.ThriftPlansBuilder;
import org.apache.doris.thrift.PaloInternalServiceVersion;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFunction;
import org.apache.doris.thrift.TPipelineFragmentParams;
import org.apache.doris.thrift.TS3Resource;
import org.apache.doris.thrift.TUniqueId;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ToFileTranslatorTest {
    @Test
    void s3ResourceReachesQueryParametersWithoutUriValidationOrRewrite() throws Exception {
        assertResourceWire("hdfs://OtherHost/a/../b%2f?token=value#fragment");
    }

    private void assertResourceWire(String uri) throws Exception {
        Env env = Mockito.mock(Env.class);
        ResourceMgr manager = Mockito.mock(ResourceMgr.class);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        ConnectContext connection = Mockito.spy(new ConnectContext());
        PlanTranslatorContext translation = Mockito.mock(PlanTranslatorContext.class);
        S3Resource resource = Mockito.mock(S3Resource.class);
        Mockito.when(resource.toThrift()).thenCallRealMethod();
        Map<String, String> properties = new HashMap<>();
        String resourceSecretKey = "s3.secret_key";
        String backendSecretKey = "AWS_SECRET_KEY";
        properties.put(resourceSecretKey, "first");
        properties.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
        properties.put("s3.region", "us-east-1");
        properties.put("s3.bucket", "bucket");
        properties.put("s3.access_key", "fake-access-key");
        Mockito.when(env.getResourceMgr()).thenReturn(manager);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(manager.getResource("resource")).thenReturn(resource);
        Mockito.when(access.checkResourcePriv(connection, "resource", PrivPredicate.USAGE)).thenReturn(true);
        Mockito.when(resource.getType()).thenReturn(Resource.ResourceType.S3);
        Mockito.when(resource.getCopiedProperties()).thenAnswer(ignored -> new HashMap<>(properties));
        Mockito.when(translation.getConnectContext()).thenReturn(connection);
        Mockito.when(translation.getStatementContext())
                .thenThrow(new AssertionError("S3 resource translation must not use StatementContext"));

        try (MockedStatic<Env> current = Mockito.mockStatic(Env.class)) {
            current.when(Env::getCurrentEnv).thenReturn(env);
            ToFile expression = new ToFile(new StringLiteral("resource"), new StringLiteral(uri));
            Expr first = ExpressionTranslator.translate(expression, translation);
            TExpr wire = ExprToThriftVisitor.treeToThrift(first);
            TFunction function = wire.getNodes().get(0).getFn();
            assertFunctionHasNoResource(function, backendSecretKey);
            Map<String, TS3Resource> queryResources = ThriftPlansBuilder.collectS3Resources(connection);
            Assertions.assertEquals(1, queryResources.size());
            TS3Resource config = queryResources.get("resource");
            Assertions.assertEquals("first", config.getProperties().get(backendSecretKey));
            Assertions.assertEquals(uri, wire.getNodes().get(2).getStringLiteral().getValue());
            TPipelineFragmentParams params = new TPipelineFragmentParams()
                    .setProtocolVersion(PaloInternalServiceVersion.V1).setQueryId(new TUniqueId(1, 2))
                    .setPerExchNumSenders(Map.of()).setDestinations(List.of()).setS3Resources(queryResources);
            TPipelineFragmentParams received = new TPipelineFragmentParams();
            new TDeserializer().deserialize(received, new TSerializer().serialize(params));
            Assertions.assertEquals(queryResources, received.getS3Resources());
            ByteArrayOutputStream persisted = new ByteArrayOutputStream();
            first.getFn().write(new DataOutputStream(persisted));
            Assertions.assertFalse(persisted.toString(java.nio.charset.StandardCharsets.UTF_8)
                    .contains(backendSecretKey));

            properties.put(resourceSecretKey, "second");
            config.getProperties().put(backendSecretKey, "mutated wire copy");
            Assertions.assertEquals("first", ThriftPlansBuilder.collectS3Resources(connection)
                    .get("resource").getProperties().get(backendSecretKey));
            Expr sameQuery = ExpressionTranslator.translate(expression, translation);
            assertFunctionHasNoResource(wireSnapshot(sameQuery), backendSecretKey);
            Assertions.assertEquals("first", ThriftPlansBuilder.collectS3Resources(connection)
                    .get("resource").getProperties().get(backendSecretKey));
            Mockito.verify(resource, Mockito.times(1)).getCopiedProperties();

            TFunction cloned = FunctionToThriftConverter.toThrift(first.getFn().clone(), first.getType(),
                    new Type[] {Type.STRING, Type.STRING}, new Boolean[] {false, false});
            assertFunctionHasNoResource(cloned, backendSecretKey);

            connection.setStartTime();
            Assertions.assertTrue(connection.getS3Resources().isEmpty());
            Expr nextExecute = ExpressionTranslator.translate(expression, translation);
            assertFunctionHasNoResource(wireSnapshot(nextExecute), backendSecretKey);
            Assertions.assertEquals("second", ThriftPlansBuilder.collectS3Resources(connection)
                    .get("resource").getProperties().get(backendSecretKey));
            Assertions.assertEquals("first", received.getS3Resources().get("resource")
                    .getProperties().get(backendSecretKey));
            Mockito.verify(resource, Mockito.times(2)).getCopiedProperties();
        }
    }

    private void assertFunctionHasNoResource(TFunction function, String propertyKey) throws Exception {
        String bytes = new String(new TSerializer().serialize(function),
                java.nio.charset.StandardCharsets.ISO_8859_1);
        Assertions.assertFalse(bytes.contains(propertyKey));
    }

    private TFunction wireSnapshot(Expr expression) {
        return ExprToThriftVisitor.treeToThrift(expression).getNodes().get(0).getFn();
    }
}
