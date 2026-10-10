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
import org.apache.doris.catalog.FileResourceSnapshot;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionName;
import org.apache.doris.catalog.FunctionToThriftConverter;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.datasource.storage.StorageTypeId;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToFile;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFileResourceSnapshot;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TFunction;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ToFileTranslatorTest {
    @Test
    void s3SnapshotReachesWireWithoutUriValidationOrRewrite() {
        assertSnapshotWire(Resource.ResourceType.S3, TFileType.FILE_S3,
                "hdfs://OtherHost/a/../b%2f?token=value#fragment");
    }

    @Test
    void hdfsSnapshotReachesWireWithoutUriValidationOrRewrite() {
        assertSnapshotWire(Resource.ResourceType.HDFS, TFileType.FILE_HDFS, "relative path with spaces");
    }

    @Test
    void otherResourceConfigurationsUseOrdinaryFilesystemBinding() {
        StorageAdapter storage = Mockito.mock(StorageAdapter.class);
        Mockito.when(storage.getType()).thenReturn(StorageTypeId.AZURE);
        Mockito.when(storage.getBackendConfigProperties()).thenReturn(Map.of());
        try (MockedStatic<StorageAdapter> binding = Mockito.mockStatic(StorageAdapter.class)) {
            binding.when(() -> StorageAdapter.of(Mockito.anyMap())).thenReturn(storage);
            assertSnapshotWire(Resource.ResourceType.AZURE, TFileType.FILE_S3, "arbitrary:uri?query#fragment");
            binding.verify(() -> StorageAdapter.of(Map.of("secret_key", "first")));
            binding.verify(() -> StorageAdapter.of(Map.of("secret_key", "second")));
        }
    }

    @Test
    void resourceMetadataIsCopiedAndNeverPersistedWithCatalogFunction() throws Exception {
        org.apache.doris.catalog.ScalarFunction function = new org.apache.doris.catalog.ScalarFunction(
                new FunctionName("to_file"), List.of(Type.STRING, Type.STRING),
                Type.FILE, false, true);
        TFileResourceSnapshot snapshot = new TFileResourceSnapshot().setResourceName("resource")
                .setFileType(TFileType.FILE_S3).setProperties(new HashMap<>(Map.of("secret_key", "execution-secret")));
        function.setFileResource(snapshot);
        snapshot.getProperties().clear();
        function.getFileResource().getProperties().clear();
        Assertions.assertEquals("execution-secret", function.getFileResource().getProperties().get("secret_key"));

        ByteArrayOutputStream serialized = new ByteArrayOutputStream();
        function.write(new DataOutputStream(serialized));
        Assertions.assertFalse(serialized.toString(java.nio.charset.StandardCharsets.UTF_8)
                .contains("execution-secret"));
        Function restored = Function.read(new DataInputStream(new ByteArrayInputStream(serialized.toByteArray())));
        Assertions.assertFalse(FunctionToThriftConverter.toThrift(restored, restored.getReturnType(),
                restored.getArgs(), new Boolean[] {false, false}).isSetFileResource());
    }

    private void assertSnapshotWire(Resource.ResourceType resourceType, TFileType fileType, String uri) {
        Env env = Mockito.mock(Env.class);
        ResourceMgr manager = Mockito.mock(ResourceMgr.class);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        ConnectContext connection = Mockito.mock(ConnectContext.class);
        PlanTranslatorContext translation = Mockito.mock(PlanTranslatorContext.class);
        Resource resource = Mockito.mock(Resource.class);
        Map<String, String> properties = new HashMap<>();
        String resourceSecretKey = resourceType == Resource.ResourceType.S3 ? "s3.secret_key"
                : resourceType == Resource.ResourceType.HDFS ? "hadoop.username" : "secret_key";
        String backendSecretKey = resourceType == Resource.ResourceType.S3 ? "AWS_SECRET_KEY" : resourceSecretKey;
        properties.put(resourceSecretKey, "first");
        if (resourceType == Resource.ResourceType.S3) {
            properties.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
            properties.put("s3.region", "us-east-1");
            properties.put("s3.bucket", "bucket");
            properties.put("s3.access_key", "fake-access-key");
        } else if (resourceType == Resource.ResourceType.HDFS) {
            properties.put("fs.defaultFS", "hdfs://fake-namenode:8020");
        }
        Mockito.when(env.getResourceMgr()).thenReturn(manager);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(manager.getResource("resource")).thenReturn(resource);
        Mockito.when(access.checkResourcePriv(connection, "resource", PrivPredicate.USAGE)).thenReturn(true);
        Mockito.when(resource.getType()).thenReturn(resourceType);
        Mockito.when(resource.getCopiedProperties()).thenAnswer(ignored -> new HashMap<>(properties));
        StatementContext statement = new StatementContext(connection, null);
        Mockito.when(translation.getStatementContext()).thenReturn(statement);

        try (MockedStatic<Env> current = Mockito.mockStatic(Env.class)) {
            current.when(Env::getCurrentEnv).thenReturn(env);
            ToFile expression = new ToFile(new StringLiteral("resource"), new StringLiteral(uri));
            Expr first = ExpressionTranslator.translate(expression, translation);
            TExpr wire = ExprToThriftVisitor.treeToThrift(first);
            TFunction function = wire.getNodes().get(0).getFn();
            Assertions.assertTrue(function.isSetFileResource());
            Assertions.assertEquals("resource", function.getFileResource().getResourceName());
            Assertions.assertEquals(fileType, function.getFileResource().getFileType());
            Assertions.assertEquals("first", function.getFileResource().getProperties().get(backendSecretKey));
            Assertions.assertEquals(uri, wire.getNodes().get(2).getStringLiteral().getValue());

            properties.put(resourceSecretKey, "second");
            function.getFileResource().getProperties().put(backendSecretKey, "mutated wire copy");
            Assertions.assertEquals("first", wireSnapshot(first).getFileResource()
                    .getProperties().get(backendSecretKey));
            Expr sameQuery = ExpressionTranslator.translate(expression, translation);
            Assertions.assertEquals("first", wireSnapshot(sameQuery).getFileResource()
                    .getProperties().get(backendSecretKey));
            Assertions.assertEquals("first", FileResourceSnapshot.resolveForToFile(statement, "resource")
                    .getProperties().get(resourceSecretKey));
            Mockito.verify(resource, Mockito.times(1)).getCopiedProperties();

            TFunction cloned = FunctionToThriftConverter.toThrift(first.getFn().clone(), first.getType(),
                    new Type[] {Type.STRING, Type.STRING}, new Boolean[] {false, false});
            Assertions.assertTrue(cloned.isSetFileResource());
            Assertions.assertEquals("first", cloned.getFileResource().getProperties().get(backendSecretKey));

            StatementContext next = statement.createNextExecuteContext();
            Mockito.when(translation.getStatementContext()).thenReturn(next);
            Expr nextExecute = ExpressionTranslator.translate(expression, translation);
            Assertions.assertEquals("second", wireSnapshot(nextExecute).getFileResource()
                    .getProperties().get(backendSecretKey));
            Assertions.assertEquals("first", wireSnapshot(first).getFileResource()
                    .getProperties().get(backendSecretKey));
            Mockito.verify(resource, Mockito.times(2)).getCopiedProperties();
        }
    }

    private TFunction wireSnapshot(Expr expression) {
        return ExprToThriftVisitor.treeToThrift(expression).getNodes().get(0).getFn();
    }
}
