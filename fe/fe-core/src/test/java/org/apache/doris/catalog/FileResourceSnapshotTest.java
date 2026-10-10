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

import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ToFile;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileResourceSnapshot;
import org.apache.doris.thrift.TFileType;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

class FileResourceSnapshotTest {
    @Test
    void snapshotIsImmutableSharedWithinQueryAndFreshForNextExecute() throws Exception {
        Env env = Mockito.mock(Env.class);
        ResourceMgr manager = Mockito.mock(ResourceMgr.class);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        S3Resource resource = Mockito.spy(new S3Resource("s3_resource"));
        resource.setProperties(ImmutableMap.<String, String>builder()
                .put("s3.endpoint", "https://oss-cn-beijing.aliyuncs.com")
                .put("s3.region", "cn-beijing")
                .put("s3.bucket", "bucket")
                .put("s3.access_key", "fake-access-key")
                .put("s3.secret_key", "first")
                .put("s3.session_token", "fake-session-token")
                .put("s3.connection.maximum", "77")
                .put("use_path_style", "true")
                .put("s3_validity_check", "false")
                .build());
        Mockito.when(env.getResourceMgr()).thenReturn(manager);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(manager.getResource("s3_resource")).thenReturn(resource);
        Mockito.when(access.checkResourcePriv(context, "s3_resource", PrivPredicate.USAGE)).thenReturn(true);

        try (MockedStatic<Env> current = Mockito.mockStatic(Env.class)) {
            current.when(Env::getCurrentEnv).thenReturn(env);
            StatementContext statement = new StatementContext(context, null);
            FileResourceSnapshot first = FileResourceSnapshot.resolveForToFile(statement, "s3_resource");
            resource.modifyProperties(new HashMap<>(Map.of("s3.secret_key", "second", "s3_validity_check", "false")));
            FileResourceSnapshot sameQuery = FileResourceSnapshot.resolveForToFile(statement, "s3_resource");
            Assertions.assertSame(first, sameQuery);
            Assertions.assertEquals("first", sameQuery.getProperties().get("s3.secret_key"));
            Assertions.assertThrows(UnsupportedOperationException.class, () -> first.getProperties().clear());

            TFileResourceSnapshot thrift = first.toThrift();
            assertBackendProperties(thrift, "https://oss-cn-beijing.aliyuncs.com", "cn-beijing",
                    "fake-access-key", "first");
            Assertions.assertEquals("fake-session-token", thrift.getProperties().get(S3ResourceCompat.Env.TOKEN));
            Assertions.assertEquals("77", thrift.getProperties().get(S3ResourceCompat.Env.MAX_CONNECTIONS));
            Assertions.assertEquals("true", thrift.getProperties().get(S3ResourceCompat.USE_PATH_STYLE));
            Assertions.assertEquals("bucket", sameQuery.getProperties().get("s3.bucket"));
            Assertions.assertFalse(sameQuery.getProperties().containsKey("AWS_BUCKET"));
            Assertions.assertFalse(thrift.getProperties().containsKey(S3ResourceCompat.Env.BUCKET));
            thrift.getProperties().put("AWS_SECRET_KEY", "changed wire copy");
            Assertions.assertEquals("first", first.toThrift().getProperties().get(S3ResourceCompat.Env.SECRET_KEY));
            Assertions.assertEquals("s3_resource", thrift.getResourceName());
            Assertions.assertEquals(TFileType.FILE_S3, thrift.getFileType());
            Mockito.verify(resource, Mockito.times(1)).getCopiedProperties();

            FileResourceSnapshot next = FileResourceSnapshot.resolveForToFile(
                    statement.createNextExecuteContext(), "s3_resource");
            Assertions.assertEquals("second", next.getProperties().get("s3.secret_key"));
            Assertions.assertEquals("second", next.toThrift().getProperties().get(S3ResourceCompat.Env.SECRET_KEY));
            Assertions.assertNotSame(first, next);
        }
    }

    @Test
    void legacyAwsResourcePropertiesReachBackendWithResourceBucket() throws Exception {
        S3Resource resource = new S3Resource("legacy_resource");
        resource.setProperties(ImmutableMap.of(
                "AWS_ENDPOINT", "https://s3.us-east-1.amazonaws.com",
                "AWS_REGION", "us-east-1",
                "AWS_BUCKET", "legacy-bucket",
                "AWS_ACCESS_KEY", "fake-legacy-access-key",
                "AWS_SECRET_KEY", "fake-legacy-secret-key",
                "s3_validity_check", "false"));
        FileResourceSnapshot snapshot = resolveSnapshot(resource);
        assertBackendProperties(snapshot.toThrift(), "https://s3.us-east-1.amazonaws.com", "us-east-1",
                "fake-legacy-access-key", "fake-legacy-secret-key");
        Assertions.assertEquals("legacy-bucket", snapshot.getProperties().get(S3ResourceCompat.BUCKET));
        Assertions.assertEquals("legacy-bucket", snapshot.toThrift().getProperties().get(S3ResourceCompat.Env.BUCKET));
    }

    @Test
    void nativeOssPropertiesAreNormalizedWithoutLosingResourceBucket() {
        Resource resource = Mockito.mock(Resource.class);
        Mockito.when(resource.getType()).thenReturn(Resource.ResourceType.S3);
        Mockito.when(resource.getCopiedProperties()).thenReturn(ImmutableMap.of(
                "oss.endpoint", "https://oss-cn-beijing.aliyuncs.com",
                "oss.region", "cn-beijing",
                "OSS_BUCKET", "oss-bucket",
                "oss.access_key", "fake-oss-access-key",
                "oss.secret_key", "fake-oss-secret-key"));
        FileResourceSnapshot snapshot = resolveSnapshot(resource);
        assertBackendProperties(snapshot.toThrift(), "https://oss-cn-beijing.aliyuncs.com", "cn-beijing",
                "fake-oss-access-key", "fake-oss-secret-key");
        Assertions.assertEquals("oss-bucket", snapshot.getProperties().get("OSS_BUCKET"));
        Assertions.assertFalse(snapshot.getProperties().containsKey("AWS_BUCKET"));
        Assertions.assertEquals("oss-bucket", snapshot.toThrift().getProperties().get("OSS_BUCKET"));
        Assertions.assertFalse(snapshot.toThrift().getProperties().containsKey(S3ResourceCompat.Env.BUCKET));
    }

    @Test
    void endpointOnlyReplayedStandardS3ResourceIsNormalizedWithoutRequiringBucket() {
        S3Resource resource = replayS3Resource(ImmutableMap.of(
                "s3.endpoint", "https://oss-cn-beijing.aliyuncs.com",
                "s3.region", "cn-beijing",
                "s3.access_key", "fake-access-key",
                "s3.secret_key", "fake-secret-key"));
        FileResourceSnapshot snapshot = resolveSnapshot(resource);
        assertBackendProperties(snapshot.toThrift(), "https://oss-cn-beijing.aliyuncs.com", "cn-beijing",
                "fake-access-key", "fake-secret-key");
        assertNoBucket(snapshot);
    }

    @Test
    void endpointOnlyReplayedLegacyAwsResourceIsNormalizedWithoutRequiringBucket() {
        S3Resource resource = replayS3Resource(ImmutableMap.of(
                "AWS_ENDPOINT", "https://s3.us-east-1.amazonaws.com",
                "AWS_REGION", "us-east-1",
                "AWS_ACCESS_KEY", "fake-legacy-access-key",
                "AWS_SECRET_KEY", "fake-legacy-secret-key"));
        FileResourceSnapshot snapshot = resolveSnapshot(resource);
        assertBackendProperties(snapshot.toThrift(), "https://s3.us-east-1.amazonaws.com", "us-east-1",
                "fake-legacy-access-key", "fake-legacy-secret-key");
        assertNoBucket(snapshot);
    }

    private static S3Resource replayS3Resource(Map<String, String> properties) {
        // Image replay accepts stored properties without repeating CREATE RESOURCE's ping checks.
        String json = GsonUtils.GSON.toJson(Map.of("clazz", "S3Resource", "type", "S3",
                "name", "endpoint_only_resource", "properties", properties));
        return (S3Resource) GsonUtils.GSON.fromJson(json, Resource.class);
    }

    private static void assertNoBucket(FileResourceSnapshot snapshot) {
        Assertions.assertFalse(snapshot.getProperties().containsKey(S3ResourceCompat.BUCKET));
        Assertions.assertFalse(snapshot.getProperties().containsKey(S3ResourceCompat.Env.BUCKET));
        Assertions.assertFalse(snapshot.toThrift().getProperties().containsKey(S3ResourceCompat.Env.BUCKET));
    }

    private static void assertBackendProperties(TFileResourceSnapshot snapshot, String endpoint, String region,
            String accessKey, String secretKey) {
        Assertions.assertEquals(TFileType.FILE_S3, snapshot.getFileType());
        Assertions.assertEquals(endpoint, snapshot.getProperties().get(S3ResourceCompat.Env.ENDPOINT));
        Assertions.assertEquals(region, snapshot.getProperties().get(S3ResourceCompat.Env.REGION));
        Assertions.assertEquals(accessKey, snapshot.getProperties().get(S3ResourceCompat.Env.ACCESS_KEY));
        Assertions.assertEquals(secretKey, snapshot.getProperties().get(S3ResourceCompat.Env.SECRET_KEY));
    }

    private static FileResourceSnapshot resolveSnapshot(Resource resource) {
        Env env = Mockito.mock(Env.class);
        ResourceMgr manager = Mockito.mock(ResourceMgr.class);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Mockito.when(env.getResourceMgr()).thenReturn(manager);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(manager.getResource("resource")).thenReturn(resource);
        Mockito.when(access.checkResourcePriv(context, "resource", PrivPredicate.USAGE)).thenReturn(true);
        try (MockedStatic<Env> current = Mockito.mockStatic(Env.class)) {
            current.when(Env::getCurrentEnv).thenReturn(env);
            return FileResourceSnapshot.resolveForToFile(new StatementContext(context, null), "resource");
        }
    }

    @Test
    void resourceBindingChecksUsageWithoutCopyingPropertiesOrCheckingFamily() {
        Env env = Mockito.mock(Env.class);
        ResourceMgr manager = Mockito.mock(ResourceMgr.class);
        AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        ConnectContext context = Mockito.mock(ConnectContext.class);
        Resource resource = Mockito.mock(Resource.class);
        Mockito.when(env.getResourceMgr()).thenReturn(manager);
        Mockito.when(env.getAccessManager()).thenReturn(access);
        Mockito.when(manager.getResource("resource")).thenReturn(resource);
        Mockito.when(resource.getType()).thenReturn(Resource.ResourceType.JDBC);
        Mockito.when(access.checkResourcePriv(context, "resource", PrivPredicate.USAGE)).thenReturn(true);

        try (MockedStatic<Env> current = Mockito.mockStatic(Env.class);
                MockedStatic<ConnectContext> connection = Mockito.mockStatic(ConnectContext.class)) {
            current.when(Env::getCurrentEnv).thenReturn(env);
            connection.when(ConnectContext::get).thenReturn(context);
            ToFile function = new ToFile(new StringLiteral("resource"), new StringLiteral("arbitrary:uri"));
            function.checkLegalityBeforeTypeCoercion();
            function.checkLegalityAfterRewrite();
            Mockito.verify(resource, Mockito.never()).getCopiedProperties();
            Mockito.verify(resource, Mockito.never()).getType();
            Assertions.assertThrows(AnalysisException.class,
                    () -> FileResourceSnapshot.checkResource("missing", context));
            Mockito.when(access.checkResourcePriv(context, "resource", PrivPredicate.USAGE)).thenReturn(false);
            Assertions.assertThrows(AnalysisException.class, function::checkLegalityAfterRewrite);
        }
    }
}
