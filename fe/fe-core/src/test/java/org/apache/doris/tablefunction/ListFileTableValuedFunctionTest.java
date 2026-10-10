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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FileResourceSnapshot;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ResourceMgr;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.mysql.privilege.AccessControllerManager;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.functions.table.ListFile;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TDataGenFunctionName;
import org.apache.doris.thrift.TTVFListFileScanRange;

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class ListFileTableValuedFunctionTest {
    @Test
    void exposesPathSizeNullableTimestampAndFileColumnsWithoutRemoteIo() throws Exception {
        try (Fixture fixture = new Fixture()) {
            TableValuedFunctionIf function = TableValuedFunctionIf.getTableFunction("LIST_FILE", fixture.params());
            List<Column> columns = function.getTableColumns();
            assertSchema(columns);
            Assertions.assertInstanceOf(DataGenTableValuedFunction.class, function);
            ListFile nereids = new ListFile(new Properties(fixture.params()));
            assertSchema(nereids.getTableColumns());
        }
    }

    @Test
    void requiresResourceAndUriAndRejectsUnknownProperties() {
        try (Fixture fixture = new Fixture()) {
            assertInvalid(ImmutableMap.of("uri", "s3://bucket/"), "resource");
            assertInvalid(ImmutableMap.of("resource", "s3_resource"), "uri");
            assertInvalid(ImmutableMap.of("resource", "", "uri", "s3://bucket/"), "resource");
            assertInvalid(ImmutableMap.of("resource", "s3_resource", "uri", ""), "uri");
            Map<String, String> params = fixture.params();
            params.put("unknown", "value");
            assertInvalid(params, "unknown");
        }
    }

    @Test
    void acceptsCaseInsensitiveKeysAndStrictBooleanValues() throws Exception {
        try (Fixture fixture = new Fixture()) {
            ListFileTableValuedFunction function = new ListFileTableValuedFunction(
                    ImmutableMap.of("RESOURCE", "s3_resource", "URI", "s3://bucket/", "RECURSIVE", "TRUE"));
            Assertions.assertTrue(range(function).isRecursive());
            Assertions.assertFalse(range(new ListFileTableValuedFunction(fixture.params())).isRecursive());
            for (String value : new String[] {"yes", "1", "", " false "}) {
                Map<String, String> params = fixture.params();
                params.put("recursive", value);
                assertInvalid(params, "recursive");
            }
        }
    }

    @Test
    void acceptsBucketRootAndPreservesDirectoryUriForBackend() throws Exception {
        try (Fixture fixture = new Fixture()) {
            for (String uri : new String[] {"s3://bucket", "s3://bucket/", "s3://bucket/dir", "s3://bucket/dir/"}) {
                Map<String, String> params = fixture.params();
                params.put("uri", uri);
                Assertions.assertEquals(uri, range(new ListFileTableValuedFunction(params)).getUri());
            }
        }
    }

    @Test
    void rejectsNonS3UrisAndDifferentBuckets() {
        try (Fixture fixture = new Fixture()) {
            for (String uri : new String[] {"dir/", "hdfs://bucket/dir/", "s3:///dir/"}) {
                Map<String, String> params = fixture.params();
                params.put("uri", uri);
                assertInvalid(params, "uri");
            }
            Map<String, String> params = fixture.params();
            params.put("uri", "s3://another-bucket/");
            assertInvalid(params, "bucket");
        }
    }

    @Test
    void requiresExistingS3ResourceAndUsage() {
        try (Fixture fixture = new Fixture()) {
            assertInvalid(ImmutableMap.of("resource", "missing", "uri", "s3://bucket/"), "Can not find resource");
            Mockito.when(fixture.resource.getType()).thenReturn(Resource.ResourceType.HDFS);
            assertInvalid(fixture.params(), "S3");
            Mockito.when(fixture.resource.getType()).thenReturn(Resource.ResourceType.S3);
            Mockito.when(fixture.access.checkResourcePriv(fixture.context, "s3_resource", PrivPredicate.USAGE))
                    .thenReturn(false);
            assertInvalid(fixture.params(), "denied");
        }
    }

    @Test
    void snapshotsAtExecutionSharesToFileSnapshotAndRefreshesNextExecute() throws Exception {
        try (Fixture fixture = new Fixture()) {
            fixture.properties.put("s3.secret_key", "prepare");
            ListFileTableValuedFunction function = new ListFileTableValuedFunction(fixture.params());
            fixture.properties.put("s3.secret_key", "first execute");
            FileResourceSnapshot.resolveForToFile(fixture.statement, "s3_resource");
            fixture.properties.put("s3.secret_key", "second execute");
            TTVFListFileScanRange first = range(function);
            Assertions.assertEquals("first execute", first.getResource().getProperties().get("AWS_SECRET_KEY"));
            first.getResource().getProperties().put("AWS_SECRET_KEY", "mutated wire copy");
            Assertions.assertEquals("first execute",
                    range(function).getResource().getProperties().get("AWS_SECRET_KEY"));
            StatementContext next = fixture.statement.createNextExecuteContext();
            Mockito.when(fixture.context.getStatementContext()).thenReturn(next);
            Assertions.assertEquals("second execute",
                    range(function).getResource().getProperties().get("AWS_SECRET_KEY"));
            Mockito.when(fixture.access.checkResourcePriv(fixture.context, "s3_resource", PrivPredicate.USAGE))
                    .thenReturn(false);
            Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class, function::getTasks);
        }
    }

    @Test
    void assignsExactlyOneTaskToAnAliveBackendAndRejectsNoAliveBackends() throws Exception {
        try (Fixture fixture = new Fixture()) {
            Backend dead = Mockito.mock(Backend.class);
            Mockito.when(fixture.system.getBackendsByCurrentCluster())
                    .thenReturn(ImmutableMap.of(1L, dead, 2L, fixture.backend));
            ListFileTableValuedFunction function = new ListFileTableValuedFunction(fixture.params());
            List<TableValuedFunctionTask> tasks = function.getTasks();
            Assertions.assertEquals(1, tasks.size());
            Assertions.assertSame(fixture.backend, tasks.get(0).getBackend());
            Assertions.assertEquals(TDataGenFunctionName.LIST_FILE, function.getDataGenFunctionName());
            Assertions.assertFalse(tasks.get(0).getExecParams().getDataGenScanRange().isSetNumbersParams());
            Mockito.when(fixture.system.getBackendsByCurrentCluster()).thenReturn(ImmutableMap.of(1L, dead));
            Assertions.assertThrows(org.apache.doris.common.AnalysisException.class, function::getTasks);
        }
    }

    private static void assertSchema(List<Column> columns) {
        Assertions.assertEquals(4, columns.size());
        Assertions.assertEquals("path", columns.get(0).getName());
        Assertions.assertEquals(Type.STRING, columns.get(0).getType());
        Assertions.assertFalse(columns.get(0).isAllowNull());
        Assertions.assertEquals("size", columns.get(1).getName());
        Assertions.assertEquals(Type.BIGINT, columns.get(1).getType());
        Assertions.assertFalse(columns.get(1).isAllowNull());
        Assertions.assertEquals("modification_time", columns.get(2).getName());
        Assertions.assertEquals(ScalarType.createDatetimeV2Type(3), columns.get(2).getType());
        Assertions.assertTrue(columns.get(2).isAllowNull());
        Assertions.assertEquals("file", columns.get(3).getName());
        Assertions.assertTrue(columns.get(3).getType().isFileType());
        Assertions.assertFalse(columns.get(3).isAllowNull());
    }

    private static TTVFListFileScanRange range(ListFileTableValuedFunction function) throws Exception {
        return function.getTasks().get(0).getExecParams().getDataGenScanRange().getListFileParams();
    }

    private static void assertInvalid(Map<String, String> params, String message) {
        Exception error = Assertions.assertThrows(Exception.class,
                () -> TableValuedFunctionIf.getTableFunction("list_file", params));
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
    }

    private static class Fixture implements AutoCloseable {
        final Env env = Mockito.mock(Env.class);
        final ResourceMgr manager = Mockito.mock(ResourceMgr.class);
        final AccessControllerManager access = Mockito.mock(AccessControllerManager.class);
        final ConnectContext context = Mockito.mock(ConnectContext.class);
        final Resource resource = Mockito.mock(Resource.class);
        final SystemInfoService system = Mockito.mock(SystemInfoService.class);
        final Backend backend = Mockito.mock(Backend.class);
        final StatementContext statement = new StatementContext(context, null);
        final Map<String, String> properties = new HashMap<>();
        final MockedStatic<Env> current = Mockito.mockStatic(Env.class);
        final MockedStatic<ConnectContext> connection = Mockito.mockStatic(ConnectContext.class);

        Fixture() {
            current.when(Env::getCurrentEnv).thenReturn(env);
            current.when(Env::getCurrentSystemInfo).thenReturn(system);
            connection.when(ConnectContext::get).thenReturn(context);
            Mockito.when(env.getResourceMgr()).thenReturn(manager);
            Mockito.when(env.getAccessManager()).thenReturn(access);
            Mockito.when(manager.getResource("s3_resource")).thenReturn(resource);
            Mockito.when(access.checkResourcePriv(context, "s3_resource", PrivPredicate.USAGE)).thenReturn(true);
            Mockito.when(resource.getType()).thenReturn(Resource.ResourceType.S3);
            properties.put(S3ResourceCompat.BUCKET, "bucket");
            properties.put("s3.endpoint", "https://s3.us-east-1.amazonaws.com");
            properties.put("s3.region", "us-east-1");
            properties.put("s3.access_key", "fake-access-key");
            properties.put("s3.secret_key", "fake-secret-key");
            Mockito.when(resource.getCopiedProperties()).thenAnswer(invocation -> new HashMap<>(properties));
            Mockito.when(context.getStatementContext()).thenReturn(statement);
            Mockito.when(backend.isAlive()).thenReturn(true);
            try {
                Mockito.when(system.getBackendsByCurrentCluster()).thenReturn(ImmutableMap.of(1L, backend));
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new AssertionError(e);
            }
        }

        Map<String, String> params() {
            return new HashMap<>(ImmutableMap.of("resource", "s3_resource", "uri", "s3://bucket/"));
        }

        @Override
        public void close() {
            connection.close();
            current.close();
        }
    }
}
