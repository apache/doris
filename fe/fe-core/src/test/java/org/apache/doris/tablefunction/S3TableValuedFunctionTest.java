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

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.filesystem.FileEntry;
import org.apache.doris.filesystem.FileSystem;
import org.apache.doris.filesystem.GlobListing;
import org.apache.doris.filesystem.Location;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.functions.table.S3;
import org.apache.doris.proto.InternalService.PFetchTableSchemaRequest;
import org.apache.doris.proto.InternalService.PFetchTableSchemaResult;
import org.apache.doris.proto.Types.PScalarType;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.proto.Types.PTypeDesc;
import org.apache.doris.proto.Types.PTypeNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TFileScanRange;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TTypeNodeType;

import org.apache.thrift.TDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class S3TableValuedFunctionTest {
    private static final Map<String, String> PROPERTIES = Map.of(
            "uri", "s3://bucket/logs/*", "format", "csv", "s3.endpoint", "s3.us-west-2.amazonaws.com",
            "s3.region", "us-west-2", "s3.access_key", "ak", "s3.secret_key", "sk");
    private boolean runningUnitTest;
    private ConnectContext previousContext;

    @BeforeEach
    public void setUp() {
        runningUnitTest = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        previousContext = ConnectContext.get();
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDown() {
        FeConstants.runningUnitTest = runningUnitTest;
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    public void testExactFilesKeepLiteralPathsAndSkipListing() throws Exception {
        FeConstants.runningUnitTest = false;
        TBrokerFileStatus file = file("a,b[1]*?.csv", 123);
        try (MockedStatic<S3Util> s3 = Mockito.mockStatic(S3Util.class);
                MockedStatic<FileSystemFactory> filesystems = Mockito.mockStatic(FileSystemFactory.class)) {
            S3TableValuedFunction tvf = new S3TableValuedFunction(PROPERTIES, Collections.singletonList(file));
            Assertions.assertEquals(Collections.singletonList(file), tvf.getFileStatuses());
            s3.verify(() -> S3Util.validateAndTestEndpoint("s3.us-west-2.amazonaws.com"));
            filesystems.verifyNoInteractions();
        }
    }

    @Test
    public void testSchemaFailurePreservesFilesAndAllowsRetry() throws Exception {
        assertSchemaFailure(TStatusCode.NOT_FOUND);
        assertSchemaFailure(TStatusCode.INTERNAL_ERROR);
    }

    @ParameterizedTest
    @ValueSource(strings = {"s3://bucket/logs/a.csv", "s3://bucket/logs/{a,b}.csv"})
    public void testInitialListingUsesNullCursor(String path) throws Exception {
        FeConstants.runningUnitTest = false;
        Map<String, String> properties = new HashMap<>(PROPERTIES);
        properties.put("uri", path);
        FileSystem fs = Mockito.mock(FileSystem.class);
        Mockito.when(fs.globListWithLimit(Mockito.any(Location.class), Mockito.isNull(),
                        Mockito.eq(0L), Mockito.eq(0L)))
                .thenReturn(new GlobListing(List.of(
                        new FileEntry(Location.of("s3://bucket/logs/a.csv"), 10L, false, 123L, null)),
                        "bucket", "logs/", "logs/a.csv"));
        try (MockedStatic<S3Util> s3 = Mockito.mockStatic(S3Util.class);
                MockedStatic<FileSystemFactory> filesystems = Mockito.mockStatic(FileSystemFactory.class)) {
            filesystems.when(() -> FileSystemFactory.getFileSystem(Mockito.any(BrokerDesc.class)))
                    .thenReturn(fs);

            S3TableValuedFunction tvf = new S3TableValuedFunction(properties);

            Assertions.assertEquals(1, tvf.getFileStatuses().size());
            Assertions.assertEquals("s3://bucket/logs/a.csv", tvf.getFileStatuses().get(0).getPath());
            Assertions.assertEquals(123L, tvf.getFileStatuses().get(0).getModificationTime());
            Mockito.verify(fs).globListWithLimit(Mockito.argThat(location -> location.uri().equals(path)),
                    Mockito.isNull(), Mockito.eq(0L), Mockito.eq(0L));
            Mockito.verify(fs).close();
            s3.verify(() -> S3Util.validateAndTestEndpoint("s3.us-west-2.amazonaws.com"));
        }
    }

    @Test
    public void testSchemaInferenceUsesNextFileAfterNotFound() throws Exception {
        S3TableValuedFunction tvf = tvf();
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        PFetchTableSchemaResult schema = PFetchTableSchemaResult.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(TStatusCode.OK.getValue()))
                .setColumnNums(1).addColumnNames("id")
                .addColumnTypes(PTypeDesc.newBuilder().addTypes(PTypeNode.newBuilder()
                        .setType(TTypeNodeType.SCALAR.getValue())
                        .setScalarType(PScalarType.newBuilder().setType(TPrimitiveType.INT.getValue())))).build();
        Mockito.when(proxy.fetchTableStructureAsync(Mockito.any(), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(result(TStatusCode.NOT_FOUND)),
                        CompletableFuture.completedFuture(schema));
        try (MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
            service.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Assertions.assertEquals("id", tvf.getTableColumns().get(0).getName());
            Assertions.assertEquals(PrimitiveType.INT, tvf.getTableColumns().get(0).getType().getPrimitiveType());
            ArgumentCaptor<PFetchTableSchemaRequest> requests =
                    ArgumentCaptor.forClass(PFetchTableSchemaRequest.class);
            Mockito.verify(proxy, Mockito.times(2)).fetchTableStructureAsync(Mockito.any(), requests.capture());
            for (int i = 0; i < 2; i++) {
                TFileScanRange range = new TFileScanRange();
                new TDeserializer().deserialize(range, requests.getAllValues().get(i).getFileScanRange().toByteArray());
                Assertions.assertEquals(tvf.getFileStatuses().get(i).getPath(), range.getRanges().get(0).getPath());
            }
            Assertions.assertEquals(2, tvf.getFileStatuses().size());
        }
    }

    @Test
    public void testExactFilesAreScopedToTvf() throws Exception {
        S3TableValuedFunction first = new S3TableValuedFunction(PROPERTIES,
                Collections.singletonList(file("a.csv", 10)));
        S3TableValuedFunction second = new S3TableValuedFunction(PROPERTIES,
                Collections.singletonList(file("b.csv", 20)));
        Assertions.assertEquals("s3://bucket/logs/a.csv", first.getFileStatuses().get(0).getPath());
        Assertions.assertEquals("s3://bucket/logs/b.csv", second.getFileStatuses().get(0).getPath());
        Assertions.assertTrue(new S3TableValuedFunction(PROPERTIES).getFileStatuses().isEmpty());
    }

    @Test
    public void testExactFilesParticipateInExpressionEquality() {
        S3 first = S3.withFiles(new Properties(PROPERTIES), Collections.singletonList(file("a.csv", 10)));
        S3 same = S3.withFiles(new Properties(PROPERTIES), Collections.singletonList(file("a.csv", 10)));
        S3 other = S3.withFiles(new Properties(PROPERTIES), Collections.singletonList(file("b.csv", 10)));
        Assertions.assertEquals(first, same);
        Assertions.assertEquals(first.hashCode(), same.hashCode());
        Assertions.assertNotEquals(first, other);
        Assertions.assertNotEquals(first, new S3(new Properties(PROPERTIES)));
    }

    private void assertSchemaFailure(TStatusCode code) throws Exception {
        S3TableValuedFunction tvf = tvf();
        BackendServiceProxy proxy = Mockito.mock(BackendServiceProxy.class);
        Mockito.when(proxy.fetchTableStructureAsync(Mockito.any(), Mockito.any()))
                .thenReturn(CompletableFuture.completedFuture(result(code)));
        try (MockedStatic<BackendServiceProxy> service = Mockito.mockStatic(BackendServiceProxy.class)) {
            service.when(BackendServiceProxy::getInstance).thenReturn(proxy);
            Assertions.assertThrows(AnalysisException.class, tvf::getTableColumns);
            int attempts = code == TStatusCode.NOT_FOUND ? 2 : 1;
            Mockito.verify(proxy, Mockito.times(attempts))
                    .fetchTableStructureAsync(Mockito.any(), Mockito.any());
            Assertions.assertThrows(AnalysisException.class, tvf::getTableColumns);
            Mockito.verify(proxy, Mockito.times(attempts * 2))
                    .fetchTableStructureAsync(Mockito.any(), Mockito.any());
            Assertions.assertEquals(2, tvf.getFileStatuses().size());
        }
    }

    private S3TableValuedFunction tvf() throws Exception {
        S3TableValuedFunction tvf = Mockito.spy(new S3TableValuedFunction(PROPERTIES,
                Arrays.asList(file("a.csv", 10), file("b.csv", 10))));
        Backend backend = Mockito.mock(Backend.class);
        Mockito.when(backend.getHost()).thenReturn("127.0.0.1");
        Mockito.when(backend.getBrpcPort()).thenReturn(8060);
        Mockito.doReturn(backend).when(tvf).getBackend();
        return tvf;
    }

    private static TBrokerFileStatus file(String key, long size) {
        return new TBrokerFileStatus("s3://bucket/logs/" + key, false, size, true);
    }

    private static PFetchTableSchemaResult result(TStatusCode code) {
        return PFetchTableSchemaResult.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(code.getValue()).addErrorMsgs("schema unavailable"))
                .setColumnNums(0).build();
    }
}
