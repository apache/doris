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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.S3Util;
import org.apache.doris.connector.ConnectorFactory;
import org.apache.doris.connector.PluginFileFormat;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorFileSource;
import org.apache.doris.connector.spi.ConnectorFileSourceOps;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.scan.PluginDrivenScanNode;
import org.apache.doris.fs.FileSystemFactory;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TExplainLevel;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The file table functions reading a format that a connector plugin serves ({@code ConnectorProvider#getFileFormats}):
 * the engine hands the plugin the location and storage after the endpoint check, lists nothing, takes the columns
 * from the plugin and scans through it, and only {@code s3()} (and {@code file()} over object storage) do so.
 */
public class PluginFileFormatTableValuedFunctionTest {
    private static final String FORMAT = "test_format";
    private static final long FORMAT_CATALOG_ID = -7;
    private static final String ENDPOINT = "s3.us-west-2.amazonaws.com";
    private static final Map<String, String> PROPERTIES = Map.of(
            "uri", "s3://bucket/data/set.test", "format", FORMAT, "s3.endpoint", ENDPOINT,
            "s3.region", "us-west-2", "s3.access_key", "access-key", "s3.secret_key", "secret-key");

    private final List<ConnectorFileSource> sources = new ArrayList<>();
    private boolean runningUnitTest;
    private ConnectContext previousContext;
    private ConnectorTableHandle handle;
    private ConnectorMetadata metadata;
    private Connector connector;
    private MockedStatic<ConnectorFactory> factory;

    @BeforeEach
    public void setUp() {
        runningUnitTest = FeConstants.runningUnitTest;
        // Outside the unit-test shortcut s3() lists the files at the uri; a plugin's format must never be listed.
        FeConstants.runningUnitTest = false;
        previousContext = ConnectContext.get();
        ConnectContext ctx = new ConnectContext();
        StatementContext statement = new StatementContext();
        statement.initializeBeExecVersion(Config.be_exec_version);
        ctx.setStatementContext(statement);
        ctx.setThreadLocalInfo();

        handle = Mockito.mock(ConnectorTableHandle.class);
        metadata = Mockito.mock(ConnectorMetadata.class,
                Mockito.withSettings().extraInterfaces(ConnectorFileSourceOps.class));
        Mockito.when(((ConnectorFileSourceOps) metadata).getFileTableHandle(Mockito.any(), Mockito.any()))
                .thenAnswer(invocation -> {
                    sources.add(invocation.getArgument(1));
                    return handle;
                });
        Mockito.when(metadata.getTableSchema(Mockito.any(), Mockito.eq(handle))).thenReturn(new ConnectorTableSchema(
                "set", Arrays.asList(new ConnectorColumn("id", ConnectorType.of("INT"), "", true, null),
                        new ConnectorColumn("name", ConnectorType.of("STRING"), "", true, null)),
                FORMAT, Collections.emptyMap()));
        connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        // A real format reader, built through the constructor the plugin manager uses.
        PluginFileFormat format = Mockito.mock(PluginFileFormat.class, Mockito.withSettings()
                .useConstructor(FORMAT, connector, FORMAT_CATALOG_ID).defaultAnswer(Mockito.CALLS_REAL_METHODS));
        factory = Mockito.mockStatic(ConnectorFactory.class, Mockito.CALLS_REAL_METHODS);
        factory.when(() -> ConnectorFactory.getFileFormat(FORMAT)).thenReturn(Optional.of(format));
    }

    @AfterEach
    public void tearDown() {
        factory.close();
        FeConstants.runningUnitTest = runningUnitTest;
        ConnectContext.remove();
        if (previousContext != null) {
            previousContext.setThreadLocalInfo();
        }
    }

    @Test
    public void s3HandsTheDataSetToThePluginAndScansThroughIt() throws Exception {
        try (MockedStatic<S3Util> s3 = Mockito.mockStatic(S3Util.class);
                MockedStatic<FileSystemFactory> filesystems = Mockito.mockStatic(FileSystemFactory.class)) {
            S3TableValuedFunction tvf = new S3TableValuedFunction(PROPERTIES);

            // The FE reads the data set at the user's endpoint, so the endpoint check of a listing runs first.
            s3.verify(() -> S3Util.validateAndTestEndpoint(ENDPOINT));
            filesystems.verifyNoInteractions();
            Assertions.assertTrue(tvf.getFileStatuses().isEmpty());
            Assertions.assertEquals(1, sources.size());
            ConnectorFileSource source = sources.get(0);
            Assertions.assertEquals(FORMAT, source.getFormat());
            Assertions.assertEquals("s3://bucket/data/set.test", source.getLocation());
            Assertions.assertEquals(1, source.getStorageProperties().size());
            Map<String, String> backend = source.getBackendStorageProperties();
            Assertions.assertEquals(ENDPOINT, backend.get("AWS_ENDPOINT"), backend.keySet().toString());
            Assertions.assertEquals("access-key", backend.get("AWS_ACCESS_KEY"));
            Assertions.assertEquals("secret-key", backend.get("AWS_SECRET_KEY"));

            // The columns are the plugin's: no BE is asked to read a file's schema.
            List<Column> columns = tvf.getTableColumns();
            Assertions.assertEquals(Arrays.asList("id", "name"),
                    columns.stream().map(Column::getName).collect(Collectors.toList()));
            Assertions.assertEquals(PrimitiveType.INT, columns.get(0).getType().getPrimitiveType());
            Assertions.assertEquals(PrimitiveType.STRING, columns.get(1).getType().getPrimitiveType());

            ScanNode node = scanNode(tvf);
            Assertions.assertInstanceOf(PluginDrivenScanNode.class, node);
            // The table of a table function has no database: EXPLAIN names the format's connector instead.
            String explain = node.getNodeExplainString("", TExplainLevel.NORMAL);
            Assertions.assertTrue(explain.contains("TABLE: null.null.S3TableValuedFunction\n"), explain);
            Assertions.assertTrue(explain.contains("CONNECTOR: " + FORMAT + "\n"), explain);
            Assertions.assertFalse(explain.contains("secret-key"), explain);
        }
    }

    @Test
    public void fileOverObjectStorageReadsThePluginFormatThroughS3() throws Exception {
        try (MockedStatic<S3Util> s3 = Mockito.mockStatic(S3Util.class);
                MockedStatic<FileSystemFactory> filesystems = Mockito.mockStatic(FileSystemFactory.class)) {
            FileTableValuedFunction tvf = new FileTableValuedFunction(PROPERTIES);

            s3.verify(() -> S3Util.validateAndTestEndpoint(ENDPOINT));
            Assertions.assertEquals(1, sources.size());
            Assertions.assertEquals("s3://bucket/data/set.test", sources.get(0).getLocation());
            Assertions.assertEquals(Arrays.asList("id", "name"),
                    tvf.getTableColumns().stream().map(Column::getName).collect(Collectors.toList()));
            Assertions.assertInstanceOf(PluginDrivenScanNode.class, scanNode(tvf));
        }
    }

    @Test
    public void onlyTheS3TableFunctionHandsADataSetToAPlugin() {
        // hdfs() and http() refuse the format before their storage is parsed; branch-4.x refuses them too.
        AnalysisException hdfs = Assertions.assertThrows(AnalysisException.class, () -> new HdfsTableValuedFunction(
                Map.of("uri", "hdfs://nameservice/data/set.test", "format", FORMAT,
                        "fs.defaultFS", "hdfs://nameservice")));
        Assertions.assertTrue(hdfs.getMessage().contains("Format " + FORMAT + " is read only by the s3() table"
                + " function, or by file() over S3-compatible storage"), hdfs.getMessage());
        AnalysisException http = Assertions.assertThrows(AnalysisException.class, () -> new HttpTableValuedFunction(
                Map.of("uri", "http://127.0.0.1:8000/data/set.test", "format", FORMAT)));
        Assertions.assertTrue(http.getMessage().contains("is read only by the s3() table function"),
                http.getMessage());
        Mockito.verifyNoInteractions(connector);
    }

    @Test
    public void pathPartitionKeysAreRefused() {
        Map<String, String> properties = new HashMap<>(PROPERTIES);
        properties.put("path_partition_keys", "dt");
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> new S3TableValuedFunction(properties));
        Assertions.assertTrue(e.getMessage().contains("'path_partition_keys' is not supported for format " + FORMAT),
                e.getMessage());
        Mockito.verifyNoInteractions(connector);
    }

    @Test
    public void listOfFilesIsRefused() {
        // A streaming job hands s3() the files it picked; the plugin reads the data set at the uri as a whole.
        AnalysisException e = Assertions.assertThrows(AnalysisException.class, () -> new S3TableValuedFunction(
                PROPERTIES, Collections.singletonList(
                        new TBrokerFileStatus("s3://bucket/data/set.test/data/a.test", false, 10, true))));
        Assertions.assertTrue(e.getMessage().contains("reads the data set at the uri as a whole"), e.getMessage());
        Mockito.verifyNoInteractions(connector);
    }

    @Test
    public void refusedEndpointNeverReachesThePlugin() {
        try (MockedStatic<S3Util> s3 = Mockito.mockStatic(S3Util.class)) {
            s3.when(() -> S3Util.validateAndTestEndpoint(ENDPOINT))
                    .thenThrow(new UserException("endpoint is not in the white list"));
            AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                    () -> new S3TableValuedFunction(PROPERTIES));
            Assertions.assertTrue(e.getMessage().contains("endpoint is not in the white list"), e.getMessage());
            Mockito.verifyNoInteractions(connector);
        }
    }

    @Test
    public void thePredicateReachesThePluginBesideAnInternalTable() throws Exception {
        try (MockedStatic<S3Util> s3 = Mockito.mockStatic(S3Util.class);
                MockedStatic<FileSystemFactory> filesystems = Mockito.mockStatic(FileSystemFactory.class)) {
            S3TableValuedFunction tvf = new S3TableValuedFunction(PROPERTIES);
            ScanNode node = scanNode(tvf);
            SlotRef id = Mockito.mock(SlotRef.class);
            Mockito.when(id.getColumnName()).thenReturn("id");
            Mockito.when(id.getType()).thenReturn(Type.INT);
            node.addConjunct(new BinaryPredicate(BinaryPredicate.Operator.EQ, id, new IntLiteral(1L, Type.INT)));
            // An internal table in the statement (a join, or the target of INSERT ... SELECT) takes the plan read
            // lock, which sends a catalog table's scan down a path that a table function's table must not take.
            OlapTable target = Mockito.mock(OlapTable.class);
            Mockito.when(target.needReadLockWhenPlan()).thenReturn(true);
            ConnectContext.get().getStatementContext().getInsertTargetTables()
                    .put(ImmutableList.of("internal", "db", "target"), target);

            Deencapsulation.invoke(node, "convertPredicate");

            Mockito.verify(metadata).applyFilter(Mockito.any(), Mockito.eq(handle), Mockito.any());
        }
    }

    @Test
    public void theFormatIsNeverWritten() {
        // OUTFILE, INSERT INTO s3() and routine load resolve their format here, which knows no plugin's.
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> FileFormatProperties.createFileFormatProperties(FORMAT));
    }

    private static ScanNode scanNode(ExternalFileTableValuedFunction tvf) throws Exception {
        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(tvf.getTable());
        return tvf.getScanNode(new PlanNodeId(0), desc, new SessionVariable());
    }
}
