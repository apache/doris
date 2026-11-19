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

package org.apache.doris.datasource.scan;

import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.hive.HiveScanRange;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertiesResult;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.datasource.split.PluginDrivenSplit;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.spi.Split;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPushAggOp;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TTableFormatFileDesc;
import org.apache.doris.thrift.TUniqueId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class PluginDrivenScanNodeBeSplitTest {
    private static final long MB = 1024L * 1024L;
    private static final AtomicLong NEXT_QUERY = new AtomicLong();

    private static final class Fixture implements AutoCloseable {
        private final TUniqueId queryId = new TUniqueId(66802, NEXT_QUERY.incrementAndGet());
        private final ConnectorSession session = Mockito.mock(ConnectorSession.class);
        private final AtomicReference<ConnectorScanRequest> planned = new AtomicReference<>();
        private final ConnectorScanPlanProvider provider;
        private Function<ConnectorScanRequest, List<ConnectorScanRange>> ranges = request -> Collections.emptyList();
        private final PluginDrivenScanNode node;

        private Fixture(SessionVariable variables) {
            ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
            provider = Mockito.mock(ConnectorScanPlanProvider.class,
                    Mockito.CALLS_REAL_METHODS);
            Mockito.when(provider.planScan(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
                planned.set(invocation.getArgument(1));
                return ranges.apply(planned.get());
            });
            Connector connector = Mockito.mock(Connector.class);
            Mockito.when(connector.getScanPlanProvider(handle)).thenReturn(provider);
            Mockito.when(session.getQueryId()).thenReturn(DebugUtil.printId(queryId));
            Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
            TupleDescriptor descriptor = new TupleDescriptor(new TupleId(0));
            TableIf table = Mockito.mock(TableIf.class);
            Mockito.when(table.getFullSchema()).thenReturn(List.of(new Column("k", Type.INT)));
            descriptor.setTable(table);
            node = new PluginDrivenScanNode(new PlanNodeId(0), descriptor, false, variables,
                    ScanContext.EMPTY, connector, session, handle);
            ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class, Mockito.CALLS_REAL_METHODS);
            Mockito.doReturn(Collections.emptyMap()).when(metadata).getColumnHandles(session, handle);
            Deencapsulation.setField(node, "cachedMetadata", metadata);
            SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
            Mockito.when(assignment.getScanContext()).thenReturn(new ConnectorScanContext());
            Deencapsulation.setField(node, "splitAssignment", assignment);
            Deencapsulation.setField(node, "params", new TFileScanRangeParams());
            Deencapsulation.setField(node, "cachedPropertiesResult",
                    ScanNodePropertiesResult.of(Map.of(ScanNodePropertyKeys.FILE_FORMAT_TYPE, "parquet")));
        }

        private ConnectorScanRequest batchRequest() {
            return Deencapsulation.invoke(node, "prepareBatchScan",
                    PluginDrivenScanNode.registerQueryFinishCleanup(session));
        }

        @Override
        public void close() {
            QeProcessorImpl.INSTANCE.unregisterQuery(queryId);
        }
    }

    @Test
    public void sampledNativeScansRetainLegacyFeGranularity() throws Exception {
        for (TableSample sample : List.of(new TableSample(true, 4L, 7L),
                new TableSample(false, 4 * MB, 7L))) {
            SessionVariable variables = new SessionVariable();
            variables.setFileSplitSize(16 * MB);
            try (Fixture fixture = new Fixture(variables)) {
                Mockito.when(fixture.provider.supportsTableSample()).thenReturn(true);
                fixture.ranges = request -> fileRanges(request, 16 * MB, 400 * MB);
                fixture.node.setTableSample(sample);
                List<Split> splits = fixture.node.getSplits(2);
                Assertions.assertEquals(1, splits.size());
                Assertions.assertEquals(16 * MB, splits.get(0).getLength());
                Assertions.assertEquals(-1, fixture.planned.get().getCoarseSplitSize());
                Assertions.assertFalse(fixture.planned.get().isCountPushdown());
            }
        }
    }

    @Test
    public void synchronousCoarseRangeIsPlacedOnBothBackends() throws Exception {
        SessionVariable variables = new SessionVariable();
        variables.enableFileCache = false;
        variables.enableExternalTableBatchMode = false;
        try (Fixture fixture = new Fixture(variables)) {
            fixture.ranges = request -> fileRanges(request, 256 * MB, 400 * MB);
            List<TScanRangeLocations> locations = placeOnBackends(fixture, variables, 2);
            HashSet<Long> owners = new HashSet<>();
            List<TFileRangeDesc> files = new ArrayList<>();
            for (TScanRangeLocations location : locations) {
                owners.add(location.getLocations().get(0).getBackendId());
                files.addAll(location.getScanRange().getExtScanRange().getFileScanRange().getRanges());
            }
            Assertions.assertEquals(new HashSet<>(List.of(1L, 2L)), owners);
            files.sort(Comparator.comparingLong(TFileRangeDesc::getStartOffset));
            long nextOffset = 0;
            for (TFileRangeDesc file : files) {
                Assertions.assertEquals(nextOffset, file.getStartOffset());
                Assertions.assertEquals(400 * MB, file.getFileSize());
                Assertions.assertEquals(64 * MB, file.getTargetSplitSize());
                Assertions.assertEquals(List.of("p"), file.getColumnsFromPathKeys());
                Assertions.assertEquals(List.of("7"), file.getColumnsFromPath());
                nextOffset += file.getSize();
            }
            Assertions.assertEquals(400 * MB, nextOffset);
            Mockito.verify(fixture.provider).planScan(Mockito.any(), Mockito.any());
        }
    }

    @Test
    public void synchronousSmallFileIsSubdividedWithOneByteBeTarget() throws Exception {
        SessionVariable variables = new SessionVariable();
        variables.enableFileCache = false;
        variables.enableExternalTableBatchMode = false;
        variables.setFileSplitSizeOnBe(1);
        try (Fixture fixture = new Fixture(variables)) {
            fixture.ranges = request -> fileRanges(request, 256 * MB, 101);
            List<TFileRangeDesc> files = new ArrayList<>();
            for (TScanRangeLocations location : placeOnBackends(fixture, variables, 2)) {
                files.addAll(location.getScanRange().getExtScanRange().getFileScanRange().getRanges());
            }
            files.sort(Comparator.comparingLong(TFileRangeDesc::getStartOffset));
            Assertions.assertEquals(2, files.size());
            Assertions.assertEquals(0, files.get(0).getStartOffset());
            Assertions.assertEquals(50, files.get(0).getSize());
            Assertions.assertEquals(50, files.get(1).getStartOffset());
            Assertions.assertEquals(51, files.get(1).getSize());
            for (TFileRangeDesc file : files) {
                Assertions.assertEquals(101, file.getFileSize());
                Assertions.assertEquals(1, file.getTargetSplitSize());
                Assertions.assertEquals(List.of("p"), file.getColumnsFromPathKeys());
                Assertions.assertEquals(List.of("7"), file.getColumnsFromPath());
            }
        }
    }

    @Test
    public void backendPlacementKeepsAmplePlansCoarseAndBoundsSmallPlans() throws Exception {
        // backend count, file size in MiB, file count, max split count, resulting FE range count
        for (int[] scenario : new int[][] {{2, 400, 3, 100000, 3}, {1, 400, 1, 100000, 1},
                {2, 400, 1, 1, 1}, {8, 400, 1, 2, 2}, {8, 100, 1, 100000, 1},
                {8, 128, 1, 100000, 2}, {3, 401, 1, 100000, 3}}) {
            SessionVariable variables = new SessionVariable();
            variables.enableFileCache = false;
            variables.enableExternalTableBatchMode = false;
            variables.setMaxFileSplitNum(scenario[3]);
            try (Fixture fixture = new Fixture(variables)) {
                fixture.ranges = request -> {
                    List<ConnectorScanRange> ranges = new ArrayList<>();
                    for (int i = 0; i < scenario[2]; i++) {
                        ranges.addAll(fileRanges(request, 256 * MB, scenario[1] * MB,
                                "s3://bucket/file-" + i + ".parquet"));
                    }
                    return ranges;
                };
                List<TScanRangeLocations> locations = placeOnBackends(fixture, variables, scenario[0]);
                Assertions.assertEquals(scenario[4], locations.size());
                Assertions.assertEquals(512 * MB, fixture.planned.get().getCoarseSplitSize());
                long totalBytes = 0;
                for (TScanRangeLocations location : locations) {
                    for (TFileRangeDesc file : location.getScanRange().getExtScanRange()
                            .getFileScanRange().getRanges()) {
                        Assertions.assertTrue(file.getSize() >= 64 * MB);
                        totalBytes += file.getSize();
                    }
                }
                Assertions.assertEquals((long) scenario[1] * MB * scenario[2], totalBytes);
                Mockito.verify(fixture.provider).planScan(Mockito.any(), Mockito.any());
            }
        }
    }

    @Test
    public void placementDoesNotSubdivideUnsupportedOrPrecomputedRanges() throws Exception {
        for (String format : List.of("parquet", "orc", "jni")) {
            for (boolean supported : new boolean[] {false, true}) {
                for (long rowCount : new long[] {-1, 42}) {
                    SessionVariable variables = new SessionVariable();
                    variables.enableFileCache = false;
                    variables.enableExternalTableBatchMode = false;
                    try (Fixture fixture = new Fixture(variables)) {
                        ConnectorScanRange range = Mockito.mock(ConnectorScanRange.class,
                                Mockito.CALLS_REAL_METHODS);
                        Mockito.when(range.getPath()).thenReturn(Optional.of("s3://bucket/data"));
                        Mockito.when(range.getLength()).thenReturn(400 * MB);
                        Mockito.when(range.getFileSize()).thenReturn(400 * MB);
                        Mockito.when(range.getFileFormat()).thenReturn(format);
                        Mockito.when(range.getPushDownRowCount()).thenReturn(rowCount);
                        Mockito.when(range.supportsBeSplitRefinement()).thenReturn(supported);
                        fixture.ranges = request -> List.of(range);
                        List<TScanRangeLocations> locations = placeOnBackends(fixture, variables, 2);
                        Assertions.assertEquals(supported && rowCount < 0 && !"jni".equals(format) ? 2 : 1,
                                locations.size());
                    }
                }
            }
        }
    }

    @Test
    public void splitCopiesRetainConnectorMetadataAndProportionalWeight() {
        ConnectorScanRange range = HiveScanRange.builder().path("s3://bucket/data.parquet")
                .start(100 * MB).length(400 * MB).fileSize(500 * MB).modificationTime(17)
                .partitionValues(Map.of("p", "7")).build();
        PluginDrivenSplit parent = new PluginDrivenSplit(range);
        parent.setSelfSplitWeight(400 * MB);
        parent.setTargetSplitSize(512 * MB);
        PluginDrivenSplit child = parent.withRange(300 * MB, 200 * MB);
        Assertions.assertSame(range, child.getConnectorScanRange());
        Assertions.assertSame(parent.getPath(), child.getPath());
        Assertions.assertEquals(300 * MB, child.getStart());
        Assertions.assertEquals(200 * MB, child.getLength());
        Assertions.assertEquals(500 * MB, child.getFileLength());
        Assertions.assertEquals(17, child.getModificationTime());
        Assertions.assertEquals(List.of("7"), child.getPartitionValues());
        Assertions.assertEquals(200 * MB, child.getSelfSplitWeight());
        Assertions.assertEquals(512 * MB, child.getTargetSplitSize());
        Assertions.assertEquals(400 * MB, parent.getLength());
    }

    private static List<TScanRangeLocations> placeOnBackends(Fixture fixture, SessionVariable variables,
            int backendCount) throws Exception {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setSessionVariable(variables);
        ComputeGroup group = Mockito.mock(ComputeGroup.class);
        List<Backend> backends = new ArrayList<>();
        for (int i = 1; i <= backendCount; i++) {
            Backend backend = new Backend(i, "192.0.2." + i, 9050);
            backend.setAlive(true);
            backends.add(backend);
        }
        Mockito.when(group.getBackendList()).thenReturn(backends);
        context.setComputeGroup(group);
        context.setThreadLocalInfo();
        try {
            Deencapsulation.setField(fixture.node, "backendPolicy",
                    new FederationBackendPolicy(NodeSelectionStrategy.ROUND_ROBIN));
            fixture.node.initBackendPolicy();
            fixture.node.createScanRangeLocations();
            return fixture.node.getScanRangeLocations(0);
        } finally {
            ConnectContext.remove();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    private static List<ConnectorScanRange> fileRanges(ConnectorScanRequest request, long legacySize, long size) {
        return fileRanges(request, legacySize, size, "s3://bucket/two-level.parquet");
    }

    private static List<ConnectorScanRange> fileRanges(ConnectorScanRequest request, long legacySize,
            long size, String path) {
        long target = request.getCoarseSplitSize() > 0 ? request.getCoarseSplitSize() : legacySize;
        List<ConnectorScanRange> ranges = new ArrayList<>();
        for (long offset = 0; offset < size; offset += target) {
            ranges.add(HiveScanRange.builder().path(path)
                    .start(offset).length(Math.min(target, size - offset)).fileSize(size)
                    .modificationTime(17).partitionValues(Map.of("p", "7")).build());
        }
        return ranges;
    }

    @Test
    public void synchronousAndStreamingRequestsUseActualNodeSerialLimit() throws Exception {
        SessionVariable variables = new SessionVariable();
        variables.enableAdaptivePipelineTaskSerialReadOnLimit = true;
        try (Fixture fixture = new Fixture(variables)) {
            fixture.node.getSplits(1);
            Assertions.assertEquals(512L << 20, fixture.planned.get().getCoarseSplitSize());
            Assertions.assertEquals(512L << 20, fixture.batchRequest().getCoarseSplitSize());
            fixture.node.setLimit(1);
            fixture.node.getSplits(1);
            Assertions.assertEquals(-1, fixture.planned.get().getCoarseSplitSize());
            Assertions.assertEquals(-1, fixture.batchRequest().getCoarseSplitSize());
            fixture.node.setLimit(-1);
            variables.maxFileScannersConcurrency = 1;
            fixture.node.getSplits(1);
            Assertions.assertEquals(-1, fixture.planned.get().getCoarseSplitSize());
            Assertions.assertEquals(-1, fixture.batchRequest().getCoarseSplitSize());
        }
    }

    @Test
    public void metadataCountBatchKeepsLegacyPlanningHint() throws Exception {
        try (Fixture fixture = new Fixture(new SessionVariable())) {
            fixture.node.setPushDownAggNoGrouping(TPushAggOp.COUNT);
            fixture.node.setPushDownCountSlotIds(Collections.emptyList());
            Assertions.assertEquals(-1, fixture.batchRequest().getCoarseSplitSize());
        }
    }

    @Test
    public void finalConnectorFormatAndCapabilityControlTheCoalescingTarget() throws Exception {
        try (Fixture fixture = new Fixture(new SessionVariable())) {
            TFileScanRangeParams params = new TFileScanRangeParams();
            params.setFormatType(TFileFormatType.FORMAT_JNI);
            Deencapsulation.setField(fixture.node, "params", params);
            for (boolean supported : new boolean[] {false, true}) {
                for (TFileFormatType format : new TFileFormatType[] {
                        TFileFormatType.FORMAT_PARQUET, TFileFormatType.FORMAT_ORC, TFileFormatType.FORMAT_JNI}) {
                    ConnectorScanRange range = new ConnectorScanRange() {
                        @Override
                        public Map<String, String> getProperties() {
                            return Collections.emptyMap();
                        }

                        @Override
                        public Optional<String> getPath() {
                            return Optional.of("file:///tmp/two-level.parquet");
                        }

                        @Override
                        public boolean supportsBeSplitRefinement() {
                            return supported;
                        }

                        @Override
                        public void populateRangeParams(TTableFormatFileDesc table, TFileRangeDesc file) {
                            file.setFormatType(format);
                        }
                    };
                    TScanRangeLocations locations = Deencapsulation.invoke(fixture.node, "splitToScanRange",
                            new Backend(1, "127.0.0.1", 9050), Collections.emptyMap(), new PluginDrivenSplit(range),
                            Collections.emptyList(), true);
                    TFileRangeDesc file = locations.getScanRange().getExtScanRange()
                            .getFileScanRange().getRanges().get(0);
                    Assertions.assertEquals(supported && format != TFileFormatType.FORMAT_JNI,
                            file.isSetTargetSplitSize());
                    if (file.isSetTargetSplitSize()) {
                        Assertions.assertEquals(64L << 20, file.getTargetSplitSize());
                    }
                }
            }
        }
    }
}
