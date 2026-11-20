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

import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanContext;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.datasource.split.PluginDrivenSplit;
import org.apache.doris.datasource.split.SplitAssignment;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.qe.QeProcessorImpl;
import org.apache.doris.qe.SessionVariable;
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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PluginDrivenScanNodeBeSplitTest {
    private static final AtomicLong NEXT_QUERY = new AtomicLong();

    private static final class Fixture implements AutoCloseable {
        private final TUniqueId queryId = new TUniqueId(66802, NEXT_QUERY.incrementAndGet());
        private final ConnectorSession session = Mockito.mock(ConnectorSession.class);
        private final AtomicReference<ConnectorScanRequest> planned = new AtomicReference<>();
        private final PluginDrivenScanNode node;

        private Fixture(SessionVariable variables) {
            ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
            ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class,
                    Mockito.CALLS_REAL_METHODS);
            Mockito.when(provider.planScan(Mockito.any(), Mockito.any())).thenAnswer(invocation -> {
                planned.set(invocation.getArgument(1));
                return Collections.emptyList();
            });
            Connector connector = Mockito.mock(Connector.class);
            Mockito.when(connector.getScanPlanProvider(handle)).thenReturn(provider);
            Mockito.when(session.getQueryId()).thenReturn(DebugUtil.printId(queryId));
            Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
            TupleDescriptor descriptor = new TupleDescriptor(new TupleId(0));
            descriptor.setTable(Mockito.mock(TableIf.class));
            node = new PluginDrivenScanNode(new PlanNodeId(0), descriptor, false, variables,
                    ScanContext.EMPTY, connector, session, handle);
            ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class, Mockito.CALLS_REAL_METHODS);
            Mockito.doReturn(Collections.emptyMap()).when(metadata).getColumnHandles(session, handle);
            Deencapsulation.setField(node, "cachedMetadata", metadata);
            SplitAssignment assignment = Mockito.mock(SplitAssignment.class);
            Mockito.when(assignment.getScanContext()).thenReturn(new ConnectorScanContext());
            Deencapsulation.setField(node, "splitAssignment", assignment);
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
