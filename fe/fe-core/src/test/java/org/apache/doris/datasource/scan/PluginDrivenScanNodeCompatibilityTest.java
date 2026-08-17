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

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TableSample;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PartitionItem;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.datasource.connector.converter.ConnectorComputeVariantType;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan.SelectedPartitions;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TFileScanRangeParams;
import org.apache.doris.thrift.TPushAggOp;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Tests the mixed-version safety gate for plugin-driven Variant scans. */
public class PluginDrivenScanNodeCompatibilityTest {

    private static final int VARIANT_EXEC_VERSION = 12;

    @Test
    public void compatibilityCheckRunsOnlyAfterScanSlotsAreFinalized() throws Exception {
        ConnectContext context = new ConnectContext();
        context.setThreadLocalInfo();
        try {
            // Keep Mockito calls qualified because FE Checkstyle forbids static member imports.
            PluginDrivenScanNode node = Mockito.mock(
                    PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
            TupleDescriptor tuple = Mockito.mock(TupleDescriptor.class);
            java.util.ArrayList<org.apache.doris.analysis.SlotDescriptor> initSlots =
                    new java.util.ArrayList<>();
            initSlots.add(Mockito.mock(org.apache.doris.analysis.SlotDescriptor.class));
            Mockito.when(tuple.getSlots()).thenReturn(initSlots);
            Mockito.when(tuple.getTable()).thenReturn(Mockito.mock(TableIf.class));
            Deencapsulation.setField(node, "desc", tuple);
            Deencapsulation.setField(node, "sessionVariable", new SessionVariable());
            Deencapsulation.setField(node, "params", new TFileScanRangeParams());
            Deencapsulation.setField(node, "cachedMetadata", Mockito.mock(ConnectorMetadata.class));
            Deencapsulation.setField(node, "connector", Mockito.mock(Connector.class));
            Deencapsulation.setField(node, "connectorSession", Mockito.mock(ConnectorSession.class));
            Deencapsulation.setField(node, "backendPolicy", Mockito.mock(FederationBackendPolicy.class));
            Mockito.doNothing().when(node).initBackendPolicy();
            Mockito.doNothing().when(node).initSchemaParams();
            Mockito.doNothing().when(node).checkVariantBackendCompatibilityForCurrentScan(
                    ArgumentMatchers.any());
            Mockito.doNothing().when(node).convertPredicate();
            Mockito.doNothing().when(node).createScanRangeLocations();
            Mockito.when(node.getPathPartitionKeys()).thenReturn(Collections.emptyList());

            node.doInitialize();
            Mockito.verify(node, Mockito.never()).checkVariantBackendCompatibilityForCurrentScan(
                    ArgumentMatchers.any());

            // Nereids prunes the scan tuple between init and finalize. The compatibility fence must
            // observe this finalized payload rather than the table-wide tuple used during init.
            initSlots.clear();
            node.doFinalize();
            Mockito.verify(node).checkVariantBackendCompatibilityForCurrentScan(
                    ArgumentMatchers.any());
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void computeVariantRejectsSmoothUpgradeSourceBackend() {
        Backend backend = new Backend(7L, "127.0.0.1", 9050);
        backend.setSmoothUpgradeSrc(true);

        UserException exception = Assert.assertThrows(UserException.class,
                () -> PluginDrivenScanNode.checkVariantBackendCompatibility(
                        true, Collections.singletonList(backend)));
        Assert.assertTrue(exception.getMessage().contains("backend 7"));
    }

    @Test
    public void compatibilityCheckIgnoresScansWithoutComputeVariant() throws UserException {
        Backend backend = new Backend(7L, "127.0.0.1", 9050);
        backend.setSmoothUpgradeSrc(true);

        PluginDrivenScanNode.checkVariantBackendCompatibility(
                false, Collections.singletonList(backend));
    }

    @Test
    public void computeVariantRejectsOldQueryWideExecutionVersion() {
        int original = Config.be_exec_version;
        try {
            Config.be_exec_version = VARIANT_EXEC_VERSION - 1;
            Backend backend = new Backend(8L, "127.0.0.1", 9050);

            UserException exception = Assert.assertThrows(UserException.class,
                    () -> PluginDrivenScanNode.checkVariantBackendCompatibility(
                            true, Collections.singletonList(backend)));
            Assert.assertTrue(exception.getMessage().contains("execution version"));
        } finally {
            Config.be_exec_version = original;
        }
    }

    @Test
    public void translatedScanTuplePreservesNestedComputeVariantCarrier() {
        boolean originalEnableVariantV2 = Config.enable_variant_v2;
        try {
            Config.enable_variant_v2 = false;
            Column column = new Column("payload",
                    ArrayType.create(new ConnectorComputeVariantType(), true));
            SlotReference slot = SlotReference.fromColumn(
                    StatementScopeIdGenerator.newExprId(), org.mockito.Mockito.mock(TableIf.class), column,
                    Collections.emptyList());
            PlanTranslatorContext context = new PlanTranslatorContext();
            TupleDescriptor tuple = context.generateTupleDesc();
            context.createSlotDesc(tuple, slot);

            Assert.assertTrue(PluginDrivenScanNode.projectsComputeVariant(tuple));
            Assert.assertTrue(tuple.getSlots().get(0).getType().toThrift()
                    .types.get(1).scalar_type.variant_is_v2);
        } finally {
            Config.enable_variant_v2 = originalEnableVariantV2;
        }
    }

    @Test
    public void oldBackendAllowsOnlyFullyPrecomputedVariantCountPlans() throws UserException {
        ConnectorScanRange countRange = new ConnectorScanRange() {
            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }

            @Override
            public long getPushDownRowCount() {
                return 42L;
            }
        };
        ConnectorScanRange dataRange = new ConnectorScanRange() {
            @Override
            public Map<String, String> getProperties() {
                return Collections.emptyMap();
            }
        };
        List<Backend> backends = Collections.singletonList(
                new Backend(9L, "127.0.0.1", 9050));
        int original = Config.be_exec_version;
        try {
            Config.be_exec_version = VARIANT_EXEC_VERSION - 1;
            PluginDrivenScanNode.checkVariantBackendCompatibility(
                    PluginDrivenScanNode.plannedScanDecodesVariant(
                            true, true, Collections.singletonList(countRange)),
                    backends);

            Assert.assertThrows(UserException.class,
                    () -> PluginDrivenScanNode.checkVariantBackendCompatibility(
                            PluginDrivenScanNode.plannedScanDecodesVariant(
                                    true, true, Arrays.asList(countRange, dataRange)),
                            backends));
        } finally {
            Config.be_exec_version = original;
        }
    }

    @Test
    public void sampledMetadataCountDefersVariantFenceToPlannedRanges() throws UserException {
        ConnectorScanPlanProvider provider = metadataCountProvider();
        Mockito.when(provider.supportsTableSample()).thenReturn(true);
        PluginDrivenScanNode node = metadataCountVariantNode(provider);
        node.setTableSample(new TableSample(true, 10L, 7L));

        node.checkVariantBackendCompatibilityForCurrentScan(Collections.singletonList(oldBackend()));

        Assert.assertTrue((Boolean) Deencapsulation.getField(node, "variantCompatibilityDeferred"));
    }

    @Test
    public void metadataCountProviderCannotEnterPartitionBatchBeforeRangeProof() throws UserException {
        ConnectorScanPlanProvider provider = metadataCountProvider();
        Mockito.when(provider.streamingSplitEstimate(Mockito.any(), Mockito.any(), Mockito.any(),
                Mockito.anyBoolean())).thenReturn(-1L);
        Mockito.when(provider.supportsBatchScan(Mockito.any(), Mockito.any())).thenReturn(true);
        PluginDrivenScanNode node = metadataCountVariantNode(provider);
        Map<String, PartitionItem> partitions = new LinkedHashMap<>();
        partitions.put("pt=1", Mockito.mock(PartitionItem.class));
        Deencapsulation.setField(node, "selectedPartitions",
                new SelectedPartitions(1, partitions, true));
        SessionVariable sessionVariable = Mockito.mock(SessionVariable.class);
        Mockito.when(sessionVariable.getNumPartitionsInBatchMode()).thenReturn(1);
        Deencapsulation.setField(node, "sessionVariable", sessionVariable);

        node.checkVariantBackendCompatibilityForCurrentScan(Collections.singletonList(oldBackend()));

        Assert.assertFalse(node.isBatchMode());
    }

    private static ConnectorScanPlanProvider metadataCountProvider() {
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(provider.canServeMetadataOnlyCount(Mockito.any(), Mockito.any(), Mockito.any()))
                .thenReturn(true);
        return provider;
    }

    private static PluginDrivenScanNode metadataCountVariantNode(ConnectorScanPlanProvider provider) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getScanPlanProvider(Mockito.any())).thenReturn(provider);
        Deencapsulation.setField(node, "connector", connector);
        Deencapsulation.setField(node, "connectorSession", Mockito.mock(ConnectorSession.class));
        Deencapsulation.setField(node, "currentHandle", new ConnectorTableHandle() { });
        Deencapsulation.setField(node, "conjuncts", new ArrayList<>());
        Deencapsulation.setField(node, "pushDownAggNoGroupingOp", TPushAggOp.COUNT);
        Deencapsulation.setField(node, "pushDownCountSlotIds", Collections.emptyList());

        SlotDescriptor slot = Mockito.mock(SlotDescriptor.class);
        Mockito.when(slot.getType()).thenReturn(new ConnectorComputeVariantType());
        TupleDescriptor tuple = Mockito.mock(TupleDescriptor.class);
        ArrayList<SlotDescriptor> slots = new ArrayList<>();
        slots.add(slot);
        Mockito.when(tuple.getSlots()).thenReturn(slots);
        Deencapsulation.setField(node, "desc", tuple);
        return node;
    }

    private static Backend oldBackend() {
        return new Backend(10L, "127.0.0.1", 9050);
    }
}
