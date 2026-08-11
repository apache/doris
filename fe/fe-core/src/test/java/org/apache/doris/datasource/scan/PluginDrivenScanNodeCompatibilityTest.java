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
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.datasource.connector.converter.ConnectorComputeVariantType;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.system.Backend;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

/** Tests the mixed-version safety gate for plugin-driven Variant scans. */
public class PluginDrivenScanNodeCompatibilityTest {

    private static final int VARIANT_EXEC_VERSION = 12;

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
    public void metadataCountCapabilityUsesPinnedHandle() {
        ConnectorSession session = org.mockito.Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle latest = new ConnectorTableHandle() { };
        ConnectorTableHandle pinned = new ConnectorTableHandle() { };
        ConnectorScanPlanProvider provider =
                org.mockito.Mockito.mock(ConnectorScanPlanProvider.class);
        org.mockito.Mockito.doAnswer(invocation -> invocation.getArgument(1) == latest)
                .when(provider).canServeMetadataOnlyCount(org.mockito.Mockito.same(session),
                        org.mockito.Mockito.any(ConnectorTableHandle.class),
                        org.mockito.Mockito.eq(Optional.empty()));

        Assert.assertTrue(PluginDrivenScanNode.canServeMetadataOnlyCount(
                provider, session, latest));
        Assert.assertFalse(PluginDrivenScanNode.canServeMetadataOnlyCount(
                provider, session, pinned));
    }
}
