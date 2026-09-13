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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Which scans BE's file-cache admission governance is evaluated for.
 *
 * <p>It used to be a set of three catalog type names compiled into {@link FileQueryScanNode}. The real
 * condition is a property of the connector — whether BE reads its data with the native file readers that
 * populate the file cache at all — so it is now the connector's declaration, resolved per table through the
 * handle. That distinction is invisible in a homogeneous catalog and decisive in a heterogeneous one, where
 * one catalog serves tables of several formats through sibling connectors.</p>
 *
 * <p><b>Why this matters:</b> both failure directions are silent. Skipping the check for a connector whose
 * data IS natively read means the user's library/table admission rules quietly do not apply to those tables
 * (only a debug log). Running it for a JNI-read connector spends an admission lookup per scan for a cache
 * that will never hold its data.</p>
 */
public class PluginDrivenScanNodeFileCacheAdmissionTest {

    @Test
    public void appliesWhenTheServingConnectorDeclaresNativeFileReads() {
        Assertions.assertTrue(admissionApplicable(true),
                "a connector whose ranges BE reads natively must stay inside admission governance");
    }

    @Test
    public void doesNotApplyWhenTheServingConnectorDeclaresNothing() {
        // The SPI default. jdbc / trino / maxcompute / es keep exactly the behaviour they had while the
        // catalog-type allow-list decided this.
        Assertions.assertFalse(admissionApplicable(false),
                "a connector that does not declare native file reads must stay out of admission governance");
    }

    @Test
    public void doesNotApplyWhenNoProviderResolves() {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getScanPlanProvider(handle)).thenReturn(null);
        Deencapsulation.setField(node, "connector", connector);
        Deencapsulation.setField(node, "currentHandle", handle);

        // System tables and an unavailable plugin resolve no scan provider; asking one would NPE, and
        // defaulting to "governed" would run an admission lookup with no connector behind it.
        Assertions.assertFalse(
                (boolean) Deencapsulation.invoke(node, "isFileCacheAdmissionApplicable"));
    }

    @Test
    public void theBaseNodeStaysOutOfGovernanceByDefault() {
        // TVF and remote-Doris scan nodes were never in the allow-list; the base default keeps them out
        // without either of them having to say anything.
        FileQueryScanNode base = Mockito.mock(FileQueryScanNode.class, Mockito.CALLS_REAL_METHODS);
        Assertions.assertFalse((boolean) Deencapsulation.invoke(base, "isFileCacheAdmissionApplicable"));
    }

    /** Drives the node with a single sibling provider declaring {@code supportsFileCache() == declared}. */
    private static boolean admissionApplicable(boolean declared) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(provider.supportsFileCache()).thenReturn(declared);
        Connector connector = Mockito.mock(Connector.class);
        // Resolution goes through the handle, so in a heterogeneous catalog each table is answered by the
        // sibling that will actually serve it rather than by the catalog's type.
        Mockito.when(connector.getScanPlanProvider(handle)).thenReturn(provider);
        Deencapsulation.setField(node, "connector", connector);
        Deencapsulation.setField(node, "currentHandle", handle);
        return Deencapsulation.invoke(node, "isFileCacheAdmissionApplicable");
    }
}
