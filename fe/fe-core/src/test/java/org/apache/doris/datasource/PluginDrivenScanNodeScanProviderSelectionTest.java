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

package org.apache.doris.datasource;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.scan.ConnectorScanPlanProvider;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Guards that {@link PluginDrivenScanNode} resolves its scan plan provider PER TABLE — passing the table's
 * {@code currentHandle} to {@link Connector#getScanPlanProvider(ConnectorTableHandle)} — so a heterogeneous
 * gateway connector can route each table to the right backing scanner.
 *
 * <p><b>WHY this matters (Rule 9):</b> before this seam the node called the no-arg
 * {@code connector.getScanPlanProvider()} for every table, so one catalog could expose exactly ONE scan
 * provider. All provider look-ups now route through {@code resolveScanProvider()} keyed on the handle; a
 * mutant that drops the handle (reverts to the no-arg getter) would send an iceberg-on-HMS table to the hive
 * scanner and return wrong/empty rows. Driven on a partial ({@code CALLS_REAL_METHODS}) node with only
 * {@code connector}/{@code currentHandle} injected — the same technique as
 * {@code PluginDrivenScanNodeVerboseExplainTest}.</p>
 */
public class PluginDrivenScanNodeScanProviderSelectionTest {

    @Test
    public void resolvesProviderForCurrentHandle() {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);

        ConnectorTableHandle icebergHandle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle hiveHandle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorScanPlanProvider icebergProvider = Mockito.mock(ConnectorScanPlanProvider.class);
        ConnectorScanPlanProvider hiveProvider = Mockito.mock(ConnectorScanPlanProvider.class);

        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getScanPlanProvider(icebergHandle)).thenReturn(icebergProvider);
        Mockito.when(connector.getScanPlanProvider(hiveHandle)).thenReturn(hiveProvider);
        Deencapsulation.setField(node, "connector", connector);

        // The provider is selected by whichever handle the scan currently holds (pushdown may refine it).
        Deencapsulation.setField(node, "currentHandle", icebergHandle);
        Assertions.assertSame(icebergProvider, Deencapsulation.invoke(node, "resolveScanProvider"),
                "the node must resolve the provider for the iceberg-on-HMS handle it is scanning");

        // After the handle changes the node must re-resolve to the matching provider (per-table routing),
        // not cache the first one.
        Deencapsulation.setField(node, "currentHandle", hiveHandle);
        Assertions.assertSame(hiveProvider, Deencapsulation.invoke(node, "resolveScanProvider"),
                "after the handle changes the node must resolve the matching provider (per-table routing)");
    }
}
