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
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Set;

/**
 * Guards {@link PluginDrivenScanNode#mustReadColumnsFromConnector()} — the seam the translator asks before it
 * prunes a plugin table's scan slots ({@code PhysicalPlanTranslator.preserveConnectorMustReadSlots}), so a
 * connector whose BE-side reader merges or suppresses rows by key gets that key read even when the query
 * never mentions it.
 *
 * <p><b>WHY this matters (Rule 9):</b> the answer is the whole contract between plan translation and split
 * planning. Answering for the WRONG handle, or resolving a FRESH provider to ask, would let the connector
 * decide "combine the two sources" at split time while the tuple was pruned as if it had said no — and BE
 * would be told to suppress rows by a column that is not there. The memo assertion below is what pins
 * "asked and planned through one provider instance".</p>
 *
 * <p>Driven on a {@code CALLS_REAL_METHODS} node with only the connector/session/handle fields injected —
 * the same technique as {@link PluginDrivenScanNodeScanProviderSelectionTest}.</p>
 */
public class PluginDrivenScanNodeMustReadColumnsTest {

    private static PluginDrivenScanNode nodeWith(ConnectorScanPlanProvider provider,
            ConnectorTableHandle handle, ConnectorSession session) {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getScanPlanProvider(handle)).thenReturn(provider);
        Deencapsulation.setField(node, "connector", connector);
        Deencapsulation.setField(node, "currentHandle", handle);
        Deencapsulation.setField(node, "connectorSession", session);
        return node;
    }

    @Test
    public void forwardsTheConnectorsAnswerForTheScannedHandle() {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(provider.getMustReadColumns(session, handle)).thenReturn(ImmutableSet.of("id", "part"));
        PluginDrivenScanNode node = nodeWith(provider, handle, session);

        Set<String> mustRead = node.mustReadColumnsFromConnector();

        // WHY: the connector answers PER SCAN, from the handle this scan holds. MUTATION: passing a
        // different handle (or the table's original one after pushdown refined it) makes the connector
        // answer about another read -> the stub returns empty -> red.
        Assertions.assertEquals(ImmutableSet.of("id", "part"), mustRead);
        Mockito.verify(provider).getMustReadColumns(session, handle);
    }

    @Test
    public void connectorWithoutScanCapabilityNeedsNothing() {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        PluginDrivenScanNode node = nodeWith(null, handle, Mockito.mock(ConnectorSession.class));

        // WHY: getScanPlanProvider() is null for a connector with no scan capability; every other resolver
        // in this node degrades to its default rather than throwing. MUTATION: dropping the null check ->
        // NPE during plan translation for such a catalog -> red.
        Assertions.assertEquals(Collections.emptySet(), node.mustReadColumnsFromConnector());
    }

    @Test
    public void nullAnswerIsReadAsNoExtraColumns() {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(provider.getMustReadColumns(session, handle)).thenReturn(null);
        PluginDrivenScanNode node = nodeWith(provider, handle, session);

        // WHY: a third-party connector may return null where the SPI says "empty". Turning that into an
        // NPE inside plan translation would blame the engine for a connector's slip. MUTATION: returning
        // the raw answer -> NPE in the translator's isEmpty() -> red.
        Assertions.assertEquals(Collections.emptySet(), node.mustReadColumnsFromConnector());
    }

    @Test
    public void asksThroughTheSameProviderInstanceThatWillPlanTheSplits() {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorScanPlanProvider provider = Mockito.mock(ConnectorScanPlanProvider.class);
        Mockito.when(provider.getMustReadColumns(session, handle)).thenReturn(ImmutableSet.of("id"));
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(connector.getScanPlanProvider(handle)).thenReturn(provider);
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(node, "connector", connector);
        Deencapsulation.setField(node, "currentHandle", handle);
        Deencapsulation.setField(node, "connectorSession", session);

        node.mustReadColumnsFromConnector();
        Object providerForSplits = Deencapsulation.invoke(node, "resolveScanProvider");

        // WHY: the connector is allowed to memoize "do I combine two sources?" on its provider instance,
        // and MUST reach the same answer when it plans the splits later — the columns kept here and the
        // splits planned there have to come from one decision. A fresh provider per question loses that
        // memo and lets the two disagree. MUTATION: asking via connector.getScanPlanProvider(...) directly
        // instead of the memoized resolveScanProvider() -> two instances + a second resolve -> red.
        Assertions.assertSame(provider, providerForSplits,
                "the must-read question must go through the same memoized provider as split planning");
        Mockito.verify(connector, Mockito.times(1)).getScanPlanProvider(handle);
    }
}
