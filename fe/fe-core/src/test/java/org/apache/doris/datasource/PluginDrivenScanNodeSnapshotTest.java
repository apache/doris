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

import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccExternalTable;
import org.apache.doris.datasource.mvcc.PluginDrivenMvccSnapshot;
import org.apache.doris.datasource.scan.PluginDrivenScanNode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Map;
import java.util.Optional;

public class PluginDrivenScanNodeSnapshotTest {
    @Test
    public void testVersionAndTimestampSelectorsRemainSourceNeutral() {
        ConnectorTimeTravelSpec version = selector(TableSnapshot.versionOf("17"));
        Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.SNAPSHOT_ID, version.getKind());
        Assertions.assertEquals("17", version.getStringValue());

        for (String timestamp : new String[] {"2026-08-21 12:34:56", "2026-08-21 12:34:56.123"}) {
            ConnectorTimeTravelSpec spec = selector(TableSnapshot.timeOf(timestamp));
            Assertions.assertEquals(ConnectorTimeTravelSpec.Kind.TIMESTAMP, spec.getKind());
            Assertions.assertEquals(timestamp, spec.getStringValue());
            Assertions.assertFalse(spec.isDigital());
        }
        ConnectorTimeTravelSpec numeric = selector(TableSnapshot.timeOf("1700000000123"));
        Assertions.assertEquals("1700000000123", numeric.getStringValue());
        Assertions.assertTrue(numeric.isDigital());
    }

    @Test
    public void testScanAndWriteApplyTheSameMvccSnapshot() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle latest = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle historical = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMvccSnapshot snapshot = ConnectorMvccSnapshot.builder().snapshotId(3L).build();
        Mockito.when(metadata.applySnapshot(session, latest, snapshot)).thenReturn(historical);
        PluginDrivenMvccSnapshot pin = new PluginDrivenMvccSnapshot(snapshot, Map.of(), Map.of());

        Assertions.assertSame(historical, PluginDrivenScanNode.applyMvccSnapshotPin(
                metadata, session, latest, Optional.of(pin)));
        Mockito.verify(metadata).applySnapshot(session, latest, snapshot);
    }

    @Test
    public void testUnpinnedScansDoNotInventAHistoricalSnapshot() {
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle latest = Mockito.mock(ConnectorTableHandle.class);

        Assertions.assertSame(latest, PluginDrivenScanNode.applyMvccSnapshotPin(
                metadata, session, latest, Optional.empty()));
        Mockito.verifyNoInteractions(metadata);
    }

    @Test
    public void testCopyOnWriteSnapshotHandleSurvivesPushdownRefinements() {
        PluginDrivenScanNode node = Mockito.mock(PluginDrivenScanNode.class, Mockito.CALLS_REAL_METHODS);
        ConnectorTableHandle snapshot = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle pushed = Mockito.mock(ConnectorTableHandle.class);
        Deencapsulation.setField(node, "snapshotHandle", snapshot);
        Deencapsulation.setField(node, "currentHandle", pushed);

        Assertions.assertSame(snapshot, node.getTableHandle());
        Deencapsulation.setField(node, "currentHandle", Mockito.mock(ConnectorTableHandle.class));
        Assertions.assertSame(snapshot, node.getTableHandle());
    }

    private static ConnectorTimeTravelSpec selector(TableSnapshot selector) {
        PluginDrivenMvccExternalTable table =
                Mockito.mock(PluginDrivenMvccExternalTable.class, Mockito.CALLS_REAL_METHODS);
        return Deencapsulation.invoke(table, "toTimeTravelSpec",
                Optional.of(selector), Optional.empty(), Optional.empty());
    }
}
