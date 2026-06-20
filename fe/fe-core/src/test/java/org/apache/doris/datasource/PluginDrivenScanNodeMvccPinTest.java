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

import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Optional;

/**
 * Guards {@link PluginDrivenScanNode#applyMvccSnapshotPin}, the pure pin-vs-skip decision threaded
 * onto the table handle before every scan-side consumption (planScan + the serialized-table /
 * getScanNodeProperties path).
 *
 * <p><b>Why this matters:</b> an MVCC-capable connector (paimon today) must read the WHOLE query at
 * one pinned point-in-time snapshot — a time-travel ({@code FOR TIME AS OF}) or MTMV-consistent read.
 * If the pin is not threaded onto the handle before a consumption point, that path silently reads
 * LATEST instead, producing rows from a different snapshot than the rest of the query. The helper
 * must (1) apply the pin when a plugin snapshot is present, (2) NOT pin when there is no snapshot
 * (read latest, e.g. before the connector is MVCC-cutover or a non-MVCC table), and (3) NOT pin — and
 * not ClassCastException — on a foreign (non-plugin) {@link MvccSnapshot}. Each test kills the
 * corresponding mutation.</p>
 */
public class PluginDrivenScanNodeMvccPinTest {

    @Test
    public void pluginSnapshotPresentPinsHandle() {
        // MUTATION: a "return input handle unchanged" / "never call applySnapshot" mutation is killed
        // here — a present plugin snapshot MUST be unwrapped and threaded onto the handle, else a
        // time-travel/MTMV read silently reads LATEST. Distinct input vs pinned mock handles ensure
        // the returned value is the connector's pinned handle, not the untouched input.
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle inputHandle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle pinnedHandle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorMvccSnapshot connectorSnapshot = Mockito.mock(ConnectorMvccSnapshot.class);
        PluginDrivenMvccSnapshot snapshot = new PluginDrivenMvccSnapshot(
                connectorSnapshot, Collections.emptyMap(), Collections.emptyMap());

        Mockito.when(metadata.applySnapshot(session, inputHandle, connectorSnapshot))
                .thenReturn(pinnedHandle);

        ConnectorTableHandle result = PluginDrivenScanNode.applyMvccSnapshotPin(
                metadata, session, inputHandle, Optional.of(snapshot));

        // applySnapshot must be invoked with the UNWRAPPED ConnectorMvccSnapshot (not the wrapper).
        Mockito.verify(metadata).applySnapshot(session, inputHandle, connectorSnapshot);
        // and the pinned handle the connector returned is what flows downstream to planScan.
        Assertions.assertSame(pinnedHandle, result);
    }

    @Test
    public void emptySnapshotReadsLatestUnchanged() {
        // MUTATION: a "pin unconditionally" mutation (dropping the isPresent guard) is killed — with no
        // snapshot in context (no MVCC pin, e.g. pre-cutover or a non-MVCC table) the handle must be
        // returned UNCHANGED so the scan reads latest, and applySnapshot must NOT be called.
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle inputHandle = Mockito.mock(ConnectorTableHandle.class);

        ConnectorTableHandle result = PluginDrivenScanNode.applyMvccSnapshotPin(
                metadata, session, inputHandle, Optional.empty());

        Assertions.assertSame(inputHandle, result);
        Mockito.verifyNoInteractions(metadata);
    }

    @Test
    public void foreignSnapshotReadsLatestUnchanged() {
        // MUTATION: dropping the instanceof PluginDrivenMvccSnapshot guard is killed — a foreign
        // MvccSnapshot (some other table type's snapshot present in the same statement context) must
        // NOT be pinned and must NOT ClassCastException; the handle is returned unchanged (read latest)
        // and applySnapshot is never called for a snapshot this node cannot unwrap.
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle inputHandle = Mockito.mock(ConnectorTableHandle.class);
        MvccSnapshot foreignSnapshot = Mockito.mock(MvccSnapshot.class);

        ConnectorTableHandle result = PluginDrivenScanNode.applyMvccSnapshotPin(
                metadata, session, inputHandle, Optional.of(foreignSnapshot));

        Assertions.assertSame(inputHandle, result);
        Mockito.verifyNoInteractions(metadata);
    }
}
