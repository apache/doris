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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

/**
 * Guards {@link PluginDrivenScanNode#applyRewriteFileScopePin}, the pure pin-vs-skip decision that scopes a
 * distributed {@code rewrite_data_files} group's scan to its own files before every scan-side consumption.
 *
 * <p><b>Why this matters:</b> each group's INSERT-SELECT must scan ONLY the data files that group bin-packed.
 * If the per-group path scope is not threaded onto the handle, the group scans the WHOLE table — so every
 * group rewrites far beyond its set, producing duplicate rows and (under OCC) clobbering concurrent writes.
 * The helper must (1) thread the raw paths onto the handle when present, (2) NOT scope — and not touch the
 * connector — when the path list is null (no scope = full scan) or (3) empty (an empty scope would scan
 * nothing). Each test kills the corresponding mutation.</p>
 */
public class PluginDrivenScanNodeRewriteFileScopePinTest {

    @Test
    public void scopePresentThreadsRawPathsOntoHandle() {
        // MUTATION: a "return input handle unchanged" / "never call applyRewriteFileScope" mutation is killed
        // here — a present scope MUST be threaded onto the handle (as a Set), else the group scans the full
        // table. Distinct input vs scoped mock handles prove the returned value is the connector's scoped one.
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle inputHandle = Mockito.mock(ConnectorTableHandle.class);
        ConnectorTableHandle scopedHandle = Mockito.mock(ConnectorTableHandle.class);
        List<String> paths = Arrays.asList("s3://b/db1/t1/a.parquet", "s3://b/db1/t1/b.parquet");

        Mockito.when(metadata.applyRewriteFileScope(session, inputHandle, new HashSet<>(paths)))
                .thenReturn(scopedHandle);

        ConnectorTableHandle result = PluginDrivenScanNode.applyRewriteFileScopePin(
                metadata, session, inputHandle, paths);

        // applyRewriteFileScope must be invoked with the RAW paths as a Set (no normalization on this side).
        Mockito.verify(metadata).applyRewriteFileScope(session, inputHandle, new HashSet<>(paths));
        Assertions.assertSame(scopedHandle, result);
    }

    @Test
    public void nullScopeReadsFullTableUnchanged() {
        // MUTATION: dropping the null guard (scoping unconditionally) is killed — no scope means a normal
        // (non-rewrite) read, which must return the handle UNCHANGED and never touch the connector.
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle inputHandle = Mockito.mock(ConnectorTableHandle.class);

        ConnectorTableHandle result = PluginDrivenScanNode.applyRewriteFileScopePin(
                metadata, session, inputHandle, null);

        Assertions.assertSame(inputHandle, result);
        Mockito.verifyNoInteractions(metadata);
    }

    @Test
    public void emptyScopeReadsFullTableUnchanged() {
        // MUTATION: treating an EMPTY scope as "scope to nothing" is killed — an empty path list must be a
        // no-op (return the handle unchanged, full scan), NOT threaded down (which would scan zero files).
        ConnectorMetadata metadata = Mockito.mock(ConnectorMetadata.class);
        ConnectorSession session = Mockito.mock(ConnectorSession.class);
        ConnectorTableHandle inputHandle = Mockito.mock(ConnectorTableHandle.class);

        ConnectorTableHandle result = PluginDrivenScanNode.applyRewriteFileScopePin(
                metadata, session, inputHandle, Collections.emptyList());

        Assertions.assertSame(inputHandle, result);
        Mockito.verifyNoInteractions(metadata);
    }
}
