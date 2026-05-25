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

package org.apache.doris.connector;

import org.apache.doris.connector.api.mvcc.ConnectorMvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccSnapshot;

import java.util.Objects;

/**
 * Adapter that lets a connector-provided {@link ConnectorMvccSnapshot} flow through the
 * engine's existing {@link MvccSnapshot} contract (consumed by the nereids analyzer and
 * the scan plan). Constructed when {@code ConnectorMetadata.beginQuerySnapshot} returns
 * a value; passed unchanged through fe-core MVCC pinning, then unwrapped on the BE
 * serialization boundary via {@link #getSnapshot()}.
 */
public final class ConnectorMvccSnapshotAdapter implements MvccSnapshot {

    private final ConnectorMvccSnapshot snapshot;

    public ConnectorMvccSnapshotAdapter(ConnectorMvccSnapshot snapshot) {
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
    }

    public ConnectorMvccSnapshot getSnapshot() {
        return snapshot;
    }
}
