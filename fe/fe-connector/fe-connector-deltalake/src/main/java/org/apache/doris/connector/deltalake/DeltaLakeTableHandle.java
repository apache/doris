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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import io.delta.kernel.Snapshot;
import io.delta.kernel.engine.Engine;

import java.util.Optional;

/** Immutable statement coordinates; the in-memory snapshot avoids replay throughout planning. */
final class DeltaLakeTableHandle implements ConnectorTableHandle {
    private static final long serialVersionUID = 1L;

    private final String database;
    private final String table;
    private final DeltaLakeTableReference reference;
    private final String tableId;
    private final long version;
    private final boolean history;
    private final transient Snapshot snapshot;

    DeltaLakeTableHandle(String database, String table, Snapshot snapshot) {
        this(database, table, DeltaLakeTableReference.filesystem(snapshot.getPath()), snapshot);
    }

    DeltaLakeTableHandle(String database, String table, DeltaLakeTableReference reference, Snapshot snapshot) {
        this(database, table, reference, DeltaKernelCompat.tableId(snapshot), snapshot.getVersion(), snapshot, false);
    }

    private DeltaLakeTableHandle(String database, String table, DeltaLakeTableReference reference, String tableId,
            long version, Snapshot snapshot, boolean history) {
        this.database = database;
        this.table = table;
        this.reference = reference;
        this.tableId = tableId;
        this.version = version;
        this.snapshot = snapshot;
        this.history = history;
    }

    // Queued statistics work retains coordinates, not a potentially large cached snapshot.
    DeltaLakeTableHandle withoutSnapshot() {
        DeltaLakeTableReference coordinates =
                new DeltaLakeTableReference(reference.path, reference.registrationId, reference.managed);
        return new DeltaLakeTableHandle(database, table, coordinates, tableId, version, null, history);
    }

    DeltaLakeTableHandle asHistory() {
        return new DeltaLakeTableHandle(database, table, reference, tableId, version, snapshot, true);
    }

    boolean isHistory() {
        return history;
    }

    String getDatabase() {
        return database;
    }

    String getTable() {
        return table;
    }

    String getPath() {
        return reference.path;
    }

    DeltaLakeTableReference reference() {
        return reference;
    }

    String getTableId() {
        return tableId;
    }

    long getVersion() {
        return version;
    }

    DeltaLakeTableHandle atVersion(DeltaLakeRuntime runtime, long requestedVersion) {
        if (version == requestedVersion) {
            return this;
        }
        Snapshot pinned = reference.load(runtime.engine(reference), runtime.resolver, database, table,
                Optional.of(requestedVersion));
        if (!tableId.equals(DeltaKernelCompat.tableId(pinned))) {
            throw new DorisConnectorException("Delta table was recreated during the statement: " + getPath());
        }
        return new DeltaLakeTableHandle(database, table, reference, tableId, requestedVersion, pinned, history);
    }

    Snapshot snapshot(DeltaLakeRuntime runtime) {
        return snapshot == null ? snapshot(runtime.engine(reference), runtime.resolver) : snapshot;
    }

    Snapshot snapshot(Engine engine, DeltaLakeTableResolver resolver) {
        if (snapshot != null) {
            return snapshot;
        }
        Snapshot reloaded = reference.load(engine, resolver, database, table, Optional.of(version));
        // A recreated table can reuse version 0 at the same path; it is not the statement's table.
        if (!tableId.equals(DeltaKernelCompat.tableId(reloaded))) {
            throw new DorisConnectorException("Delta table was recreated during the statement: " + getPath());
        }
        return reloaded;
    }
}
