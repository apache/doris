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
import org.apache.doris.connector.spi.mvcc.ConnectorTimeTravelSpec;

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
        Snapshot pinned = reference.load(runtime.engine(reference), runtime.resolver, database, table,
                Optional.of(requestedVersion));
        verifyTableId(pinned);
        return wrap(pinned);
    }

    DeltaLakeTableHandle timeTravel(DeltaLakeRuntime runtime, ConnectorTimeTravelSpec.Kind kind, long coordinate) {
        Engine engine = runtime.engine(reference);
        // The catalog cache may predate an append or a change to the retention property.
        Snapshot latest = loadLatest(runtime, engine);
        Snapshot target;
        if (kind == ConnectorTimeTravelSpec.Kind.TIMESTAMP) {
            target = reference.loadAtTimestamp(engine, runtime.resolver, database, table, coordinate, latest);
        } else if (coordinate == latest.getVersion()) {
            return wrap(latest);
        } else {
            target = reference.load(engine, runtime.resolver, database, table, Optional.of(coordinate));
        }
        verifyTableId(target);
        // A version load or a UC timestamp load can observe a commit ratified after our latest read.
        if (target.getVersion() > latest.getVersion()) {
            latest = loadLatest(runtime, engine);
        }
        DeltaLakeTimeTravel.checkRetention(engine, latest, target, System.currentTimeMillis());
        return wrap(target);
    }

    private Snapshot loadLatest(DeltaLakeRuntime runtime, Engine engine) {
        Snapshot latest = reference.load(engine, runtime.resolver, database, table, Optional.empty());
        verifyTableId(latest);
        return latest;
    }

    private void verifyTableId(Snapshot pinned) {
        if (!tableId.equals(DeltaKernelCompat.tableId(pinned))) {
            throw new DorisConnectorException("Delta table was recreated during the statement: " + getPath());
        }
    }

    private DeltaLakeTableHandle wrap(Snapshot pinned) {
        DeltaKernelCompat.prepareForConcurrentReads(pinned);
        return new DeltaLakeTableHandle(database, table, reference, tableId, pinned.getVersion(), pinned, history);
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
        verifyTableId(reloaded);
        return reloaded;
    }
}
